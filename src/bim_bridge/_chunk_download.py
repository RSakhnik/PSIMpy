"""Resume immutable Pilot results through ReadResultChunkAsync, without HTTP."""
import base64
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from time import perf_counter_ns

from ._download import _hash, _state
from ._files import publish
from .errors import BimBridgeError


def download_chunks(transport, task_id, destination, metadata, *, overwrite=False,
                    decompress=False, progress=None, resume=True, telemetry=None):
    resume_started = perf_counter_ns() if telemetry is not None else None
    destination = Path(destination)
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    length, chunks, size, sha = (metadata.get(k) for k in ('TotalLength','TotalChunks','ChunkSizeBytes','Sha256'))
    if (type(length) is not int or length < 0 or type(size) is not int or not 0 < size <= transport.max_chunk_bytes
            or type(chunks) is not int or chunks != (length+size-1)//size
            or not isinstance(sha,str) or not re.fullmatch('[a-fA-F0-9]{64}',sha)
            or str(metadata.get('TaskId','')).lower() != task_id.lower()):
        raise BimBridgeError('INVALID_RESPONSE','Некорректные metadata immutable artifact')
    destination.parent.mkdir(parents=True,exist_ok=True)
    partial = destination.with_name(destination.name+'.inprogress')
    state_path = destination.with_name(destination.name+'.download.json')
    identity = {'Source':hashlib.sha256((transport.base_url+'\0'+transport._token).encode()).hexdigest(),
                'TaskId':task_id,'TotalLength':length,'TotalChunks':chunks,'ChunkSizeBytes':size,
                'Sha256':sha.lower(),'ContentEncoding':metadata.get('ContentEncoding'),'Decompress':decompress}
    count = 0
    if not resume:
        partial.unlink(missing_ok=True);state_path.unlink(missing_ok=True)
    if state_path.exists():
        try: saved=json.loads(state_path.read_text(encoding='utf-8'))
        except (ValueError,OSError) as error:
            raise BimBridgeError('INTEGRITY_ERROR','Повреждено состояние скачивания') from error
        if saved.get('Identity') != identity:
            raise BimBridgeError('ARTIFACT_CHANGED','Immutable artifact изменился; выберите другой путь')
        count=saved.get('Downloaded')
        if count == 0 and not partial.exists() and saved.get('PartialSha256') == hashlib.sha256(b'').hexdigest():
            partial.touch(exist_ok=False)
        if type(count) is not int or not 0 <= count <= length or (count != length and count%size) or not partial.is_file() or partial.stat().st_size<count:
            raise BimBridgeError('INTEGRITY_ERROR','Некорректная граница сохранённого chunk')
        with partial.open('r+b') as stream: stream.truncate(count)
        if _hash(partial)!=saved.get('PartialSha256'):
            raise BimBridgeError('INTEGRITY_ERROR','Сохранённые chunks повреждены')
    elif partial.exists() and (not partial.is_file() or partial.stat().st_size != 0):
        raise BimBridgeError('INTEGRITY_ERROR','Partial artifact не имеет состояния')
    else:
        # A crash can occur after creating the empty file and before publishing
        # its first journal. No bytes/identity are reused: initialize from zero.
        partial.touch(exist_ok=True)
        _state(state_path,{'Identity':identity,'Downloaded':0,'PartialSha256':hashlib.sha256(b'').hexdigest()})
    digest=hashlib.sha256()
    with partial.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''): digest.update(block)
    if telemetry is not None:
        telemetry['ResumeValidationWallMs'] = (perf_counter_ns()-resume_started)/1e6
        telemetry['ResumedBytes'] = count
        telemetry['ChunkCalls'] = 0
    transfer_started = perf_counter_ns() if telemetry is not None else None
    serve_ms = 0.0
    serve_known = True
    try:
        with partial.open('r+b') as stream:
            stream.seek(count)
            for index in range((count+size-1)//size,chunks):
                if telemetry is not None: telemetry['ChunkCalls'] += 1
                try:
                    response=transport.rpc('ReadResultChunk',TaskId=task_id,ChunkIndex=index)
                except BaseException:
                    serve_known = False
                    raise
                if telemetry is not None:
                    value=response.get('ManagerServeWallMs')
                    if type(value) in (int,float) and math.isfinite(value) and value>=0:
                        serve_ms += value
                    else:
                        serve_known = False
                if response.get('TaskId','').lower()!=task_id.lower() or response.get('ChunkIndex')!=index:
                    raise BimBridgeError('INVALID_RESPONSE','Идентификатор chunk не совпал')
                try: data=base64.b64decode(response['Data'],validate=True)
                except (ValueError,KeyError,TypeError) as error:
                    raise BimBridgeError('INVALID_RESPONSE','Повреждённые данные chunk') from error
                if len(data)!=min(size,length-count) or response.get('IsLast')!=(index==chunks-1):
                    raise BimBridgeError('INTEGRITY_ERROR','Длина или флаг последнего chunk не совпали')
                stream.write(data);stream.flush();os.fsync(stream.fileno())
                digest.update(data);count+=len(data)
                _state(state_path,{'Identity':identity,'Downloaded':count,'PartialSha256':digest.hexdigest()})
                if progress: progress(count,length)
        if telemetry is not None:
            telemetry['ChunkTransferWallMs'] = (perf_counter_ns()-transfer_started)/1e6
            telemetry['DownloadedBytes'] = count-telemetry['ResumedBytes']
            telemetry['ManagerServeWallMs'] = serve_ms if serve_known else None
        if count!=length or digest.hexdigest()!=sha.lower():
            partial.unlink();state_path.unlink()
            raise BimBridgeError('INTEGRITY_ERROR','TotalLength или SHA-256 artifact не совпал')
        temporary=None
        finalize_started = perf_counter_ns() if telemetry is not None else None
        try:
            source_path=partial
            if decompress and (metadata.get('ContentEncoding') or '').lower()=='gzip':
                fd,temporary=tempfile.mkstemp(prefix='.bim-unpacked-',dir=destination.parent)
                with os.fdopen(fd,'wb') as target,gzip.open(partial,'rb') as source:
                    for block in iter(lambda:source.read(1024*1024),b''): target.write(block)
                source_path=temporary
            publish(source_path,destination,overwrite)
            partial.unlink(missing_ok=True);state_path.unlink()
            if telemetry is not None:
                telemetry['LocalFinalizeWallMs'] = (perf_counter_ns()-finalize_started)/1e6
            return destination
        finally:
            if temporary and os.path.exists(temporary):os.unlink(temporary)
    except BaseException:
        if telemetry is not None:
            telemetry.setdefault('ChunkTransferWallMs', (perf_counter_ns()-transfer_started)/1e6)
            telemetry['DownloadedBytes'] = count-telemetry['ResumedBytes']
            telemetry['ManagerServeWallMs'] = serve_ms if serve_known else None
        if not resume:
            partial.unlink(missing_ok=True);state_path.unlink(missing_ok=True)
        raise
