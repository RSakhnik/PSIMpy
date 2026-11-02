"""Durable byte-range downloads. State binds partial bytes to immutable content.

One writer per destination. The caller must fetch fresh result metadata on every
invocation (Task.download does so). Compression applies only after wire integrity.
"""
import gzip
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import tempfile
import time

from .errors import BimBridgeError
from ._files import publish


def _state(path, value):
    fd, temporary = tempfile.mkstemp(prefix='.download-state-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _hash(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def download_resumable(http, path, destination, metadata, *, overwrite, decompress, progress):
    destination = Path(destination)
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    length, sha = metadata.get('TotalLength'), metadata.get('Sha256')
    # Preserve support for old endpoints that have no immutable identity. They
    # cannot safely resume; the original atomic downloader remains available.
    if type(length) is not int or length < 0 or not isinstance(sha, str) or not re.fullmatch('[a-fA-F0-9]{64}', sha):
        return http.download(path, destination, metadata, overwrite=overwrite,
                             decompress=decompress, progress=progress, resume=False)
    sha = sha.lower()
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + '.inprogress')
    state_path = destination.with_name(destination.name + '.download.json')
    etag = metadata.get('ETag') or '"sha256-' + sha + '"'
    identity = {'Source': hashlib.sha256((http.base_url + path + '\0' + http._token).encode()).hexdigest(),
                'TotalLength': length, 'Sha256': sha, 'ETag': etag,
                'ContentEncoding': metadata.get('ContentEncoding'), 'Decompress': decompress}
    count = 0
    if state_path.exists():
        try:
            saved = json.loads(state_path.read_text(encoding='utf-8'))
        except (ValueError, OSError) as error:
            raise BimBridgeError('INTEGRITY_ERROR', 'Повреждено состояние скачивания') from error
        if saved.get('Identity') != identity:
            raise BimBridgeError('ARTIFACT_CHANGED', 'Удалённый artifact или параметры изменились; используйте другой путь')
        if not partial.is_file() or type(saved.get('Downloaded')) is not int:
            raise BimBridgeError('INTEGRITY_ERROR', 'Файл продолжения отсутствует или состояние некорректно')
        count = saved['Downloaded']
        if not 0 <= count <= length or partial.stat().st_size < count:
            raise BimBridgeError('INTEGRITY_ERROR', 'Длина частичного файла не соответствует состоянию')
        # Uncommitted tail can remain after a crash; resume only flushed bytes.
        with partial.open('r+b') as stream:
            stream.truncate(count)
        if _hash(partial) != saved.get('PartialSha256'):
            raise BimBridgeError('INTEGRITY_ERROR', 'Частичный файл повреждён')
    elif partial.exists():
        raise BimBridgeError('INTEGRITY_ERROR', 'Частичный файл не имеет состояния; используйте другой путь')
    else:
        with partial.open('xb'):
            pass
        _state(state_path, {'Identity': identity, 'Downloaded': 0, 'PartialSha256': _hash(partial)})

    # Prefix hash is restored once per invocation, not once per incoming block.
    digest = hashlib.sha256()
    with partial.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)

    def checkpoint():
        _state(state_path, {'Identity': identity, 'Downloaded': count, 'PartialSha256': digest.hexdigest()})

    for attempt in range(http.retries + 1):
        if count == length:
            break
        headers = {'Range': f'bytes={count}-', 'If-Range': etag} if count else {}
        try:
            with http.open('GET', path, headers=headers) as source:
                status = source.status
                response_etag = source.headers.get('ETag')
                if response_etag and response_etag != etag:
                    raise BimBridgeError('ARTIFACT_CHANGED', 'ETag результата изменился')
                if status == 206:
                    value = source.headers.get('Content-Range', '')
                    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', value)
                    if not match or tuple(map(int, match.groups())) != (count, length-1, length):
                        raise BimBridgeError('INVALID_RANGE', 'Сервер вернул неверный Content-Range')
                elif status == 200:
                    # Legacy server ignores Range. Restart from zero; never append
                    # a complete response to a partial prefix.
                    count = 0
                    digest = hashlib.sha256()
                    with partial.open('r+b') as target:
                        target.truncate(0)
                    checkpoint()
                else:
                    raise BimBridgeError('INVALID_RANGE', 'Ожидался HTTP 200 или 206')
                with partial.open('r+b') as target:
                    target.seek(count)
                    try:
                        while True:
                            chunk = source.read(1024*1024)
                            if not chunk:
                                break
                            if count + len(chunk) > length:
                                raise BimBridgeError('INTEGRITY_ERROR', 'Скачивание превысило TotalLength')
                            target.write(chunk)
                            digest.update(chunk)
                            count += len(chunk)
                            if progress:
                                progress(count, length)
                    finally:
                        target.flush()
                        os.fsync(target.fileno())
                        checkpoint()
                if count != length:
                    raise ConnectionError('Download ended before TotalLength')
            break
        except (OSError, EOFError, HTTPException) as error:
            if attempt == http.retries:
                raise BimBridgeError('TRANSPORT_ERROR', 'Скачивание прервано; повторите Task.download для продолжения', retryable=True) from error
            time.sleep(min(2**attempt, 5))
    if digest.hexdigest() != sha:
        # A poisoned prefix must never be reused; keep the previous destination.
        partial.unlink()
        state_path.unlink()
        raise BimBridgeError('INTEGRITY_ERROR', 'SHA-256 скачанного artifact не совпал')
    temporary = None
    try:
        source_path = partial
        if decompress and (metadata.get('ContentEncoding') or '').lower() == 'gzip':
            fd, temporary = tempfile.mkstemp(prefix='.bim-unpacked-', dir=destination.parent)
            with os.fdopen(fd, 'wb') as target, gzip.open(partial, 'rb') as source:
                for chunk in iter(lambda: source.read(1024*1024), b''):
                    target.write(chunk)
            source_path = temporary
        publish(source_path, destination, overwrite)
        partial.unlink(missing_ok=True)
        state_path.unlink()
        return destination
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
