"""BBSPAT2 framed binary geometry, stream readers and optional NumPy memmaps."""
from dataclasses import dataclass
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import struct
import tempfile

from .errors import BimBridgeError, IncompleteResult
from .types import guid, utc

MAGIC=b'BBSPAT2\n'
FRAME=struct.Struct('<IQ')
FIELDS={'Vertices':'<f4','Normals':'<f4','Indices':'<u4','EdgeIndices':'<u4'}


def _invalid(message):
    return BimBridgeError('INVALID_ARTIFACT',message)


@dataclass(frozen=True)
class SpatialBufferSection:
    path: Path
    byte_offset: int
    metadata: dict

    def read(self):
        with self.path.open('rb') as stream:
            stream.seek(self.byte_offset)
            data=stream.read(self.metadata['Count']*4)
        if len(data)!=self.metadata['Count']*4 or hashlib.sha256(data).hexdigest()!=self.metadata['Sha256']:
            raise _invalid('Бинарный buffer повреждён')
        return data

    def values(self):
        scalar='<f' if self.metadata['DType']=='<f4' else '<I'
        return (item[0] for item in struct.iter_unpack(scalar,self.read()))

    def numpy(self, *, mmap=True):
        import numpy as np
        self.read() # verify this bounded block before exposing mapped bytes
        if mmap and self.metadata['Count']:
            return np.memmap(self.path,mode='r',dtype=self.metadata['DType'],
                             offset=self.byte_offset,shape=(self.metadata['Count'],))
        return np.frombuffer(self.read(),dtype=self.metadata['DType'])


class SpatialArtifact:
    def __init__(self,path,*,expected_sha256=None):
        self.path=Path(path)
        self.expected_sha256=expected_sha256

    def frames(self):
        """Yield (metadata, section-or-None), bounded by a single binary block."""
        with self.path.open('rb') as stream:
            if stream.read(8)!=MAGIC:raise _invalid('Неизвестная сигнатура spatial artifact')
            while True:
                header=stream.read(FRAME.size)
                if not header:return
                if len(header)!=FRAME.size:raise _invalid('Оборванный заголовок frame')
                meta_size,blob_size=FRAME.unpack(header)
                if not 0<meta_size<=1024*1024 or blob_size>1024*1024:
                    raise _invalid('Frame превышает лимит формата')
                raw_metadata=stream.read(meta_size)
                if len(raw_metadata)!=meta_size:raise _invalid('Оборванная metadata frame')
                try:metadata=json.loads(raw_metadata.decode('utf-8'))
                except (ValueError,UnicodeError) as error:raise _invalid('Некорректная metadata frame') from error
                if not isinstance(metadata,dict):raise _invalid('Frame metadata должна быть объектом')
                offset=stream.tell()
                section=None
                if metadata.get('Type')=='buffer':
                    field=metadata.get('Field');count=metadata.get('Count')
                    if (field not in FIELDS or metadata.get('DType')!=FIELDS[field] or type(count) is not int or count<0
                            or blob_size!=count*4 or type(metadata.get('Offset')) is not int or metadata['Offset']<0
                            or not isinstance(metadata.get('Sha256'),str)):
                        raise _invalid('Некорректные тип/размер binary buffer')
                    section=SpatialBufferSection(self.path,offset,metadata)
                    blob=stream.read(blob_size)
                    if len(blob)!=blob_size or hashlib.sha256(blob).hexdigest()!=metadata['Sha256']:
                        raise _invalid('Бинарный buffer повреждён')
                elif blob_size:raise _invalid('Неизвестный бинарный payload frame')
                stream.seek(offset+blob_size)
                yield metadata,section

    def buffers(self, *, mesh_key=None, field=None):
        for metadata,section in self.frames():
            if section is not None and (mesh_key is None or metadata['MeshDefinitionKey']==mesh_key) and (field is None or metadata['Field']==field):
                yield section

    def validate(self, *, strict=True):
        try:return self._validate(strict=strict)
        except (KeyError,TypeError,ValueError,OverflowError,struct.error) as error:
            raise _invalid('Повреждена структура spatial artifact') from error

    def _validate(self, *, strict=True):
        """Validate structure, block hashes, mesh references and summary on disk."""
        if self.expected_sha256:
            digest=hashlib.sha256()
            with self.path.open('rb') as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
            if digest.hexdigest()!=self.expected_sha256.lower():raise _invalid('SHA-256 spatial artifact не совпал')
        manifest=None;summary=None;root=None;mesh=None;counts={};sources=instances=meshes=errors=0
        with tempfile.TemporaryDirectory(prefix='bim-spatial-reader-') as directory:
            with closing(sqlite3.connect(str(Path(directory)/'index.db'))) as index:
                index.execute('PRAGMA cache_size=-2048')
                index.execute('CREATE TABLE meshes(key TEXT PRIMARY KEY) WITHOUT ROWID')
                for metadata,section in self.frames():
                    kind=metadata.get('Type')
                    if summary is not None:raise _invalid('Frames после итоговой summary')
                    if manifest is None:
                        if kind!='manifest' or metadata.get('Format')!='bim-spatial' or metadata.get('FormatVersion')!=2:
                            raise _invalid('Неизвестный spatial manifest')
                        if not metadata.get('ModelVersionUtc'):raise _invalid('Manifest без snapshot')
                        utc(metadata['ModelVersionUtc']);manifest=metadata;continue
                    if kind=='source-begin':
                        if root is not None or mesh is not None:raise _invalid('Вложенные source frames')
                        root=_key(metadata['Element']);sources+=1
                    elif kind=='source-end':
                        if root!=_key(metadata['Element']) or mesh is not None:raise _invalid('Несогласованная граница source')
                        if strict and metadata.get('State') not in ('complete','no-geometry'):
                            raise IncompleteResult('Spatial source неполный',metadata)
                        root=None
                    elif kind=='mesh-begin':
                        if root is None or mesh is not None:raise _invalid('Неожиданный mesh-begin')
                        mesh=metadata['MeshDefinitionKey'];counts=dict.fromkeys(FIELDS,0)
                        if mesh!=guid(metadata['ModelPartId'])+'/'+guid(metadata['MeshId']):raise _invalid('Несогласованный mesh key')
                        try:index.execute('INSERT INTO meshes(key) VALUES(?)',(mesh,))
                        except sqlite3.IntegrityError:raise _invalid('Повторное определение mesh') from None
                        meshes+=1
                    elif kind=='buffer':
                        field=metadata['Field']
                        if mesh!=metadata.get('MeshDefinitionKey') or metadata['Offset']!=counts[field]:
                            raise _invalid('Несогласованное смещение mesh buffer')
                        values=section.values()
                        if field in ('Vertices','Normals'):
                            if metadata['Count']%3 or any(not math.isfinite(v) for v in values):raise _invalid('Повреждены vector triples')
                        elif any(value>=counts['Vertices']//3 for value in values):raise _invalid('Mesh index вне массива vertices')
                        counts[field]+=metadata['Count']
                    elif kind=='mesh-end':
                        if mesh!=metadata.get('MeshDefinitionKey') or any(metadata.get(field)!=count for field,count in counts.items()):
                            raise _invalid('Счётчики mesh не совпали')
                        if counts['Normals'] not in (0,counts['Vertices']) or counts['Indices']%3 or counts['EdgeIndices']%2:
                            raise _invalid('Некорректные длины mesh arrays')
                        mesh=None
                    elif kind=='instance':
                        if root is None or mesh is not None:raise _invalid('Instance вне source')
                        if manifest.get('IncludeMeshes') and not index.execute('SELECT 1 FROM meshes WHERE key=?',(metadata['MeshDefinitionKey'],)).fetchone():
                            raise _invalid('Instance ссылается на отсутствующий mesh')
                        _matrix(metadata['ModelPlacement']);_key(metadata['Element']);instances+=1
                    elif kind=='error':
                        if root is None:raise _invalid('Ошибка вне source')
                        errors+=1
                        if strict:raise IncompleteResult('Spatial export содержит ошибку',metadata)
                    elif kind=='summary':
                        if root is not None or mesh is not None:raise _invalid('Незакрытая source/mesh секция')
                        if (metadata.get('SourceCount')!=sources or metadata.get('InstanceCount')!=instances or metadata.get('MeshCount')!=meshes
                                or metadata.get('FailedCount')!=errors or utc(metadata.get('ModelVersionUtc'))!=utc(manifest['ModelVersionUtc'])):
                            raise _invalid('Spatial summary не совпала с данными/snapshot')
                        if strict and not metadata.get('Complete'):raise IncompleteResult('Spatial export неполный',metadata)
                        summary=metadata
                    else:raise _invalid('Неизвестный тип spatial frame')
        if summary is None:raise _invalid('Отсутствует завершающая spatial summary')
        return {'Manifest':manifest,'Summary':summary}

    def iter_placed_vertices(self, element=None, *, strict=True):
        """Stream transformed XYZ; scratch index is disk-backed, no NumPy required."""
        if strict:self.validate()
        selected=_key(element) if element is not None else None
        current=None;manifest=None
        with tempfile.TemporaryDirectory(prefix='bim-vertices-') as directory:
            with closing(sqlite3.connect(str(Path(directory)/'index.db'))) as index:
                index.execute('PRAGMA cache_size=-2048')
                index.execute('CREATE TABLE buffers(key TEXT,position INTEGER,count INTEGER,sha TEXT)')
                for metadata,section in self.frames():
                    kind=metadata.get('Type')
                    if kind=='manifest':
                        manifest=metadata
                        if not manifest.get('IncludeMeshes'):raise ValueError('Artifact не содержит meshes')
                    elif kind=='source-begin':current=_key(metadata['Element'])
                    elif kind=='source-end':current=None
                    elif kind=='buffer' and metadata['Field']=='Vertices':
                        index.execute('INSERT INTO buffers VALUES(?,?,?,?)',(metadata['MeshDefinitionKey'],section.byte_offset,metadata['Count'],metadata['Sha256']))
                    elif kind=='instance' and (selected is None or current==selected):
                        matrix=_matrix(metadata['ModelPlacement'])
                        rows=index.execute('SELECT position,count,sha FROM buffers WHERE key=? ORDER BY position',(metadata['MeshDefinitionKey'],))
                        for position,count,sha in rows:
                            buffer=SpatialBufferSection(self.path,position,{'Count':count,'DType':'<f4','Sha256':sha})
                            data=buffer.read()
                            if count%3:raise _invalid('Vertices не образуют XYZ triples')
                            for x,y,z in struct.iter_unpack('<fff',data):
                                yield tuple(matrix[row*4]*x+matrix[row*4+1]*y+matrix[row*4+2]*z+matrix[row*4+3] for row in range(3))


def _key(element):
    if hasattr(element,'payload'):element=element.payload()
    element=element.get('Element',element)
    return guid(element['ModelPartId']),guid(element['ElementId'])


def _matrix(value):
    if not isinstance(value,list) or len(value)!=16 or any(isinstance(v,(bool,str)) or not math.isfinite(v) for v in value):
        raise _invalid('Некорректная матрица placement')
    if any(abs(value[i])>1e-12 for i in (12,13,14)) or abs(value[15]-1)>1e-12:
        raise _invalid('Неподдерживаемая неаффинная матрица')
    return value
