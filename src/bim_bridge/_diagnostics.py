"""Decode optional worker diagnostics after the Manager has forwarded them."""
import base64
import binascii
import hashlib
import json
import math


_CONTENT_TYPE = 'application/vnd.pilot.bimbridge.spatial-performance+json'
_MAX_BYTES = 64 * 1024
_COUNTS = ('SourceElements', 'TraversedNodes', 'Instances', 'UniqueMeshes',
           'MeshCacheHits', 'MeshCacheMisses', 'SnapshotIndexHits', 'SnapshotIndexMisses',
           'WorkerMeshCacheEntries', 'WorkerMeshCacheBytes', 'WorkerMeshCacheEvictions',
           'WorkerSnapshotIndexEntries', 'WorkerSnapshotIndexBytes', 'WorkerSnapshotIndexEvictions')
_DURATIONS = ('SdkTraversalReadMs', 'SdkMeshAcquireMs', 'SdkVerticesReadMs', 'BoundsCpuMs',
              'BinaryEncodingActiveMs', 'ArtifactWriteWallMs', 'ArtifactFinalizeWallMs',
              'ModelAccessWallMs', 'SnapshotIndexWallMs', 'SdkDispatchWaitWallMs',
              'SpoolPreparationWallMs')


def _valid_spatial(value, record, required):
    if not isinstance(value, dict):
        return False
    version = value.get('SchemaVersion')
    if version is not None and (type(version) is not int or version != 1):
        return False
    if required and version is None:
        return False
    for key in ('TaskId', 'AssignmentId'):
        if record.get(key) and value.get(key) and str(value[key]).casefold() != str(record[key]).casefold():
            return False
    for index, key in enumerate(_COUNTS):
        field = value.get(key)
        if field is None:
            if required and index < 4:
                return False
        elif type(field) is not int or field < 0:
            return False
    for index, key in enumerate(_DURATIONS):
        field = value.get(key)
        if field is None:
            if required and index < 4:
                return False
        elif type(field) not in (int, float) or not math.isfinite(field) or field < 0:
            return False
    return True


def spatial_performance(record):
    """Return a valid known schema, or the legacy field when no usable envelope exists."""
    envelope = record.get('Diagnostics') or {}
    try:
        if envelope.get('ContentType') != _CONTENT_TYPE or type(envelope.get('SchemaVersion')) is not int \
                or envelope['SchemaVersion'] != 1:
            raise ValueError('unknown schema')
        encoded = envelope['PayloadBytes']
        if type(encoded) is not str or len(encoded) > (_MAX_BYTES + 2) // 3 * 4:
            raise ValueError('invalid payload length')
        payload = base64.b64decode(encoded, validate=True)
        if not payload or len(payload) > _MAX_BYTES or \
                hashlib.sha256(payload).hexdigest().casefold() != envelope['Sha256'].casefold():
            raise ValueError('invalid payload hash')
        value = json.loads(payload.decode('utf-8'))
        if not _valid_spatial(value, record, required=True):
            raise ValueError('invalid spatial schema')
        return value
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, binascii.Error, AttributeError):
        legacy = record.get('SpatialPerformance')
        return legacy if _valid_spatial(legacy, record, required=False) else None
