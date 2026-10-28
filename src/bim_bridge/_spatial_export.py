"""Потоковая проверка и атомарная файловая выгрузка пространственных ответов."""
from contextlib import contextmanager
import hashlib
import importlib
import json
import os
from pathlib import Path
import tempfile
import uuid

from .bulk import _atomic_json, _export_snapshot
from .errors import BimBridgeError, IncompleteResult, TaskFailed
from .spatial import OPERATION, _batch_size, _inputs, _options, _pair
from .types import guid, utc


def _streaming():
    try:
        return importlib.import_module('ijson')
    except ImportError as error:
        raise ImportError("Для export_spatial_data установите pilot-bim-bridge[streaming]") from error


def _digest(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _key(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _read_json(path):
    try:
        result = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(result, dict):
            raise ValueError('Ожидался объект')
        return result
    except (ValueError, OSError) as error:
        raise BimBridgeError('INTEGRITY_ERROR', 'Повреждён манифест или файл состояния выгрузки') from error


@contextmanager
def _lock(directory):
    path = directory/'.spatial-export.lock'
    with open(path, 'a+b') as stream:
        if stream.tell() == 0:
            stream.write(b'0');stream.flush()
        stream.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise BimBridgeError('EXPORT_BUSY', 'Каталог уже используется выгрузкой или не поддерживает блокировки') from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _inspect(path, requested, parser):
    """Only keep root identifiers/states and scalar metadata, not meshes/instances."""
    expected = {_pair(e) for e in requested}
    actual = []
    complete = True
    metadata = {}
    current = None
    started = False
    elements_seen = False
    top_fields = set()
    try:
        with open(path, 'rb') as stream:
            for prefix, event, value in parser.parse(stream, use_float=True):
                if not started:
                    if prefix != '' or event != 'start_map':
                        raise ValueError('Ожидался JSON-объект')
                    started = True
                if prefix == '' and event == 'map_key':
                    if value in top_fields:
                        raise ValueError('Повторное поле верхнего уровня')
                    top_fields.add(value)
                if prefix in ('ModelVersionUtc', 'CoordinateSpace', 'SceneCoordinatesVerified'):
                    if event not in ('string', 'boolean', 'null'):
                        raise ValueError('Некорректные метаданные')
                    metadata[prefix] = value
                if prefix == 'Elements' and event == 'start_array':
                    elements_seen = True
                if prefix == 'Elements.item' and event == 'start_map':
                    current = {}
                if prefix in ('Elements.item.Element.ModelPartId', 'Elements.item.Element.ElementId', 'Elements.item.State'):
                    if current is None or event != 'string':
                        raise ValueError('Некорректный исходный элемент')
                    field = prefix.rsplit('.', 1)[1]
                    if field in current:
                        raise ValueError('Повторное поле исходного элемента')
                    current[field] = value
                if prefix == 'Elements.item' and event == 'end_map':
                    if len(actual) >= 100:
                        raise ValueError('Ответ содержит больше 100 исходных элементов')
                    actual.append(_pair(current))
                    complete &= current.get('State') in ('complete', 'no-geometry')
                    current = None
                if prefix == 'Truncated' and event not in ('map_key',):
                    if event != 'boolean':
                        raise ValueError('Truncated должен быть bool')
                    complete &= not value
                if prefix in ('FailedElementIds.item', 'ElementErrors.item', 'Warnings.item') and event not in ('end_map', 'end_array'):
                    complete = False
                if prefix in ('FailedElementIds', 'ElementErrors', 'Warnings') and event not in ('start_array', 'end_array', 'null'):
                    raise ValueError('Некорректный массив ошибок или предупреждений')
    except (OSError, KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        raise BimBridgeError('INVALID_RESPONSE', 'Повреждённый пространственный JSON') from error
    if not started or not elements_seen or not metadata.get('ModelVersionUtc'):
        raise IncompleteResult('Пространственный файл не подтвердил Elements и ModelVersionUtc', metadata)
    try:
        metadata['ModelVersionUtc'] = utc(metadata['ModelVersionUtc'])
    except (ValueError, TypeError) as error:
        raise BimBridgeError('INVALID_RESPONSE', 'Некорректная версия пространственного файла') from error
    metadata['Complete'] = bool(complete and len(actual) == len(set(actual)) and set(actual) == expected)
    metadata['ReturnedCount'] = len(actual)
    return metadata


def export_files(client, model_key, elements, directory, *, batch_size=None,
                 include_descendants=False, include_meshes=False, model_version_utc=None,
                 snapshot=None, checkpoints=None, strict=True, **options):
    parser = _streaming()  # Fail before consuming input or creating a task.
    model_key = guid(model_key)
    size = _batch_size(batch_size, include_meshes)
    snapshot = _export_snapshot(snapshot, model_version_utc)
    if checkpoints is not None and checkpoints.client is not client:
        raise ValueError('Checkpoints принадлежит другому клиенту')
    options = _options(options)
    if 'client_request_id' in options:
        raise ValueError('ClientRequestId создаётся отдельно для каждого пакета. Используйте сохранённый каталог выгрузки.')
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    parameters = {'Identity': _key([client._http.base_url, client._http._token]),
                  'ModelKey': model_key, 'IncludeDescendants': include_descendants,
                  'IncludeMeshes': include_meshes, 'BatchSize': size}
    manifest_path = directory/'manifest.json'
    state_directory = (checkpoints.directory/'spatial-files'/_key(str(directory)) if checkpoints else directory/'.checkpoints')
    state_directory.mkdir(parents=True, exist_ok=True)
    wait_options = {k: options.pop(k) for k in ('timeout', 'on_status', 'cancel_on_timeout') if k in options}
    download_options = {k: options.pop(k) for k in ('progress',) if k in options}
    # Directory lock spans iteration, including yields. Closing the generator releases it.
    with _lock(directory):
        manifest = _read_json(manifest_path) if manifest_path.exists() else {
            'Parameters': parameters, 'ModelVersionUtc': snapshot.version, 'Files': {}, 'Splits': {}}
        if manifest.get('Parameters') != parameters:
            raise ValueError('Каталог принадлежит другой выгрузке. Используйте отдельный каталог.')
        if manifest.get('ModelVersionUtc'):
            snapshot.accept({'ModelVersionUtc': manifest['ModelVersionUtc']})
        _atomic_json(manifest_path, manifest)

        def fetch(payload):
            payload = snapshot.payload(payload)
            key = _key(payload)
            if manifest.get('Splits', {}).get(key):
                middle = len(payload['ElementIds'])//2
                if middle == 0:
                    raise BimBridgeError('INTEGRITY_ERROR', 'Некорректное сохранённое разбиение пакета')
                for half in (payload['ElementIds'][:middle], payload['ElementIds'][middle:]):
                    yield from fetch(dict(payload, ElementIds=half))
                return
            path = directory/(key+'.json')
            entry = manifest['Files'].get(key)
            if entry and entry.get('Complete') and path.is_file() and _digest(path) == entry.get('Sha256'):
                # Revalidate semantic metadata too; never trust only a mutable manifest.
                info = _inspect(path, payload['ElementIds'], parser)
                snapshot.accept(info)
                if info['Complete']:
                    yield dict(entry, **info, Path=path)
                    return
            state_path = state_directory/(key+'.task.json')
            state = _read_json(state_path) if state_path.exists() else None
            if state is None or state.get('Incomplete'):
                state = {'ClientRequestId': str(uuid.uuid4()), 'Payload': payload}
            elif state.get('Payload') != payload:
                raise BimBridgeError('INTEGRITY_ERROR', 'Состояние задания не соответствует пакету')
            _atomic_json(state_path, state)
            if not state.get('TaskId'):
                task = client.submit(OPERATION, payload, target_model_key=model_key,
                                     client_request_id=state['ClientRequestId'], **options)
                state['TaskId'] = task.id
                _atomic_json(state_path, state)
            task = client.task(state['TaskId'])
            try:
                task.wait(**wait_options)
            except TaskFailed as error:
                if error.code != 'RESULT_TOO_LARGE' or len(payload['ElementIds']) == 1:
                    raise
                middle = len(payload['ElementIds'])//2
                for half in (payload['ElementIds'][:middle], payload['ElementIds'][middle:]):
                    yield from fetch(dict(payload, ElementIds=half))
                manifest.setdefault('Splits', {})[_key(snapshot.payload(payload))] = True
                _atomic_json(manifest_path, manifest)
                return
            # State is durable before both waiting and downloading. An interruption
            # resumes the same completed task, not another expensive SDK operation.
            fd, temporary = tempfile.mkstemp(prefix='.spatial-', suffix='.inprogress', dir=directory)
            os.close(fd)
            try:
                task.download(temporary, overwrite=True, decompress=True, **download_options)
                info = _inspect(temporary, payload['ElementIds'], parser)
                snapshot.accept(info)
                manifest['ModelVersionUtc'] = snapshot.version
                # First response pins an initially unversioned export. Move its
                # durable handle into the pinned key before publication.
                pinned_payload = snapshot.payload(payload)
                pinned_key = _key(pinned_payload)
                pinned_state_path = state_directory/(pinned_key+'.task.json')
                state['Payload'] = pinned_payload
                state['Incomplete'] = not info['Complete']
                _atomic_json(pinned_state_path, state)
                _atomic_json(manifest_path, manifest)
                if strict and not info['Complete']:
                    raise IncompleteResult('Пространственный файл неполный', info)
                path = directory/(pinned_key+'.json')
                entry = dict(info, FileName=path.name, Sha256=_digest(temporary), TaskId=task.id)
                os.replace(temporary, path)
                manifest['Files'][pinned_key] = entry
                _atomic_json(manifest_path, manifest)
                yield dict(entry, Path=path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

        for payload in _inputs(model_key, elements, size, include_descendants, include_meshes, snapshot):
            yield from fetch(payload)
