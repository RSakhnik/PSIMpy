"""Пространственные данные без пересчёта единиц и геодезических предположений."""
import itertools
import math
from .types import element_payload, guid, utc
from .errors import BimBridgeError, IncompleteResult, TaskFailed
from .bulk import _export_snapshot, _run

OPERATION = 'get-element-spatial-data'


def _options(options):
    options = dict(options)
    options.setdefault('can_retry', False)
    options.setdefault('max_attempts', 1)
    return options


def _payload(model_key, elements, descendants=False, meshes=False, version=None):
    if not isinstance(descendants, bool) or not isinstance(meshes, bool):
        raise TypeError('include_descendants и include_meshes должны быть bool')
    raw = list(itertools.islice(iter(elements), 101))
    if not 1 <= len(raw) <= 100:
        raise ValueError('Нужны от 1 до 100 исходных элементов. Для больших наборов используйте iter_spatial_data.')
    versions = {utc(e['ModelVersionUtc']) for e in raw if isinstance(e, dict) and e.get('ModelVersionUtc')}
    if version:
        versions.add(utc(version))
    if len(versions) > 1:
        raise BimBridgeError('MODEL_CHANGED', 'Входные элементы относятся к разным снимкам', retryable=True)
    keys, seen = [], set()
    for element in raw:
        key = element_payload(element)
        pair = (key['ModelPartId'], key['ElementId'])
        if pair not in seen:
            keys.append(key)
            seen.add(pair)
    result = {'TargetModelKey': guid(model_key), 'ElementIds': keys,
              'IncludeDescendants': descendants, 'IncludeMeshes': meshes}
    if versions:
        result['ModelVersionUtc'] = next(iter(versions))
    return result


def _batch_size(size, meshes):
    size = (5 if meshes else 20) if size is None else size
    if type(size) is not int or not 1 <= size <= 100:
        raise ValueError('batch_size должен быть целым числом от 1 до 100')
    return size


def _inputs(model_key, elements, size, descendants, meshes, snapshot):
    iterator = iter(elements)
    while raw := list(itertools.islice(iterator, size)):
        payload = _payload(model_key, raw, descendants, meshes, snapshot.version)
        if payload.get('ModelVersionUtc'):
            snapshot.accept({'ModelVersionUtc': payload['ModelVersionUtc']})
        yield payload


def _pair(element):
    return guid(element['ModelPartId']), guid(element['ElementId'])


def _complete(result, requested):
    """Predicate for spatial cache admission; never counts no-geometry as failure."""
    try:
        if not isinstance(result, dict) or not result.get('ModelVersionUtc'):
            return False
        utc(result['ModelVersionUtc'])
        if any(result.get(k) for k in ('FailedElementIds', 'ElementErrors', 'Warnings', 'Truncated')):
            return False
        elements = result['Elements']
        expected = {_pair(e) for e in requested}
        actual = [_pair(e['Element']) for e in elements]
        return (len(actual) == len(set(actual)) and set(actual) == expected
                and all(e.get('State') in ('complete', 'no-geometry') for e in elements))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _check(result, payload, snapshot, strict):
    if not isinstance(result, dict) or not result.get('ModelVersionUtc'):
        raise IncompleteResult('Worker не подтвердил ModelVersionUtc пространственного ответа', result)
    snapshot.accept(result)
    complete = _complete(result, payload['ElementIds'])
    if strict and not complete:
        raise IncompleteResult('Пространственный пакет неполный. Проверьте State и ElementErrors.', result)
    return complete


def iter_spatial_data(client, model_key, elements, *, batch_size=None, include_descendants=False,
                      include_meshes=False, model_version_utc=None, snapshot=None,
                      checkpoints=None, strict=True, **options):
    """Последовательно выдаёт полные ответы пакетов вместе с MeshDefinitions.

    RESULT_TOO_LARGE делит только завершившийся ошибкой пакет worker.
    Сетевые ошибки и ограничения локального JSON-парсера не повторяются.
    """
    model_key = guid(model_key)
    size = _batch_size(batch_size, include_meshes)
    snapshot = _export_snapshot(snapshot, model_version_utc)
    options = _options(options)
    if 'client_request_id' in options:
        raise ValueError('ClientRequestId должен быть отдельным для каждого пакета. Используйте Checkpoints.')

    def fetch(payload):
        payload = snapshot.payload(payload)
        try:
            result = _run(client, checkpoints, OPERATION, payload, model_key, options)
        except TaskFailed as error:
            batch = payload['ElementIds']
            if error.code != 'RESULT_TOO_LARGE' or len(batch) == 1:
                raise
            middle = len(batch)//2
            for half in (batch[:middle], batch[middle:]):
                yield from fetch(dict(payload, ElementIds=half))
            return
        _check(result, payload, snapshot, strict)
        yield result

    for payload in _inputs(model_key, elements, size, include_descendants, include_meshes, snapshot):
        yield from fetch(payload)


def _finite(value):
    if isinstance(value, (str, bytes, bool)):
        raise ValueError('Координата должна быть числом')
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('Координата должна быть конечным числом')
    return value


def iter_placed_vertices(result, element=None):
    """Кортежи XYZ после ModelPlacement, без NumPy и пересчёта масштаба.

    element — Element, составной словарь или строка из result['Elements'].
    Для ответа с одним исходным элементом аргумент можно не задавать.
    """
    elements = result.get('Elements') or []
    if element is None:
        if len(elements) != 1:
            raise ValueError('Укажите исходный элемент для ответа с несколькими Elements')
        selected = elements[0]
    else:
        key = _pair(element_payload(element.get('Element', element) if isinstance(element, dict) else element))
        matches = [e for e in elements if _pair(e['Element']) == key]
        if len(matches) != 1:
            raise ValueError('Исходный элемент не найден однозначно в результате')
        selected = matches[0]
    if selected.get('State') not in ('complete', 'no-geometry'):
        raise IncompleteResult('Нельзя считать вершины неполного исходного элемента', result)
    instances = selected.get('Instances') or []
    if not instances:
        return
    if 'MeshDefinitions' not in result:
        raise ValueError('Запросите IncludeMeshes=True для получения вершин')
    definitions = {}
    for definition in result['MeshDefinitions']:
        key = definition['Key']
        if key in definitions:
            raise BimBridgeError('INVALID_RESPONSE', 'Повторный ключ определения меша')
        definitions[key] = definition
    for instance in instances:
        try:
            vertices = definitions[instance['MeshDefinitionKey']]['Vertices']
            matrix = [_finite(x) for x in instance['ModelPlacement']]
            if len(matrix) != 16 or len(vertices)%3 or any(abs(matrix[i]) > 1e-12 for i in (12, 13, 14)) or abs(matrix[15]-1) > 1e-12:
                raise ValueError('Неподдерживаемая матрица или массив вершин')
            for index in range(0, len(vertices), 3):
                point = [_finite(x) for x in vertices[index:index+3]]
                yield tuple(_finite(sum(matrix[row*4+j]*point[j] for j in range(3))+matrix[row*4+3]) for row in range(3))
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise BimBridgeError('INVALID_RESPONSE', 'Повреждённые вершины или размещение меша') from error


def export_spatial_data(client, model_key, elements, directory, *, batch_size=None,
                        include_descendants=False, include_meshes=False, model_version_utc=None,
                        snapshot=None, checkpoints=None, strict=True, **options):
    """Итератор описаний файлов. Требует optional extra streaming.

    Каталог — одна выгрузка одного снимка. Контекст заданий сохраняется на диске
    автоматически. Checkpoints, если указан, должен принадлежать этому клиенту.
    """
    from ._spatial_export import export_files
    yield from export_files(client, model_key, elements, directory, batch_size=batch_size,
                            include_descendants=include_descendants, include_meshes=include_meshes,
                            model_version_utc=model_version_utc, snapshot=snapshot,
                            checkpoints=checkpoints, strict=strict, **options)
