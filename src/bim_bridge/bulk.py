"""Последовательные выгрузки без скрытого усечения и локальные checkpoints."""
import collections
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import tempfile
import uuid
from .errors import BimBridgeError, IncompleteResult
from .types import Condition, guid, element_payload
from ._files import publish

def _atomic_json(path, value):
    fd, temp = tempfile.mkstemp(prefix=".checkpoint-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)

class Checkpoints:
    """Одна папка — один снимок выгрузки. Для свежих данных создайте новую папку."""
    def __init__(self, client, directory):
        self.client = client
        # Не смешиваем результаты разных серверов/токенов. Сам токен не сохраняется.
        identity = hashlib.sha256((client._http.base_url + '\0' + client._http._token).encode()).hexdigest()
        self.directory = Path(directory) / identity
        self.directory.mkdir(parents=True, exist_ok=True)

    def run(self, operation, payload, **options):
        wait_options = {k: options.pop(k) for k in ("timeout", "on_status", "cancel_on_timeout") if k in options}
        key = hashlib.sha256(json.dumps([operation, payload, options], sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()
        result_path = self.directory / (key + '.result.json')
        state_path = self.directory / (key + '.task.json')
        if result_path.exists():
            return json.loads(result_path.read_text(encoding="utf-8"))
        state = json.loads(state_path.read_text()) if state_path.exists() else {"request_id": str(uuid.uuid4())}
        for attempt in range(2):
            _atomic_json(state_path, state)
            if not state.get("task_id"):
                task = self.client.submit(operation, payload, client_request_id=state['request_id'], **options)
                state['task_id'] = task.id
                _atomic_json(state_path, state)
            task = self.client.task(state['task_id'])
            try:
                task.wait(**wait_options)
                result = task.result()
            except BimBridgeError as exc:
                if attempt == 0 and (exc.status_code in (404, 410) or exc.code in ("RESULT_EXPIRED", "EXPIRED")):
                    state = {"request_id": str(uuid.uuid4())}
                    continue
                raise
            _atomic_json(result_path, result)
            return result

def select_parts(context, *, project=None, name_contains=None, model_key=None, allow_partial=False):
    if not allow_partial and context.get("CatalogState") != "complete":
        raise IncompleteResult("Каталог неполный. Обновите его или явно задайте allow_partial=True", context)
    found, seen = [], set()
    for model in context.get("Models", []):
        if project and model.get("ProjectName", "").casefold() != project.casefold():
            continue
        if model_key and model.get("ModelKey", "").lower() != guid(model_key):
            continue
        for part in model.get("ModelParts", []):
            if name_contains and name_contains.casefold() not in part.get("DisplayName", "").casefold():
                continue
            key = (model['ModelKey'], part['ModelPartId'])
            if key not in seen:
                found.append(dict(part, ModelKey=model['ModelKey'], ProjectName=model.get('ProjectName'),
                                  CatalogState=context.get('CatalogState')))
                seen.add(key)
    return found

def _run(client, checkpoints, operation, payload, model_key, options):
    if checkpoints is not None and checkpoints.client is not client:
        raise ValueError("Checkpoints принадлежит другому клиенту")
    runner = checkpoints.run if checkpoints else client.run
    return runner(operation, payload, target_model_key=model_key, **options)

def _checked(result):
    if result.get('FailedElementIds') or result.get('Warnings'):
        raise IncompleteResult("Ответ содержит пропуски или предупреждения. Подробности: exception.result", result)
    return result

def _ifc_id(value):
    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_$'
    number, output = uuid.UUID(value).int, ''
    for _ in range(22):
        output = alphabet[number & 63] + output
        number >>= 6
    return output

def iter_search_elements(client, model_key, part_ids, *, conditions=(), max_results=10000,
                         checkpoints=None, max_queries=10000, **options):
    """Все найденные элементы по явно заданным частям. Фильтры соединяются AND.

    Если индекс не позволяет безопасно разделить выборку, возбуждается
    IncompleteResult. Ограничения воркера не отключаются.
    """
    model_key = guid(model_key)
    if max_results <= 0 or max_queries <= 0:
        raise ValueError("Лимиты должны быть положительными")
    base = [c.payload() if isinstance(c, Condition) else c for c in conditions]
    queries = 0
    def query(part, filters):
        nonlocal queries
        queries += 1
        if queries > max_queries:
            raise IncompleteResult("Достигнут лимит max_queries, выгрузка не завершена")
        result = _checked(_run(client, checkpoints, 'search-elements',
            {'TargetModelKey': model_key, 'TargetModelPartIds': [part], 'LogicalOperator': 'And',
             'Conditions': filters, 'MaxResults': max_results}, model_key, options))
        for element in result['Elements']:
            if element['ModelPartId'].lower() != part:
                raise IncompleteResult("Поиск вернул элемент чужой части", result)
        return result
    def split(part, filters, depth=0):
        result = query(part, filters)
        values = [_ifc_id(e['ElementId']) for e in result['Elements']]
        # Проверяем соответствие GUID и GlobalId также на конечных диапазонах.
        for f in filters[len(base):]:
            if f['Operator'] == 'Regexp' and any(not re.search(f['Value'], v) for v in values):
                raise IncompleteResult("GlobalId индекса не соответствует ElementId. Нужно другое условие поиска", result)
        if not result.get('Truncated'):
            yield from result['Elements']
            return
        if depth >= 64 or not result['Elements']:
            raise IncompleteResult("Не удалось разделить усечённый результат", result)
        candidates = []
        for pos in range(22):
            counts = collections.Counter(v[pos] for v in values)
            chars, total = sorted(counts), 0
            for n, ch in enumerate(chars[:-1]):
                total += counts[ch]
                candidates.append((abs(len(values)-2*total), pos, ''.join(chars[:n+1])))
        if not candidates:
            raise IncompleteResult("Нельзя разделить повторяющийся GlobalId", result)
        _, pos, chars = min(candidates)
        for negate in ('', '^'):
            regex = '^.{' + str(pos) + '}[' + negate + re.escape(chars) + '].*$'
            yield from split(part, filters + [Condition('GlobalId', 'Regexp', regex, 'Common_Properties').payload()], depth+1)
    for part in dict.fromkeys(guid(p) for p in part_ids):
        seen = set()
        for element in split(part, base + [Condition('GlobalId', category='Common_Properties').payload()]):
            if element['ElementId'] in seen:
                raise IncompleteResult("Поисковые диапазоны пересеклись")
            seen.add(element['ElementId'])
            yield element
        missing = query(part, base + [Condition('GlobalId', 'NotDefined', category='Common_Properties').payload()])
        if missing.get('Truncated'):
            raise IncompleteResult("Слишком много элементов без GlobalId. Нужен более узкий фильтр", missing)
        for element in missing['Elements']:
            if element['ElementId'] in seen:
                raise IncompleteResult("Defined и NotDefined вернули один элемент", missing)
            seen.add(element['ElementId'])
            yield element

def _batches(elements, size):
    if size <= 0:
        raise ValueError("Размер пакета должен быть положительным")
    iterator = iter(elements)
    while batch := list(itertools.islice(iterator, size)):
        yield [element_payload(e) for e in batch]

def iter_properties(client, model_key, elements, *, batch_size=500, property_names=None,
                    checkpoints=None, strict=True, **options):
    """Возвращает пакеты результатов, включая FailedElementIds/Warnings."""
    for batch in _batches(elements, batch_size):
        result = _run(client, checkpoints, 'load-element-properties',
            {'TargetModelKey': guid(model_key), 'ElementIds': batch, 'PropertyNames': property_names,
             'BatchSize': min(batch_size, 100)}, model_key, options)
        if strict:
            _checked(result)
            expected = {(e['ModelPartId'], e['ElementId']) for e in batch}
            actual = {(e['ModelPartId'], e['ElementId']) for e in result['Elements']}
            if actual != expected:
                raise IncompleteResult("Пакет свойств неполный", result)
        yield result

def iter_relations(client, model_key, elements, *, batch_size=500, include_parent=True,
                   include_children=True, max_children=10000, checkpoints=None, strict=True, **options):
    if not (include_parent or include_children):
        raise ValueError("Нужно запросить родителя или детей")
    def fetch(batch):
        result = _run(client, checkpoints, 'get-element-relations',
            {'TargetModelKey': guid(model_key), 'ElementIds': batch, 'IncludeParent': include_parent,
             'IncludeChildren': include_children, 'MaxChildren': max_children}, model_key, options)
        if strict:
            _checked(result)
            expected = {(e['ModelPartId'], e['ElementId']) for e in batch}
            actual = {(e['Element']['ModelPartId'], e['Element']['ElementId']) for e in result['Elements']}
            if actual != expected:
                raise IncompleteResult("Пакет связей неполный", result)
            if result.get('Truncated') or any(e.get('ChildrenTruncated') for e in result['Elements']):
                if len(batch) == 1:
                    raise IncompleteResult("Даже один элемент превышает лимит детей API", result)
                middle = len(batch)//2
                yield from fetch(batch[:middle]); yield from fetch(batch[middle:])
                return
        yield result
    for batch in _batches(elements, batch_size):
        yield from fetch(batch)

def write_jsonl(records, destination, *, overwrite=False):
    """Потоковая запись. Неполный результат не публикуется как готовый файл."""
    destination = Path(destination)
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix='.bim-jsonl-', dir=destination.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False) + '\n')
        publish(temp, destination, overwrite)
        return destination
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
