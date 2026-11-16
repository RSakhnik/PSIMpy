"""Resumable traversal of model relations."""
from collections import deque
import hashlib
import json
from pathlib import Path

from .bulk import ExportSnapshot, _atomic_json, iter_relations
from .errors import IncompleteResult
from .types import element_payload, guid


def _key(element):
    return (guid(element['ModelPartId']), guid(element['ElementId']))


def walk_relations(client, model_key, start_elements, *, direction='children', order='bfs',
                   batch_size=100, predicate=None, stop_branch=None, state_path=None,
                   model_version_utc=None, max_elements=None, **options):
    """Yield relation descriptors; a saved state may repeat the last yielded row.

    Callbacks are applied again on replay and should be deterministic. The state
    is advanced after the caller requests the next row, giving at-least-once
    delivery rather than silently dropping a row on interruption.
    """
    if direction not in ('children', 'parent', 'both') or order not in ('bfs', 'dfs'):
        raise ValueError('Invalid traversal direction or order')
    if batch_size <= 0 or max_elements is not None and max_elements <= 0:
        raise ValueError('Batch size and max_elements must be positive')
    model_key = guid(model_key)
    starts = []
    start_keys = set()
    for element in start_elements:
        payload = element_payload(element)
        key = _key(payload)
        if key not in start_keys:
            starts.append(payload)
            start_keys.add(key)
    identity = hashlib.sha256(json.dumps([model_key, starts, direction, order, options],
                                          sort_keys=True, default=str).encode()).hexdigest()
    path = Path(state_path) if state_path else None
    if path and path.exists():
        state = json.loads(path.read_text(encoding='utf-8'))
        if state.get('Identity') != identity:
            raise ValueError('Traversal state belongs to different inputs or options')
        if model_version_utc and state.get('ModelVersionUtc') and state['ModelVersionUtc'] != model_version_utc:
            raise ValueError('Traversal state belongs to another model version')
    else:
        state = {'Identity': identity, 'Queue': starts, 'Visited': [],
                 'ModelVersionUtc': model_version_utc, 'Complete': False}
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            _atomic_json(path, state)
    if state['Complete']:
        return
    queue = deque(state['Queue'])
    visited = {tuple(item) for item in state['Visited']}
    snapshot = ExportSnapshot(state['ModelVersionUtc'])
    while queue:
        if max_elements is not None and len(visited) >= max_elements:
            if any(_key(item) not in visited for item in queue):
                raise IncompleteResult('Traversal reached max_elements; resume with a larger budget', state)
            queue.clear()
            break
        batch = []
        selected = set()
        for item in queue if order == 'bfs' else reversed(queue):
            key = _key(item)
            if key not in visited and key not in selected:
                batch.append(item)
                selected.add(key)
            if len(batch) >= batch_size or (max_elements is not None and
                                            len(batch) >= max_elements - len(visited)):
                break
        if not batch:
            queue.clear()
            break
        responses = iter_relations(client, model_key, batch, batch_size=len(batch),
                                   include_parent=direction in ('parent', 'both'),
                                   include_children=direction in ('children', 'both'),
                                   snapshot=snapshot, strict=True, **options)
        rows = {_key(row['Element']): row for response in responses for row in response['Elements']}
        if path and not snapshot.verified:
            raise IncompleteResult('Worker did not confirm ModelVersionUtc; cannot resume safely')
        for item in batch:
            key = _key(item)
            row = rows[key]
            if predicate is None or predicate(row):
                yield row
            visited.add(key)
            queue = deque(e for e in queue if _key(e) != key)
            if stop_branch is None or not stop_branch(row):
                neighbors = ([] if direction == 'parent' else row.get('Children', []))
                if direction in ('parent', 'both') and row.get('Parent'):
                    neighbors = [row['Parent']] + neighbors
                for neighbor in neighbors:
                    if _key(neighbor) not in visited and not any(_key(e) == _key(neighbor) for e in queue):
                        queue.append(neighbor)
            state.update(Queue=list(queue), Visited=[list(k) for k in visited],
                         ModelVersionUtc=snapshot.version)
            if path:
                _atomic_json(path, state)
            if max_elements is not None and len(visited) >= max_elements and queue:
                raise IncompleteResult('Traversal reached max_elements; resume with a larger budget', state)
    state.update(Queue=[], Visited=[list(k) for k in visited],
                 ModelVersionUtc=snapshot.version, Complete=True)
    if path:
        _atomic_json(path, state)
