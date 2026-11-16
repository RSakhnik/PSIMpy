"""Budgeted, resumable sampling of BIM property schemas."""
import hashlib
import json
from pathlib import Path
import re
import time

from ._paging import iter_search_complete
from .bulk import _atomic_json, iter_properties
from .errors import IncompleteResult
from .types import Condition, guid


def _default_group(name):
    return re.sub(r'\d+', '#', (name or '').casefold()).strip()


def discover_property_schemas(client, model_key, part_ids, state_path, *,
                              conditions=(), page_size=1000, max_pages=1000,
                              max_loads=100, max_seconds=None, stable_after=None,
                              group_key=None, group_name='default', property_names=None):
    """Return sampled schemas and an explicit stop reason.

    A stable sample is never reported as a complete model inventory.
    """
    if max_loads <= 0 or max_pages <= 0 or max_seconds is not None and max_seconds <= 0:
        raise ValueError('Discovery budgets must be positive')
    if stable_after is not None and (type(stable_after) is not int or stable_after <= 0):
        raise ValueError('stable_after must be a positive integer')
    model_key = guid(model_key)
    part_ids = [guid(part) for part in part_ids]
    conditions = [c.payload() if isinstance(c, Condition) else c for c in conditions]
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(json.dumps([model_key, part_ids, conditions, page_size,
                                         group_name, property_names], sort_keys=True,
                                        default=str).encode()).hexdigest()
    if path.exists():
        state = json.loads(path.read_text(encoding='utf-8'))
        if state.get('Identity') != identity:
            raise ValueError('Schema discovery state belongs to different parameters')
    else:
        state = {'Identity': identity, 'Searched': 0, 'Loaded': 0, 'LastElement': None,
                 'Groups': [], 'Schemas': [], 'History': [], 'Status': 'running'}
        _atomic_json(path, state)
    groups = set(state['Groups'])
    schemas = {tuple(schema) for schema in state['Schemas']}
    started = time.monotonic()
    unchanged = 0
    reason = None
    search_path = Path(str(path) + '.search.json')
    try:
        for element in iter_search_complete(client, model_key, part_ids, conditions=conditions,
                                            page_size=page_size, state_path=search_path,
                                            max_pages=max_pages):
            element_key = [guid(element['ModelPartId']), guid(element['ElementId'])]
            if element_key == state.get('LastElement'):
                continue
            if max_seconds is not None and time.monotonic() - started >= max_seconds:
                reason = 'time_budget'; break
            if state['Loaded'] >= max_loads:
                reason = 'load_budget'; break
            name = element.get('Name') or element.get('DisplayName') or ''
            group = (group_key or _default_group)(name)
            state['Searched'] += 1
            if group not in groups:
                result = next(iter_properties(client, model_key, [element], batch_size=1,
                                              property_names=property_names, strict=True,
                                              model_version_utc=element['ModelVersionUtc']))
                new = []
                for row in result['Elements']:
                    for section in row.get('PropertySets') or []:
                        for prop in section.get('Properties') or []:
                            value = prop.get('Value')
                            schema = (section.get('Category'), prop.get('Name'),
                                      prop.get('ValueType') or prop.get('Type') or type(value).__name__,
                                      prop.get('Unit'))
                            if schema not in schemas:
                                schemas.add(schema); new.append(schema)
                groups.add(group)
                state['Loaded'] += 1
                state['History'].append({'Searched': state['Searched'], 'Loaded': state['Loaded'],
                                         'Group': group, 'NewSchemas': [list(s) for s in new]})
                unchanged = 0 if new else unchanged + 1
            state.update(Groups=sorted(groups), Schemas=[list(s) for s in sorted(schemas, key=str)],
                         LastElement=element_key)
            _atomic_json(path, state)
            if stable_after is not None and unchanged >= stable_after:
                reason = 'sample_stable'; break
        else:
            reason = 'search_exhausted'
    except IncompleteResult as error:
        if 'budget' not in error.message.lower() and 'max_pages' not in error.message.lower():
            raise
        reason = 'page_budget'
    state['Status'] = reason
    state['CompleteInventory'] = reason == 'search_exhausted' and state['Loaded'] == state['Searched']
    state['Coverage'] = {'SearchedElements': state['Searched'],
                         'LoadedRepresentatives': state['Loaded'],
                         'ObservedGroups': len(groups), 'ObservedSchemas': len(schemas)}
    _atomic_json(path, state)
    return state
