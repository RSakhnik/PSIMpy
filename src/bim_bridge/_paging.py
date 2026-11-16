"""Negotiated opaque-cursor iteration; regexp partitioning remains legacy only."""
import hashlib
import json
from pathlib import Path
import sqlite3
from .bulk import _checked, _export_snapshot, _run
from .bulk import _atomic_json
from .errors import BimBridgeError, IncompleteResult
from .types import Condition, guid


def iter_search_pages(client, model_key, part_ids=None, *, conditions=(), logical_operator='And',
                      page_size=10000, cursor=None, cursor_worker_instance_id=None,
                      checkpoints=None, snapshot=None, model_version_utc=None,max_pages=10000, **options):
    model_key=guid(model_key)
    if type(page_size) is not int or page_size<=0 or type(max_pages) is not int or max_pages<=0:
        raise ValueError('page_size и max_pages должны быть положительными целыми')
    supports=getattr(client,'supports',None)
    if supports is None or not supports('paged-search-v2',model_key=model_key):
        raise BimBridgeError('CAPABILITY_NOT_AVAILABLE','Worker не поддерживает paged-search-v2')
    snapshot=_export_snapshot(snapshot,model_version_utc)
    payload={'TargetModelKey':model_key,'Conditions':[c.payload() if isinstance(c,Condition) else c for c in conditions],
             'LogicalOperator':logical_operator,'PageSize':page_size}
    if part_ids is not None:payload['TargetModelPartIds']=[guid(p) for p in part_ids]
    seen=set()
    for _ in range(max_pages):
        request=snapshot.payload(dict(payload,Cursor=cursor))
        submit=dict(options,required_capabilities=['paged-search-v2'])
        if cursor_worker_instance_id:submit['preferred_worker_instance_id']=cursor_worker_instance_id
        result=snapshot.accept(_checked(_run(client,checkpoints,'search-elements',request,model_key,submit)))
        if not result.get('ModelVersionUtc'):
            raise IncompleteResult('Страница не подтвердила снимок',result)
        elements=result.get('Elements')
        if not isinstance(elements,list) or len(elements)>page_size or type(result.get('HasMore')) is not bool:
            raise BimBridgeError('INVALID_RESPONSE','Некорректная страница поиска')
        if payload.get('TargetModelPartIds') and any(guid(e['ModelPartId']) not in payload['TargetModelPartIds'] for e in elements):
            raise IncompleteResult('Страница содержит элементы чужой части',result)
        more=result['HasMore'];next_cursor=result.get('NextCursor')
        if more and (not isinstance(next_cursor,str) or not next_cursor or next_cursor==cursor or next_cursor in seen or not elements):
            raise BimBridgeError('INVALID_CURSOR','Worker вернул неполноценное или циклическое продолжение')
        if not more and next_cursor:
            raise BimBridgeError('INVALID_CURSOR','Последняя страница содержит NextCursor')
        if more and not result.get('CursorWorkerInstanceId'):
            raise BimBridgeError('INVALID_CURSOR','Страница не указала worker для продолжения')
        yield result
        if not more:return
        seen.add(next_cursor);cursor=next_cursor;cursor_worker_instance_id=result['CursorWorkerInstanceId']
    raise IncompleteResult('Достигнут max_pages, выгрузка не завершена')


def iter_search_complete(client, model_key, part_ids=None, *, conditions=(),
                         logical_operator='And', page_size=10000, state_path,
                         max_pages=10000, max_restarts=2, **options):
    """Resume a paged search, replaying from page one when a cursor expires.

    The SQLite ID ledger is durable. A row may be replayed after interruption
    between yielding it and recording acknowledgement on the next iteration.
    """
    if max_restarts < 0 or max_pages <= 0:
        raise ValueError('Invalid search budgets')
    model_key = guid(model_key)
    parts = None if part_ids is None else [guid(p) for p in part_ids]
    filters = [c.payload() if isinstance(c, Condition) else c for c in conditions]
    identity = hashlib.sha256(json.dumps([model_key, parts, filters, logical_operator, page_size],
                                         sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        state = json.loads(path.read_text(encoding='utf-8'))
        if state.get('Identity') != identity:
            raise ValueError('Search state belongs to different parameters')
    else:
        state = {'Identity': identity, 'ModelVersionUtc': None, 'Cursor': None,
                 'CursorWorkerInstanceId': None, 'PendingCursor': None,
                 'Pages': 0, 'Restarts': 0, 'Complete': False}
        _atomic_json(path, state)
    ledger_path = Path(str(path) + '.ids.sqlite')
    if (state['Pages'] or state.get('Pending')) and not ledger_path.exists():
        raise IncompleteResult('Search ID ledger is missing; start a new search', state)
    database = sqlite3.connect(ledger_path)
    try:
        database.execute('CREATE TABLE IF NOT EXISTS ids (part TEXT NOT NULL, element TEXT NOT NULL, '
                         'PRIMARY KEY (part, element))')
        database.execute('CREATE TABLE IF NOT EXISTS metadata (identity TEXT NOT NULL)')
        stored = database.execute('SELECT identity FROM metadata').fetchone()
        if stored and stored[0] != identity:
            raise ValueError('Search ID ledger belongs to different parameters')
        if not stored:
            database.execute('INSERT INTO metadata VALUES (?)', (identity,))
            database.commit()
        if state['Complete']:
            return
        while state['Pages'] < max_pages:
            cursor = state['Cursor']
            try:
                pages = iter_search_pages(client, model_key, parts, conditions=filters,
                    logical_operator=logical_operator, page_size=page_size, cursor=cursor,
                    cursor_worker_instance_id=state['CursorWorkerInstanceId'],
                    model_version_utc=state['ModelVersionUtc'], max_pages=1, **options)
                page = next(pages)
            except BimBridgeError as error:
                if error.code != 'CURSOR_EXPIRED' or not cursor:
                    raise
                if state['Restarts'] >= max_restarts:
                    raise IncompleteResult('Search restart budget exhausted', state) from error
                state.update(Cursor=None, CursorWorkerInstanceId=None, Restarts=state['Restarts']+1)
                _atomic_json(path, state)
                continue
            version = page['ModelVersionUtc']
            if state['ModelVersionUtc'] and version != state['ModelVersionUtc']:
                raise BimBridgeError('MODEL_CHANGED', 'Model version changed during search')
            state['ModelVersionUtc'] = version
            replay = state.get('PendingCursor') == cursor and state.get('Pending', False)
            state.update(PendingCursor=cursor, Pending=True)
            _atomic_json(path, state)
            local = set()
            for element in page['Elements']:
                key = (guid(element['ModelPartId']), guid(element['ElementId']))
                if key in local:
                    raise IncompleteResult('Search page contains duplicate IDs', page)
                local.add(key)
                if database.execute('SELECT 1 FROM ids WHERE part=? AND element=?', key).fetchone():
                    if state['Restarts'] == 0 and not replay:
                        raise IncompleteResult('Search pages overlap', page)
                    continue
                yield dict(element, ModelVersionUtc=version, SnapshotVerified=True)
                database.execute('INSERT OR IGNORE INTO ids VALUES (?, ?)', key)
                database.commit()
            state.update(Pages=state['Pages']+1, Cursor=page.get('NextCursor'),
                         CursorWorkerInstanceId=page.get('CursorWorkerInstanceId') if page['HasMore'] else None,
                         Pending=False, Complete=not page['HasMore'])
            _atomic_json(path, state)
            if state['Complete']:
                return
        raise IncompleteResult('Search page budget exhausted', state)
    finally:
        database.close()
