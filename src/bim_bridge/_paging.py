"""Negotiated opaque-cursor iteration; regexp partitioning remains legacy only."""
from .bulk import _checked, _export_snapshot, _run
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
