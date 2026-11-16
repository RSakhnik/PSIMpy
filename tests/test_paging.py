import copy
import json
import types
import tempfile
import unittest
from pathlib import Path
from bim_bridge import BimBridgeError, IncompleteResult, iter_search_elements, iter_search_pages, iter_search_complete
from spatial_support import MODEL, PART, V1, V2, elements


class PageClient:
    def __init__(self):self.calls=[];self.version=V1;self.transform=lambda r:r
    def supports(self,capability,**options):return capability=='paged-search-v2'
    def run(self,operation,payload,**options):
        self.calls.append((copy.deepcopy(payload),options))
        cursor=payload.get('Cursor')
        index={None:0,'opaque-1':1,'opaque-2':2}.get(cursor,1)
        keys=elements(5)[index*2:index*2+2]
        return self.transform({'ModelVersionUtc':self.version,'Elements':keys,'HasMore':index<2,
                               'NextCursor':f'opaque-{index+1}' if index<2 else None,'CursorWorkerInstanceId':'worker-1'})


class PagingTests(unittest.TestCase):
    def test_pages_pin_snapshot_and_worker_affinity(self):
        client=PageClient();pages=list(iter_search_pages(client,MODEL,[PART],page_size=2))
        self.assertEqual([len(p['Elements']) for p in pages],[2,2,1])
        self.assertNotIn('ModelVersionUtc',client.calls[0][0])
        self.assertEqual(client.calls[1][0]['ModelVersionUtc'],V1)
        self.assertEqual(client.calls[1][1]['preferred_worker_instance_id'],'worker-1')
        self.assertEqual(client.calls[0][1]['required_capabilities'],['paged-search-v2'])
    def test_bulk_iterator_uses_pages_without_globalid_partition(self):
        client=PageClient();keys=list(iter_search_elements(client,MODEL,[PART],max_results=2))
        self.assertEqual(len(keys),5)
        self.assertTrue(all(k['SnapshotVerified'] for k in keys))
        self.assertTrue(all(call[0]['Conditions']==[] for call in client.calls))
    def test_changed_snapshot_and_bad_cursor_stop(self):
        client=PageClient();iterator=iter_search_pages(client,MODEL,[PART],page_size=2)
        next(iterator);client.version=V2
        with self.assertRaises(BimBridgeError) as caught:next(iterator)
        self.assertEqual(caught.exception.code,'MODEL_CHANGED')
        client=PageClient();client.transform=lambda r:dict(r,NextCursor='same')
        iterator=iter_search_pages(client,MODEL,[PART],page_size=2);next(iterator)
        with self.assertRaises(BimBridgeError) as caught:next(iterator)
        self.assertEqual(caught.exception.code,'INVALID_CURSOR')
    def test_empty_final_page_capability_and_page_limit(self):
        client=PageClient();client.transform=lambda r:dict(r,Elements=[],HasMore=False,NextCursor=None)
        self.assertEqual(len(list(iter_search_pages(client,MODEL,[PART]))),1)
        client=PageClient();client.supports=lambda *a,**k:False
        with self.assertRaises(BimBridgeError) as caught:list(iter_search_pages(client,MODEL,[PART]))
        self.assertEqual(caught.exception.code,'CAPABILITY_NOT_AVAILABLE')
        with self.assertRaises(IncompleteResult):list(iter_search_pages(PageClient(),MODEL,[PART],max_pages=1))

    def test_complete_search_restarts_expired_cursor_without_duplicate_ids(self):
        client=PageClient();expired=[False]
        events=[];client._http=types.SimpleNamespace(journal=events.append)
        original=client.run
        def run(operation,payload,**options):
            if payload.get('Cursor')=='opaque-1' and not expired[0]:
                expired[0]=True
                raise BimBridgeError('CURSOR_EXPIRED','expired')
            return original(operation,payload,**options)
        client.run=run
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'search.json'
            rows=list(iter_search_complete(client,MODEL,[PART],page_size=2,state_path=path))
            self.assertEqual(len(rows),5)
            self.assertEqual(len({(r['ModelPartId'],r['ElementId']) for r in rows}),5)
            self.assertEqual(list(iter_search_complete(client,MODEL,[PART],page_size=2,state_path=path)),[])
            self.assertEqual(json.loads(path.read_text())['Status'],'completed')
            self.assertEqual(json.loads(path.read_text())['ReprocessedElements'],2)
            self.assertIn('cursor_expired',[event['EventType'] for event in events])
            self.assertIn('completed',[event['EventType'] for event in events])

    def test_complete_search_detects_changed_version_on_restart(self):
        client=PageClient();original=client.run
        def run(operation,payload,**options):
            if payload.get('Cursor')=='opaque-1':
                client.version=V2
                raise BimBridgeError('CURSOR_EXPIRED','expired')
            return original(operation,payload,**options)
        client.run=run
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(BimBridgeError) as caught:
                list(iter_search_complete(client,MODEL,[PART],page_size=2,
                                          state_path=Path(directory)/'search.json'))
            self.assertEqual(caught.exception.code,'MODEL_CHANGED')
            self.assertEqual(json.loads((Path(directory)/'search.json').read_text())['Status'],'model_changed')

    def test_complete_search_resumes_mid_page(self):
        client=PageClient()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'search.json'
            iterator=iter_search_complete(client,MODEL,[PART],page_size=2,state_path=path)
            first=next(iterator)
            second=next(iterator)
            iterator.close()
            self.assertEqual(json.loads(path.read_text())['Status'],'interrupted')
            remaining=list(iter_search_complete(client,MODEL,[PART],page_size=2,state_path=path))
            self.assertEqual(len(remaining),4)
            self.assertEqual(remaining[0]['ElementId'],second['ElementId'])
            self.assertEqual(len({r['ElementId'] for r in [first]+remaining}),5)

    def test_complete_search_budget_status(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'search.json'
            with self.assertRaises(IncompleteResult):
                list(iter_search_complete(PageClient(),MODEL,[PART],page_size=2,
                                          state_path=path,max_pages=1))
            self.assertEqual(json.loads(path.read_text())['Status'],'budget_exhausted')
