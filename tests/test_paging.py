import copy
import unittest
from bim_bridge import BimBridgeError, IncompleteResult, iter_search_elements, iter_search_pages
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
