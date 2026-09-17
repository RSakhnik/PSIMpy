import json
from pathlib import Path
import re
import tempfile
import types
import unittest
import uuid
from bim_bridge import Checkpoints, IncompleteResult, select_parts, iter_search_elements, iter_properties, iter_relations, write_jsonl, BimBridgeError
from bim_bridge.bulk import _ifc_id

MODEL=str(uuid.UUID(int=1));PART=str(uuid.UUID(int=2))
class FakeClient:
    def __init__(self):
        self._http=types.SimpleNamespace(base_url='https://fake',_token='not-a-real-token')
        self.calls=[];self.tasks={};self.posts=0
    def run(self, operation, payload, **options):
        self.calls.append((operation,payload))
        if operation=='search-elements':
            filters=payload['Conditions']
            if any(c['Operator']=='NotDefined' for c in filters):return {'Elements':[],'Truncated':False}
            elements=[{'ModelPartId':PART,'ElementId':str(uuid.UUID(int=i))} for i in range(1,25)]
            for c in filters:
                if c['Operator']=='Regexp':elements=[e for e in elements if re.search(c['Value'],_ifc_id(e['ElementId']))]
            cap=min(payload['MaxResults'],3)
            return {'Elements':elements[:cap],'Truncated':len(elements)>cap}
        if operation=='load-element-properties':return {'Elements':[dict(e,PropertySets=[]) for e in payload['ElementIds']]}
        if operation=='get-element-relations':return {'Elements':[{'Element':e,'Children':[]} for e in payload['ElementIds']], 'Truncated':len(payload['ElementIds'])>1}
        return {'ok':True}
    def submit(self, operation, payload, **options):
        self.posts+=1
        ident=str(uuid.uuid4());self.tasks[ident]=self.run(operation,payload)
        return self.task(ident)
    def task(self, ident):
        if ident not in self.tasks:
            def missing(**kwargs):raise BimBridgeError('TASK_NOT_FOUND','expired',status_code=404)
            return types.SimpleNamespace(id=ident,wait=missing)
        return types.SimpleNamespace(id=ident,wait=lambda **kw:None,result=lambda:self.tasks[ident])

class BulkTests(unittest.TestCase):
    def test_complete_partition_and_cap(self):
        client=FakeClient();results=list(iter_search_elements(client,MODEL,[PART],max_results=10))
        self.assertEqual(len(results),24);self.assertEqual(len({e['ElementId'] for e in results}),24)
        self.assertGreater(len(client.calls),1)
        with self.assertRaises(IncompleteResult):list(iter_search_elements(client,MODEL,[PART],max_queries=1))
    def test_catalog_partial_and_name_filter(self):
        c={'CatalogState':'partial','Models':[{'ModelKey':MODEL,'ProjectName':'УПС','ModelParts':[{'ModelPartId':PART,'DisplayName':'100-Piping'}]}]}
        with self.assertRaises(IncompleteResult):select_parts(c,project='УПС')
        self.assertEqual(len(select_parts(c,project='упс',name_contains='-piping',allow_partial=True)),1)
        self.assertEqual(select_parts(c,project='другой',allow_partial=True),[])
    def test_properties_batches_and_recursive_relations(self):
        client=FakeClient();elements=[{'ModelPartId':PART,'ElementId':str(uuid.UUID(int=i))} for i in range(1,6)]
        self.assertEqual([len(x['Elements']) for x in iter_properties(client,MODEL,iter(elements),batch_size=2)],[2,2,1])
        self.assertEqual(len(list(iter_relations(client,MODEL,elements,batch_size=5))),5)
    def test_strict_partial_failure(self):
        client=FakeClient();client.run=lambda *a,**kw:{'Elements':[],'FailedElementIds':['x']}
        with self.assertRaises(IncompleteResult):list(iter_properties(client,MODEL,[{'ModelPartId':PART,'ElementId':MODEL}]))
        self.assertEqual(len(list(iter_properties(client,MODEL,[{'ModelPartId':PART,'ElementId':MODEL}],strict=False))),1)
    def test_checkpoint_cache_and_expiry(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as tmp:
            cp=Checkpoints(client,tmp)
            self.assertEqual(cp.run('echo',{}),{'ok':True})
            self.assertEqual(cp.run('echo',{}),{'ok':True});self.assertEqual(client.posts,1)
            for file in cp.directory.glob('*.result.json'):file.unlink()
            client.tasks.clear()
            self.assertEqual(cp.run('echo',{}),{'ok':True});self.assertEqual(client.posts,2)
            for file in cp.directory.glob('*.json'):self.assertNotIn('not-a-real-token',file.read_text())
            client._http._token='different'
            self.assertNotEqual(Checkpoints(client,tmp).directory,cp.directory)
    def test_atomic_jsonl_on_interruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest=Path(tmp)/'data.jsonl'
            write_jsonl([{'x':'я'}],dest)
            self.assertEqual(json.loads(dest.read_text(encoding='utf-8')),{'x':'я'})
            def broken():yield {'x':2};raise RuntimeError('interrupted')
            with self.assertRaises(RuntimeError):write_jsonl(broken(),dest,overwrite=True)
            self.assertEqual(json.loads(dest.read_text(encoding='utf-8')),{'x':'я'})
            self.assertEqual(len(list(Path(tmp).iterdir())),1)

    def test_checkpoint_resumes_pending_task_without_new_post(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as tmp:
            cp=Checkpoints(client,tmp)
            original=client.task
            def interrupted(ident):
                task=original(ident)
                def wait(**kwargs):raise KeyboardInterrupt()
                task.wait=wait
                return task
            client.task=interrupted
            with self.assertRaises(KeyboardInterrupt):cp.run('echo',{})
            client.task=original
            self.assertEqual(cp.run('echo',{}),{'ok':True})
            self.assertEqual(client.posts,1)

    def test_missing_global_id_and_single_relation_truncation_fail(self):
        client=FakeClient()
        client.run=lambda *a,**kw:{'Elements':[], 'Truncated':True}
        with self.assertRaises(IncompleteResult):
            list(iter_search_elements(client,MODEL,[PART]))
        client.run=lambda op,p,**kw:{'Elements':[{'Element':e,'Children':[]} for e in p['ElementIds']], 'Truncated':True}
        with self.assertRaises(IncompleteResult):
            list(iter_relations(client,MODEL,[{'ModelPartId':PART,'ElementId':MODEL}]))

if __name__=='__main__':unittest.main()
