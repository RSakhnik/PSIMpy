import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from bim_bridge import BimBridgeError, ExportArtifact, IncompleteResult
from bim_bridge.exports import export_elements
from spatial_support import MODEL, PART, V1


def records():
    return [{'Type':'manifest','Format':'bim-elements','FormatVersion':1,'ModelVersionUtc':V1},
            {'Type':'element','ModelPartId':PART,'ElementId':MODEL,'PropertySets':[{'Category':'Common','Properties':[{'Name':'Name','Value':'Pipe'}]}]},
            {'Type':'summary','ModelVersionUtc':V1,'SelectedCount':1,'ExportedCount':1,'FailedCount':0,'Complete':True}]


class Client:
    def __init__(self):
        self._http=types.SimpleNamespace(base_url='pilot://server/database',_token='identity')
        self.submits=[];self.reply_lost=False;self.downloads=0
        self.data=gzip.compress(('\n'.join(json.dumps(r) for r in records())+'\n').encode())
    def supports(self,*args,**options):return True
    def health(self):return {'Capabilities':['artifact-upload-v2']}
    def submit(self,operation,payload,**options):
        self.submits.append((operation,payload,options))
        if self.reply_lost:self.reply_lost=False;raise BimBridgeError('TRANSPORT_ERROR','injected lost submit',retryable=True)
        return self.task(MODEL)
    def task(self,task_id):
        client=self
        class Task:
            id=task_id
            def wait(self,**options):pass
            def metadata(self):return {'TaskId':task_id,'TotalLength':len(client.data),'Sha256':hashlib.sha256(client.data).hexdigest()}
            def download(self,path,**options):client.downloads+=1;Path(path).write_bytes(client.data)
        return Task()


class ExportsTests(unittest.TestCase):
    def test_worker_export_does_not_round_trip_element_ids(self):
        client=Client()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'elements.ndjson.gz'
            artifact=export_elements(client,MODEL,path,part_ids=[PART],property_names=['Name'])
            self.assertEqual(len(list(artifact.iter_elements())),1)
            self.assertEqual(artifact.to_dataframe().iloc[0]['Common.Name'],'Pipe')
            self.assertEqual(client.submits[0][0],'export-elements')
            self.assertEqual(client.submits[0][1]['ModelPartIds'],[PART])
            self.assertNotIn('ElementIds',client.submits[0][1])
            self.assertFalse(client.submits[0][2]['can_retry'])
            export_elements(client,MODEL,path,part_ids=[PART],property_names=['Name'])
            self.assertEqual(len(client.submits),1);self.assertEqual(client.downloads,1)
    def test_completed_local_export_reused_without_network(self):
        client=Client()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'elements'
            artifact=export_elements(client,MODEL,path)
            def offline(*args,**kwargs):raise AssertionError('network must not be needed')
            client.supports=client.health=client.task=offline
            reused=export_elements(client,MODEL,path)
            self.assertEqual(reused.metadata,artifact.metadata)
            self.assertEqual(len(list(reused.iter_elements())),1)

    def test_lost_submit_uses_same_client_request_id(self):
        client=Client();client.reply_lost=True
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'elements'
            with self.assertRaises(BimBridgeError):export_elements(client,MODEL,path)
            export_elements(client,MODEL,path)
            self.assertEqual(client.submits[0][2]['client_request_id'],client.submits[1][2]['client_request_id'])
            with self.assertRaises(BimBridgeError) as caught:export_elements(client,MODEL,path,property_names=['Mass'])
            self.assertEqual(caught.exception.code,'EXPORT_CONFLICT')
    def test_missing_task_without_artifact_is_resubmitted_once(self):
        client=Client()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'elements'
            export_elements(client,MODEL,path)
            path.unlink()
            state_path=path.with_name(path.name+'.export.json')
            state=json.loads(state_path.read_text())
            old_request=state['ClientRequestId'];stale_task='00000000-0000-4000-8000-000000000001'
            state['TaskId']=stale_task;state.pop('Metadata')
            state_path.write_text(json.dumps(state))
            original_task=client.task
            def task(task_id):
                if task_id==stale_task:
                    def missing(**options):raise BimBridgeError('TASK_NOT_FOUND','expired',status_code=404)
                    return types.SimpleNamespace(wait=missing)
                return original_task(task_id)
            client.task=task
            artifact=export_elements(client,MODEL,path)
            self.assertEqual(artifact.task_id,MODEL)
            self.assertEqual(len(client.submits),2)
            self.assertNotEqual(client.submits[1][2]['client_request_id'],old_request)

    def test_missing_task_with_partial_bytes_is_not_silently_resubmitted(self):
        client=Client()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'elements'
            export_elements(client,MODEL,path)
            path.unlink()
            path.with_name(path.name+'.inprogress').write_bytes(b'partial')
            state_path=path.with_name(path.name+'.export.json')
            state=json.loads(state_path.read_text());state['TaskId']='00000000-0000-4000-8000-000000000001';state.pop('Metadata')
            state_path.write_text(json.dumps(state))
            def missing_task(task_id):
                def missing(**options):raise BimBridgeError('TASK_NOT_FOUND','expired',status_code=404)
                return types.SimpleNamespace(wait=missing)
            client.task=missing_task
            with self.assertRaises(BimBridgeError) as caught:export_elements(client,MODEL,path)
            self.assertEqual(caught.exception.code,'TASK_NOT_FOUND')
            self.assertEqual(len(client.submits),1)

    def test_legacy_server_fails_capability_before_submit(self):
        client=Client();client.health=lambda:{'Capabilities':[]}
        with tempfile.TemporaryDirectory() as directory,self.assertRaises(BimBridgeError) as caught:
            export_elements(client,MODEL,Path(directory)/'elements')
        self.assertEqual(caught.exception.code,'CAPABILITY_NOT_AVAILABLE');self.assertEqual(client.submits,[])
    def test_reader_rejects_partial_summary_snapshot_change_and_truncation(self):
        for mode in ('error','version','truncated'):
            values=records()
            if mode=='error':
                values.insert(2,{'Type':'error','ElementId':MODEL,'Code':'ELEMENT_NOT_FOUND'})
                values[-1].update(SelectedCount=2,FailedCount=1,Complete=False)
            if mode=='version':values[-1]['ModelVersionUtc']='2026-01-02T00:00:00Z'
            if mode=='truncated':values.pop()
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/'records';path.write_text('\n'.join(json.dumps(r) for r in values))
                with self.subTest(mode=mode),self.assertRaises(BimBridgeError):list(ExportArtifact(path,MODEL,{}).iter_elements())
