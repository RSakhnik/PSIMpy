import gzip
import hashlib
import json
import tempfile
import threading
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from bim_bridge import BimBridgeError, IncompleteResult, iter_properties_parallel

MODEL = str(uuid.UUID(int=1))
PART = str(uuid.UUID(int=2))
VERSION = '2026-01-01T00:00:00Z'


def element(number):
    return {'ModelPartId': PART, 'ElementId': str(uuid.UUID(int=number))}


class Artifact:
    def __init__(self, rows, version=VERSION):
        self.rows = rows
        self.version = version
    def iter_elements(self, strict=True):
        return iter(self.rows)
    def records(self):
        yield {'Type': 'manifest', 'ModelVersionUtc': self.version}


class FakeManager:
    def __init__(self):
        self._http=types.SimpleNamespace(base_url='pilot://fake',_token='fake')
        self.jobs={};self.requests={};self.submits=0;self.fail_once=True
        self.lock=threading.Lock()

    def _parallel_fork(self):
        return self

    def supports(self,*args,**kwargs):
        return True

    def health(self):
        return {'Capabilities':['artifact-upload-v2']}

    def submit(self,operation,payload,**options):
        request_id=options['client_request_id']
        with self.lock:
            if request_id not in self.requests:
                task_id=str(uuid.uuid4())
                self.requests[request_id]=task_id
                self.jobs[task_id]=payload
                self.submits+=1
            return self.task(self.requests[request_id])

    def task(self,task_id):
        manager=self
        class Task:
            id=task_id
            def wait(self,**options):
                if (manager.fail_once and
                        manager.jobs[task_id]['ElementIds'][0]['ElementId']==element(2)['ElementId']):
                    manager.fail_once=False
                    raise BimBridgeError('TRANSPORT_ERROR','interrupted',retryable=True)
                return {'State':'COMPLETED'}
            def metadata(self):
                data=self._data()
                return {'TotalLength':len(data),'Sha256':hashlib.sha256(data).hexdigest()}
            def _data(self):
                rows=[{'Type':'manifest','Format':'bim-elements','FormatVersion':1,
                       'ModelVersionUtc':VERSION}]
                rows.extend(dict(Type='element',**item,PropertySets=[]) for item in
                            manager.jobs[task_id]['ElementIds'])
                rows.append({'Type':'summary','ModelVersionUtc':VERSION,
                             'SelectedCount':len(rows)-1,'ExportedCount':len(rows)-1,
                             'FailedCount':0,'Complete':True})
                return gzip.compress(('\n'.join(json.dumps(row) for row in rows)+'\n').encode(),mtime=0)
            def download(self,destination,**options):
                Path(destination).write_bytes(self._data())
        return Task()


class ParallelPropertiesTests(unittest.TestCase):
    def test_fake_manager_resumes_same_task_and_reuses_artifacts(self):
        client=FakeManager()
        with tempfile.TemporaryDirectory() as directory:
            args=(client,MODEL,[element(1),element(2),element(3)],directory)
            first=list(iter_properties_parallel(*args,batch_size=1,concurrency=2,
                                                model_version_utc=VERSION))
            self.assertEqual(sum(len(batch['Elements']) for batch in first),3)
            self.assertEqual(client.submits,3)
            second=list(iter_properties_parallel(*args,batch_size=1,concurrency=2,
                                                 model_version_utc=VERSION))
            self.assertEqual(len(second),3)
            self.assertEqual(client.submits,3)
    def test_batches_selection_and_progress(self):
        calls = []
        def export(client, model, destination, **options):
            calls.append(options)
            return Artifact(options['element_ids'])
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', side_effect=export):
            progress = []
            batches = list(iter_properties_parallel(object(), MODEL,
                [element(i) for i in range(1, 6)], directory, batch_size=2,
                concurrency=2, model_version_utc=VERSION,
                progress=lambda *args: progress.append(args)))
            self.assertEqual(sum(len(b['Elements']) for b in batches), 5)
            self.assertEqual(len(calls), 3)
            self.assertEqual(len(progress), 3)
            self.assertTrue(all(c['model_version_utc'] == VERSION for c in calls))

    def test_rejects_incomplete_and_incompatible_input(self):
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', return_value=Artifact([])):
            with self.assertRaises(IncompleteResult):
                list(iter_properties_parallel(object(), MODEL, [element(1)], directory,
                                              model_version_utc=VERSION))
            with self.assertRaises(ValueError):
                list(iter_properties_parallel(object(), MODEL, [element(2)], directory,
                                              model_version_utc=VERSION))

    def test_retries_only_invalid_batch(self):
        calls=[]
        def export(client, model, destination, **options):
            name=Path(destination).name
            calls.append(name)
            if name.startswith('batch-00000000-attempt-0'):
                return Artifact([])
            return Artifact(options['element_ids'])
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', side_effect=export):
            rows=list(iter_properties_parallel(object(),MODEL,[element(1),element(2)],directory,
                                               batch_size=1,concurrency=1,model_version_utc=VERSION))
            self.assertEqual(len(rows),2)
            self.assertEqual(calls.count('batch-00000000-attempt-1.ndjson.gz'),1)
            self.assertNotIn('batch-00000001-attempt-1.ndjson.gz',calls)

    def test_model_change_is_terminal(self):
        calls=[]
        def export(*args, **options):
            calls.append(1)
            return Artifact(options['element_ids'], '2026-01-02T00:00:00Z')
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', side_effect=export):
            with self.assertRaises(BimBridgeError) as caught:
                list(iter_properties_parallel(object(),MODEL,[element(1)],directory,
                                              model_version_utc=VERSION))
            self.assertEqual(caught.exception.code,'MODEL_CHANGED')
            self.assertEqual(len(calls),1)
    def test_transport_resume_reuses_same_artifact_path(self):
        paths=[]
        def export(client, model, destination, **options):
            paths.append(destination)
            if len(paths)==1:
                raise BimBridgeError('TRANSPORT_ERROR','lost response',retryable=True)
            return Artifact(options['element_ids'])
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', side_effect=export):
            rows=list(iter_properties_parallel(object(),MODEL,[element(1)],directory,
                                               model_version_utc=VERSION))
            self.assertEqual(len(rows),1)
            self.assertEqual(paths,[paths[0],paths[0]])
    def test_restart_reuses_verified_batch_after_failure(self):
        submissions={0:0,1:0}
        fail=[True]
        def export(client, model, destination, **options):
            index=int(Path(destination).name.split('-')[1])
            if not Path(destination).exists():
                submissions[index]+=1
                if index==1 and fail[0]:
                    fail[0]=False
                    raise BimBridgeError('WORKER_UNAVAILABLE','worker offline')
                Path(destination).write_text('verified')
            return Artifact(options['element_ids'])
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.properties_parallel.export_elements', side_effect=export):
            args=(object(),MODEL,[element(1),element(2)],directory)
            with self.assertRaises(BimBridgeError):
                list(iter_properties_parallel(*args,batch_size=1,concurrency=1,
                                              model_version_utc=VERSION))
            rows=list(iter_properties_parallel(*args,batch_size=1,concurrency=1,
                                               model_version_utc=VERSION))
            self.assertEqual(len(rows),2)
            self.assertEqual(submissions,{0:1,1:2})


if __name__ == '__main__':
    unittest.main()
