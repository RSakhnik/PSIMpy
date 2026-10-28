import importlib
import json
from pathlib import Path
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
from bim_bridge import BimBridgeError, Checkpoints, ExportSnapshot, IncompleteResult, TaskFailed, export_spatial_data
from bim_bridge._spatial_export import _inspect, _lock
from spatial_support import FakeClient, MODEL, V1, V2, elements, result_for


class SpatialFileTests(unittest.TestCase):
    def test_missing_extra_before_consuming_input_or_submitting(self):
        client=FakeClient();consumed=[]
        def source():consumed.append(1);yield elements(1)[0]
        real=importlib.import_module
        def missing(name):
            if name=='ijson':raise ImportError('missing')
            return real(name)
        with tempfile.TemporaryDirectory() as directory,patch('bim_bridge._spatial_export.importlib.import_module',side_effect=missing):
            with self.assertRaises(ImportError):list(export_spatial_data(client,MODEL,source(),directory))
            self.assertFalse(consumed);self.assertFalse(client.submits)
    def test_export_cache_hash_repair_and_snapshot(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            first=list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=2))
            self.assertEqual(len(first),2);self.assertTrue(all(e['Complete'] for e in first))
            self.assertEqual(first[0]['ModelVersionUtc'],V1)
            self.assertEqual(client.submits[1][1]['ModelVersionUtc'],V1)
            again=list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=2))
            self.assertEqual(len(client.submits),2);self.assertEqual(client.downloads,2)
            self.assertEqual(first[0]['Path'],again[0]['Path'])
            first[0]['Path'].write_text('corrupt')
            list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=2))
            self.assertEqual(len(client.submits),2);self.assertEqual(client.downloads,3)
            for path in Path(directory).rglob('*.json'):self.assertNotIn('not-a-real-secret',path.read_text())
            with self.assertRaises(BimBridgeError) as caught:list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=2,model_version_utc=V2))
            self.assertEqual(caught.exception.code,'MODEL_CHANGED')
    def test_resume_interrupted_wait_does_not_submit_new_task(self):
        client=FakeClient();client.wait_error=KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(KeyboardInterrupt):list(export_spatial_data(client,MODEL,elements(1),directory))
            state=json.loads(next(Path(directory).rglob('*.task.json')).read_text())
            self.assertTrue(state['TaskId']);self.assertTrue(state['ClientRequestId'])
            self.assertEqual(len(list(export_spatial_data(client,MODEL,elements(1),directory))),1)
            self.assertEqual(len(client.submits),1)
    def test_resume_uncertain_submit_uses_same_request_identity(self):
        client=FakeClient();client.submit_error=BimBridgeError('TRANSPORT_ERROR','reply lost')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(BimBridgeError):list(export_spatial_data(client,MODEL,elements(1),directory))
            state=json.loads(next(Path(directory).rglob('*.task.json')).read_text());self.assertNotIn('TaskId',state)
            self.assertEqual(len(list(export_spatial_data(client,MODEL,elements(1),directory))),1)
            self.assertEqual(len(client.tasks),1);self.assertEqual(len(client.submits),2)
            self.assertEqual(client.submits[0][2]['client_request_id'],client.submits[1][2]['client_request_id'])
    def test_interrupted_download_cleans_temp_and_resumes_task(self):
        client=FakeClient();client.download_error=KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(KeyboardInterrupt):list(export_spatial_data(client,MODEL,elements(1),directory))
            self.assertFalse(list(Path(directory).glob('*.inprogress')))
            result=list(export_spatial_data(client,MODEL,elements(1),directory));self.assertEqual(len(result),1)
            self.assertEqual(len(client.submits),1)
    def test_partial_files_strict_and_non_strict_are_never_complete_cache(self):
        client=FakeClient();client.state='partial'
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(IncompleteResult):list(export_spatial_data(client,MODEL,elements(1),directory))
            manifest=json.loads((Path(directory)/'manifest.json').read_text())
            self.assertFalse(manifest['Files']);self.assertFalse(list(Path(directory).glob('*.inprogress')))
            entries=list(export_spatial_data(client,MODEL,elements(1),directory,strict=False))
            self.assertFalse(entries[0]['Complete']);self.assertTrue(entries[0]['Path'].exists())
            client.state='no-geometry'
            entries=list(export_spatial_data(client,MODEL,elements(1),directory))
            self.assertTrue(entries[0]['Complete']);self.assertEqual(len(client.submits),3)
    def test_server_limit_splits_and_cached_export_does_not_repeat_parent(self):
        client=FakeClient();client.limit=1
        with tempfile.TemporaryDirectory() as directory:
            files=list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=3))
            self.assertEqual(len(files),3);self.assertEqual(len(client.submits),5)
            self.assertEqual(len(list(export_spatial_data(client,MODEL,elements(3),directory,batch_size=3))),3)
            self.assertEqual(len(client.submits),5)
        client=FakeClient();client.limit=0
        with tempfile.TemporaryDirectory() as directory,self.assertRaises(TaskFailed):list(export_spatial_data(client,MODEL,elements(1),directory))
    def test_model_change_and_integrity_errors_never_split(self):
        client=FakeClient();client.versions=[V1,V2]
        with tempfile.TemporaryDirectory() as directory:
            iterator=export_spatial_data(client,MODEL,elements(3),directory,batch_size=1)
            first=next(iterator)
            with self.assertRaises(BimBridgeError) as caught:next(iterator)
            self.assertEqual(caught.exception.code,'MODEL_CHANGED');self.assertEqual(len(client.submits),2)
            self.assertTrue(first['Path'].exists());self.assertFalse(list(Path(directory).glob('*.inprogress')))
        client=FakeClient();client.download_error=BimBridgeError('INTEGRITY_ERROR','sha mismatch')
        with tempfile.TemporaryDirectory() as directory,self.assertRaises(BimBridgeError):list(export_spatial_data(client,MODEL,elements(4),directory))
        self.assertEqual(len(client.submits),1)
    def test_checkpoint_owner_and_manifest_parameters(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory,tempfile.TemporaryDirectory() as cache:
            cp=Checkpoints(client,cache)
            list(export_spatial_data(client,MODEL,elements(1),directory,checkpoints=cp))
            self.assertTrue(list(cp.directory.rglob('*.task.json')))
            self.assertFalse((Path(directory)/'.checkpoints').exists())
            with self.assertRaises(ValueError):list(export_spatial_data(client,MODEL,elements(1),directory,include_meshes=True))
            with self.assertRaises(ValueError):list(export_spatial_data(FakeClient(),MODEL,elements(1),directory,checkpoints=cp))
    def test_close_releases_export_lock(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            iterator=export_spatial_data(client,MODEL,elements(2),directory,batch_size=1)
            next(iterator)
            with self.assertRaises(BimBridgeError),_lock(Path(directory)):pass
            iterator.close()
            with _lock(Path(directory)):pass
    def test_streaming_scans_large_vertex_array_with_bounded_memory(self):
        parser=importlib.import_module('ijson')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'large.json'
            with path.open('w') as output:
                output.write('{"ModelVersionUtc":"'+V1+'","CoordinateSpace":"sdk-placement","Elements":')
                output.write(json.dumps(result_for(elements(1))['Elements']))
                output.write(',"MeshDefinitions":[{"Vertices":[')
                for _ in range(150):output.write('1.2345,'*1000)
                output.write('0]}]}')
            tracemalloc.start()
            with patch('json.load',side_effect=AssertionError('full load forbidden')):
                info=_inspect(path,elements(1),parser)
            _,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
            self.assertTrue(info['Complete']);self.assertLess(peak,4*1024*1024)
    def test_invalid_json_and_semantically_incomplete_output(self):
        parser=importlib.import_module('ijson')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'test.json'
            for content in ['{','[]','{"ModelVersionUtc":"'+V1+'","Elements":[],"Elements":[]}']:
                path.write_text(content)
                with self.assertRaises((BimBridgeError,IncompleteResult)):_inspect(path,elements(1),parser)
            for field,value in [('FailedElementIds',[elements(1)[0]]),('ElementErrors',[{'Code':'TEST'}]),('Warnings',['warning']),('Truncated',True),('Elements',[])]:
                result=result_for(elements(1));result[field]=value;path.write_text(json.dumps(result))
                self.assertFalse(_inspect(path,elements(1),parser)['Complete'])
    def test_corrupt_manifest_stops_without_new_task(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory)/'manifest.json').write_text('broken')
            with self.assertRaises(BimBridgeError) as caught:list(export_spatial_data(client,MODEL,elements(1),directory))
            self.assertEqual(caught.exception.code,'INTEGRITY_ERROR');self.assertFalse(client.submits)
    def test_cached_metadata_is_rechecked_from_file(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            list(export_spatial_data(client,MODEL,elements(1),directory))
            path=Path(directory)/'manifest.json';manifest=json.loads(path.read_text())
            next(iter(manifest['Files'].values()))['ModelVersionUtc']=V2
            path.write_text(json.dumps(manifest))
            result=list(export_spatial_data(client,MODEL,elements(1),directory))
            self.assertEqual(result[0]['ModelVersionUtc'],V1);self.assertEqual(len(client.submits),1)


if __name__=='__main__':unittest.main()
