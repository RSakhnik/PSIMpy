import tempfile
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


class ParallelPropertiesTests(unittest.TestCase):
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
