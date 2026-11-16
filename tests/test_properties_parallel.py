import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from bim_bridge import IncompleteResult, iter_properties_parallel

MODEL = str(uuid.UUID(int=1))
PART = str(uuid.UUID(int=2))
VERSION = '2026-01-01T00:00:00Z'


def element(number):
    return {'ModelPartId': PART, 'ElementId': str(uuid.UUID(int=number))}


class Artifact:
    def __init__(self, rows):
        self.rows = rows
    def iter_elements(self, strict=True):
        return iter(self.rows)
    def records(self):
        yield {'Type': 'manifest', 'ModelVersionUtc': VERSION}


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


if __name__ == '__main__':
    unittest.main()
