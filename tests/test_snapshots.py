import json
import tempfile
import unittest
import warnings
from bim_bridge import ExportSnapshot, Checkpoints, BimBridgeError, IncompleteResult
from bim_bridge import iter_search_elements, iter_properties, iter_relations
from test_bulk import FakeClient, MODEL, PART

V1 = '2026-10-13T10:00:00Z'
V2 = '2026-10-13T11:00:00Z'

class VersionedClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.version = V1
    def run(self, operation, payload, **options):
        result = super().run(operation, payload, **options)
        return dict(result, ModelVersionUtc=self.version)

class SnapshotTests(unittest.TestCase):
    def test_pilot_tick_precision_survives_roundtrip(self):
        from bim_bridge.types import utc
        stamp = '2026-08-27T14:15:13.8343723Z'
        self.assertEqual(utc(stamp), stamp)
        self.assertEqual(utc('2026-08-27T17:15:13.8343723+03:00'), stamp)
        snapshot = ExportSnapshot(stamp)
        self.assertEqual(snapshot.payload({})['ModelVersionUtc'], stamp)
        snapshot.accept({'ModelVersionUtc': stamp})
        with self.assertRaises(BimBridgeError):
            snapshot.accept({'ModelVersionUtc': '2026-08-27T14:15:13.834372Z'})
    def test_search_pins_all_partitions_and_properties(self):
        client = VersionedClient()
        snapshot = ExportSnapshot()
        elements = list(iter_search_elements(client, MODEL, [PART], max_results=3, snapshot=snapshot))
        self.assertEqual(len(elements), 24)
        self.assertNotIn('ModelVersionUtc', client.calls[0][1])
        for _, payload in client.calls[1:]:
            self.assertEqual(payload['ModelVersionUtc'], V1)
        self.assertTrue(all(e['SnapshotVerified'] for e in elements))
        list(iter_properties(client, MODEL, elements, batch_size=5))
        for op, payload in client.calls:
            if op == 'load-element-properties':
                self.assertEqual(payload['ModelVersionUtc'], V1)
                self.assertNotIn('ModelVersionUtc', payload['ElementIds'][0])

    def test_changed_snapshot_stops_search_and_batch_reads(self):
        client = VersionedClient()
        elements = list(iter_search_elements(client, MODEL, [PART]))
        client.version = V2
        with self.assertRaises(BimBridgeError) as error:
            list(iter_properties(client, MODEL, elements))
        self.assertEqual(error.exception.code, 'MODEL_CHANGED')
        with self.assertRaises(BimBridgeError) as error:
            list(iter_relations(client, MODEL, elements, snapshot=ExportSnapshot(V1)))
        self.assertEqual(error.exception.code, 'MODEL_CHANGED')

    def test_legacy_worker_is_explicitly_unverified(self):
        client = FakeClient()
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            elements = list(iter_search_elements(client, MODEL, [PART]))
        self.assertEqual(len(captured), 1)
        self.assertTrue(all(not e['SnapshotVerified'] for e in elements))

    def test_missing_version_cannot_confirm_a_pinned_request(self):
        with self.assertRaises(IncompleteResult):
            list(iter_properties(FakeClient(), MODEL, [{'ModelPartId': PART, 'ElementId': MODEL}], snapshot=ExportSnapshot(V1)))

    def test_element_errors_are_fatal_but_empty_properties_are_valid(self):
        client = VersionedClient()
        elements = [{'ModelPartId': PART, 'ElementId': MODEL}]
        self.assertEqual(len(list(iter_properties(client, MODEL, elements))), 1)
        client.run = lambda *a, **kw: {'ModelVersionUtc': V1, 'Elements': [], 'ElementErrors': [{'Code': 'ELEMENT_NOT_FOUND'}]}
        with self.assertRaises(IncompleteResult):
            list(iter_properties(client, MODEL, elements))

    def test_checkpoints_do_not_reuse_unpinned_results_or_other_versions(self):
        client = VersionedClient()
        with tempfile.TemporaryDirectory() as directory:
            cp = Checkpoints(client, directory)
            cp.run('search-elements', {'Conditions': [], 'MaxResults': 1})
            first_posts = client.posts
            client.version = V2
            r = cp.run('search-elements', {'Conditions': [], 'MaxResults': 1})
            self.assertEqual(client.posts, first_posts + 1)
            self.assertEqual(r['ModelVersionUtc'], V2)
            cp.run('search-elements', {'Conditions': [], 'MaxResults': 1, 'ModelVersionUtc': V2})
            posts = client.posts
            cp.run('search-elements', {'Conditions': [], 'MaxResults': 1, 'ModelVersionUtc': V2})
            self.assertEqual(client.posts, posts)
            self.assertGreaterEqual(len(list(cp.directory.glob('*.result.json'))), 2)

    def test_different_versions_in_one_input_batch_are_rejected(self):
        elements = [dict(ModelPartId=PART, ElementId=MODEL, ModelVersionUtc=v) for v in [V1, V2]]
        with self.assertRaises(BimBridgeError) as error:
            list(iter_properties(VersionedClient(), MODEL, elements))
        self.assertEqual(error.exception.code, 'MODEL_CHANGED')

if __name__ == '__main__':
    unittest.main()
