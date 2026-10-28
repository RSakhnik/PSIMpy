import unittest
from bim_bridge.dataframe import search_dataframe
from bim_bridge import BimBridgeError, Condition
from test_dataframe import FakeClient, MODEL, PART

V1 = '2026-10-13T10:00:00Z'

class Client(FakeClient):
    def __init__(self, changed=False):
        self.changed = changed
        self.payloads = []
    def search_elements(self, *args, **kwargs):
        return dict(super().search_elements(*args, **kwargs), ModelVersionUtc=V1)
    def run(self, operation, payload, **kwargs):
        self.payloads.append(payload)
        return dict(super().run(operation, payload, **kwargs), ModelVersionUtc='2026-10-13T11:00:00Z' if self.changed else V1)

class FrameSnapshotTests(unittest.TestCase):
    def test_dataframe_pins_version_from_search(self):
        client = Client()
        frame = search_dataframe(client, MODEL, [Condition('Name')], part_ids=[PART])
        self.assertEqual(len(frame), 2)
        self.assertEqual(client.payloads[0]['ModelVersionUtc'], V1)
        self.assertEqual(frame.attrs['model_version_utc'], V1)
        self.assertTrue(frame.attrs['snapshot_verified'])
    def test_dataframe_rejects_changed_properties(self):
        with self.assertRaises(BimBridgeError) as error:
            search_dataframe(Client(changed=True), MODEL, [Condition('Name')], part_ids=[PART])
        self.assertEqual(error.exception.code, 'MODEL_CHANGED')
