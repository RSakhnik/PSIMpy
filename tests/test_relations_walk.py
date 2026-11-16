import tempfile
import unittest
import uuid
from pathlib import Path

from bim_bridge import BimBridgeError, IncompleteResult, walk_relations

MODEL = str(uuid.UUID(int=1))
PART = str(uuid.UUID(int=2))
VERSION = '2026-01-01T00:00:00Z'


def item(number):
    return {'ModelPartId': PART, 'ElementId': str(uuid.UUID(int=number))}


class RelationsClient:
    def __init__(self):
        self.calls = []

    def run(self, operation, payload, **options):
        self.calls.append(payload)
        rows = []
        for element in payload['ElementIds']:
            number = uuid.UUID(element['ElementId']).int
            rows.append({'Element': element,
                         'Children': [item(2)] if number == 1 else
                                     [item(3)] if number == 2 else [item(1)],
                         'Parent': item(1) if number == 2 else None})
        return {'Elements': rows, 'ModelVersionUtc': VERSION, 'Truncated': False}


class WalkTests(unittest.TestCase):
    def test_cycle_and_resume_after_interruption(self):
        client = RelationsClient()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'walk.json'
            iterator = walk_relations(client, MODEL, [item(1)], state_path=path)
            self.assertEqual(next(iterator)['Element']['ElementId'], item(1)['ElementId'])
            iterator.close()
            rows = list(walk_relations(client, MODEL, [item(1)], state_path=path))
            self.assertEqual([r['Element']['ElementId'] for r in rows],
                             [item(i)['ElementId'] for i in (1, 2, 3)])
            self.assertEqual(list(walk_relations(client, MODEL, [item(1)], state_path=path)), [])

    def test_input_mismatch_and_predicate(self):
        client = RelationsClient()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'walk.json'
            rows = list(walk_relations(client, MODEL, [item(1)], state_path=path,
                                       predicate=lambda row: row['Element']['ElementId'] != item(2)['ElementId']))
            self.assertEqual(len(rows), 2)
            with self.assertRaises(ValueError):
                list(walk_relations(client, MODEL, [item(2)], state_path=path))

    def test_changed_model_version_stops_resume(self):
        client = RelationsClient()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'walk.json'
            iterator = walk_relations(client, MODEL, [item(1)], state_path=path)
            next(iterator)
            next(iterator)
            iterator.close()
            original = client.run
            def changed(*args, **kwargs):
                response = original(*args, **kwargs)
                response['ModelVersionUtc'] = '2026-01-02T00:00:00Z'
                return response
            client.run = changed
            with self.assertRaises(BimBridgeError) as caught:
                list(walk_relations(client, MODEL, [item(1)], state_path=path))
            self.assertEqual(caught.exception.code, 'MODEL_CHANGED')

    def test_duplicate_seeds_and_budgeted_resume(self):
        client = RelationsClient()
        seeds = [item(1), item(1)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'walk.json'
            with self.assertRaises(IncompleteResult):
                list(walk_relations(client, MODEL, seeds, state_path=path,
                                    batch_size=3, max_elements=1))
            self.assertEqual([len(call['ElementIds']) for call in client.calls], [1])
            with self.assertRaises(IncompleteResult):
                list(walk_relations(client, MODEL, seeds, state_path=path,
                                    batch_size=3, max_elements=1))
            self.assertEqual(len(client.calls), 1)
            rows = list(walk_relations(client, MODEL, seeds, state_path=path,
                                       batch_size=3, max_elements=3))
            self.assertEqual([row['Element']['ElementId'] for row in rows],
                             [item(i)['ElementId'] for i in (2, 3)])
            self.assertEqual([len(call['ElementIds']) for call in client.calls], [1, 1, 1])


if __name__ == '__main__':
    unittest.main()
