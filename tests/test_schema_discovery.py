import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from bim_bridge import discover_property_schemas

MODEL = str(uuid.UUID(int=1))
PART = str(uuid.UUID(int=2))
VERSION = '2026-01-01T00:00:00Z'


def item(i, name):
    return {'ModelPartId': PART, 'ElementId': str(uuid.UUID(int=i)),
            'Name': name, 'ModelVersionUtc': VERSION}


class DiscoveryTests(unittest.TestCase):
    def test_budget_resume_and_stable_sample_are_distinct(self):
        items = [item(10,'Pipe 1'), item(11,'Pipe 2'), item(12,'Valve 1')]
        def search(*args, **kwargs):
            yield from items
        calls=[]
        def properties(client, model, elements, **options):
            calls.append(elements[0]['ElementId'])
            yield {'Elements':[dict(elements[0], PropertySets=[{
                'Category':'Common', 'Properties':[{'Name':'Mass','Value':1,'Unit':'kg'}]}])],
                'ModelVersionUtc': VERSION}
        with tempfile.TemporaryDirectory() as directory, patch(
                'bim_bridge.schema_discovery.iter_search_complete', side_effect=search), patch(
                'bim_bridge.schema_discovery.iter_properties', side_effect=properties):
            path=Path(directory)/'schemas.json'
            first=discover_property_schemas(object(),MODEL,[PART],path,max_loads=1)
            self.assertEqual(first['Status'],'load_budget')
            self.assertFalse(first['CompleteInventory'])
            second=discover_property_schemas(object(),MODEL,[PART],path,max_loads=3)
            self.assertEqual(second['Status'],'search_exhausted')
            self.assertEqual(second['Coverage']['ObservedGroups'],2)
            self.assertEqual(len(calls),2)
            self.assertFalse(second['CompleteInventory'])


if __name__ == '__main__':
    unittest.main()
