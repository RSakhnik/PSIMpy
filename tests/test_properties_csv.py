import csv
import tempfile
import unittest
import uuid
from pathlib import Path

from bim_bridge import IncompleteResult, write_properties_csv

PART = str(uuid.UUID(int=1))
ELEMENT = str(uuid.UUID(int=2))
VERSION = '2026-01-01T00:00:00Z'


def batch():
    return {'ModelVersionUtc': VERSION, 'SnapshotVerified': True,
            'Elements': [{'ModelPartId': PART, 'ElementId': ELEMENT,
                          'PropertySets': [{'Category': 'Common', 'Properties': [
                              {'Name': 'Mass', 'Value': 12, 'Unit': 'kg'}]}]}]}


class CsvTests(unittest.TestCase):
    def test_streams_explicit_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'properties.csv'
            write_properties_csv(iter([batch()]),path)
            with path.open(encoding='utf-8',newline='') as stream:
                rows=list(csv.DictReader(stream))
            self.assertEqual(len(rows),1)
            self.assertEqual(rows[0]['ModelPartId'],PART)
            self.assertEqual(rows[0]['Category'],'Common')
            self.assertEqual(rows[0]['Unit'],'kg')
            self.assertEqual(rows[0]['Value'],'12')

    def test_failure_does_not_publish_partial_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'properties.csv'
            incomplete=dict(batch(),Warnings=['missing'])
            with self.assertRaises(IncompleteResult):
                write_properties_csv(iter([batch(),incomplete]),path)
            self.assertFalse(path.exists())

    def test_element_without_properties_keeps_identifiers(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'properties.csv'
            empty=dict(batch(),Elements=[{'ModelPartId':PART,'ElementId':ELEMENT,'PropertySets':[]}])
            write_properties_csv([empty],path)
            with path.open(encoding='utf-8',newline='') as stream:
                rows=list(csv.DictReader(stream))
            self.assertEqual(rows[0]['ElementId'],ELEMENT)
            self.assertEqual(rows[0]['PropertyName'],'')


if __name__=='__main__':
    unittest.main()
