import json
import tempfile
import unittest
from pathlib import Path

from bim_bridge import EventJournal, SessionProfile


class SessionProfileTests(unittest.TestCase):
    def test_invalid_body_is_unfinished_operation(self):
        events=[{'SessionId':'s','OperationId':'one','EventType':'http_attempt',
                 'Success':True,'NetworkWallMs':1},
                {'SessionId':'s','OperationId':'one','EventType':'response_validation',
                 'Success':False,'ErrorCode':'INVALID_RESPONSE'}]
        data=SessionProfile(events).data
        self.assertEqual(data['Operations'],1)
        self.assertEqual(data['UnfinishedOperations'],1)
        self.assertEqual(data['ErrorCounts']['INVALID_RESPONSE'],1)
        self.assertIsNone(data['FirstAttemptSuccessRate'])
        recovered=SessionProfile(events+[{'SessionId':'s','OperationId':'one',
            'EventType':'http_attempt','Success':True}]).data
        self.assertEqual(recovered['UnfinishedOperations'],0)
        self.assertEqual(recovered['RecoveredOperations'],1)

    def test_parallel_intervals_and_recovered_error(self):
        events = [
            dict(SessionId='s', OperationId='a', StartTime='2026-01-01T00:00:00Z',
                 EndTime='2026-01-01T00:00:02Z', NetworkWallMs=2000,
                 RetryWaitMs=1000, Success=False, ErrorCode='TRANSPORT_ERROR'),
            dict(SessionId='s', OperationId='b', StartTime='2026-01-01T00:00:01Z',
                 EndTime='2026-01-01T00:00:03Z', NetworkWallMs=2000,
                 RetryWaitMs=0, Success=True, ErrorCode=None),
            dict(SessionId='s', OperationId='a', StartTime='2026-01-01T00:00:03Z',
                 EndTime='2026-01-01T00:00:04Z', NetworkWallMs=1000,
                 RetryWaitMs=0, Success=True, ErrorCode=None)]
        profile = SessionProfile(events).data
        self.assertEqual(profile['WallSeconds'], 4)
        self.assertEqual(profile['NetworkActiveSeconds'], 4)
        self.assertEqual(profile['NetworkRequestSeconds'], 5)
        self.assertEqual(profile['RecoveredOperations'], 1)
        self.assertEqual(profile['ErrorCounts']['TRANSPORT_ERROR'], 1)
        self.assertIsNone(profile['WorkerProcessingSeconds'])

    def test_reads_journal_and_exports_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'events.jsonl'
            sink=EventJournal(path,session_id='session-1')
            sink({'OperationId':'one','Success':True,'Secret':'must-not-appear'})
            profile=SessionProfile.from_jsonl(path)
            target=Path(directory)/'profile.json'
            profile.save_json(target)
            self.assertEqual(json.loads(target.read_text())['SessionId'],'session-1')
            self.assertNotIn('must-not-appear',path.read_text())
            self.assertTrue(any(row['Metric']=='Operations' for row in profile.rows()))


if __name__=='__main__':
    unittest.main()
