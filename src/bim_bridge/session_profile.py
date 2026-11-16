"""Aggregate measured REST journal intervals without double-counting overlap."""
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path


def _seconds(stamp):
    return datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()


def _union(intervals):
    total = 0.0
    end = None
    for start, stop in sorted(intervals):
        if end is None or start > end:
            total += stop - start
            end = stop
        elif stop > end:
            total += stop - end
            end = stop
    return total


class SessionProfile:
    """REST attempt profile; missing worker measurements remain None."""

    def __init__(self, events, *, session_id=None):
        events = list(events)
        if session_id is None and events:
            session_id = events[-1].get('SessionId')
        self.session_id = session_id
        self.events = [e for e in events if e.get('SessionId') == session_id]
        self.data = self._build()

    @classmethod
    def from_jsonl(cls, path, *, session_id=None):
        with Path(path).open(encoding='utf-8') as stream:
            return cls((json.loads(line) for line in stream if line.strip()), session_id=session_id)

    def _build(self):
        timed = []
        for event in self.events:
            if event.get('StartTime') and event.get('EndTime'):
                start, end = _seconds(event['StartTime']), _seconds(event['EndTime'])
                if end >= start:
                    timed.append((start, end))
        operations = defaultdict(list)
        for event in self.events:
            key = event.get('ClientRequestId') or event.get('OperationId')
            if key:
                operations[key].append(event)
        failures = Counter(e.get('ErrorCode') or 'UNKNOWN' for e in self.events if e.get('Success') is False)
        def succeeded(items):
            outcomes = [e['Success'] for e in items if e.get('Success') is not None]
            return bool(outcomes) and outcomes[-1] is True
        completed = [items for items in operations.values()
                     if any(e.get('Success') for e in items) and succeeded(items)]
        recovered = [items for items in completed if any(e.get('Success') is False for e in items)]
        first_try = [items for items in completed if items[0].get('Success') is True]
        unfinished = [items for items in operations.values()
                      if not any(e.get('Success') for e in items) or not succeeded(items)]
        wall = max(end for _, end in timed) - min(start for start, _ in timed) if timed else None
        elements = sum(e.get('ElementCount', 0) for e in self.events if e.get('Success') and
                       isinstance(e.get('ElementCount'), (int, float)))
        has_elements = any(e.get('ElementCount') is not None for e in self.events)
        return {'SessionId': self.session_id, 'WallSeconds': wall,
                'NetworkActiveSeconds': _union(timed) if timed else None,
                'NetworkRequestSeconds': sum(e['NetworkWallMs'] for e in self.events
                    if isinstance(e.get('NetworkWallMs'), (int, float))) / 1000
                    if any(e.get('NetworkWallMs') is not None for e in self.events) else None,
                'RetryWaitSumSeconds': sum(e.get('RetryWaitMs', 0) for e in self.events) / 1000
                    if any(e.get('RetryWaitMs') is not None for e in self.events) else None,
                'ErrorCounts': dict(failures), 'Operations': len(operations),
                'FirstAttemptSuccessRate': len(first_try) / len(completed) if completed else None,
                'RecoveredOperations': len(recovered), 'UnfinishedOperations': len(unfinished),
                'EndToEndElementsPerMinute': elements * 60 / wall if has_elements and wall and wall > 0 else None,
                'WorkerProcessingSeconds': None, 'QueueWaitSeconds': None,
                'RecoveryWaitSeconds': None, 'ReprocessedElements': None,
                'MeasurementNotes': {'WallSeconds': 'measured from event timestamps',
                    'NetworkActiveSeconds': 'union of overlapping HTTP attempt intervals',
                    'NetworkRequestSeconds': 'sum of per-attempt monotonic durations; may overlap',
                    'RetryWaitSumSeconds': 'sum of scheduled delays; may overlap'}}

    def save_json(self, path):
        Path(path).write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding='utf-8')
        return Path(path)

    def rows(self):
        return [{'Metric': key, 'Value': value} for key, value in self.data.items()
                if key != 'MeasurementNotes']
