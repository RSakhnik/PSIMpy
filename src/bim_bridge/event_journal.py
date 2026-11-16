"""Durable, append-only and secret-filtered JSONL event sink."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import uuid


_FIELDS = frozenset(('SessionId', 'OperationId', 'TaskId', 'ClientRequestId',
    'Operation', 'EventType', 'Status', 'ModelKey', 'ModelVersionUtc',
    'WorkerInstanceId', 'BatchId', 'PageIndex',
    'StartTime', 'EndTime', 'NetworkWallMs', 'TransportWaitMs', 'ConnectWallMs',
    'RetryWaitMs', 'Attempt',
    'RetryCount', 'Success', 'ErrorCode', 'HttpStatus', 'Retryable',
    'PayloadBytes', 'ResultBytes', 'ElementCount', 'RecoveryReason', 'RecoveryStrategy',
    'ReprocessedElements', 'LostWork'))


class EventJournal:
    """Append one fsynced JSON record per event, including failed attempts."""

    def __init__(self, path, *, session_id=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id or str(uuid.uuid4())
        self._lock = threading.Lock()

    def __call__(self, event):
        record = {key: value for key, value in event.items() if key in _FIELDS}
        record.setdefault('SessionId', self.session_id)
        record['RecordedAtUtc'] = datetime.now(timezone.utc).isoformat()
        line = (json.dumps(record, ensure_ascii=False, default=str) + '\n').encode('utf-8')
        with self._lock:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(descriptor, line)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
