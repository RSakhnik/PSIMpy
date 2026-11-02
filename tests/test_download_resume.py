import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bim_bridge import BimBridge, BimBridgeError


class Response:
    def __init__(self, data, status=200, headers=None, interrupt=False):
        self.stream = io.BytesIO(data)
        self.status = status
        self.headers = headers or {}
        self.interrupt = interrupt
        self.reads = 0
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, size):
        self.reads += 1
        if self.interrupt and self.reads == 2:
            raise ConnectionResetError('injected network loss')
        return self.stream.read(min(size, 3))


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.client = BimBridge('https://offline.invalid', token='secret', retries=0)
        self.data = b'0123456789abcdefgh'
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.metadata = {'TotalLength': len(self.data), 'Sha256': self.sha}
        self.http = self.client._http
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name)/'output'
    def download(self, **options):
        return self.http.download('/tasks/example/result/content', self.path, self.metadata, resume=True, **options)
    def interrupt(self):
        with patch.object(self.http, 'open', return_value=Response(self.data, interrupt=True)):
            with self.assertRaises(BimBridgeError) as caught: self.download()
        self.assertEqual(caught.exception.code, 'TRANSPORT_ERROR')
        self.assertEqual(self.path.with_name('output.inprogress').read_bytes(), self.data[:3])
    def test_resume_range_after_restart_and_atomic_destination(self):
        self.path.write_bytes(b'previous destination')
        with patch.object(self.http, 'open', return_value=Response(self.data, interrupt=True)):
            with self.assertRaises(BimBridgeError): self.download(overwrite=True)
        self.assertEqual(self.path.read_bytes(), b'previous destination')
        self.client = BimBridge('https://offline.invalid', token='secret', retries=0)
        self.http = self.client._http
        response = Response(self.data[3:], 206, {'Content-Range': f'bytes 3-{len(self.data)-1}/{len(self.data)}',
                                                'ETag': '"sha256-'+self.sha+'"'})
        with patch.object(self.http, 'open', return_value=response) as request:
            self.download(overwrite=True)
        self.assertEqual(request.call_args.kwargs['headers']['Range'], 'bytes=3-')
        self.assertEqual(self.path.read_bytes(), self.data)
        self.assertEqual(list(Path(self.folder.name).iterdir()), [self.path])
    def test_remote_change_and_local_partial_corruption_are_fatal(self):
        self.interrupt()
        self.metadata['Sha256'] = '0'*64
        with self.assertRaises(BimBridgeError) as caught: self.download()
        self.assertEqual(caught.exception.code, 'ARTIFACT_CHANGED')
        self.metadata['Sha256'] = self.sha
        self.path.with_name('output.inprogress').write_bytes(b'xxx')
        with self.assertRaises(BimBridgeError) as caught: self.download()
        self.assertEqual(caught.exception.code, 'INTEGRITY_ERROR')
    def test_server_ignoring_range_restarts_without_append(self):
        self.interrupt()
        with patch.object(self.http, 'open', return_value=Response(self.data)):
            self.download()
        self.assertEqual(self.path.read_bytes(), self.data)
    def test_wrong_range_and_changed_etag_never_append(self):
        self.interrupt()
        for headers, code in [({'Content-Range': 'bytes 2-17/18'}, 'INVALID_RANGE'),
                              ({'ETag': '"different"'}, 'ARTIFACT_CHANGED')]:
            with patch.object(self.http, 'open', return_value=Response(self.data[3:], 206, headers)):
                with self.assertRaises(BimBridgeError) as caught: self.download()
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.path.with_name('output.inprogress').read_bytes(), self.data[:3])
    def test_sha_mismatch_preserves_previous_destination(self):
        self.path.write_bytes(b'old')
        with patch.object(self.http, 'open', return_value=Response(b'x'*len(self.data))):
            with self.assertRaises(BimBridgeError) as caught: self.download(overwrite=True)
        self.assertEqual(caught.exception.code, 'INTEGRITY_ERROR')
        self.assertEqual(self.path.read_bytes(), b'old')
        self.assertEqual(list(Path(self.folder.name).iterdir()), [self.path])
    def test_uncommitted_tail_is_truncated_before_resume(self):
        self.interrupt()
        with self.path.with_name('output.inprogress').open('ab') as stream: stream.write(b'crash tail')
        with patch.object(self.http, 'open', return_value=Response(self.data[3:],206,{'Content-Range':'bytes 3-17/18'})):
            self.download()
        self.assertEqual(self.path.read_bytes(),self.data)


if __name__ == '__main__': unittest.main()
