import base64
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from bim_bridge import BimBridge, BimBridgeError
from bim_bridge._chunk_download import download_chunks
from bim_bridge.pilot import PilotTransport

TASK='11111111-1111-4111-8111-111111111111'


class ChunkSource:
    base_url='pilot://example/database'
    _token='test-user'
    max_chunk_bytes=4*1024*1024
    def __init__(self,data):
        self.data=data;self.calls=[];self.interrupt=None;self.transform=lambda r:r
        self.metadata={'TaskId':TASK,'TotalLength':len(data),'TotalChunks':(len(data)+3)//4,
                       'ChunkSizeBytes':4,'Sha256':hashlib.sha256(data).hexdigest(),'ContentEncoding':'identity'}
    def rpc(self,method,**args):
        assert method=='ReadResultChunk'
        index=args['ChunkIndex'];self.calls.append(index)
        if self.interrupt==index:
            self.interrupt=None
            raise BimBridgeError('PILOT_TRANSPORT_ERROR','injected lost connection',retryable=True)
        return self.transform({'TaskId':TASK,'ChunkIndex':index,'Data':base64.b64encode(self.data[index*4:index*4+4]).decode(),
                               'IsLast':index==self.metadata['TotalChunks']-1})


class ChunkDownloadTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.path=Path(self.directory.name)/'artifact'
        self.source=ChunkSource(b'0123456789')
    def download(self,**options):
        return download_chunks(self.source,TASK,self.path,self.source.metadata,**options)
    def test_restart_reads_first_missing_chunk_and_preserves_destination(self):
        self.path.write_bytes(b'previous')
        self.source.interrupt=1
        with self.assertRaises(BimBridgeError):self.download(overwrite=True)
        self.assertEqual(self.path.read_bytes(),b'previous')
        self.assertEqual(self.path.with_name('artifact.inprogress').read_bytes(),b'0123')
        self.source=ChunkSource(b'0123456789')
        self.download(overwrite=True)
        self.assertEqual(self.source.calls,[1,2])
        self.assertEqual(self.path.read_bytes(),self.source.data)
        self.assertEqual(list(Path(self.directory.name).iterdir()),[self.path])
    def test_changed_artifact_does_not_read_or_append(self):
        self.source.interrupt=1
        with self.assertRaises(BimBridgeError):self.download()
        self.source=ChunkSource(b'abcdefghij')
        with self.assertRaises(BimBridgeError) as caught:self.download()
        self.assertEqual(caught.exception.code,'ARTIFACT_CHANGED');self.assertEqual(self.source.calls,[])
    def test_corrupt_prefix_and_metadata_rejected(self):
        self.source.interrupt=1
        with self.assertRaises(BimBridgeError):self.download()
        self.path.with_name('artifact.inprogress').write_bytes(b'xxxx')
        with self.assertRaises(BimBridgeError) as caught:self.download()
        self.assertEqual(caught.exception.code,'INTEGRITY_ERROR')
        self.source.metadata['TotalChunks']=999
        with self.assertRaises(BimBridgeError) as caught:self.download()
        self.assertEqual(caught.exception.code,'INVALID_RESPONSE')
    def test_bad_chunk_identity_length_and_sha(self):
        for field,value,code in [('TaskId','wrong','INVALID_RESPONSE'),('ChunkIndex',8,'INVALID_RESPONSE'),
                                 ('Data',base64.b64encode(b'x').decode(),'INTEGRITY_ERROR')]:
            with self.subTest(field=field):
                self.source.transform=lambda r,field=field,value=value:dict(r,**{field:value})
                with self.assertRaises(BimBridgeError) as caught:self.download(resume=False)
                self.assertEqual(caught.exception.code,code)
        self.source.transform=lambda r:dict(r,Data=base64.b64encode(b'x'*len(base64.b64decode(r['Data']))).decode())
        with self.assertRaises(BimBridgeError) as caught:self.download()
        self.assertEqual(caught.exception.code,'INTEGRITY_ERROR')
        self.assertFalse(self.path.exists())
    def test_uncommitted_tail_and_empty_result(self):
        self.source.interrupt=1
        with self.assertRaises(BimBridgeError):self.download()
        with self.path.with_name('artifact.inprogress').open('ab') as stream:stream.write(b'uncommitted')
        self.download();self.assertEqual(self.path.read_bytes(),self.source.data)
        self.source=ChunkSource(b'');self.download(overwrite=True)
        self.assertEqual(self.path.read_bytes(),b'');self.assertEqual(self.source.calls,[])
    def test_interrupting_progress_resumes_at_committed_chunk(self):
        def interrupt(*args):raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):self.download(progress=interrupt)
        self.source.calls=[];self.download()
        self.assertEqual(self.source.calls,[1,2])


class PilotAdapterTests(unittest.TestCase):
    def test_submit_maps_json_payload_to_custom_api_without_http(self):
        transport=PilotTransport('https://pilot/database','user','not-real',adapter='unused.dll')
        client=BimBridge.from_pilot('https://pilot/database','user','not-real',adapter='unused.dll')
        with patch.object(client._http,'rpc',return_value={'TaskId':TASK}) as rpc:
            task=client.submit('echo',{'Message':'hello'})
        method=rpc.call_args.args[0];body=rpc.call_args.kwargs['Request']
        self.assertEqual(method,'SubmitTask');self.assertEqual(task.id,TASK)
        self.assertEqual(json.loads(base64.b64decode(body['payload'])),{'Message':'hello'})
        self.assertEqual(body['protocolVersion'],'1.0')
        with patch.object(transport,'rpc',return_value={}) as rpc:
            transport.json('GET',f'/tasks/{TASK}/result/metadata')
        rpc.assert_called_once_with('GetResultMetadata',TaskId=TASK)
    def test_stdio_adapter_uses_no_new_listener_and_can_reconnect(self):
        with tempfile.TemporaryDirectory() as directory:
            helper=Path(directory)/'fake.py'
            helper.write_text('import sys,json\nfor line in sys.stdin:\n r=json.loads(line)\n print(json.dumps({"Connected":True} if r["Method"]=="Connect" else {"State":"READY"}),flush=True)\n')
            transport=PilotTransport('pilot/db','user','not-real',adapter=helper,dotnet=sys.executable)
            try:
                self.assertEqual(transport.json('GET','/health')['State'],'READY')
                transport.close()
                self.assertEqual(transport.json('GET','/health')['State'],'READY')
            finally:transport.close()


if __name__=='__main__':unittest.main()


class TokenHttpChunkCompatibilityTests(unittest.TestCase):
    def test_existing_http_client_resumes_by_protocol_chunks_without_ranges(self):
        from bim_bridge.transport import Transport
        source=ChunkSource(b'0123456789');source.metadata['DownloadProtocol']='token-http-chunks-v2'
        transport=Transport('https://example.invalid/bim-bridge','test-token',retries=0)
        calls=[]
        def json_response(method,path,payload=None):
            self.assertEqual(method,'GET')
            if path==f'/tasks/{TASK}/result/metadata':return source.metadata
            prefix=f'/tasks/{TASK}/result/chunks/'
            self.assertTrue(path.startswith(prefix))
            index=int(path[len(prefix):]);calls.append(index)
            return source.rpc('ReadResultChunk',TaskId=TASK,ChunkIndex=index)
        transport.json=json_response
        def no_full_http_request(*args,**kwargs):raise AssertionError('Range/full download must not be used')
        transport.open=no_full_http_request
        client=BimBridge('https://example.invalid/bim-bridge',token='test-token');client._http=transport
        with tempfile.TemporaryDirectory() as directory:
            destination=Path(directory)/'result'
            source.interrupt=1
            with self.assertRaises(BimBridgeError):
                client.task(TASK).download(destination,resume=True)
            calls.clear();source.interrupt=None
            client.task(TASK).download(destination,resume=True)
            self.assertEqual(calls,[1,2]);self.assertEqual(destination.read_bytes(),source.data)
