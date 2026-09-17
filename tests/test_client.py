import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from bim_bridge import BimBridge, Condition, Element, FileReference, BimBridgeError, TaskFailed, TaskTimeout

ID='11111111-1111-4111-8111-111111111111'
PART='22222222-2222-4222-8222-222222222222'
ELEMENT='33333333-3333-4333-8333-333333333333'

class ClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calls=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self): self.respond()
            def do_POST(self): self.respond()
            def do_DELETE(self): self.respond()
            def respond(self):
                body=self.rfile.read(int(self.headers.get('Content-Length',0)))
                cls.calls.append((self.command,self.path,dict(self.headers),json.loads(body) if body else None))
                status,headers,data=cls.handler(self.command,self.path,body)
                if not isinstance(data,bytes):data=json.dumps(data).encode()
                self.send_response(status)
                self.send_header('Content-Length',str(len(data)))
                for k,v in headers.items():self.send_header(k,v)
                self.end_headers();self.wfile.write(data)
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.url=f'http://127.0.0.1:{cls.server.server_port}/bridge'
    @classmethod
    def tearDownClass(cls):cls.server.shutdown();cls.server.server_close();cls.thread.join()
    def setUp(self):
        self.calls.clear()
        type(self).handler=staticmethod(self.default)
        self.client=BimBridge(self.url,token='test-secret',poll_interval=.001,retries=0)
    @staticmethod
    def default(method,path,body):
        if method=='POST':return 202,{}, {'TaskId':ID}
        if path.endswith('/health'):return 200,{}, {'Ready':True}
        if path.endswith('/workers'):return 200,{}, []
        if path.endswith('/result'):return 200,{}, {'Elements':[], 'Truncated':False}
        return 200,{}, {'TaskId':ID,'State':'COMPLETED'}
    def test_health_workers_auth(self):
        self.assertTrue(self.client.health()['Ready']);self.assertEqual(self.client.workers(),[])
        self.assertEqual(self.calls[0][2]['X-Api-Token'],'test-secret')
    def test_bearer_file_token_and_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'key';path.write_text('\ufefftest-secret\n',encoding='utf-8')
            BimBridge(self.url,token_file=path,auth='bearer').health()
            self.assertEqual(self.calls[-1][2]['Authorization'],'Bearer test-secret')
        for url in ['file:///tmp',self.url+'?token=x','https://user:pass@host/']:
            with self.assertRaises(ValueError):BimBridge(url,token='test')
        with self.assertRaises(ValueError):BimBridge(self.url,token='a\nb')
    def test_every_json_operation_payload(self):
        self.client.model_context()
        self.assertEqual(self.calls[0][3]['requiredCapabilities'],['model-context'])
        self.client.search_elements(ID,[Condition('Name','Equal','Pipe')],part_ids=[PART],logical_operator='Or',max_results=4)
        body=[c[3] for c in self.calls if c[0]=='POST'][-1]
        self.assertEqual(body['payload']['TargetModelPartIds'],[PART]);self.assertEqual(body['payload']['LogicalOperator'],'Or')
        self.client.load_properties(ID,[Element(PART,ELEMENT)],property_names=['Name'])
        self.client.get_relations(ID,[Element(PART,ELEMENT)],include_children=False)
        self.client.read_excel(FileReference(ID,PART,'x.xlsx','2026-08-03T15:00:00+03:00'),sheet_names=['TDSheet'],include_formulas=True,include_empty_cells=True)
        body=[c[3] for c in self.calls if c[0]=='POST'][-1]['payload']
        self.assertEqual(body['FileReference']['SnapshotCreatedAtUtc'],'2026-08-03T12:00:00Z')
        self.assertTrue(body['IncludeFormulas']);self.assertTrue(body['IncludeEmptyCells'])
        for op in ['echo','delay','generate-test-result','fail-test']:
            self.client.diagnostic(op,message='test',delay_ms=1,size=1)
            self.assertEqual([c[3] for c in self.calls if c[0]=='POST'][-1]['requiredCapabilities'],['diagnostics'])
    def test_all_submission_options(self):
        task=self.client.submit('custom',{'anything':1},target_model_key=PART,preferred_worker_instance_id='worker',required_capabilities=['custom'],priority=2,can_retry=False,max_attempts=1,expires_at_utc='2027-01-01T00:00:00Z',client_request_id=ELEMENT,protocol_version='v')
        body=self.calls[-1][3]
        self.assertEqual(body['clientRequestId'],ELEMENT);self.assertEqual(task.client_request_id,ELEMENT)
        self.assertEqual(body['preferredWorkerInstanceId'],'worker');self.assertFalse(body['canRetry'])
        self.assertEqual(body['protocolVersion'],'v');self.assertEqual(body['priority'],2)
    def test_cancel_timeout_and_terminal_errors(self):
        type(self).handler=staticmethod(lambda *a:(200,{}, {'State':'RUNNING'}))
        with self.assertRaises(TaskTimeout) as caught:self.client.task(ID).wait(timeout=.003,cancel_on_timeout=True)
        self.assertEqual(caught.exception.task_id,ID);self.assertEqual(self.calls[-1][0],'DELETE')
        for state in ['FAILED','CANCELED','CANCELLED','ABANDONED','EXPIRED']:
            type(self).handler=staticmethod(lambda *a:(200,{}, {'State':state,'TaskError':{'Code':'TEST','Message':'test-secret','IsRetryable':True}}))
            with self.assertRaises(TaskFailed) as caught:self.client.task(ID).wait()
            self.assertTrue(caught.exception.retryable);self.assertNotIn('test-secret',str(caught.exception))
    def test_timeout_does_not_cancel_by_default(self):
        type(self).handler=staticmethod(lambda *a:(200,{}, {'State':'RUNNING'}))
        with self.assertRaises(TaskTimeout):self.client.task(ID).wait(timeout=.001)
        self.assertFalse(any(c[0]=='DELETE' for c in self.calls))
    def test_http_codes_and_post_not_retried(self):
        for status in [400,401,404,409,410,413,429,503]:
            type(self).handler=staticmethod(lambda *a:(status,{}, {'Error':{'Code':'TEST','Message':'test-secret'}}))
            with self.assertRaises(BimBridgeError) as caught:self.client.health()
            self.assertEqual(caught.exception.status_code,status);self.assertNotIn('test-secret',str(caught.exception))
        self.calls.clear();self.client._http.retries=2
        with self.assertRaises(BimBridgeError) as caught:self.client.submit('echo')
        self.assertEqual(len(self.calls),1);self.assertIsNotNone(caught.exception.client_request_id)
    def test_get_retry_and_redirect_block(self):
        type(self).handler=staticmethod(lambda *a:(503,{}, {}) if len(self.calls)==1 else (200,{}, {'Ready':True}))
        self.client._http.retries=1
        with patch('bim_bridge.transport.time.sleep'):
            self.assertTrue(self.client.health()['Ready'])
        self.assertEqual(len(self.calls),2)
        self.calls.clear()
        type(self).handler=staticmethod(lambda *a:(302,{'Location':self.url+'/elsewhere'},b''))
        with self.assertRaises(BimBridgeError):self.client.health()
        self.assertEqual(len(self.calls),1)
    def test_gzip_magic_and_limit(self):
        zipped=gzip.compress(json.dumps({'x':'я'*100}).encode())
        type(self).handler=staticmethod(lambda *a:(200,{},zipped))
        self.assertEqual(self.client.task(ID).result()['x'],'я'*100)
        self.client._http.max_json_bytes=200
        with self.assertRaises(BimBridgeError) as caught:self.client.task(ID).result()
        self.assertEqual(caught.exception.code,'RESULT_TOO_LARGE')
    def test_binary_download_hash_cleanup_and_excel_raw(self):
        raw=gzip.compress(b'{"hello": 1}')
        metadata={'TotalLength':len(raw),'Sha256':hashlib.sha256(raw).hexdigest(),'ContentEncoding':'gzip'}
        def handler(method,path,body):
            if path.endswith('/metadata'):return 200,{},metadata
            if path.endswith('/content'):return 200,{'Content-Encoding':'gzip'},raw
            return self.default(method,path,body)
        type(self).handler=staticmethod(handler)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'out.json';counts=[]
            self.client.task(ID).download(path,decompress=True,progress=lambda n,t:counts.append((n,t)))
            self.assertEqual(path.read_bytes(),b'{"hello": 1}');self.assertEqual(counts[-1],(len(raw),len(raw)))
            with self.assertRaises(FileExistsError):self.client.task(ID).download(path)
            metadata['Sha256']='bad'
            with self.assertRaises(BimBridgeError):self.client.task(ID).download(path,overwrite=True)
            self.assertEqual(path.read_bytes(),b'{"hello": 1}');self.assertEqual(len(list(Path(tmp).iterdir())),1)
            metadata['Sha256']=hashlib.sha256(raw).hexdigest()
            self.client.download_file(FileReference(ID),Path(tmp)/'source.bin')
            self.assertEqual((Path(tmp)/'source.bin').read_bytes(),raw)
    def test_bad_length_and_partial_stream_cleanup(self):
        type(self).handler=staticmethod(lambda *a:(200,{},b'abc'))
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(BimBridgeError):self.client._http.download('/content',Path(tmp)/'file',{'TotalLength':4})
            self.assertEqual(list(Path(tmp).iterdir()),[])
            with self.assertRaises(RuntimeError):
                self.client._http.download('/content',Path(tmp)/'file',{},progress=lambda *a:(_ for _ in ()).throw(RuntimeError()))
            self.assertEqual(list(Path(tmp).iterdir()),[])

if __name__=='__main__':unittest.main()
