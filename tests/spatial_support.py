import copy
import json
from pathlib import Path
import types
import uuid
from bim_bridge import BimBridgeError, TaskFailed

MODEL='11111111-1111-4111-8111-111111111111'
PART='22222222-2222-4222-8222-222222222222'
V1='2026-01-01T00:00:00Z'
V2='2026-01-02T00:00:00Z'


def elements(count):
    return [{'ModelPartId':PART,'ElementId':str(uuid.UUID(int=i+1))} for i in range(count)]


def result_for(keys,version=V1,state='no-geometry'):
    return {'ModelKey':MODEL,'ModelVersionUtc':version,'CoordinateSpace':'sdk-placement','SceneCoordinatesVerified':False,
            'Elements':[{'Element':e,'State':state,'Scope':'self','ProcessedNodes':1,'GeometryElementCount':0,
                         'InstanceCount':0,'BoundsMin':None,'BoundsMax':None,'BoundsCenter':None,'Instances':[]} for e in keys],
            'FailedElementIds':[],'ElementErrors':[],'Warnings':[],'Truncated':False}


class FakeClient:
    def __init__(self):
        self._http=types.SimpleNamespace(base_url='https://offline.invalid/bridge',_token='not-a-real-secret')
        self.calls=[];self.submits=[];self.tasks={};self.requests={};self.downloads=0
        self.limit=100;self.versions=[];self.state='no-geometry';self.reply=None
        self.error=None;self.wait_error=None;self.download_error=None;self.submit_error=None
        self.transform=lambda r:r
    def response(self,payload):
        if self.error:raise self.error
        if len(payload['ElementIds'])>self.limit:raise TaskFailed('RESULT_TOO_LARGE','worker size limit')
        version=self.versions.pop(0) if self.versions else payload.get('ModelVersionUtc',V1)
        result=copy.deepcopy(self.reply) if self.reply is not None else result_for(payload['ElementIds'],version,self.state)
        return self.transform(result)
    def run(self,operation,payload,**options):
        self.calls.append((operation,copy.deepcopy(payload),options))
        return self.response(payload)
    def submit(self,operation,payload,**options):
        self.submits.append((operation,copy.deepcopy(payload),options))
        request=options.get('client_request_id',str(uuid.uuid4()))
        if request in self.requests:return self.task(self.requests[request])
        task_id=str(uuid.uuid4());self.requests[request]=task_id
        try:self.tasks[task_id]=self.response(payload)
        except BimBridgeError as error:self.tasks[task_id]=error
        if self.submit_error:
            error=self.submit_error;self.submit_error=None;raise error
        return self.task(task_id)
    def task(self,task_id):
        client=self
        class FakeTask:
            id=task_id
            def wait(self,**options):
                if client.wait_error:
                    error=client.wait_error;client.wait_error=None;raise error
                if task_id not in client.tasks:raise BimBridgeError('TASK_NOT_FOUND','expired',status_code=404)
                if isinstance(client.tasks[task_id],Exception):raise client.tasks[task_id]
            def result(self):return copy.deepcopy(client.tasks[task_id])
            def download(self,path,**options):
                client.downloads+=1
                if client.download_error:
                    Path(path).write_text('incomplete',encoding='utf-8')
                    error=client.download_error;client.download_error=None;raise error
                Path(path).write_text(json.dumps(client.tasks[task_id]),encoding='utf-8')
                return Path(path)
        return FakeTask()
