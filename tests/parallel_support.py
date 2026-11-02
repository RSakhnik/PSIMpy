"""In-process pull workers and protocol chunks; never contacts a live Pilot."""
from collections import Counter
import base64
import hashlib
import io
import json
from pathlib import Path
import queue
import struct
import threading
import time
import types
import uuid

from bim_bridge import BimBridgeError
from bim_bridge._chunk_download import download_chunks
from bim_bridge.binary_spatial import MAGIC, FRAME
from spatial_support import MODEL, PART, V1


def elements(count):
    return [dict(ModelPartId=PART, ElementId=str(uuid.UUID(int=i+1))) for i in range(count)]


def binary(payload, version=None):
    stream=io.BytesIO();stream.write(MAGIC)
    def frame(data, blob=b''):
        data=json.dumps(data).encode();stream.write(FRAME.pack(len(data),len(blob)));stream.write(data);stream.write(blob)
    version=version or payload['ModelVersionUtc']
    full=payload['IncludeMeshes']
    frame(dict(Type='manifest',Format='bim-spatial',FormatVersion=2,ModelVersionUtc=version,IncludeMeshes=full))
    mesh=PART+'/'+MODEL
    for number,element in enumerate(payload['ElementIds']):
        frame(dict(Type='source-begin',Element=element))
        if full:
            if number==0:
                frame(dict(Type='mesh-begin',MeshDefinitionKey=mesh,ModelPartId=PART,MeshId=MODEL))
                data=struct.pack('<ffffff',0,0,0,1,2,3)
                frame(dict(Type='buffer',MeshDefinitionKey=mesh,Field='Vertices',DType='<f4',Offset=0,Count=6,
                           Sha256=hashlib.sha256(data).hexdigest()),data)
                frame(dict(Type='mesh-end',MeshDefinitionKey=mesh,Vertices=6,Normals=0,Indices=0,EdgeIndices=0))
            frame(dict(Type='instance',Element=element,MeshDefinitionKey=mesh,
                       ModelPlacement=[1,0,0,10,0,1,0,20,0,0,1,30,0,0,0,1]))
        frame(dict(Type='source-end',Element=element,State='complete' if full else 'no-geometry'))
    frame(dict(Type='summary',ModelVersionUtc=version,SourceCount=len(payload['ElementIds']),
               InstanceCount=len(payload['ElementIds']) if full else 0,MeshCount=1 if full else 0,FailedCount=0,Complete=True))
    return stream.getvalue()


class Farm:
    def __init__(self, workers=2, *, sdk=.015, upload=.01, download=.005, slow_first=False, slow_worker=False):
        self.count=workers;self.sdk=sdk;self.upload=upload;self.download=download;self.slow_first=slow_first;self.slow_worker=slow_worker
        self.lock=threading.RLock();self.stop=threading.Event();self.queue=queue.Queue();self.jobs={};self.requests={}
        self.threads=[];self.assignments=Counter();self.chunk_reads=Counter();self.downloads=Counter()
        self.active_downloads=0;self.max_downloads=0;self.max_outstanding=0;self.max_pipeline_outstanding=0;self.submit_calls=0
        self.version=V1;self.bad_version=None;self.fail_first=False;self.fail_first_retryable=False;self.expire_once=None;self.expire_status_once=None
        self.interrupt_once=None;self.lost_submit=False;self.lost_all_submits=False;self.status_failure_once=None;self.disappear_once=False;self.requeues=0
        for number in range(workers):
            prepared=queue.Queue();slots=threading.Semaphore(2)
            for target,args in ((self._worker,(number,prepared,slots)),(self._uploader,(prepared,slots))):
                thread=threading.Thread(target=target,args=args,daemon=True);thread.start();self.threads.append(thread)

    def _worker(self, number, prepared, slots):
        while not self.stop.is_set():
            if not slots.acquire(timeout=.02):continue
            try:task_id=self.queue.get(timeout=.02)
            except queue.Empty:slots.release();continue
            job=self.jobs[task_id]
            with self.lock:
                self.assignments[number]+=1
                job.update(State='RUNNING',WorkerInstanceId=f'worker-{number}')
                if self.disappear_once:
                    self.disappear_once=False;self.requeues+=1;job['State']='QUEUED'
                    self.queue.put(task_id);slots.release();return
            delay=self.sdk*(8 if self.slow_first and job['index']==0 else 1)*(3 if number==1 and (self.slow_first or self.slow_worker) else 1)
            if self.stop.wait(delay):slots.release();return
            with self.lock:
                if job['State']=='CANCELLED':slots.release();continue
                if self.expire_status_once==job['index']:
                    self.expire_status_once=None
                    job.update(State='EXPIRED',TaskError=dict(Code='TASK_EXPIRED',Message='expired task'))
                    slots.release();continue
                if (self.fail_first or self.fail_first_retryable) and job['index']==0:
                    job.update(State='FAILED',TaskError=dict(Code='ELEMENT_NOT_FOUND',Message='injected failure',IsRetryable=self.fail_first_retryable))
                    slots.release();continue
                job['data']=binary(job['payload'],self.bad_version)
                job['State']='RESULT_PREPARED'
            prepared.put(job)

    def _uploader(self, prepared, slots):
        while not self.stop.is_set():
            try:job=prepared.get(timeout=.02)
            except queue.Empty:continue
            if self.stop.wait(self.upload):slots.release();return
            with self.lock:
                if job['State']!='CANCELLED':job['State']='COMPLETED'
            slots.release()

    def close(self):
        self.stop.set()
        for thread in self.threads:thread.join(2)

    def client(self):return Client(self)


class Client:
    poll_interval=.003
    def __init__(self, farm):
        self.farm=farm
        self._http=types.SimpleNamespace(base_url='pilot://fake/db',_token='test',max_chunk_bytes=256,rpc=self.rpc)
    def health(self):return {'Capabilities':['artifact-upload-v2','artifact-chunk-download-v2']}
    def workers(self):
        return [dict(WorkerInstanceId=f'worker-{i}',State='READY',Capabilities=['export-spatial','spatial-binary-v2'],
                     AvailableModelKeys=[MODEL]) for i in range(self.farm.count)]
    def get_model_context(self, **options):return {'Models':[dict(ModelKey=MODEL,DataVersionUtc=self.farm.version)]}
    def submit(self, operation, payload, **options):
        f=self.farm
        with f.lock:
            f.submit_calls+=1
            request=options['client_request_id']
            if request in f.requests:
                if f.lost_all_submits:raise BimBridgeError('TRANSPORT_ERROR','lost repeated submit',retryable=True)
                return self.task(f.requests[request])
            task_id=str(uuid.uuid4());f.requests[request]=task_id
            job=dict(TaskId=task_id,payload=payload,options=options,State='QUEUED',index=len(f.jobs))
            f.jobs[task_id]=job;f.queue.put(task_id)
            f.max_outstanding=max(f.max_outstanding,sum(j['State'] in {'QUEUED','RUNNING'} for j in f.jobs.values()))
            f.max_pipeline_outstanding=max(f.max_pipeline_outstanding,sum(j['State'] not in {'COMPLETED','FAILED','CANCELLED'} for j in f.jobs.values()))
            if f.lost_submit or f.lost_all_submits:
                f.lost_submit=False
                raise BimBridgeError('TRANSPORT_ERROR','lost submit',retryable=True)
        return self.task(task_id)
    def task(self, task_id):return FakeTask(self,task_id)
    def rpc(self, method, **args):
        f=self.farm;task_id=args['TaskId'];index=args['ChunkIndex']
        with f.lock:
            job=f.jobs[task_id];f.chunk_reads[task_id,index]+=1
            if f.interrupt_once==job['index'] and index==1:
                f.interrupt_once=None
                raise BimBridgeError('TRANSPORT_ERROR','interrupted chunk',retryable=True)
            data=job['data'];block=data[index*256:(index+1)*256]
        time.sleep(f.download)
        return dict(TaskId=task_id,ChunkIndex=index,Data=base64.b64encode(block).decode(),IsLast=(index+1)*256>=len(data))


class FakeTask:
    def __init__(self, client, task_id):self.client=client;self.id=task_id
    def status(self):
        with self.client.farm.lock:
            farm=self.client.farm;job=farm.jobs[self.id]
            if farm.status_failure_once==job['index']:
                farm.status_failure_once=None
                raise BimBridgeError('TRANSPORT_ERROR','status failed',retryable=True)
            return dict(job)
    def cancel(self):
        with self.client.farm.lock:self.client.farm.jobs[self.id]['State']='CANCELLED'
    def metadata(self):
        f=self.client.farm
        with f.lock:
            job=f.jobs[self.id]
            if f.expire_once==job['index']:
                f.expire_once=None
                raise BimBridgeError('RESULT_EXPIRED','expired artifact')
            data=job['data']
        return dict(TaskId=self.id,TotalLength=len(data),TotalChunks=(len(data)+255)//256,ChunkSizeBytes=256,
                    Sha256=hashlib.sha256(data).hexdigest(),ContentEncoding='identity',ResultKind='spatial-binary-v2')
    def download(self, path, **options):
        f=self.client.farm
        with f.lock:
            f.downloads[self.id]+=1;f.active_downloads+=1;f.max_downloads=max(f.max_downloads,f.active_downloads)
        try:return download_chunks(self.client._http,self.id,path,self.metadata(),**options)
        finally:
            with f.lock:f.active_downloads-=1
