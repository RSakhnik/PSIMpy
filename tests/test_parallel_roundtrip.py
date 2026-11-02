"""Cross-language pipeline: real Manager/store/writer/uploader; fake SDK only."""
import json
import os
from pathlib import Path
import queue
import socket
import sys
import subprocess
import tempfile
import threading
import unittest
from bim_bridge import BimBridge
from parallel_support import elements
from spatial_support import MODEL, V1


@unittest.skipUnless(os.environ.get('BIM_BRIDGE_TEST_HARNESS'), 'Build .NET protocol harness first')
class ParallelRoundTripTests(unittest.TestCase):
    def line(self, process):
        response=queue.Queue()
        threading.Thread(target=lambda:response.put(process.stdout.readline()),daemon=True).start()
        text=response.get(timeout=20)
        if not text:
            raise AssertionError('Harness exited: '+process.stderr.read())
        return json.loads(text)

    def test_real_manager_worker_pipeline_and_python_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            environment=dict(os.environ,BIM_BRIDGE_HARNESS_ROOT=directory)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
            prefix=f'http://127.0.0.1:{port}/bim-bridge/'
            process=subprocess.Popen(['dotnet',os.environ['BIM_BRIDGE_TEST_HARNESS'],'--parallel',prefix,'2'],
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            client=BimBridge(prefix,token='parallel-test-only-token',poll_interval=.005,http_timeout=10,retries=0)
            try:
                self.assertTrue(self.line(process)['Ready'])
                for meshes in (False,True):
                    with client.export_spatial_parallel(MODEL,Path(directory)/str(meshes),element_ids=elements(20),
                            model_version_utc=V1,include_meshes=meshes,include_normals=meshes,shard_size=1,
                            task_prefetch=6,download_workers=2) as export:
                        export.wait(30)
                        sources=list(export.iter_sources(completion_order=False))
                        self.assertEqual([s['Source'] for s in sources],elements(20))
                        for artifact in export.artifacts():
                            info=artifact.spatial().validate()
                            self.assertEqual(info['Summary']['SourceCount'],1)
                            if meshes:
                                self.assertEqual(list(artifact.spatial().iter_placed_vertices()),
                                                 [(10.,20.,30.),(11.,20.,30.),(10.,21.,30.)])
                        self.assertTrue(all(s['WorkerInstanceId'] for s in export.manifest['Shards']))
                        self.assertGreater(export.summary['WorkerProcessingElementsPerMin'],0)
                        self.assertIsNone(export.summary['SDKActiveElementsPerMin'])
                process.stdin.write('metrics\n');process.stdin.flush();metrics=self.line(process)
                self.assertFalse(metrics['Errors'],metrics['Errors'])
                self.assertEqual(metrics['Manager']['CompletedTasks'],40)
                self.assertEqual(client.statistics()['AssignmentsTotal'],40)
                self.assertEqual(metrics['Manager']['AssignmentsTotal'],40)
                self.assertEqual(metrics['Manager']['ActiveUploads'],0)
                self.assertEqual(metrics['Manager']['ActiveDownloads'],0)
                self.assertGreater(metrics['Manager']['DownloadedBytes'],0)
                self.assertGreater(metrics['Manager']['ManagerShaDurationMs'],0)
                counts=metrics['Manager']['AssignmentsByWorker']
                self.assertGreater(counts['fake-machine:user:durable-0'],counts['fake-machine:user:durable-1'])
                self.assertTrue(all(w['MaxSdkConcurrency']==1 for w in metrics['Workers'].values()))
                self.assertTrue(all(w['MaxPreparedDepth']<=2 for w in metrics['Workers'].values()))
                self.assertGreater(sum(w['UploadOverlapStarts'] for w in metrics['Workers'].values()),0)
            finally:
                client.close()
                if process.poll() is None:
                    try:process.stdin.write('stop\n');process.stdin.flush();process.wait(10)
                    except (OSError,subprocess.TimeoutExpired):process.kill();process.wait(5)
                process.stdin.close();process.stdout.close();process.stderr.close()

    def test_python_process_crash_and_restart_resume_same_remote_tasks_and_ready_files(self):
        with tempfile.TemporaryDirectory() as directory:
            environment=dict(os.environ,BIM_BRIDGE_HARNESS_ROOT=directory)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
            prefix=f'http://127.0.0.1:{port}/bim-bridge/'
            host=subprocess.Popen(['dotnet',os.environ['BIM_BRIDGE_TEST_HARNESS'],'--parallel',prefix,'2'],
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            first=None;client=BimBridge(prefix,token='parallel-test-only-token',poll_interval=.005,http_timeout=10,retries=0)
            destination=Path(directory)/'process-resume'
            environment.update(PARALLEL_TEST_URL=prefix,PARALLEL_TEST_DESTINATION=str(destination))
            setup="""import os,json,time
from bim_bridge import BimBridge
from parallel_support import elements
from spatial_support import MODEL,V1
client=BimBridge(os.environ['PARALLEL_TEST_URL'],token='parallel-test-only-token',poll_interval=.005,http_timeout=10,retries=0)
"""
            try:
                self.assertTrue(self.line(host)['Ready'])
                first=subprocess.Popen([sys.executable,'-u','-c',setup+"""
import bim_bridge._chunk_download as chunks
save_state=chunks._state
def crash_window(path,value):
 if 'shard-000002.' in path.name and value['Downloaded']==0:
  while globals().get('export') is None or export.manifest['CompletedShards']<2:time.sleep(.005)
  print(json.dumps({'Checkpoint':True,'EmptyJournalWindow':True}),flush=True)
  while True:time.sleep(1)
 return save_state(path,value)
chunks._state=crash_window
export=client.export_spatial_parallel(MODEL,os.environ['PARALLEL_TEST_DESTINATION'],element_ids=elements(40),model_version_utc=V1,shard_size=2,task_prefetch=4)
while True:time.sleep(1)
"""],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                checkpoint=self.line(first)
                self.assertTrue(checkpoint['Checkpoint']);self.assertTrue(checkpoint['EmptyJournalWindow'])
                first.kill();first.wait(5) # abrupt kernel/process exit; no close()/cleanup
                before=json.loads((destination/'manifest.json').read_text(encoding='utf-8'))
                ready={s['ShardId']:(s['TaskId'],(destination/s['Destination']).stat().st_mtime_ns)
                       for s in before['Shards'] if s['State'] in {'READY','CONSUMED'}}
                self.assertTrue(ready)
                resumed=subprocess.run([sys.executable,'-u','-c',setup+"""
with client.export_spatial_parallel(MODEL,os.environ['PARALLEL_TEST_DESTINATION'],element_ids=None,shard_size=2,task_prefetch=4) as export:
 export.wait(30)
 print(json.dumps({'Complete':export.manifest['Complete']}),flush=True)
client.close()
"""],capture_output=True,text=True,env=environment,timeout=40,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                diagnostic=json.loads((destination/'manifest.json').read_text(encoding='utf-8'))
                failures=[s for s in diagnostic['Shards'] if s['State']=='FAILED']
                self.assertEqual(resumed.returncode,0,resumed.stdout+resumed.stderr+json.dumps(failures))
                self.assertTrue(json.loads(resumed.stdout)['Complete'])
                after=json.loads((destination/'manifest.json').read_text(encoding='utf-8'))
                self.assertEqual(after['ExportSessionId'],before['ExportSessionId'])
                self.assertEqual(after['CompletedShards'],20)
                self.assertEqual(client.statistics()['AssignmentsTotal'],20)
                for number,(task_id,modified) in ready.items():
                    shard=after['Shards'][number]
                    self.assertEqual(shard['TaskId'],task_id)
                    self.assertEqual((destination/shard['Destination']).stat().st_mtime_ns,modified)
            finally:
                client.close()
                if first is not None:
                    if first.poll() is None:first.kill();first.wait(5)
                    first.stdout.close();first.stderr.close()
                if host.poll() is None:
                    try:host.stdin.write('stop\n');host.stdin.flush();host.wait(10)
                    except (OSError,subprocess.TimeoutExpired):host.kill();host.wait(5)
                host.stdin.close();host.stdout.close();host.stderr.close()
