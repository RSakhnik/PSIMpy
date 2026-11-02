"""Cross-language pipeline: real Manager/store/writer/uploader; fake SDK only."""
import json
import os
from pathlib import Path
import queue
import socket
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
