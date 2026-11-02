import json
import errno
from unittest.mock import patch
from pathlib import Path
import tempfile
import threading
import time
import unittest

from bim_bridge import BimBridgeError, ParallelExport
from parallel_support import Farm, elements
from spatial_support import MODEL, V1


class ParallelTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.farms=[];self.exports=[]
    def tearDown(self):
        for export in self.exports:export.close()
        for farm in self.farms:farm.close()
        self.directory.cleanup()
    def farm(self, *args, **options):
        farm=Farm(*args,**options);self.farms.append(farm);return farm
    def start(self, farm, *, count=9, **options):
        args=dict(element_ids=elements(count),shard_size=1,poll_interval=.003)
        args.update(options)
        export=ParallelExport(farm.client(),MODEL,self.directory.name,**args);self.exports.append(export);return export
    def test_one_worker_many_shards_and_snapshot_pinned_before_submission(self):
        farm=self.farm(1);export=self.start(farm).wait(10)
        self.assertTrue(export.manifest['Complete']);self.assertEqual(len(farm.jobs),9)
        self.assertEqual(export.summary['TaskPrefetch'],2)
        self.assertLessEqual(farm.max_outstanding,2)
        for job in farm.jobs.values():
            self.assertEqual(job['payload']['ModelVersionUtc'],V1)
            self.assertIsNone(job['options']['preferred_worker_instance_id'])
        self.assertEqual(len(list(export.iter_sources(completion_order=False))),9)
    def test_two_workers_out_of_order_and_parallel_downloads(self):
        farm=self.farm(2,sdk=.015,upload=.005,download=.01,slow_first=True)
        export=self.start(farm,count=12,task_prefetch=4)
        order=[artifact.task_id for artifact in export.artifacts()]
        self.assertTrue(export.manifest['Complete'])
        self.assertNotEqual(order[0],next(iter(farm.jobs)))
        self.assertGreater(farm.max_downloads,1);self.assertLessEqual(farm.max_downloads,2)
        self.assertLessEqual(farm.max_outstanding,4)

    def test_deterministic_reader_order_does_not_block_scheduler(self):
        farm=self.farm(2,slow_first=True);export=self.start(farm)
        order=[a.task_id for a in export.artifacts(completion_order=False)]
        self.assertEqual(order,[s['TaskId'] for s in export.manifest['Shards']])
    def test_failed_shard_does_not_mark_export_complete(self):
        farm=self.farm();farm.fail_first=True;export=self.start(farm)
        with self.assertRaises(BimBridgeError):export.wait(10)
        self.assertFalse(export.manifest['Complete']);self.assertEqual(export.manifest['FailedShards'],1)
        self.assertEqual(export.manifest['CompletedShards'],8)
    def test_worker_disappears_and_task_requeued_to_other_worker(self):
        farm=self.farm();farm.disappear_once=True;export=self.start(farm).wait(10)
        self.assertTrue(export.manifest['Complete']);self.assertEqual(farm.requeues,1)
        self.assertEqual(len(farm.jobs),9)
    def test_interrupted_download_resumes_from_next_chunk(self):
        farm=self.farm();farm.interrupt_once=0;export=self.start(farm).wait(10)
        task_id=next(iter(farm.jobs))
        self.assertEqual(farm.downloads[task_id],2)
        self.assertEqual(farm.chunk_reads[task_id,0],1)
        self.assertEqual(farm.chunk_reads[task_id,1],2)
    def test_restart_reuses_completed_artifacts_and_unfinished_task_ids(self):
        farm=self.farm(sdk=.03,download=.01);export=self.start(farm,count=15)
        deadline=time.monotonic()+5
        while not export.manifest['CompletedShards'] and time.monotonic()<deadline:time.sleep(.01)
        export.close();before=export.manifest;submitted=len(farm.jobs);downloads=dict(farm.downloads)
        ready={s['TaskId'] for s in before['Shards'] if s['State']=='READY'}
        self.assertTrue(ready)
        resumed=self.start(farm,count=15).wait(10)
        self.assertEqual(len(farm.jobs),15)
        for task_id in ready:self.assertEqual(farm.downloads[task_id],downloads[task_id])
        self.assertEqual(resumed.manifest['ExportSessionId'],before['ExportSessionId'])
    def test_verified_local_export_reused_offline(self):
        farm=self.farm();self.start(farm).wait(10)
        client=farm.client()
        def offline(*args,**kwargs):raise AssertionError('network used')
        client.health=client.workers=client.task=offline
        export=ParallelExport(client,MODEL,self.directory.name,shard_size=1);self.exports.append(export)
        export.wait(5);self.assertTrue(export.manifest['Complete'])
    def test_only_expired_shard_is_resubmitted_with_same_snapshot(self):
        farm=self.farm();farm.expire_once=0;export=self.start(farm).wait(10)
        self.assertEqual(len(farm.jobs),10)
        self.assertEqual(sum(s['RetryCount'] for s in export.manifest['Shards']),1)
        self.assertEqual({j['payload']['ModelVersionUtc'] for j in farm.jobs.values()},{V1})
    def test_snapshot_mismatch_fails_integrity_gate(self):
        farm=self.farm();farm.bad_version='2026-01-02T00:00:00Z';export=self.start(farm)
        with self.assertRaises(BimBridgeError):export.wait(10)
        self.assertFalse(export.manifest['Complete'])
        self.assertTrue(all(s['Error']['Code']=='MODEL_CHANGED' for s in export.manifest['Shards']))
    def test_cancel_entire_export_stops_submissions_and_cancels_remote(self):
        farm=self.farm(sdk=.2);export=self.start(farm,count=30)
        time.sleep(.05);export.cancel();count=len(farm.jobs);time.sleep(.05)
        self.assertEqual(len(farm.jobs),count)
        self.assertTrue(all(j['State'] in {'CANCELLED','COMPLETED'} for j in farm.jobs.values()))
        self.assertEqual(export.manifest['Status'],'CANCELLED')
        self.assertFalse(export.manifest['Complete'])
    def test_lost_submit_response_keeps_idempotency_key_and_prefetch_bound(self):
        farm=self.farm();farm.lost_submit=True;export=self.start(farm).wait(10)
        self.assertEqual(len(farm.jobs),9)
        self.assertGreater(farm.submit_calls,9);self.assertLessEqual(farm.max_outstanding,4)
    def test_capability_and_model_filtering_and_legacy_manager(self):
        farm=self.farm();client=farm.client();client.health=lambda:{'Capabilities':[]}
        with self.assertRaises(BimBridgeError) as error:
            ParallelExport(client,MODEL,self.directory.name,element_ids=elements(1),model_version_utc=V1)
        self.assertEqual(error.exception.code,'CAPABILITY_NOT_AVAILABLE');self.assertFalse(farm.jobs)
    def test_session_owner_lock_and_changed_selection_rejected(self):
        farm=self.farm(sdk=.05);export=self.start(farm)
        with self.assertRaises(BimBridgeError) as error:self.start(farm)
        self.assertEqual(error.exception.code,'EXPORT_IN_USE')
        export.wait(10)
        with self.assertRaises(BimBridgeError) as error:self.start(farm,count=8)
        self.assertEqual(error.exception.code,'EXPORT_CONFLICT')
    def test_output_byte_quota_fails_without_publishing_unverified_file(self):
        farm=self.farm();export=self.start(farm,max_output_bytes=10)
        with self.assertRaises(BimBridgeError):export.wait(10)
        self.assertFalse(list(Path(self.directory.name).glob('*.bbsp')))
        self.assertTrue(all(s['Error']['Code']=='RESOURCE_LIMIT' for s in export.manifest['Shards']))

    def test_full_mesh_shards_remain_independent_and_duplicate_bytes_measured(self):
        farm=self.farm();export=self.start(farm,count=6,include_meshes=True).wait(10)
        diagnostic=export.mesh_duplication()
        self.assertEqual(diagnostic['MeshDefinitions'],6)
        self.assertEqual(diagnostic['UniqueMeshDefinitions'],1)
        self.assertEqual(diagnostic['TotalMeshBufferBytes'],144)
        self.assertEqual(diagnostic['RepeatedMeshBufferBytes'],120)
        for artifact in export.artifacts():
            self.assertEqual(list(artifact.spatial().iter_placed_vertices()),[(10.,20.,30.),(11.,22.,33.)])
        self.assertEqual([s['Source'] for s in export.iter_sources(completion_order=False)],elements(6))

    def test_faster_worker_naturally_pulls_more_shards(self):
        farm=self.farm(2,sdk=.02,upload=.001,download=.0001,slow_worker=True)
        self.start(farm,count=30,task_prefetch=8).wait(10)
        self.assertGreater(farm.assignments[0],farm.assignments[1])

    def test_cancel_is_terminal_and_resume_uses_close_instead(self):
        farm=self.farm(sdk=.2);export=self.start(farm,count=30);time.sleep(.03);export.cancel()
        with self.assertRaises(BimBridgeError) as error:self.start(farm,count=30)
        self.assertEqual(error.exception.code,'EXPORT_CANCELLED')
        self.assertEqual(export.manifest['PendingCancellations'],[])

    def test_download_transport_pool_is_bounded_reused_and_closed(self):
        farm=self.farm(download=.01);client=farm.client();created=[];closed=[]
        def fork():
            clone=farm.client();clone.close=lambda:closed.append(clone)
            created.append(clone);return clone
        client._parallel_fork=fork
        export=ParallelExport(client,MODEL,self.directory.name,element_ids=elements(12),shard_size=1,poll_interval=.003)
        self.exports.append(export);export.wait(10)
        self.assertGreater(len(created),0);self.assertLessEqual(len(created),2)
        self.assertEqual(len(closed),len(created))

    def test_repeated_source_ids_are_deduplicated_across_shard_boundaries(self):
        farm=self.farm();source=elements(4)
        export=self.start(farm,element_ids=iter(source+source),count=4).wait(10)
        self.assertEqual(export.manifest['ShardCount'],4)
        self.assertEqual([s['Source'] for s in export.iter_sources(completion_order=False)],source)
        self.assertEqual(len(farm.jobs),4)
    def test_selection_limit_fails_before_any_remote_submission(self):
        farm=self.farm()
        with self.assertRaises(BimBridgeError) as error:self.start(farm,count=10,max_source_elements=3)
        self.assertEqual(error.exception.code,'RESOURCE_LIMIT')
        self.assertFalse(farm.jobs)
        self.assertTrue(self.start(farm,count=10).wait(10).manifest['Complete'])

    def test_corrupt_manifest_cannot_silently_drop_shards(self):
        farm=self.farm();self.start(farm).wait(10)
        path=Path(self.directory.name)/'manifest.json';manifest=json.loads(path.read_text(encoding='utf-8'))
        manifest['Shards'].pop();path.write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaises(BimBridgeError) as error:self.start(farm,element_ids=None)
        self.assertEqual(error.exception.code,'INTEGRITY_ERROR')

    def test_restart_recovers_failed_transient_download_without_resubmitting(self):
        farm=self.farm();farm.interrupt_once=0
        first=self.start(farm,max_shard_retries=0)
        with self.assertRaises(BimBridgeError):first.wait(10)
        self.assertEqual(first.manifest['FailedShards'],1)
        second=self.start(farm).wait(10)
        self.assertEqual(len(farm.jobs),9)
        task_id=next(iter(farm.jobs));self.assertEqual(farm.chunk_reads[task_id,0],1)
        self.assertEqual(farm.downloads[task_id],2)
    def test_expired_task_code_resubmits_only_affected_shard(self):
        farm=self.farm();farm.expire_status_once=0
        export=self.start(farm).wait(10)
        self.assertEqual(len(farm.jobs),10)
        self.assertEqual(export.manifest['CompletedShards'],9)
    def test_retryable_terminal_task_uses_new_task_instead_of_polling_failed_id(self):
        farm=self.farm();farm.fail_first_retryable=True
        export=self.start(farm).wait(10)
        self.assertEqual(len(farm.jobs),10)
        self.assertTrue(export.manifest['Complete'])

    def test_cancel_reconciles_lost_submit_reply_before_cancelling_unknown_task(self):
        farm=self.farm(sdk=.5);farm.lost_submit=True
        export=self.start(farm,count=20,poll_interval=.5)
        deadline=time.monotonic()+3
        while not farm.submit_calls and time.monotonic()<deadline:time.sleep(.005)
        self.assertEqual(export.cancel(),[])
        self.assertFalse(export.manifest['PendingCancellationRequests'])
        self.assertTrue(all(job['State'] in {'CANCELLED','COMPLETED'} for job in farm.jobs.values()))
        self.assertLessEqual(len(farm.jobs),4)

    def test_cancel_includes_locally_failed_shard_whose_remote_task_may_still_run(self):
        farm=self.farm(sdk=.5);farm.status_failure_once=0
        export=self.start(farm,count=10,max_shard_retries=0)
        deadline=time.monotonic()+3
        while not export.manifest['FailedShards'] and time.monotonic()<deadline:time.sleep(.005)
        self.assertEqual(export.manifest['FailedShards'],1)
        task_id=export.manifest['Shards'][0]['TaskId']
        self.assertEqual(export.cancel(),[])
        self.assertEqual(farm.jobs[task_id]['State'],'CANCELLED')

    def test_accepted_but_unacknowledged_failures_cannot_bypass_prefetch_limit(self):
        farm=self.farm(sdk=.5);farm.lost_all_submits=True
        export=self.start(farm,count=100,max_shard_retries=2,task_prefetch=2)
        with self.assertRaises(BimBridgeError):export.wait(5)
        self.assertLessEqual(len(farm.jobs),2)
        self.assertGreater(export.summary['UncertainRemoteShards'],0)
        self.assertTrue(any(s['State']=='PENDING' for s in export.manifest['Shards']))
        farm.lost_all_submits=False
        self.assertEqual(export.cancel(),[])
    def test_execution_prefetch_and_result_backlog_have_separate_bounds(self):
        farm=self.farm(2,sdk=.005,upload=.001,download=.05)
        export=self.start(farm,count=20,task_prefetch=2,result_prefetch=4).wait(15)
        self.assertLessEqual(farm.max_outstanding,2)
        self.assertLessEqual(farm.max_pipeline_outstanding,6)
        self.assertEqual(export.summary['MaxInflightShards'],6)
    def test_restart_downloads_completed_remote_results_with_zero_online_workers(self):
        farm=self.farm(1,sdk=.005,upload=.005,download=.05)
        first=self.start(farm,count=2,task_prefetch=2)
        deadline=time.monotonic()+3
        while (len(farm.jobs)<2 or any(j['State']!='COMPLETED' for j in farm.jobs.values())) and time.monotonic()<deadline:time.sleep(.005)
        first.close();farm.close();client=farm.client();client.workers=lambda:[]
        resumed=ParallelExport(client,MODEL,self.directory.name,shard_size=1,poll_interval=.003)
        self.exports.append(resumed);resumed.wait(10)
        self.assertTrue(resumed.manifest['Complete']);self.assertEqual(len(farm.jobs),2)

    def test_concurrent_consumer_and_observer_never_race_manifest_serialization(self):
        farm=self.farm(2,sdk=.001,upload=.001,download=.0001)
        export=self.start(farm,count=60,task_prefetch=8)
        stopped=threading.Event();failures=[]
        def observe():
            try:
                while not stopped.wait(.0005):
                    export.manifest;export.summary
            except BaseException as error:failures.append(error)
        observer=threading.Thread(target=observe);observer.start()
        try:
            self.assertEqual(len(list(export.artifacts())),60)
            self.assertFalse(failures)
            self.assertTrue(export.manifest['Complete'])
        finally:stopped.set();observer.join(3)

    def test_mounted_filesystem_without_advisory_locks_uses_real_host_local_owner_lock(self):
        from bim_bridge.parallel import _SessionLock
        acquire=_SessionLock._acquire
        def mounted(stream):
            if str(stream.name).endswith('.session.lock'):
                raise OSError(errno.ENOSYS,'advisory locks unsupported')
            return acquire(stream)
        with patch.object(_SessionLock,'_acquire',new=staticmethod(mounted)):
            owner=_SessionLock(Path(self.directory.name))
            try:
                self.assertTrue(owner.local_fallback)
                with self.assertRaises(BimBridgeError) as error:_SessionLock(Path(self.directory.name))
                self.assertEqual(error.exception.code,'EXPORT_IN_USE')
            finally:owner.close()
            restarted=_SessionLock(Path(self.directory.name));restarted.close()

    def test_owned_shard_publish_uses_atomic_replace_without_hard_link_fallback(self):
        farm=self.farm()
        with patch('bim_bridge._files.os.link',side_effect=OSError(errno.ENOTSUP,'no hard links')) as link:
            self.start(farm,count=4).wait(10)
            link.assert_not_called()
