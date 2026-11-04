import csv
import base64
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from bim_bridge.profile import ExportProfile
from bim_bridge._diagnostics import spatial_performance


class ProfileTests(unittest.TestCase):
    def test_opaque_spatial_diagnostics_and_legacy_fallback(self):
        worker = dict(SchemaVersion=1, TaskId='task', AssignmentId='assignment',
                      SourceElements=5, TraversedNodes=8, Instances=3, UniqueMeshes=1,
                      SdkTraversalReadMs=1.0, SdkMeshAcquireMs=2.0,
                      SdkVerticesReadMs=3.0, BoundsCpuMs=4.0)
        raw = json.dumps(worker).encode()
        envelope = dict(ContentType='application/vnd.pilot.bimbridge.spatial-performance+json',
                        SchemaVersion=1, PayloadBytes=base64.b64encode(raw).decode(),
                        Sha256=hashlib.sha256(raw).hexdigest())
        record = dict(TaskId='task', AssignmentId='assignment', Diagnostics=envelope,
                      SpatialPerformance=dict(SourceElements=99))
        self.assertEqual(spatial_performance(record)['SourceElements'], 5)
        profile = ExportProfile(dict(Shards=[dict(ShardId=0, State='READY', **record)])).data
        self.assertEqual(profile['Attempts'][0]['ProcessedSources'], 5)
        bad = dict(envelope, Sha256='0' * 64)
        self.assertEqual(spatial_performance(dict(record, Diagnostics=bad))['SourceElements'], 99)
        self.assertIsNone(spatial_performance(dict(Diagnostics=bad)))
        for broken in (dict(envelope, SchemaVersion=999),
                       dict(envelope, PayloadBytes=base64.b64encode(b'{').decode(),
                            Sha256=hashlib.sha256(b'{').hexdigest()),
                       dict(envelope, PayloadBytes=base64.b64encode(b' ' * (64 * 1024 + 1)).decode()),
                       dict(envelope, ContentType='application/vnd.other+json')):
            self.assertIsNone(spatial_performance(dict(Diagnostics=broken)))
        self.assertIsNone(spatial_performance({}))

    def test_system_kpis_and_scaling_need_complete_measured_runs(self):
        def run(workers, wall):
            return ExportProfile(dict(ModelKey='m', ModelVersionUtc='v', Options={'Meshes': True},
                ShardSize=10, EligibleWorkers=workers, TotalWall=wall, Complete=True,
                Shards=[dict(ShardId=i, State='READY', ElementIdentity=[i],
                             WorkerInstanceId=f'worker-{i % workers}',
                             ProcessingWallMs=duration, SpatialPerformance=dict(SourceElements=10),
                             Metadata={'TotalLength': 100})
                        for i, duration in enumerate((10, 20, 30, 40, 50))]))
        prior, current = run(1, 10), run(2, 6)
        self.assertEqual(current.data['ProcessedSources'], 50)
        self.assertEqual(current.data['ActiveWorkers'], 2)
        self.assertEqual(current.data['EndToEndSourcesPerMinute'], 500)
        self.assertAlmostEqual(current.data['ArtifactBytesPerSecond'], 500 / 6)
        self.assertEqual(current.data['ShardProcessingDuration'],
                         dict(Known=5, Total=5, MinMs=10, MedianMs=30, P95Ms=50, MaxMs=50))
        comparison = current.compare(prior)
        self.assertFalse(comparison['Comparable'])
        self.assertTrue(comparison['ScalingComparable'])
        self.assertAlmostEqual(comparison['Speedup'], 10 / 6)
        self.assertAlmostEqual(comparison['ScalingEfficiency'], 5 / 6)
        missing = run(2, 6)
        missing.data['Complete'] = False
        self.assertFalse(current.compare(missing)['ScalingComparable'])
        incomplete = ExportProfile(dict(Complete=True, TotalWall=6, Shards=[
            dict(ShardId=0, State='READY', SpatialPerformance=dict(SourceElements=3)),
            dict(ShardId=1, State='FAILED', Metadata={'TotalLength': 4})]))
        self.assertIsNone(incomplete.data['ProcessedSources'])
        self.assertIsNone(incomplete.data['ArtifactBytes'])
        self.assertIsNone(incomplete.data['EndToEndSourcesPerMinute'])
        unknown_identity = run(2, 6)
        unknown_identity.data['SelectionIdentity'] = None
        self.assertFalse(unknown_identity.compare(prior)['ScalingComparable'])

    def test_worker_rate_uses_reported_sources_and_marks_incomplete_attempts_unknown(self):
        shards = [dict(ShardId=number, State='READY', WorkerInstanceId='worker',
                       ElementCount=planned, ProcessingWallMs=1000,
                       SpatialPerformance=dict(SourceElements=actual))
                  for number, (planned, actual) in enumerate(((12, 10), (18, 20)))]
        worker = ExportProfile(dict(TotalWall=3, Shards=shards)).data['Workers'][0]
        self.assertEqual(worker['AssignedElements'], 30)
        self.assertEqual(worker['ProcessedSources'], 30)
        self.assertEqual(worker['ProcessingSourcesPerMinute'], 900)
        shards.append(dict(ShardId=2, State='READY', WorkerInstanceId='worker',
                           ElementCount=5, ProcessingWallMs=1000))
        incomplete = ExportProfile(dict(TotalWall=4, Shards=shards)).data['Workers'][0]
        self.assertIsNone(incomplete['ProcessedSources'])
        self.assertIsNone(incomplete['ProcessingSourcesPerMinute'])

    def test_missing_metrics_are_unknown_and_reports_open_without_server(self):
        manifest = dict(ExportSessionId='session', ModelKey='model', ModelVersionUtc='v1',
                        Options={'IncludeMeshes': False}, ShardSize=100, EligibleWorkers=2,
                        TotalWall=3.0, Complete=True, Shards=[
                            dict(ShardId=0, TaskId='task-a', WorkerInstanceId='worker-a',
                                 State='READY', ElementCount=12, RetryCount=0,
                                 ProcessingWallMs=100.0, SpatialPerformance=dict(SdkTraversalReadMs=24.0,
                                     SdkMeshAcquireMs=3.0, SdkVerticesReadMs=8.0, BoundsCpuMs=4.0),
                                 StatusTimes=dict(CreatedAtUtc='2026-11-04T10:00:00Z',
                                     AssignedAtUtc='2026-11-04T10:00:01Z',StartedAtUtc='2026-11-04T10:00:01.5Z',
                                     PreparedAtUtc='2026-11-04T10:00:02Z',CompletedAtUtc='2026-11-04T10:00:03Z'),
                                 Metadata={'TotalLength': 48}),
                            dict(ShardId=1, TaskId='task-b', WorkerInstanceId='worker-b',
                                 State='READY', ElementCount=10, RetryCount=1,
                                 Metadata={'TotalLength': 32})])
        profile = ExportProfile(manifest)
        self.assertEqual(profile.data['TotalShards'], 2)
        self.assertEqual(profile.data['ArtifactBytes'], 80)
        self.assertIsNone(profile.data['Workers'][1]['ProcessingWallMs'])
        self.assertIsNone(profile.data['Stages'][7]['ActiveMs'])
        with TemporaryDirectory() as directory:
            root = Path(directory)
            profile.save_json(root/'profile.json')
            profile.save_csv(root/'stages.csv')
            profile.save_html(root/'timeline.html')
            self.assertEqual(json.loads((root/'profile.json').read_text(encoding='utf-8'))['CompletedShards'], 2)
            self.assertIn('sdk-traversal', (root/'stages.csv').read_text(encoding='utf-8-sig'))
            self.assertIn('WorkerInstanceId', (root/'timeline.html').read_text(encoding='utf-8'))
            self.assertIn('class="lane"', (root/'timeline.html').read_text(encoding='utf-8'))
            self.assertIn('class="delivery"', (root/'timeline.html').read_text(encoding='utf-8'))

    def test_comparison_warns_when_snapshot_or_selection_differs(self):
        first = ExportProfile(dict(ModelKey='model', ModelVersionUtc='v1', Options={'IncludeMeshes': False},
                                   ShardSize=100, EligibleWorkers=2, TotalWall=2, Shards=[]))
        second = ExportProfile(dict(ModelKey='model', ModelVersionUtc='v2', Options={'IncludeMeshes': True},
                                    ShardSize=100, EligibleWorkers=3, TotalWall=3, Shards=[]))
        comparison = second.compare(first)
        self.assertFalse(comparison['Comparable'])
        self.assertEqual(comparison['Differences'], ['ModelVersionUtc', 'Options', 'EligibleWorkers'])
        with TemporaryDirectory() as directory:
            path = second.save_html(Path(directory)/'compare.html', compare=first)
            html = path.read_text(encoding='utf-8')
            self.assertIn('Запуски нельзя считать сопоставимыми', html)
            self.assertIn('ModelVersionUtc', html)

    def test_previous_attempts_remain_visible_and_unknown_wall_is_not_zero(self):
        profile = ExportProfile(dict(Shards=[dict(ShardId=0, State='READY', ElementCount=10,
            TaskId='new', WorkerInstanceId='worker-b', ProcessingWallMs=40,
            Attempts=[dict(TaskId='old', WorkerInstanceId='worker-a', ProcessingWallMs=60,
                           QueueWaitMs=12, Error={'Code': 'WORKER_LOST'})])]))
        self.assertIsNone(profile.data['TotalWallMs'])
        self.assertEqual(profile.data['Attempts'][0]['TaskId'], 'old')
        old_queue = next(row for row in profile.data['Stages']
                         if row['Stage'] == 'manager-queue' and row['AttemptNumber'] == 1)
        self.assertEqual(old_queue['WallMs'], 12)
        self.assertEqual(profile.data['Workers'][0]['ProcessingWallMs'], 60)
        self.assertIsNone(profile.compare({'TotalWallMs': 100})['WallChangeMs'])

    def test_session_preparation_stages_are_kept_separate_from_shards(self):
        profile = ExportProfile(dict(SnapshotPinWallMs=12, SelectionShardingWallMs=28,
                                     SchedulerWaitWallMs=15,
                                     Shards=[dict(ShardId=0, State='READY', ElementCount=1)]))
        stages = {row['Stage']: row for row in profile.data['Stages']}
        self.assertEqual(stages['client-snapshot-pin']['WallMs'], 12)
        self.assertEqual(stages['client-selection-sharding']['WallMs'], 28)
        self.assertIsNone(stages['client-selection-sharding']['ShardId'])
        self.assertEqual(stages['client-scheduler-wait']['WallMs'], 15)

    def test_encoding_and_artifact_write_have_distinct_time_semantics(self):
        profile = ExportProfile(dict(Shards=[dict(ShardId=0, State='READY',
            SpatialPerformance=dict(BinaryEncodingActiveMs=8, ModelAccessWallMs=6,
                                    SnapshotIndexWallMs=4, SdkDispatchWaitWallMs=5,
                                    SnapshotIndexHits=1, WorkerMeshCacheEntries=3,
                                    WorkerMeshCacheBytes=512,WorkerMeshCacheEvictions=2,
                                    WorkerSnapshotIndexEntries=1,WorkerSnapshotIndexBytes=256,
                                    WorkerSnapshotIndexEvictions=4,
                                    ArtifactWriteWallMs=14, ArtifactFinalizeWallMs=3,
                                    SpoolPreparationWallMs=2),
            ArtifactPublicationMs=2,
            ResultPreparationWallMs=1.5,UploadRetryCount=2,UploadRetryWaitMs=3.5,
            ChunkTransferWallMs=9, LocalFinalizeWallMs=1)]))
        stages = {row['Stage']: row for row in profile.data['Stages']}
        self.assertEqual(stages['binary-encoding']['ActiveMs'], 8)
        self.assertIsNone(stages['binary-encoding']['WallMs'])
        self.assertEqual(stages['artifact-write']['WallMs'], 14)
        self.assertEqual(stages['artifact-finalize']['WallMs'], 3)
        self.assertEqual(stages['model-access']['WallMs'], 6)
        self.assertIsNone(stages['model-access']['ActiveMs'])
        self.assertEqual(stages['snapshot-index']['WallMs'], 4)
        self.assertEqual(stages['sdk-dispatch-wait']['WallMs'], 5)
        self.assertIsNone(stages['sdk-dispatch-wait']['ActiveMs'])
        self.assertEqual(stages['spool-preparation']['WallMs'], 2)
        self.assertEqual(stages['manager-preparation']['WallMs'], 1.5)
        self.assertEqual(stages['worker-upload-retry-wait']['WallMs'], 3.5)
        self.assertEqual(profile.data['Attempts'][0]['UploadRetryCount'], 2)
        self.assertEqual(profile.data['Attempts'][0]['SnapshotIndexHits'], 1)
        self.assertEqual(profile.data['Attempts'][0]['WorkerMeshCacheBytes'], 512)
        self.assertEqual(profile.data['Attempts'][0]['WorkerSnapshotIndexEvictions'], 4)
        self.assertEqual(stages['manager-publication']['WallMs'], 2)
        self.assertEqual(stages['client-chunk-acquire']['WallMs'], 9)
        self.assertEqual(stages['client-finalize']['WallMs'], 1)

    def test_download_retries_are_visible_as_separate_records(self):
        profile = ExportProfile(dict(Shards=[dict(ShardId=0, State='READY',
            DownloadAttempts=[dict(Success=False, ErrorCode='TRANSPORT_ERROR', ChunkCalls=2,
                                   ChunkTransferWallMs=8),
                              dict(Success=True, ChunkCalls=1, ResumedBytes=4,
                                   ChunkTransferWallMs=5)])]))
        self.assertEqual(len(profile.data['DownloadAttempts']), 2)
        self.assertEqual(profile.data['DownloadAttempts'][0]['DownloadAttemptNumber'], 1)
        self.assertEqual(profile.data['DownloadAttempts'][1]['ResumedBytes'], 4)
        with TemporaryDirectory() as directory:
            path=profile.save_csv(Path(directory)/'stages.csv')
            with path.open(encoding='utf-8-sig', newline='') as stream:
                rows=[row for row in csv.DictReader(stream) if row['Stage']=='client-chunk-acquire']
        self.assertEqual([(row['DownloadAttemptNumber'],row['WallMs']) for row in rows],
                         [('1','8'),('2','5')])

    def test_manager_reassignment_keeps_distinct_attempts_and_queue_times(self):
        profile = ExportProfile(dict(Shards=[dict(ShardId=0, TaskId='task', State='READY',
            WorkerInstanceId='new', AssignmentId='assignment-2', RemoteAttemptNumber=2,
            QueueWaitMs=80, AttemptQueueWaitMs=30, ProcessingWallMs=50,
            TaskAcceptanceWallMs=2, AssignmentWallMs=3,
            ManagerAttempts=[dict(AttemptNumber=1, AssignmentId='assignment-1',
                WorkerInstanceId='old', State='ABANDONED', QueueWaitMs=50,
                AssignmentWallMs=4,
                TaskError={'Code': 'LEASE_EXPIRED'},
                AssignedAtUtc='2026-11-04T10:00:00Z',
                EndedAtUtc='2026-11-04T10:00:01Z')])]))
        attempts = profile.data['Attempts']
        self.assertEqual(len(attempts), 2)
        self.assertEqual([item['AssignmentId'] for item in attempts],
                         ['assignment-1', 'assignment-2'])
        self.assertEqual([item['QueueWaitMs'] for item in attempts], [50, 30])
        self.assertEqual([row['WallMs'] for row in profile.data['Stages']
                          if row['Stage'] == 'manager-queue'], [50, 30])
        self.assertNotIn('StatusTimes', profile.data['Stages'][0])
        self.assertEqual([item['AssignmentWallMs'] for item in attempts], [4, 3])
        self.assertEqual([row['WallMs'] for row in profile.data['Stages']
                          if row['Stage']=='manager-task-acceptance' and row['WallMs'] is not None], [2])
        self.assertEqual(attempts[0]['Error']['Code'], 'LEASE_EXPIRED')
        self.assertEqual([item['WorkerInstanceId'] for item in profile.data['Workers']],
                         ['old', 'new'])
        self.assertIn('assignment', attempts[0]['AssignmentId'])
        self.assertIn('Shard 0, attempt 1, old', profile._timeline_html())
        with TemporaryDirectory() as directory:
            html=profile.save_html(Path(directory)/'attempts.html').read_text(encoding='utf-8')
            self.assertIn('Назначения tasks', html)
            self.assertIn('assignment-1', html)


if __name__ == '__main__':
    unittest.main()
