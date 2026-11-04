"""Portable reports for one durable BIM Bridge export session."""
import csv
from datetime import datetime, timezone
from html import escape
import json
import hashlib
from pathlib import Path
from statistics import median
from ._diagnostics import spatial_performance


_WORKER_STAGES = (
    ('sdk-traversal', 'SdkTraversalReadMs'),
    ('sdk-mesh-acquire', 'SdkMeshAcquireMs'),
    ('sdk-vertices', 'SdkVerticesReadMs'),
    ('bounds', 'BoundsCpuMs'),
    ('binary-encoding', 'BinaryEncodingActiveMs'),
)

_WORKER_WALL_STAGES = (
    ('model-access', 'ModelAccessWallMs'),
    ('snapshot-index', 'SnapshotIndexWallMs'),
    ('sdk-dispatch-wait', 'SdkDispatchWaitWallMs'),
    ('artifact-write', 'ArtifactWriteWallMs'),
    ('artifact-finalize', 'ArtifactFinalizeWallMs'),
    ('spool-preparation', 'SpoolPreparationWallMs'),
)

_DOWNLOAD_STAGES = (
    ('manager-chunk-serving', 'ManagerServeWallMs'),
    ('client-download', 'DownloadWallMs'),
    ('client-resume-validation', 'ResumeValidationWallMs'),
    ('client-chunk-acquire', 'ChunkTransferWallMs'),
    ('client-finalize', 'LocalFinalizeWallMs'),
)

def _attempt_records(shard):
    for local in [*shard.get('Attempts', []), shard]:
        for remote in local.get('ManagerAttempts') or []:
            times = {key: remote[key] for key in
                     ('AssignedAtUtc', 'StartedAtUtc') if remote.get(key)}
            if remote.get('EndedAtUtc'):
                times['CompletedAtUtc'] = remote['EndedAtUtc']
            yield dict(TaskId=local.get('TaskId'), State=remote.get('State'),
                       WorkerInstanceId=remote.get('WorkerInstanceId'),
                       AssignmentId=remote.get('AssignmentId'),
                       RemoteAttemptNumber=remote.get('AttemptNumber'),
                       AttemptQueueWaitMs=remote.get('QueueWaitMs'),
                       AssignmentWallMs=remote.get('AssignmentWallMs'),
                       ResultPreparationWallMs=remote.get('ResultPreparationWallMs'),
                       UploadRetryCount=remote.get('UploadRetryCount'),
                       UploadRetryWaitMs=remote.get('UploadRetryWaitMs'),
                       ProcessingWallMs=remote.get('ProcessingWallMs'),
                       SpatialPerformance=spatial_performance(remote),
                       Error=remote.get('TaskError'), StatusTimes=times)
        yield local


class ExportProfile:
    """A snapshot of persisted shard telemetry; absent values remain unknown."""

    def __init__(self, manifest):
        self.data = self._build(manifest)

    @staticmethod
    def _build(manifest):
        shards = []
        stages = []
        attempts = []
        downloads = []
        for shard in manifest.get('Shards', []):
            shard_attempts = []
            for number, record in enumerate(_attempt_records(shard), 1):
                identity = {key: record.get(key) for key in ('TaskId', 'WorkerInstanceId', 'State')}
                identity['ShardId'] = shard.get('ShardId')
                identity['AttemptNumber'] = number
                identity['RemoteAttemptNumber'] = record.get('RemoteAttemptNumber')
                identity['AssignmentId'] = record.get('AssignmentId')
                identity['DownloadAttemptNumber'] = None
                identity['ElementCount'] = shard.get('ElementCount')
                identity['ArtifactBytes'] = (record.get('Metadata') or {}).get('TotalLength')
                worker = spatial_performance(record)
                identity['ProcessedSources'] = worker.get('SourceElements') if worker else None
                for field in ('ProcessingWallMs', 'QueueWaitMs', 'TaskAcceptanceWallMs',
                              'AssignmentWallMs', 'ResultPreparationWallMs', 'HashVerificationMs',
                              'ArtifactPublicationMs', 'UploadWallMs',
                              'UploadRetryCount', 'UploadRetryWaitMs',
                              'SubmitWallMs', 'MonitoringRpcWallMs', 'DownloadWallMs',
                              'ValidationWallMs', 'ResumeValidationWallMs', 'ChunkTransferWallMs',
                              'LocalFinalizeWallMs', 'DownloadedBytes', 'ResumedBytes', 'ChunkCalls',
                              'ManagerServeWallMs',
                              'Error', 'StatusTimes'):
                    identity[field] = (record.get('AttemptQueueWaitMs')
                                       if field == 'QueueWaitMs' and record.get('AttemptQueueWaitMs') is not None
                                       else record.get(field))
                for field in ('MeshCacheHits', 'MeshCacheMisses', 'SnapshotIndexHits',
                              'SnapshotIndexMisses', 'WorkerMeshCacheEntries',
                              'WorkerMeshCacheBytes', 'WorkerMeshCacheEvictions',
                              'WorkerSnapshotIndexEntries', 'WorkerSnapshotIndexBytes',
                              'WorkerSnapshotIndexEvictions'):
                    identity[field] = worker.get(field) if worker else None
                attempts.append(identity)
                shard_attempts.append(identity)
                stage_identity = {key: identity[key] for key in
                                  ('ShardId', 'TaskId', 'AssignmentId', 'WorkerInstanceId',
                                   'AttemptNumber', 'RemoteAttemptNumber', 'DownloadAttemptNumber')}
                download_attempts = record.get('DownloadAttempts') or []
                for download_number, download in enumerate(download_attempts, 1):
                    downloads.append(dict(ShardId=identity['ShardId'], AttemptNumber=number,
                                          DownloadAttemptNumber=download_number, **download))
                    for name, field in _DOWNLOAD_STAGES:
                        value = download.get(field)
                        stages.append(dict(stage_identity, DownloadAttemptNumber=download_number,
                                           Stage=name, WallMs=value, ActiveMs=None,
                                           Source=('manager-monotonic' if name.startswith('manager-') else 'client-monotonic')
                                           if value is not None else 'unavailable'))
                for name, field in _WORKER_STAGES:
                    stages.append(dict(stage_identity, Stage=name, WallMs=None,
                                       ActiveMs=worker.get(field) if worker else None,
                                       Source='worker-monotonic' if worker else 'unavailable'))
                for name, field in _WORKER_WALL_STAGES:
                    value=worker.get(field) if worker else None
                    stages.append(dict(stage_identity, Stage=name, WallMs=value, ActiveMs=None,
                                       Source='worker-monotonic' if value is not None else 'unavailable'))
                for name, field in (('manager-task-acceptance', 'TaskAcceptanceWallMs'),
                                    ('manager-assignment', 'AssignmentWallMs'),
                                    ('manager-preparation', 'ResultPreparationWallMs'),
                                    ('manager-queue', 'QueueWaitMs'),
                                    ('manager-hash', 'HashVerificationMs'),
                                    ('manager-publication', 'ArtifactPublicationMs'),
                                    ('worker-upload', 'UploadWallMs'),
                                    ('worker-upload-retry-wait', 'UploadRetryWaitMs'),
                                    ('client-submit', 'SubmitWallMs'),
                                    ('client-monitor-rpc', 'MonitoringRpcWallMs'),
                                    ('worker-processing', 'ProcessingWallMs'),
                                    ('client-validation', 'ValidationWallMs')):
                    value = identity[field] if field == 'QueueWaitMs' else record.get(field)
                    stages.append(dict(stage_identity, Stage=name, WallMs=value, ActiveMs=None,
                                       Source='monotonic' if value is not None else 'unavailable'))
                if not download_attempts:
                    for name, field in _DOWNLOAD_STAGES:
                        value = record.get(field)
                        stages.append(dict(stage_identity, Stage=name, WallMs=value, ActiveMs=None,
                                           Source=('manager-monotonic' if name.startswith('manager-') else 'client-monotonic')
                                           if value is not None else 'unavailable'))
            identity['Retries'] = shard.get('RetryCount', 0)
            identity['PreviousAttempts'] = shard_attempts[:-1]
            shards.append(identity)
        for name, field in (('client-snapshot-pin', 'SnapshotPinWallMs'),
                            ('client-selection-sharding', 'SelectionShardingWallMs'),
                            ('client-scheduler-wait', 'SchedulerWaitWallMs')):
            value = manifest.get(field)
            stages.append(dict(ShardId=None, TaskId=None, WorkerInstanceId=None,
                               AttemptNumber=None, DownloadAttemptNumber=None,
                               Stage=name, WallMs=value, ActiveMs=None,
                               Source='client-monotonic' if value is not None else 'unavailable'))
        ready = [shard for shard in shards if shard['State'] in ('READY', 'CONSUMED')]
        active_workers = (len({shard['WorkerInstanceId'] for shard in ready})
                          if ready and all(shard['WorkerInstanceId'] for shard in ready) else None)
        workers = {}
        for shard in attempts:
            owner = shard['WorkerInstanceId']
            if owner:
                row = workers.setdefault(owner, dict(WorkerInstanceId=owner, Attempts=0,
                                                     AssignedElements=0, ProcessedSources=0,
                                                     KnownSourceAttempts=0, ProcessingWallMs=0,
                                                     KnownProcessingAttempts=0))
                row['Attempts'] += 1
                row['AssignedElements'] += shard['ElementCount'] or 0
                if shard['ProcessedSources'] is not None:
                    row['ProcessedSources'] += shard['ProcessedSources']
                    row['KnownSourceAttempts'] += 1
                if shard['ProcessingWallMs'] is not None:
                    row['ProcessingWallMs'] += shard['ProcessingWallMs']
                    row['KnownProcessingAttempts'] += 1
        for row in workers.values():
            if row['KnownProcessingAttempts'] != row['Attempts']:
                row['ProcessingWallMs'] = None
            if row['KnownSourceAttempts'] != row['Attempts']:
                row['ProcessedSources'] = None
            row['ProcessingSourcesPerMinute'] = (
                row['ProcessedSources'] * 60000 / row['ProcessingWallMs']
                if row['ProcessedSources'] is not None and row['ProcessingWallMs'] else None)
            total_wall = manifest.get('TotalWall')
            row['ProcessingUtilization'] = (row['ProcessingWallMs'] / (total_wall * 1000)
                                            if row['ProcessingWallMs'] is not None and total_wall else None)
        total_wall_ms = manifest['TotalWall'] * 1000 if manifest.get('TotalWall') is not None else None
        complete = manifest.get('Complete') is True and len(ready) == len(shards)
        ready_sources = [shard['ProcessedSources'] for shard in ready]
        processed_sources = (sum(ready_sources) if complete and ready_sources
                             and all(value is not None for value in ready_sources) else None)
        artifact_lengths = [shard['ArtifactBytes'] for shard in ready]
        artifact_bytes = (sum(artifact_lengths) if complete and artifact_lengths
                          and all(value is not None for value in artifact_lengths) else None)
        processing_times = sorted(shard['ProcessingWallMs'] for shard in ready
                                  if shard['ProcessingWallMs'] is not None)
        shard_duration = dict(Known=len(processing_times), Total=len(shards),
                              MinMs=processing_times[0] if processing_times else None,
                              MedianMs=median(processing_times) if processing_times else None,
                              P95Ms=processing_times[(19 * len(processing_times) + 19) // 20 - 1]
                              if processing_times else None,
                              MaxMs=processing_times[-1] if processing_times else None)
        return dict(SchemaVersion=1, ExportSessionId=manifest.get('ExportSessionId'),
                    ModelKey=manifest.get('ModelKey'), ModelVersionUtc=manifest.get('ModelVersionUtc'),
                    Options=manifest.get('Options'), ShardSize=manifest.get('ShardSize'),
                    SelectionIdentity=hashlib.sha256(json.dumps([s.get('ElementIdentity') for s in manifest.get('Shards', [])],
                        sort_keys=True).encode()).hexdigest() if manifest.get('Shards') else None,
                    EligibleWorkers=manifest.get('EligibleWorkers'),
                    TotalWallMs=total_wall_ms,
                    Complete=manifest.get('Complete'), CompletedShards=len(ready),
                    ActiveWorkers=active_workers,
                    TotalShards=len(shards), ArtifactBytes=artifact_bytes,
                    ProcessedSources=processed_sources,
                    EndToEndSourcesPerMinute=(processed_sources * 60000 / total_wall_ms
                                              if processed_sources is not None and total_wall_ms else None),
                    ArtifactBytesPerSecond=(artifact_bytes * 1000 / total_wall_ms
                                            if artifact_bytes is not None and total_wall_ms else None),
                    ShardProcessingDuration=shard_duration,
                    Shards=shards, Attempts=attempts, DownloadAttempts=downloads,
                    Stages=stages, Workers=list(workers.values()),
                    RpcEvents=manifest.get('RpcEvents'),
                    SchedulerStallWallMs=manifest.get('SchedulerStallWallMs'))

    def save_json(self, path):
        Path(path).write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding='utf-8')
        return Path(path)

    def save_csv(self, path):
        with Path(path).open('w', newline='', encoding='utf-8-sig') as stream:
            fields = ('ShardId', 'TaskId', 'AssignmentId', 'WorkerInstanceId', 'AttemptNumber',
                      'RemoteAttemptNumber', 'DownloadAttemptNumber', 'Stage',
                      'WallMs', 'ActiveMs', 'Source')
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
            writer.writeheader(); writer.writerows(self.data['Stages'])
        return Path(path)

    def compare(self, other):
        previous = other.data if isinstance(other, ExportProfile) else other
        fields = ('ModelKey', 'ModelVersionUtc', 'Options', 'SelectionIdentity', 'ShardSize', 'EligibleWorkers')
        warnings = [field for field in fields if self.data.get(field) != previous.get(field)]
        current = self.data['TotalWallMs']; prior = previous.get('TotalWallMs')
        scaling_fields = tuple(field for field in fields if field != 'EligibleWorkers')
        scaling_comparable = (all(self.data.get(field) is not None and
                                  self.data.get(field) == previous.get(field) for field in scaling_fields)
                              and self.data.get('Complete') is True and previous.get('Complete') is True
                              and isinstance(self.data.get('EligibleWorkers'), int)
                              and isinstance(previous.get('EligibleWorkers'), int)
                              and self.data['EligibleWorkers'] > 0 and previous['EligibleWorkers'] > 0
                              and current is not None and prior is not None and current > 0)
        speedup = prior / current if scaling_comparable else None
        efficiency = (speedup * previous['EligibleWorkers'] / self.data['EligibleWorkers']
                      if scaling_comparable else None)
        return dict(Comparable=not warnings, Differences=warnings, CurrentWallMs=current,
                    PreviousWallMs=prior, WallChangeMs=current-prior if current is not None and prior is not None else None,
                    ScalingComparable=scaling_comparable, Speedup=speedup, ScalingEfficiency=efficiency)

    def _timeline_html(self):
        def instant(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
            except (ValueError, TypeError, AttributeError):
                return None
        lanes = []
        for shard in self.data['Shards']:
            current = shard.get('PreviousAttempts', []) + [shard]
            for attempt, item in enumerate(current):
                times = item.get('StatusTimes') or {}
                created = instant(times.get('CreatedAtUtc'))
                assigned = instant(times.get('AssignedAtUtc'))
                started = instant(times.get('StartedAtUtc'))
                prepared = instant(times.get('PreparedAtUtc'))
                completed = instant(times.get('CompletedAtUtc'))
                origin = created if created is not None else (assigned if assigned is not None else started)
                if origin is not None and completed is not None and completed >= origin:
                    lanes.append((shard['ShardId'], attempt, item.get('WorkerInstanceId'),
                                  origin, assigned, started, prepared, completed, created))
        if not lanes:
            return '<p>UTC события для timeline недоступны.</p>'
        first = min(item[3] for item in lanes)
        last = max(item[7] for item in lanes)
        span = max(last-first, .001)
        rows = []
        for shard_id, attempt, worker, origin, assigned, started, prepared, completed, created in lanes:
            def segment(start, end, kind):
                left = 100*(start-first)/span
                width = max(.2, 100*(end-start)/span)
                return f'<span class="{kind}" style="left:{left:.4f}%;width:{width:.4f}%"></span>'
            points = [(i, value) for i, value in enumerate((created, assigned, started, prepared, completed))
                      if value is not None]
            if any(right[1] < left[1] for left, right in zip(points, points[1:])):
                bars = segment(origin, completed, 'unknown')
            else:
                kinds = ('queued', 'assigned', 'processing', 'delivery')
                bars = ''.join(segment(left[1], right[1], kinds[left[0]]
                                       if right[0] == left[0] + 1 else 'unknown')
                               for left, right in zip(points, points[1:]))
            label = escape(f'Shard {shard_id}, attempt {attempt+1}, {worker or "worker unknown"}')
            rows.append(f'<div class="timeline-row"><span>{label}</span><div class="lane">{bars}</div></div>')
        return ('<p>Шкала: '+escape(datetime.fromtimestamp(first, timezone.utc).isoformat())+
                ' → '+escape(datetime.fromtimestamp(last, timezone.utc).isoformat())+
                '</p><p>Серый: очередь; голубой: ожидание запуска; синий: обработка; зелёный: доставка и публикация; '
                'штриховка: фаза неизвестна. UTC события одного Manager определяют границы, '
                'а длительности стадий берутся из монотонных часов.</p>'+''.join(rows))

    def _bottleneck_html(self):
        known = {}
        for row in self.data['Stages']:
            if row['Stage'] == 'worker-processing':
                continue  # Encloses SDK, encoding, and artifact preparation.
            value = row['WallMs'] if row['WallMs'] is not None else row['ActiveMs']
            if value is not None:
                known[row['Stage']] = known.get(row['Stage'], 0) + value
        if not known:
            return '<p>Измерений пока недостаточно.</p>'
        stage, total = max(known.items(), key=lambda item: item[1])
        return ('<p>Наибольшая сумма известных интервалов: '+escape(stage)+f' — {total:.1f} ms. '
                'Перекрывающиеся и вложенные интервалы нельзя складывать в end-to-end wall time.</p>')

    def save_html(self, path, *, compare=None):
        data = self.data
        def cell(value):
            return escape('—' if value is None else str(value))
        def table(fields, rows):
            head = ''.join('<th>'+cell(name)+'</th>' for name in fields)
            body = ''.join('<tr>'+''.join('<td>'+cell(row.get(name))+'</td>' for name in fields)+'</tr>' for row in rows)
            return '<table><thead><tr>'+head+'</tr></thead><tbody>'+body+'</tbody></table>'
        comparison = ''
        if compare is not None:
            result = self.compare(compare)
            differences = result['Differences']
            warning = ('<p class="warning">Запуски нельзя считать сопоставимыми: '+
                       ', '.join(cell(field) for field in differences)+'.</p>') if differences else ''
            comparison = ('<section><h2>Сравнение запусков</h2>'+warning+
                          table(('CurrentWallMs','PreviousWallMs','WallChangeMs','Speedup','ScalingEfficiency'), [result])+
                          '<p>Масштабирование оценивается только для завершённых запусков с одинаковыми '
                          'моделью, выборкой, настройками и размером шарда. Число workers может различаться; '
                          'efficiency = speedup / отношение числа workers.</p></section>')
        errors = [dict(ShardId=a['ShardId'], AttemptNumber=a['AttemptNumber'],
                       Error=a['Error']) for a in data['Attempts'] if a['Error']]
        html = ('<!doctype html><html lang="ru"><meta charset="utf-8">'
                '<title>BIM Bridge performance profile</title><style>'
                'body{font:15px system-ui;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#1d2733}'
                'table{border-collapse:collapse;width:100%;margin:1rem 0}th,td{padding:.45rem;border-bottom:1px solid #ddd;text-align:left}'
                'th{background:#eef3f8}section{overflow-x:auto}.kpi{display:flex;gap:1rem;flex-wrap:wrap}'
                '.kpi div{padding:1rem;background:#eef3f8;border-radius:.5rem}'
                '.warning{padding:1rem;background:#fff0cd;border-left:4px solid #c27a00}'
                '.timeline-row{display:grid;grid-template-columns:240px 1fr;gap:1rem;align-items:center;margin:.35rem 0}'
                '.lane{position:relative;height:18px;background:#eef3f8}.lane span{position:absolute;height:18px}'
                '.queued{background:#8896a5}.assigned{background:#78b7db}'
                '.processing{background:#3976bd}.delivery{background:#48a56a}'
                '.unknown{background:repeating-linear-gradient(45deg,#b7bfc8,#b7bfc8 4px,#e2e5e9 4px,#e2e5e9 8px)}</style>'
                '<h1>BIM Bridge performance profile</h1><p>Session: '+cell(data['ExportSessionId'])+'</p>'
                '<div class="kpi"><div>Wall: '+cell(data['TotalWallMs'])+' ms</div><div>Shards: '
                +cell(data['CompletedShards'])+'/'+cell(data['TotalShards'])+'</div><div>Active workers: '
                +cell(data['ActiveWorkers'])+'</div><div>Bytes: '
                +cell(data['ArtifactBytes'])+'</div><div>Sources/min: '
                +cell(data['EndToEndSourcesPerMinute'])+'</div><div>Artifact bytes/s: '
                +cell(data['ArtifactBytesPerSecond'])+'</div></div>'
                '<p>Отсутствующие измерения обозначены «—». UTC отметки разных машин показывают порядок событий; '
                'длительности рассчитаны монотонными часами соответствующего компонента.</p>'
                '<section><h2>Стадии</h2>'+table(('ShardId','AssignmentId','WorkerInstanceId','AttemptNumber','DownloadAttemptNumber','Stage','WallMs','ActiveMs','Source'),data['Stages'])+'</section>'
                '<section><h2>Timeline</h2>'+self._timeline_html()+'</section>'
                '<section><h2>Вероятное ограничение</h2>'+self._bottleneck_html()+'</section>'
                '<section><h2>Workers</h2><p>Processing utilization — сумма известных интервалов обработки '
                'worker / wall time сессии; при неполных измерениях значение неизвестно.</p>'+
                table(('WorkerInstanceId','Attempts','AssignedElements','ProcessedSources','ProcessingWallMs','ProcessingSourcesPerMinute','ProcessingUtilization'),data['Workers'])+'</section>'
                '<section><h2>Длительность обработки shards</h2>'+
                table(('Known','Total','MinMs','MedianMs','P95Ms','MaxMs'), [data['ShardProcessingDuration']])+'</section>'
                '<section><h2>Shards</h2>'+table(('ShardId','TaskId','WorkerInstanceId','State','ElementCount','StatusTimes','QueueWaitMs','MonitoringRpcWallMs','UploadWallMs','HashVerificationMs','ArtifactPublicationMs','DownloadWallMs','ManagerServeWallMs','ChunkTransferWallMs','LocalFinalizeWallMs','ValidationWallMs','ResumedBytes','DownloadedBytes','ChunkCalls','MeshCacheHits','MeshCacheMisses','SnapshotIndexHits','SnapshotIndexMisses'),data['Shards'])+'</section>'
                '<section><h2>Кэши worker</h2><p>Hits/misses относятся к задаче. Entries, bytes и evictions — снимок накопительного состояния worker при завершении задачи; их не суммируют по shards.</p>'+
                table(('ShardId','WorkerInstanceId','AttemptNumber','MeshCacheHits','MeshCacheMisses','SnapshotIndexHits','SnapshotIndexMisses','WorkerMeshCacheEntries','WorkerMeshCacheBytes','WorkerMeshCacheEvictions','WorkerSnapshotIndexEntries','WorkerSnapshotIndexBytes','WorkerSnapshotIndexEvictions'),data['Attempts'])+'</section>'
                '<section><h2>Назначения tasks</h2>'+table(('ShardId','TaskId','AssignmentId','WorkerInstanceId','AttemptNumber','RemoteAttemptNumber','State','TaskAcceptanceWallMs','AssignmentWallMs','ResultPreparationWallMs','QueueWaitMs','ProcessingWallMs','UploadRetryCount','UploadRetryWaitMs','Error'),data['Attempts'])+'</section>'
                '<section><h2>Попытки скачивания</h2>'+table(('ShardId','AttemptNumber','DownloadAttemptNumber','Success','ErrorCode','DownloadWallMs','ResumeValidationWallMs','ManagerServeWallMs','ChunkTransferWallMs','LocalFinalizeWallMs','ResumedBytes','DownloadedBytes','ChunkCalls'),data['DownloadAttempts'])+'</section>'
                '<section><h2>Ошибки и retries</h2>'+table(('ShardId','AttemptNumber','Error'),errors)+'</section>'
                +comparison+'</html>')
        Path(path).write_text(html, encoding='utf-8')
        return Path(path)
