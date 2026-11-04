"""Bounded shard submission, monitoring and durable binary artifact downloads."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, closing
import copy
import errno
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
import threading
import time
import uuid

from .bulk import _atomic_json
from .errors import BimBridgeError, TaskTimeout, TaskFailed
from .exports import ExportArtifact
from .types import element_payload, guid, utc

READY = {'READY', 'CONSUMED'}
REMOTE = {'SUBMITTED', 'QUEUED', 'RUNNING', 'DELIVERING', 'COMPLETED_REMOTE', 'DOWNLOADING'}
DELIVERY = {'DELIVERING', 'COMPLETED_REMOTE', 'DOWNLOADING'}
EXPIRED = {'TASK_NOT_FOUND', 'RESULT_EXPIRED', 'TRANSFER_EXPIRED', 'TASK_EXPIRED', 'EXPIRED', 'UPLOAD_NOT_FOUND'}
WORKER_CAPABILITIES = {'export-spatial', 'spatial-binary-v2'}
MANAGER_CAPABILITIES = {'artifact-upload-v2', 'artifact-chunk-download-v2'}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _local_valid(path, metadata):
    if not metadata or not path.is_file() or path.stat().st_size != metadata.get('TotalLength'):
        return False
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest() == str(metadata.get('Sha256', '')).lower()


class _SessionLock:
    @staticmethod
    def _acquire(stream):
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _open(path):
        stream = path.open('a+b')
        if stream.seek(0, 2) == 0:
            stream.write(b'0'); stream.flush()
        return stream

    def __init__(self, directory):
        self.local_fallback = False
        self.stream = self._open(directory / '.session.lock')
        try:
            self._acquire(self.stream)
        except OSError as error:
            self.stream.close()
            unsupported = {errno.ENOSYS, getattr(errno, 'ENOTSUP', -1), getattr(errno, 'EOPNOTSUPP', -2)}
            if error.errno not in unsupported:
                raise BimBridgeError('EXPORT_IN_USE', 'Этот parallel export уже выполняется') from error
            # FUSE/mounted storage may not implement advisory locks. Durable data
            # stays on the selected filesystem; only the OS owner lock is local.
            user = str(os.getuid()) if hasattr(os, 'getuid') else 'user'
            root = Path(tempfile.gettempdir()) / ('psimpy-owner-locks-'+user)
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            key = hashlib.sha256(os.path.normcase(str(directory)).encode()).hexdigest()
            self.stream = self._open(root / (key+'.lock'))
            try:
                self._acquire(self.stream)
            except OSError as fallback_error:
                self.stream.close()
                code = 'LOCK_UNAVAILABLE' if fallback_error.errno in unsupported else 'EXPORT_IN_USE'
                raise BimBridgeError(code, 'Не удалось получить локальный owner lock export') from fallback_error
            self.local_fallback = True

    def close(self):
        self.stream.close()


def eligible_workers(client, model_key):
    workers = client.workers()
    if isinstance(workers, dict):
        workers = workers.get('Workers', [])
    # Match Manager pull scheduling exactly: resolver capability is not a model key.
    return [w for w in workers if str(w.get('State', '')).upper() not in {'OFFLINE', 'UNREGISTERED'}
            and WORKER_CAPABILITIES.issubset(w.get('Capabilities', []))
            and model_key in [str(k).lower() for k in w.get('AvailableModelKeys', [])]]


def _pin(client, model_key):
    context = client.get_model_context(target_model_key=model_key)
    candidates = [m for m in context.get('Models', [])
                  if str(m.get('ModelKey', m.get('ModelId', ''))).lower() == model_key
                  or model_key in [str(p).lower() for p in m.get('ModelPartIds', [])]]
    if len(candidates) != 1:
        raise BimBridgeError('SNAPSHOT_UNAVAILABLE', 'Нельзя однозначно определить snapshot; задайте model_version_utc')
    version = candidates[0].get('DataVersionUtc') or candidates[0].get('VersionUtc')
    if not version:
        raise BimBridgeError('SNAPSHOT_UNAVAILABLE', 'Model context не содержит snapshot')
    return utc(version)


class ParallelExport:
    """A logical export; background scheduling continues while artifacts are consumed.

    Use as a context manager, or call wait()/close(). Final artifacts are retained.
    artifacts(completion_order=False) provides deterministic source-shard ordering.
    """
    def __init__(self, client, model_key, destination, *, element_ids=None, model_version_utc=None,
                 include_descendants=True, include_meshes=False, include_normals=False,
                 include_indices=True, include_edge_indices=False, shard_size=1000,
                 task_prefetch=None, result_prefetch=None, download_workers=2, max_shard_retries=2,
                 max_output_bytes=64*1024**3, max_source_elements=10000000, poll_interval=None):
        for name, value, minimum, maximum in (
                ('shard_size', shard_size, 1, 1000), ('download_workers', download_workers, 1, 4),
                ('max_shard_retries', max_shard_retries, 0, 20), ('max_output_bytes', max_output_bytes, 1, 2**63-1),
                ('max_source_elements', max_source_elements, 1, 100000000)):
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f'{name}: требуется целое от {minimum} до {maximum}')
        if task_prefetch is not None and (type(task_prefetch) is not int or not 1 <= task_prefetch <= 128):
            raise ValueError('task_prefetch: требуется целое 1–128')
        if result_prefetch is not None and (type(result_prefetch) is not int or not 1 <= result_prefetch <= 128):
            raise ValueError('result_prefetch: требуется целое 1–128')
        self._result_prefetch = max(2, 2*download_workers) if result_prefetch is None else result_prefetch
        self.client = client; self.directory = Path(destination).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self._owner = _SessionLock(self.directory)
        self._lock = threading.RLock(); self._changed = threading.Condition(self._lock)
        self._cancel = threading.Event(); self._stop = threading.Event(); self._done = threading.Event()
        self._exception = None; self._thread = None
        self._interval = client.poll_interval if poll_interval is None else poll_interval
        if self._interval <= 0:
            self._owner.close(); raise ValueError('poll_interval должен быть положительным')
        self._download_workers = download_workers; self._max_retries = max_shard_retries
        self._max_source_elements = max_source_elements
        self._output_limit = max_output_bytes; self._started = time.monotonic(); self._ended = None
        self._last_save_ns = 0
        self._scheduler_wait_ms = 0.0
        self._manifest_path = self.directory / 'manifest.json'
        self._download_local = threading.local(); self._download_clients = []
        try:
            model_key = guid(model_key)
        except BaseException:
            self._owner.close(); raise
        options = dict(IncludeDescendants=bool(include_descendants), IncludeMeshes=bool(include_meshes),
                       IncludeNormals=bool(include_normals), IncludeIndices=bool(include_indices),
                       IncludeEdgeIndices=bool(include_edge_indices))
        source = _digest([client._http.base_url, client._http._token])
        try:
            resuming = self._manifest_path.exists()
            if resuming:
                self._manifest = json.loads(self._manifest_path.read_text(encoding='utf-8'))
                m = self._manifest
                if (m.get('FormatVersion') != 1 or m.get('ModelKey') != model_key or m.get('Options') != options
                        or m.get('SourceIdentity') != source or m.get('ShardSize') != shard_size
                        or (model_version_utc is not None and m['ModelVersionUtc'] != utc(model_version_utc))):
                    raise BimBridgeError('EXPORT_CONFLICT', 'Каталог принадлежит другому export')
                if m.get('Status') == 'CANCELLED' and not m.get('Complete'):
                    raise BimBridgeError('EXPORT_CANCELLED', 'Export отменён; для приостановки и resume используйте close(), для новой выгрузки — новый каталог')
                if element_ids is not None:
                    self._check_elements(element_ids)
            else:
                if element_ids is None:
                    raise ValueError('Новый parallel export требует explicit element_ids')
                pin_started = time.perf_counter_ns()
                version = utc(model_version_utc) if model_version_utc is not None else _pin(client, model_key)
                pin_wall_ms = ((time.perf_counter_ns() - pin_started) / 1e6
                               if model_version_utc is None else None)
                self._manifest = dict(FormatVersion=1, ExportSessionId=str(uuid.uuid4()), SourceIdentity=source,
                                      ModelKey=model_key, ModelVersionUtc=version, Options=options, ShardSize=shard_size,
                                      Shards=[], Complete=False, TotalWall=0, Status='PREPARING',
                                      SnapshotPinWallMs=pin_wall_ms)
                preparation_started = time.perf_counter_ns()
                self._write_elements(element_ids)
                self._manifest['SelectionShardingWallMs'] = (time.perf_counter_ns() - preparation_started) / 1e6
            self._validate_manifest_state()
            # Verify local immutable artifacts before consulting the network.
            for shard in self._manifest['Shards']:
                shard.pop('_ReplacementBytes', None)
                path = self._path(shard['Destination'])
                cached_valid = _local_valid(path, shard.get('Metadata'))
                error = shard.get('Error') or {}
                if not cached_valid and shard['State'] == 'FAILED' and (error.get('Retryable') or error.get('Code') in EXPIRED):
                    if error.get('RemoteTerminal') or error.get('Code') in EXPIRED:
                        self._new_attempt(shard)
                    else:
                        shard['RetryCount'] += 1
                        shard['State'] = 'COMPLETED_REMOTE' if shard.get('Metadata') else ('QUEUED' if shard.get('TaskId') else 'SUBMITTED')
                    shard.pop('Error', None)
                # RetryCount is cumulative; each explicit restart has a fresh
                # bounded automatic retry budget, without losing history.
                shard['RunRetryBase'] = shard['RetryCount']
                if cached_valid:
                    self._validate(path, shard['Metadata'])
                    shard['State'] = 'READY'
                elif shard['State'] in READY:
                    shard['State'] = 'COMPLETED_REMOTE' if shard.get('TaskId') else 'PENDING'
                elif shard['State'] == 'DOWNLOADING':
                    shard['State'] = 'COMPLETED_REMOTE'
            pending = any(s['State'] not in READY for s in self._manifest['Shards'])
            if pending:
                if not MANAGER_CAPABILITIES.issubset(client.health().get('Capabilities', [])):
                    raise BimBridgeError('CAPABILITY_NOT_AVAILABLE', 'Manager требует artifact-upload-v2 и artifact-chunk-download-v2')
                self._eligible = len(eligible_workers(client, model_key))
                if not self._eligible and not (resuming and any(s.get('TaskId') for s in self._manifest['Shards'])):
                    raise BimBridgeError('CAPABILITY_NOT_AVAILABLE', 'Нет eligible export-spatial/spatial-binary-v2 workers для модели')
            else:
                self._eligible = 0
            self._prefetch = task_prefetch if task_prefetch is not None else min(128, max(2, 2*self._eligible))
            self._prior_wall = self._manifest.get('TotalWall', 0)
            self._scheduler_wait_ms = self._manifest.get('SchedulerWaitWallMs', 0.0)
            self._manifest.update(Status='RUNNING', TaskPrefetch=self._prefetch,
                                  DownloadConcurrency=download_workers, EligibleWorkers=self._eligible, ResultPrefetch=self._result_prefetch,
                                  WriterLockScope='host-local-fallback' if self._owner.local_fallback else 'filesystem')
            self._save()
            self._thread = threading.Thread(target=self._run, name='psimpy-export-scheduler', daemon=True)
            self._thread.start()
        except BaseException:
            self._owner.close()
            raise

    def _validate_manifest_state(self):
        try:
            m = self._manifest; shards = m['Shards']
            if not isinstance(shards, list) or m['ShardCount'] != len(shards) or not utc(m['ModelVersionUtc']):
                raise ValueError('invalid session shape')
            count = 0
            for number, shard in enumerate(shards):
                if (shard['ShardId'] != number or shard['State'] not in READY | REMOTE | {'PENDING', 'FAILED'}
                        or type(shard['ElementCount']) is not int or not 1 <= shard['ElementCount'] <= m['ShardSize']
                        or type(shard['RetryCount']) is not int or shard['RetryCount'] < 0):
                    raise ValueError('invalid shard shape')
                guid(shard['ClientRequestId']); self._path(shard['Destination']); self._path(shard['Selection'])
                if shard.get('TaskId'):
                    guid(shard['TaskId'])
                elif shard['State'] not in {'PENDING', 'SUBMITTED', 'FAILED'}:
                    raise ValueError('missing task identity')
                count += shard['ElementCount']
            if count > self._max_source_elements:
                raise BimBridgeError('RESOURCE_LIMIT', 'max_source_elements exceeded')
        except (KeyError, TypeError, ValueError) as error:
            raise BimBridgeError('INTEGRITY_ERROR', 'Повреждён session manifest') from error

    def _path(self, relative):
        path = (self.directory / relative).resolve()
        if self.directory not in path.parents:
            raise BimBridgeError('INTEGRITY_ERROR', 'Путь shard выходит за каталог export')
        return path

    def _batches(self, elements):
        # Deterministic first-occurrence order with bounded-memory cross-shard
        # deduplication. Selection scratch is capped at 1 GiB, never geometry.
        with tempfile.TemporaryDirectory(prefix='psimpy-selection-') as directory:
            with closing(sqlite3.connect(str(Path(directory)/'selection.sqlite'))) as index:
                index.execute('PRAGMA page_size=4096')
                index.execute('PRAGMA journal_mode=OFF')
                index.execute('PRAGMA synchronous=OFF')
                index.execute('PRAGMA cache_size=-2048')
                index.execute('PRAGMA max_page_count=262144')
                index.execute('CREATE TABLE ids(part TEXT,element TEXT,PRIMARY KEY(part,element)) WITHOUT ROWID')
                batch = []; count = 0
                for element in elements:
                    value = element_payload(element)
                    try:
                        added = index.execute('INSERT OR IGNORE INTO ids VALUES(?,?)',
                                              (value['ModelPartId'], value['ElementId'])).rowcount
                    except sqlite3.OperationalError as error:
                        raise BimBridgeError('RESOURCE_LIMIT', 'Selection scratch quota exceeded') from error
                    if not added:
                        continue
                    count += 1
                    if count > self._max_source_elements:
                        raise BimBridgeError('RESOURCE_LIMIT', 'max_source_elements exceeded')
                    batch.append(value)
                    if len(batch) == self._manifest['ShardSize']:
                        yield batch; batch = []
                if batch:
                    yield batch

    def _write_elements(self, elements):
        selections = self.directory / 'selections'; selections.mkdir(exist_ok=True)
        for number, batch in enumerate(self._batches(elements)):
            name = f'selections/shard-{number:06d}.json'
            if self._path(f'shard-{number:06d}.bbsp').exists():
                raise FileExistsError('Каталог содержит artifact без session manifest')
            if self._path(name).exists():
                previous = json.loads(self._path(name).read_text(encoding='utf-8'))
                if _digest(previous) != _digest(batch):
                    raise BimBridgeError('EXPORT_CONFLICT', 'Изменилась ранее сохранённая selection')
            else:
                _atomic_json(self._path(name), batch)
            self._manifest['Shards'].append(dict(ShardId=number, Selection=name, ElementIdentity=_digest(batch),
                ElementCount=len(batch), Destination=f'shard-{number:06d}.bbsp', ClientRequestId=str(uuid.uuid4()),
                TaskId=None, State='PENDING', RetryCount=0, Metadata=None, WorkerInstanceId=None))
        self._manifest['ShardCount'] = len(self._manifest['Shards'])

    def _check_elements(self, elements):
        count = 0
        for count, batch in enumerate(self._batches(elements), 1):
            shards = self._manifest['Shards']
            if count > len(shards) or _digest(batch) != shards[count-1]['ElementIdentity']:
                raise BimBridgeError('EXPORT_CONFLICT', 'Изменился набор исходных элементов')
        if count != len(self._manifest['Shards']):
            raise BimBridgeError('EXPORT_CONFLICT', 'Изменилось число shards')

    def _save(self):
        with self._lock:
            shards = self._manifest['Shards']
            self._manifest['CompletedShards'] = sum(s['State'] in READY for s in shards)
            self._manifest['FailedShards'] = sum(s['State'] == 'FAILED' for s in shards)
            self._manifest['Complete'] = all(s['State'] in READY for s in shards)
            self._manifest['TotalWall'] = getattr(self, '_prior_wall', 0) + (self._ended or time.monotonic())-self._started
            self._manifest['SchedulerWaitWallMs'] = self._scheduler_wait_ms
            _atomic_json(self._manifest_path, self._manifest)
            self._last_save_ns = time.perf_counter_ns()
            self._changed.notify_all()

    def _validate(self, path, metadata):
        result = ExportArtifact(path, metadata['TaskId'], metadata).spatial().validate()
        if utc(result['Manifest'].get('ModelVersionUtc')) != self._manifest['ModelVersionUtc']:
            raise BimBridgeError('MODEL_CHANGED', 'Snapshot shard не совпал с export session')
        return result

    def _submit(self, shard):
        started = time.perf_counter_ns()
        elements = json.loads(self._path(shard['Selection']).read_text(encoding='utf-8'))
        if _digest(elements) != shard['ElementIdentity']:
            raise BimBridgeError('INTEGRITY_ERROR', 'Повреждён набор элементов shard')
        payload = dict(self._manifest['Options'], TargetModelKey=self._manifest['ModelKey'],
                       ModelVersionUtc=self._manifest['ModelVersionUtc'], ElementIds=elements)
        with self._lock:
            shard['State'] = 'SUBMITTED'; shard['SubmitAttempted'] = True
            self._save()  # idempotency key durable before RPC
        task = self.client.submit('export-spatial', payload, target_model_key=self._manifest['ModelKey'],
            required_capabilities=sorted(WORKER_CAPABILITIES), preferred_worker_instance_id=None,
            client_request_id=shard['ClientRequestId'], can_retry=True, max_attempts=3)
        with self._lock:
            shard.update(TaskId=task.id, State='QUEUED')
            shard['SubmitWallMs'] = shard.get('SubmitWallMs', 0) + (time.perf_counter_ns()-started)/1e6
            self._save()

    @contextmanager
    def _download_client(self):
        # One independent transport per downloader thread. Reuse its Pilot login
        # for all shards; never serialize downloads behind the monitor's RPC lock.
        client = getattr(self._download_local, 'client', None)
        if client is None:
            fork = getattr(self.client, '_parallel_fork', None)
            client = fork() if fork else self.client
            self._download_local.client = client
            if client is not self.client:
                with self._lock:
                    self._download_clients.append(client)
        yield client

    def _download(self, shard):
        def progress(received, total):
            if self._stop.is_set() or self._cancel.is_set():
                raise BimBridgeError('CLIENT_CANCELLED', 'Download остановлен; partial сохранён')
        path = self._path(shard['Destination'])
        started = time.perf_counter_ns()
        transfer = {}
        try:
            with self._download_client() as client:
                task = client.task(shard['TaskId'])
                task.download(path, overwrite=True, resume=True, progress=progress,
                              telemetry=transfer)  # owned path: atomic rename, including FUSE
        except Exception as error:
            transfer['DownloadWallMs'] = (time.perf_counter_ns()-started)/1e6
            try: error.download_telemetry = transfer
            except AttributeError: pass  # An unusual exception must keep its original type and code.
            raise
        download_ms = (time.perf_counter_ns()-started)/1e6
        validation_started = time.perf_counter_ns()
        if not _local_valid(path, shard['Metadata']):
            raise BimBridgeError('INTEGRITY_ERROR', 'Size/SHA-256 shard не совпал')
        self._validate(path, shard['Metadata'])
        return dict(DownloadWallMs=download_ms,
                    ValidationWallMs=(time.perf_counter_ns()-validation_started)/1e6,
                    **transfer)

    @staticmethod
    def _new_attempt(shard):
        previous = {key: copy.deepcopy(shard.get(key)) for key in
                    ('TaskId', 'ClientRequestId', 'WorkerInstanceId', 'State', 'Metadata',
                     'ProcessingWallMs', 'QueueWaitMs', 'HashVerificationMs', 'ArtifactPublicationMs',
                    'UploadWallMs', 'UploadRetryCount', 'UploadRetryWaitMs',
                    'SubmitWallMs', 'MonitoringRpcWallMs',
                     'ResumeValidationWallMs', 'ChunkTransferWallMs', 'LocalFinalizeWallMs',
                     'DownloadedBytes', 'ResumedBytes', 'ChunkCalls', 'ManagerServeWallMs',
                     'DownloadAttempts',
                     'SpatialPerformance', 'Diagnostics', 'StatusTimes', 'Error', 'DownloadWallMs', 'ValidationWallMs')}
        for key in ('AssignmentId', 'RemoteAttemptNumber', 'AttemptQueueWaitMs', 'ManagerAttempts',
                    'TaskAcceptanceWallMs', 'AssignmentWallMs', 'ResultPreparationWallMs'):
            previous[key] = copy.deepcopy(shard.get(key))
        if previous['TaskId'] or previous['Error']:
            shard.setdefault('Attempts', []).append(previous)
        for key in ('WorkerInstanceId', 'ProcessingWallMs', 'QueueWaitMs', 'HashVerificationMs',
                    'ArtifactPublicationMs', 'UploadWallMs', 'UploadRetryCount',
                    'UploadRetryWaitMs',
                    'SpatialPerformance', 'Diagnostics', 'StatusTimes',
                    'AssignmentId', 'RemoteAttemptNumber', 'AttemptQueueWaitMs', 'ManagerAttempts',
                    'TaskAcceptanceWallMs', 'AssignmentWallMs', 'ResultPreparationWallMs',
                    'SubmitWallMs', 'MonitoringRpcWallMs', 'DownloadWallMs', 'ValidationWallMs',
                    'ResumeValidationWallMs', 'ChunkTransferWallMs', 'LocalFinalizeWallMs',
                    'DownloadedBytes', 'ResumedBytes', 'ChunkCalls', 'ManagerServeWallMs',
                    'DownloadAttempts', 'Error'):
            shard.pop(key, None)
        shard['RetryCount'] += 1
        shard['AttemptNumber'] = shard.get('AttemptNumber', 0) + 1
        shard.update(TaskId=None, ClientRequestId=str(uuid.uuid4()), State='PENDING', Metadata=None,
                     SubmitAttempted=False, Destination=f"shard-{shard['ShardId']:06d}-attempt-{shard['AttemptNumber']}.bbsp")

    def _handle_error(self, shard, error):
        with self._lock:
            shard.pop('_ReplacementBytes', None)
            budget = shard['RetryCount']-shard.get('RunRetryBase', 0) < self._max_retries
            expired = isinstance(error, BimBridgeError) and error.code in EXPIRED
            remote_retry = isinstance(error, TaskFailed) and error.retryable
            if (expired or remote_retry) and budget:
                # New remote artifact gets a distinct immutable identity/path.
                self._new_attempt(shard)
            elif isinstance(error, BimBridgeError) and error.retryable and budget:
                shard['RetryCount'] += 1
                shard['State'] = 'COMPLETED_REMOTE' if shard.get('Metadata') else ('QUEUED' if shard.get('TaskId') else 'SUBMITTED')
            else:
                shard.update(State='FAILED', Error=dict(Code=getattr(error, 'code', 'CLIENT_ERROR'), Message=str(error),
                    Retryable=bool(getattr(error, 'retryable', False)), RemoteTerminal=isinstance(error, TaskFailed),
                    RemoteUncertain=bool((shard.get('SubmitAttempted') and not shard.get('TaskId')) or
                        (getattr(error, 'retryable', False) and not isinstance(error, TaskFailed) and not shard.get('Metadata')))))
            self._save()

    @staticmethod
    def _has_uncertain_failure(shards):
        return any(s['State'] == 'FAILED' and (s.get('Error') or {}).get('RemoteUncertain') for s in shards)

    def _submitter_tick(self, shards):
        # Reconcile existing intents first. A failed unknown request cannot block
        # reconciliation of the other already attempted requests.
        for shard in shards:
            if self._stop.is_set() or self._cancel.is_set():
                return
            if shard['State'] == 'SUBMITTED' and not shard.get('TaskId'):
                try:
                    self._submit(shard)
                except Exception as error:
                    self._handle_error(shard, error)
        if self._has_uncertain_failure(shards):
            return
        active = sum(s['State'] in {'SUBMITTED', 'QUEUED', 'RUNNING'} for s in shards)
        backlog = sum(s['State'] in DELIVERY for s in shards)
        inflight = sum(s['State'] in REMOTE for s in shards)
        for shard in shards:
            if self._stop.is_set() or self._cancel.is_set():
                return
            if shard['State'] != 'PENDING':
                continue
            if active >= self._prefetch or backlog >= self._result_prefetch or inflight >= self._prefetch+self._result_prefetch:
                break
            try:
                self._submit(shard)
            except Exception as error:
                self._handle_error(shard, error)
            if shard['State'] in REMOTE:
                active += 1; inflight += 1
            if (shard.get('Error') or {}).get('RemoteUncertain'):
                break

    def _collect_downloads(self, futures):
        for future, shard in list(futures.items()):
            if not future.done():
                continue
            del futures[future]
            try:
                timings = future.result()
                with self._lock:
                    shard.setdefault('DownloadAttempts', []).append(dict(timings, Success=True))
                    shard.update(timings)
                    shard['ReadySequence'] = 1 + max((s.get('ReadySequence', 0) for s in self._manifest['Shards']), default=0)
                    shard['State'] = 'READY'
                    shard.pop('_ReplacementBytes', None)
                    shard.pop('Error', None)
                    self._save()
            except Exception as error:
                with self._lock:
                    timing = getattr(error, 'download_telemetry', {})
                    if timing:
                        shard.setdefault('DownloadAttempts', []).append(dict(timing, Success=False,
                            ErrorCode=getattr(error, 'code', type(error).__name__)))
                        shard.update(timing)
                self._handle_error(shard, error)

    def _monitor_shards(self, shards):
        for shard in shards:
            if not shard.get('TaskId') or shard['State'] not in {'SUBMITTED', 'QUEUED', 'RUNNING', 'DELIVERING'}:
                continue
            try:
                rpc_started = time.perf_counter_ns()
                try:
                    status = self.client.task(shard['TaskId']).status()
                finally:
                    with self._lock:
                        shard['MonitoringRpcWallMs'] = shard.get('MonitoringRpcWallMs', 0) + (
                            time.perf_counter_ns() - rpc_started) / 1e6
                state = str(status.get('State', '')).upper()
                with self._lock:
                    changed = shard.get('RemoteState') != state
                    shard['RemoteState'] = state
                    owner = (status['WorkerInstanceId'] if 'WorkerInstanceId' in status
                             else shard.get('WorkerInstanceId'))
                    changed |= shard.get('WorkerInstanceId') != owner
                    shard['WorkerInstanceId'] = owner
                    for field in ('ProcessingWallMs', 'QueueWaitMs', 'HashVerificationMs',
                                  'ArtifactPublicationMs', 'UploadWallMs', 'UploadRetryCount',
                                  'UploadRetryWaitMs', 'SpatialPerformance', 'Diagnostics',
                                  'AssignmentId', 'AttemptQueueWaitMs',
                                  'TaskAcceptanceWallMs', 'AssignmentWallMs', 'ResultPreparationWallMs'):
                        if field in status:
                            changed |= shard.get(field) != status[field]
                            shard[field] = status[field]
                    if status.get('AttemptNumber') is not None:
                        changed |= shard.get('RemoteAttemptNumber') != status['AttemptNumber']
                        shard['RemoteAttemptNumber'] = status['AttemptNumber']
                    if status.get('PriorAttempts') is not None:
                        changed |= shard.get('ManagerAttempts') != status['PriorAttempts']
                        shard['ManagerAttempts'] = status['PriorAttempts']
                    times = {key: status[key] for key in
                             ('CreatedAtUtc', 'AssignedAtUtc', 'StartedAtUtc', 'PreparedAtUtc', 'CompletedAtUtc')
                             if status.get(key) is not None}
                    if times:
                        changed |= shard.get('StatusTimes') != times
                        shard['StatusTimes'] = times
                if state == 'COMPLETED':
                    rpc_started = time.perf_counter_ns()
                    try:
                        metadata = self.client.task(shard['TaskId']).metadata()
                    finally:
                        with self._lock:
                            shard['MonitoringRpcWallMs'] += (time.perf_counter_ns() - rpc_started) / 1e6
                    if str(metadata.get('TaskId', '')).lower() != shard['TaskId']:
                        raise BimBridgeError('INTEGRITY_ERROR', 'Metadata принадлежит другому TaskId')
                    with self._lock:
                        shard.update(State='COMPLETED_REMOTE', Metadata=metadata)
                    changed = True
                elif state in {'FAILED', 'CANCELLED', 'CANCELED', 'ABANDONED', 'EXPIRED'}:
                    error = status.get('TaskError') or {}
                    raise TaskFailed(error.get('Code', state), error.get('Message', state),
                                     retryable=bool(error.get('IsRetryable')))
                else:
                    with self._lock:
                        next_state = ('DELIVERING' if state in {'RESULT_PREPARED', 'RESULT_UPLOADING'}
                                      else 'QUEUED' if state == 'QUEUED' else 'RUNNING')
                        changed |= shard['State'] != next_state
                        shard['State'] = next_state
                # Poll timing can be checkpointed periodically; identity, state,
                # and newly reported server telemetry are durable immediately.
                if changed or time.perf_counter_ns() - self._last_save_ns >= 1_000_000_000:
                    self._save()
            except Exception as error:
                self._handle_error(shard, error)

    def _schedule_downloads(self, shards, futures, pool):
        for shard in shards:
            if shard['State'] != 'COMPLETED_REMOTE' or len(futures) >= self._download_workers:
                continue
            # Reserve full lengths for active downloads, independent of
            # concurrent atomic .inprogress -> final renames.
            reserved_shards = [s for s in shards if s['State'] in READY | {'DOWNLOADING'}] + [shard]
            reserved_names = {s['Destination'] for s in reserved_shards}
            reserved = sum(s['Metadata']['TotalLength'] + s.get('_ReplacementBytes', 0)
                           for s in reserved_shards)
            destination = self._path(shard['Destination'])
            replacement = destination.stat().st_size if destination.exists() else 0
            old_bytes = 0
            for path in self.directory.glob('shard-*'):
                name = path.name.removesuffix('.inprogress')
                if name.endswith('.bbsp') and name not in reserved_names:
                    try:
                        old_bytes += path.stat().st_size
                    except FileNotFoundError:
                        pass
            if reserved + old_bytes + replacement > self._output_limit:
                self._handle_error(shard, BimBridgeError('RESOURCE_LIMIT', 'max_output_bytes превышен'))
                continue
            with self._lock:
                shard['_ReplacementBytes'] = replacement
                shard['State'] = 'DOWNLOADING'
                self._save()
                download = copy.deepcopy(shard)
            futures[pool.submit(self._download, download)] = shard

    def _run(self):
        futures = {}
        try:
            with ThreadPoolExecutor(max_workers=self._download_workers, thread_name_prefix='psimpy-download') as pool:
                while not self._stop.is_set() and not self._cancel.is_set():
                    self._collect_downloads(futures)
                    shards = self._manifest['Shards']
                    self._submitter_tick(shards)
                    self._monitor_shards(shards)
                    self._schedule_downloads(shards, futures, pool)
                    if not futures and (all(s['State'] in READY | {'FAILED'} for s in shards) or
                            (self._has_uncertain_failure(shards) and not any(s['State'] in REMOTE for s in shards))):
                        break
                    wait_started = time.perf_counter_ns()
                    self._stop.wait(self._interval)
                    with self._lock:
                        self._scheduler_wait_ms += (time.perf_counter_ns() - wait_started) / 1e6
        except BaseException as error:
            self._exception = error
        finally:
            try:
                for client in self._download_clients:
                    try:
                        client.close()
                    except BaseException as error:
                        self._exception = self._exception or error
                with self._lock:
                    self._ended = time.monotonic()
                    self._manifest['Status'] = ('CANCELLED' if self._cancel.is_set() else 'STOPPED' if self._stop.is_set()
                                                else 'FAILED' if self._exception or self._manifest.get('FailedShards') else 'COMPLETED')
                    self._save()
                    _atomic_json(self.directory / 'summary.json', self.summary)
            except BaseException as error:
                self._exception = self._exception or error
                with self._lock:
                    self._manifest['Complete'] = False
            finally:
                with self._changed:
                    if not self._cancel.is_set():
                        self._owner.close()
                    self._done.set()
                    self._changed.notify_all()

    @property
    def manifest(self):
        with self._lock:
            return copy.deepcopy(self._manifest)

    def profile(self):
        """Return a portable snapshot, including completed and failed shards."""
        from .profile import ExportProfile
        return ExportProfile(self.manifest)

    @property
    def summary(self):
        m = self.manifest; shards = m['Shards']; wall = self._prior_wall + (self._ended or time.monotonic())-self._started
        counts = {state: sum(s['State'] == state for s in shards) for state in
                  ('SUBMITTED', 'QUEUED', 'RUNNING', 'DELIVERING', 'COMPLETED_REMOTE', 'DOWNLOADING', 'READY', 'CONSUMED', 'FAILED')}
        return dict(States=counts, SubmittedShards=sum(bool(s.get('TaskId')) for s in shards),
                    EligibleWorkers=self._eligible, TaskPrefetch=self._prefetch, DownloadConcurrency=self._download_workers, ResultPrefetch=self._result_prefetch,
                    MaxInflightShards=self._prefetch+self._result_prefetch,
                    UncertainRemoteShards=sum(bool((s.get('Error') or {}).get('RemoteUncertain')) for s in shards),
                    TotalWall=wall, AggregateElementsPerMin=(sum(s['ElementCount'] for s in shards if s['State'] in READY)*60/wall if wall else None),
                    WorkerProcessingElementsPerMin=self._processing_rate(shards), SDKActiveElementsPerMin=None,
                    ProcessingRateBasis='sum of worker-reported monotonic operation wall (SDK/encoding/artifact write-finalize), excludes reservation/heartbeat/prepare ack/spool publication/delivery/retry; SDK active unavailable',
                    ArtifactBytes=sum((s.get('Metadata') or {}).get('TotalLength', 0) for s in shards if s['State'] in READY))

    @staticmethod
    def _processing_rate(shards):
        ready = [s for s in shards if s['State'] in READY]
        if not ready or any(s.get('ProcessingWallMs') is None for s in ready):
            return None
        duration = sum(s['ProcessingWallMs'] for s in ready)
        return sum(s['ElementCount'] for s in ready)*60000/duration if duration > 0 else None

    def wait(self, timeout=None):
        if not self._done.wait(timeout):
            raise TaskTimeout('CLIENT_TIMEOUT', 'Parallel export продолжается; manifest сохранён')
        if self._exception:
            raise self._exception
        if not self.manifest['Complete']:
            raise BimBridgeError('INCOMPLETE_EXPORT', 'Parallel export неполный; см. manifest.json')
        return self

    def artifacts(self, *, completion_order=True):
        yielded = set()
        while True:
            with self._changed:
                shards = copy.deepcopy(self._manifest['Shards'])
                available = sorted((s for s in shards if s['ShardId'] not in yielded and s['State'] in READY), key=lambda s: s.get('ReadySequence', s['ShardId']))
                if not completion_order:
                    pending = next((s for s in shards if s['ShardId'] not in yielded), None)
                    available = [pending] if pending and pending['State'] in READY else []
                done = self._done.is_set()
                if not available and not done:
                    self._changed.wait(self._interval); continue
            for shard in available:
                yielded.add(shard['ShardId'])
                yield ExportArtifact(self._path(shard['Destination']), shard['TaskId'], shard['Metadata'])
                with self._lock:
                    self._manifest['Shards'][shard['ShardId']]['State'] = 'CONSUMED'
                    if not self._done.is_set():
                        self._save()
            if done and not available:
                self.wait(); return

    def mesh_duplication(self):
        """Measure repeated mesh buffer bytes without rewriting artifacts or caching geometry.

        Artifact-local mesh identities remain scoped to each shard. This diagnostic
        only estimates buffer bytes avoidable with an optional cross-shard dedup.
        """
        import sqlite3
        import tempfile
        self.wait()
        total = duplicate = definitions = 0
        with tempfile.TemporaryDirectory(prefix='psimpy-duplication-') as directory:
            with closing(sqlite3.connect(str(Path(directory)/'meshes.sqlite'))) as index:
                index.execute('PRAGMA cache_size=-2048')
                index.execute('CREATE TABLE meshes(key TEXT PRIMARY KEY) WITHOUT ROWID')
                for shard in self.manifest['Shards']:
                    repeated = False
                    reader = ExportArtifact(self._path(shard['Destination']), shard['TaskId'], shard['Metadata']).spatial()
                    for frame, section in reader.frames():
                        if frame['Type'] == 'mesh-begin':
                            definitions += 1
                            repeated = index.execute('INSERT OR IGNORE INTO meshes VALUES(?)', (frame['MeshDefinitionKey'],)).rowcount == 0
                        elif section is not None:
                            size = frame['Count']*4; total += size
                            if repeated:
                                duplicate += size
                unique = index.execute('SELECT COUNT(*) FROM meshes').fetchone()[0]
        return dict(MeshDefinitions=definitions, UniqueMeshDefinitions=unique, TotalMeshBufferBytes=total,
                    RepeatedMeshBufferBytes=duplicate, RepeatedBufferFraction=duplicate/total if total else 0,
                    ArtifactBytes=self.summary['ArtifactBytes'])

    def iter_sources(self, *, completion_order=True):
        for artifact in self.artifacts(completion_order=completion_order):
            current = None
            for frame, section in artifact.spatial().frames():
                if frame['Type'] == 'source-begin':
                    current = dict(Source=frame['Element'], Artifact=artifact)
                elif frame['Type'] == 'source-end':
                    current['Result'] = frame
                    yield current; current = None

    def cancel(self):
        self._cancel.set()
        # Stop submitter first: no task can be accepted after cancellation scan.
        if self._thread:
            self._thread.join()
        if self._owner.stream.closed:
            self._owner = _SessionLock(self.directory)
        try:
            for shard in self._manifest['Shards']:
                unknown_submit = not shard.get('TaskId') and shard.get('SubmitAttempted')
                if unknown_submit:
                    try:
                        # Reconcile an accepted-but-unacknowledged submit with the
                        # SAME idempotency key, then immediately cancel its TaskId.
                        self._submit(shard)
                    except Exception as error:
                        with self._lock:
                            shard['CancelAcknowledged'] = False
                            shard['CancelError'] = dict(Code=getattr(error, 'code', 'CLIENT_ERROR'), Message=str(error))
                if shard.get('TaskId') and shard['State'] not in READY:
                    try:
                        self.client.task(shard['TaskId']).cancel()
                        with self._lock:
                            shard['CancelAcknowledged'] = True
                            shard.pop('CancelError', None)
                    except BimBridgeError as error:
                        with self._lock:
                            shard['CancelAcknowledged'] = False
                            shard['CancelError'] = dict(Code=error.code, Message=str(error))
            with self._lock:
                self._manifest['PendingCancellations'] = [s['TaskId'] for s in self._manifest['Shards']
                                                           if s.get('TaskId') and s.get('CancelAcknowledged') is False]
                self._manifest['PendingCancellationRequests'] = [s['ClientRequestId'] for s in self._manifest['Shards']
                                                           if not s.get('TaskId') and s.get('CancelAcknowledged') is False]
                self._save()
            _atomic_json(self.directory / 'summary.json', self.summary)
        finally:
            self._owner.close()
        m = self.manifest
        return m['PendingCancellations'] + ['request:'+request for request in m['PendingCancellationRequests']]

    def close(self):
        """Suspend local pipeline without cancelling durable remote tasks."""
        self._stop.set()
        if self._thread:
            self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *error):
        self.close()


def export_spatial_parallel(client, model_key, destination, **options):
    return ParallelExport(client, model_key, destination, **options)
