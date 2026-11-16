"""Concurrent, durable property export for an explicit ID selection."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path

from .bulk import _atomic_json
from .errors import BimBridgeError, IncompleteResult
from .exports import export_elements
from .parallel import _pin
from .types import element_payload, guid, utc


def _key(row):
    return (guid(row['ModelPartId']), guid(row['ElementId']))


def iter_properties_parallel(client, model_key, element_ids, directory, *,
                             property_names=None, batch_size=500, concurrency=4,
                             model_version_utc=None, progress=None, max_batch_retries=2,
                             max_transport_resumes=2):
    """Yield verified batches; completed artifacts survive interruption.

    Each batch is an independent server export task. The Manager chooses an
    eligible worker; no worker affinity is imposed by this client.
    """
    if type(batch_size) is not int or batch_size <= 0 or type(concurrency) is not int or concurrency <= 0:
        raise ValueError('batch_size and concurrency must be positive integers')
    if type(max_batch_retries) is not int or max_batch_retries < 0:
        raise ValueError('max_batch_retries must be a nonnegative integer')
    if type(max_transport_resumes) is not int or max_transport_resumes < 0:
        raise ValueError('max_transport_resumes must be a nonnegative integer')
    model_key = guid(model_key)
    elements = [element_payload(e) for e in element_ids]
    keys = [_key(e) for e in elements]
    if len(keys) != len(set(keys)):
        raise ValueError('Input contains duplicate element IDs')
    version = utc(model_version_utc) if model_version_utc else _pin(client, model_key)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(json.dumps([model_key, version, elements, property_names, batch_size],
                                         sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    manifest_path = directory / 'properties.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        if manifest.get('Identity') != identity:
            raise ValueError('Properties directory belongs to different inputs or options')
    else:
        manifest = {'Identity': identity, 'ModelKey': model_key, 'ModelVersionUtc': version,
                    'BatchCount': (len(elements) + batch_size - 1) // batch_size}
        _atomic_json(manifest_path, manifest)
    batches = [elements[i:i+batch_size] for i in range(0, len(elements), batch_size)]

    def fetch(index):
        batch = batches[index]
        worker_client = client._parallel_fork() if hasattr(client, '_parallel_fork') else client
        try:
            for attempt in range(max_batch_retries + 1):
                destination = directory / f'batch-{index:08d}-attempt-{attempt}.ndjson.gz'
                for resume in range(max_transport_resumes + 1):
                    try:
                        artifact = export_elements(worker_client, model_key, destination,
                            element_ids=batch, property_names=property_names,
                            model_version_utc=version)
                        break
                    except BimBridgeError as error:
                        if (error.code == 'MODEL_CHANGED' or
                                not (error.retryable or error.code == 'CLIENT_TIMEOUT') or
                                resume == max_transport_resumes):
                            raise
                try:
                    records = artifact.records()
                    manifest = next(records, None)
                    if manifest and manifest.get('ModelVersionUtc') and utc(manifest['ModelVersionUtc']) != version:
                        raise BimBridgeError('MODEL_CHANGED', 'Property artifact belongs to another model version')
                    if not manifest or not manifest.get('ModelVersionUtc'):
                        raise IncompleteResult('Property artifact does not confirm model version',
                                               {'BatchIndex': index, 'Attempt': attempt})
                    rows = list(artifact.iter_elements(strict=True))
                    actual = [_key(row) for row in rows]
                    expected = [_key(row) for row in batch]
                    if len(actual) != len(set(actual)) or set(actual) != set(expected):
                        raise IncompleteResult('Property artifact contains missing, duplicate or foreign IDs',
                                               {'BatchIndex': index, 'Expected': expected, 'Actual': actual})
                except IncompleteResult:
                    if attempt == max_batch_retries:
                        raise
                    continue
                return index, {'Elements': rows, 'ModelVersionUtc': version,
                               'SnapshotVerified': True, 'BatchIndex': index}
        finally:
            if worker_client is not client:
                worker_client.close()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(fetch, i) for i in range(len(batches))]
        completed = 0
        for future in as_completed(futures):
            index, result = future.result()
            completed += 1
            if progress:
                progress(completed, len(batches), index)
            yield result
