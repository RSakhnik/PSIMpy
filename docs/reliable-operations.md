# Reliable operations: capabilities and recovery boundaries

This release adds client-side restartable search, relation traversal, property
export, schema sampling and durable REST attempt events. The low-level methods
remain available. The integration has been tested with fake Manager/worker
responses only; it has not been exercised against a production BIM model.

## Server requirements

* `iter_search_complete` requires `paged-search-v2`. A page must report
  `ModelVersionUtc`, `HasMore`, and a valid cursor and worker instance ID when
  more pages remain. The client restarts the search after `CURSOR_EXPIRED` and
  excludes acknowledged composite IDs. It will not continue across a model
  version change.
* `iter_properties_parallel` requires worker `export-elements` with explicit
  `ElementIds` and Manager `artifact-upload-v2`. The worker source contract
  accepts this selection without `Conditions` or `ModelPartIds`. The Manager
  chooses an eligible worker for each task. Every artifact must have a complete
  summary and a matching model version.
* Durable relation traversal requires a worker that confirms
  `ModelVersionUtc` in `get-element-relations` responses.

## Recovery boundaries

* A lost POST response is ambiguous. The reviewed Manager implementation
  deduplicates a repeated `ClientRequestId` while its task record exists and
  returns the original `TaskId`. The REST client therefore retries only a
  transport failure without an HTTP response, with the identical request ID
  and body and a finite `submit_retries` budget. It never changes IDs during
  that recovery. Once Manager retention expires, the client cannot prove that
  an older task once existed. Direct Pilot transport retains its prior policy.
  `BimBridgeError.failure_kind` marks that POST transport outcome as
  `ambiguous`; it does not attribute the cause to a particular worker.
* An expired cursor cannot be resumed in place. Search restarts from page one
  at the pinned version, with a bounded restart and page budget. The caller
  may receive the last yielded element again after interruption; output sinks
  should use `(ModelPartId, ElementId)` as their idempotency key.
* Result metadata may report `ExpiresAtUtc`; `Task.download()` rejects an
  already-expired result before fetching content. The reviewed task-status
  contract does not expose a task TTL, so the client cannot predict task
  expiration from status alone. Missing expiry metadata remains unknown.
* The property export keeps verified artifacts and uses separate bounded
  attempts for incomplete batches. A model version mismatch is terminal.
* Checkpoint and search state must remain on persistent storage, with adjacent
  SQLite ledgers and artifact files. A Colab runtime's temporary filesystem
  does not survive deletion of the runtime. Mounted cloud filesystems may not
  offer atomic rename or advisory locks with the same guarantees as local
  filesystems; validate that storage before using it for critical exports.
  `Checkpoints` writes a SHA-256 sidecar for completed JSON results and
  re-fetches a result whose sidecar or content is missing or inconsistent.
* `EventJournal` records REST and direct Pilot transport attempts, including
  failures before a successful retry. The journal does not yet cover every
  worker/Manager lifecycle transition. `SessionProfile` reports
  overlapping network intervals without adding them into wall time, and
  leaves unavailable worker/queue metrics as unknown. Existing
  `ParallelExport` manifests and `ExportProfile` retain their own richer
  shard telemetry. `ExportProfile` also reports error counts, recovered shards,
  first-attempt success and a labeled estimate of potentially lost work when
  prior-attempt processed-source counts are available. It does not present
  unmeasured reprocessing as zero.
* Schema sampling can stop because the search was exhausted, a budget was
  reached, or new schemas stopped appearing in the sampled groups. Only
  exhaustion plus loading every searched element is marked as a complete
  inventory. A stable sample is not proof of full coverage.
