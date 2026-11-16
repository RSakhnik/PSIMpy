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
* An expired cursor cannot be resumed in place. Search restarts from page one
  at the pinned version, with a bounded restart and page budget. The caller
  may receive the last yielded element again after interruption; output sinks
  should use `(ModelPartId, ElementId)` as their idempotency key.
* The property export keeps verified artifacts and uses separate bounded
  attempts for incomplete batches. A model version mismatch is terminal.
* Checkpoint and search state must remain on persistent storage, with adjacent
  SQLite ledgers and artifact files. A Colab runtime's temporary filesystem
  does not survive deletion of the runtime. Mounted cloud filesystems may not
  offer atomic rename or advisory locks with the same guarantees as local
  filesystems; validate that storage before using it for critical exports.
* `EventJournal` records REST transport attempts, including failures before a
  successful retry. Pilot direct transport and every worker/Manager lifecycle
  transition are not yet represented in this journal. `SessionProfile` reports
  overlapping network intervals without adding them into wall time, and
  leaves unavailable worker/queue metrics as unknown. Existing
  `ParallelExport` manifests and `ExportProfile` retain their own richer
  shard telemetry.
* Schema sampling can stop because the search was exhausted, a budget was
  reached, or new schemas stopped appearing in the sampled groups. Only
  exhaustion plus loading every searched element is marked as a complete
  inventory. A stable sample is not proof of full coverage.
