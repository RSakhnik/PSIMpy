# Parallel spatial exports (0.3.1)

`BimBridge.export_spatial_parallel()` — additive API. Старые `export_spatial`, `iter_spatial_data`, `get_spatial_data` сохраняют поведение. Первый вариант parallel API принимает explicit composite ElementIds; distributed search не выполняется.

```python
with client.export_spatial_parallel(
    model_key, '/content/drive/MyDrive/ups-parallel',
    element_ids=elements,  # iterable: {ModelPartId, ElementId} или Element
    model_version_utc=pinned_version,  # можно опустить: pin через get-model-context
    include_descendants=True, include_meshes=False,
    shard_size=1000, task_prefetch=None, download_workers=2,
) as export:
    for artifact in export.artifacts():
        # Каждый файл уже прошёл size/SHA и проверку BBSPAT2/snapshot.
        process(artifact.spatial())
    export.wait()
    print(export.summary)
```

Метод возвращает handle сразу после подготовки selection. Submit/monitor/download работают в фоне независимо от consumer. Контекстный менеджер закрывает локальный pipeline; если итерация consumer прервана, remote tasks сохраняются. Для ожидания без streaming: `export.wait(timeout=3600)`; timeout не отменяет export.

## Selection, workers и лимиты

Snapshot закрепляется до selection. Если дата не передана, `get-model-context` вызывается для TargetModelKey, выбирается DataVersionUtc/VersionUtc соответствующей модели; неоднозначный или отсутствующий snapshot — ошибка. На resume дата читается из manifest, заново не выбирается.

ElementIds нормализуются; повторы исключаются глобально с сохранением первого появления через disk-backed SQLite index (cache 2 MiB, scratch максимум 1 GiB). Геометрия не кешируется. `max_source_elements=10_000_000` ограничивает число уникальных sources и настраивается. Selection выполняется один раз, списки каждого shard хранятся отдельно; worker получает только собственные IDs. Сбой до первого submit допускает повтор с тем же входным iterable: совпадающие selection-файлы переиспользуются.

Default shard_size=1000 (допустимо 1–1000). Для 38 748 уникальных sources создаётся 39 задач. Каждый shard — существующий `export-spatial`, один pinned ModelVersionUtc и тот же набор geometry flags. PreferredWorkerInstanceId=None. Не создаётся операция распределённого SDK или жёсткое закрепление shards за workers.

Eligible pool выбирается из GetWorkers: online/не UNREGISTERED, обе capabilities `export-spatial` + `spatial-binary-v2`, точное совпадение AvailableModelKeys с TargetModelKey. Один `resolve-model-key` не заменяет model match, поскольку Manager делает тот же точный фильтр. По умолчанию task_prefetch=min(128,max(2,2*eligible_workers)); настройка 1–128. Manager должен объявлять artifact-upload-v2 и artifact-chunk-download-v2. Отдельная capability parallel-export не нужна: используются существующие операции и transfer protocol. Legacy v1 без этих возможностей отвергается явно. Старый bulk-v2 worker может исполнять shards последовательно; overlap execution/upload требует обновлённого worker.

Queued/running SDK tasks и SUBMITTED ограничены task_prefetch. DELIVERING (RESULT_PREPARED/UPLOADING), COMPLETED_REMOTE и DOWNLOADING образуют отдельный delivery backlog. Default result_prefetch=max(2,2*download_workers), настройка 1–128. Intake останавливается при достижении этого порога; уже принятые SDK tasks могут завершиться, поэтому общий жёсткий предел inflight — task_prefetch+result_prefetch. Так download не занимает execution-prefetch слот. Потерянный submit с неизвестным TaskId также занимает слот. Если retry budget исчерпан, но remote task может ещё выполняться, scheduler прекращает новые submissions, завершает наблюдение остальных известных задач и сохраняет FAILED/PENDING для resume. Такая неопределённость не освобождает лимит для бесконечных новых tasks. В downloader одновременно максимум download_workers=2 (настройка 1–4), без внутренней неограниченной очереди futures. Каждый downloader thread переиспользует собственный transport; Pilot Custom API не сериализует download за RPC-lock монитора. Все дочерние transport закрываются после завершения pipeline.

Проверенные shard-файлы — конечный результат, а не очередь в RAM. Их суммарный размер ограничен `max_output_bytes=64 GiB`. В лимит включаются сохранённые старые partial/attempt artifacts и резерв для активных download. Manifest/selection scratch учитываются отдельно. При нехватке места shard получает RESOURCE_LIMIT, export не считается complete.

## Файлы и resume

Каталог содержит `manifest.json`, `summary.json`, `selections/shard-NNNNNN.json`, `shard-NNNNNN.bbsp`; незавершённые download используют прежние `.inprogress`/`.download.json`. Manifest сохраняет session/model/snapshot/options/source identity, shard identity/count, ClientRequestId, TaskId, state, retry count, metadata, destination, worker identity при доступности. JSON обновляется atomic replace с fsync файла. Один OS owner lock запрещает два одновременно пишущих Python scheduler в одном каталоге. Если mounted/FUSE filesystem не поддерживает advisory locks, применяется host-local OS lock в temp, а manifest и artifacts остаются на выбранном хранилище; WriterLockScope фиксируется в manifest. При таком fallback несколько Colab VM не должны одновременно писать один каталог Drive: distributed lock намеренно не вводится.

`export.close()` приостанавливает локальный pipeline, не отменяя remote tasks. После restart:

```python
export = client.export_spatial_parallel(
    model_key, same_directory, element_ids=None,
    shard_size=1000, include_descendants=True, include_meshes=False,
)
export.wait()
```

Нужно сохранить те же semantic flags, shard_size и connection identity. ElementIds можно не передавать; при передаче проверяется совпадение deduplicated selection. Локальные файлы проверяются по длине/SHA и BBSPAT2/snapshot до обращения к сети. Полностью проверенный export переиспользуется offline. Уже существующие remote tasks/results восстанавливаются и при отсутствии online workers (Manager остаётся нужен); fallback prefetch=2. Новая сессия без eligible workers завершается явной capability-ошибкой. Незавершённые TaskId опрашиваются повторно; download продолжает прежние chunks. Если только один task/result истёк, пересоздаётся только этот shard с прежним snapshot и новым ClientRequestId. Для новой immutable identity используется attempt-specific путь: старый partial не продолжается молча. Default max_shard_retries=2 на один запуск scheduler, настройка 0–20; исчерпание бюджета или постоянная ошибка сохраняются как FAILED. RetryCount сохраняет накопительную историю. Явный повторный запуск сессии даёт новый ограниченный retry budget и восстанавливает FAILED с retryable ошибкой: прежний TaskId/download, либо только affected remote shard после terminal/expired task. Постоянные ошибки автоматически не скрываются. Изменение snapshot никогда не подменяется новой датой.

`export.cancel()` — отмена всей выгрузки, а не пауза: сначала останавливается submitter/downloader, затем запрашивается отмена известных незавершённых TaskId. Принятый submit с потерянным ответом согласуется повтором того же ClientRequestId перед отменой. Ошибки отмены, PendingCancellations и PendingCancellationRequests сохраняются; повторный cancel повторяет согласование. После отмены незавершённая сессия не возобновляет задания автоматически; для новой выгрузки нужен новый каталог. Артефакты и partial не удаляются. Отмена скачивания кооперативная после текущего bounded chunk/RPC.

## Порядок и geometry

`artifacts()` выдаёт READY artifacts в порядке готовности. `artifacts(completion_order=False)` даёт детерминированный порядок shard index; это задерживает только consumer, но не Manager/workers/download. `iter_sources()` выдаёт Source, Result и ссылку Artifact, не накапливает mesh arrays или все instances source в памяти. Для instances/buffers используются frames()/buffers() конкретного artifact; mesh keys всегда остаются в его локальной области.

IncludeMeshes=false — предпочтительный первый стендовый вариант. При IncludeMeshes=true одинаковое определение может законно повторяться в разных shard-файлах. Полного rewrite/merge и global cross-worker mesh cache нет. `export.mesh_duplication()` после завершения вычисляет число mesh definitions, уникальных mesh keys и повторные buffer bytes через ограниченный SQLite index, без изменения файлов. Это потенциальные повторные payload bytes, без JSON/frame headers. Тест с шестью shards: 144 bytes buffers, из них 120 повторных, все шесть artifacts независимо читаются и дают правильные transformed vertices.

## Метрики и диагностика

`export.manifest`, `export.summary`: состояния, SubmittedShards, EligibleWorkers, TaskPrefetch, DownloadConcurrency, TotalWall, AggregateElementsPerMin, ArtifactBytes. TotalWall — сумма monotonic wall длительностей активных Python запусков с восстановлением, включая transport/validation, без времени выключенного Python. Summary обновляет текущую скорость при чтении; сохранённый summary фиксируется по окончании запуска.

WorkerProcessingElementsPerMin использует сумму worker-reported monotonic operation wall durations: выполнение dispatcher handler, SDK/serialization/artifact write-finalize и дополнительная JSON-сериализация результата. Время reservation/heartbeat RPC, публикации spool journal, ожидания uploader, prepare acknowledgement, передачи и retry в эту метрику не входит. Это execution-visible processing, включая ожидания внутри SDK-операции, не чистый SDK active time. Измерение сохраняется в durable spool, upload descriptor и task state Manager, поэтому reconnect/restart не подменяют его календарным временем. При отсутствии измерений у старого worker возвращается None; SDKActiveElementsPerMin всегда None. `client.statistics()` доступен через существующий token binding и через Pilot Custom API.

Synthetic diagnostic: `PYTHONPATH=src;tests python tests/benchmark_parallel.py --output scaling.json --sdk-seconds 1 --upload-seconds .3 --download-seconds .3`. Это fake SDK regression diagnostic, не обещание production scaling.

## Стенд на двух машинах

Открыть одну и ту же модель/snapshot на двух Pilot-BIM, проверить разные MachineName/DurableWorkerId и независимые spool owner locks. Обновление Manager/worker устанавливается отдельно в согласованное окно. Проверить workers(), capabilities и model keys. Сначала выполнить небольшой bounds-only parallel export, сопоставить Sources/геометрию с одиночным API; проверить распределение TaskId, overlap, queue<=2 и download<=2. Затем отключить один worker/сеть, перезапустить Python с тем же каталогом и проверить requeue/resume без повторного download READY файлов. После этого проверить full meshes, размер повторных buffers, квоты и disk/SHA bottlenecks. Реальные Pilot процессы и рабочий Colab автоматическими тестами не затрагиваются.

Crash recovery: пустой temporary artifact без initial journal безопасно инициализируется заново; неизвестный непустой partial по-прежнему отвергается. Owned shard-файлы публикуются через atomic replace, включая filesystem без hard links. Проверка process restart аварийно завершает Python без close(), запускает новый Python и подтверждает прежние TaskId/mtime READY файлов и ровно 20 assignments для 20 shards.
