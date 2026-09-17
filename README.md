# Python-клиент BIM Bridge

Модули для обращения к BIM Workers Manager из Python и Google Colab.
Python 3.10 или новее. Сторонние библиотеки для работы клиента не нужны.
Проект можно опубликовать в собственном GitHub-репозитории.

## Подключение в Colab

```python
%pip install "git+https://github.com/RSakhnik/PSIMpy.git"
```

Затем создайте клиент. Токен вводится скрыто и не сохраняется в проекте:

```python
from getpass import getpass
from bim_bridge import BimBridge, Condition, Element, FileReference

bim = BimBridge(
    "https://360pilot.ru/bim-bridge",
    token=getpass("Токен BIM Bridge: "),
)
print(bim.health())
print(bim.workers())
```

Нужен токен API BIM Bridge. Если прокси требует именно Bearer, добавьте
`auth="bearer"`. По умолчанию используется `X-Api-Token`. Токен другого сервиса
может не подойти. Проверка TLS включена. Перенаправления запрещены, чтобы токен
не ушёл по другому адресу. Указывайте конечный URL API без `/health`.

В локальном Python можно использовать отдельный файл:

```python
bim = BimBridge(token_file="secrets/bim.token")
```

Файл содержит только токен. Не публикуйте его, вывод ячеек с данными и каталоги
выгрузок в GitHub. Для приватного репозитория отдельно настройте доступ Colab к
GitHub. Токен GitHub не является токеном BIM Bridge.

## Каталог моделей и частей

```python
from bim_bridge import select_parts

catalog = bim.model_context()
print(catalog["CatalogState"])
parts = select_parts(
    catalog,
    project="УПС - Установка производства серы",
    name_contains="-Piping",
)
for part in parts:
    print(part["DisplayName"], part["ModelKey"], part["ModelPartId"])
```

Название проекта сравнивается целиком без учёта регистра. Используйте название
из `catalog["Models"]`. `select_parts` по умолчанию отклоняет неполный каталог.
`allow_partial=True` разрешает выбрать только уже обнаруженные части. Это не
подтверждает полноту всей модели. Состояния `IsLoaded=false` и `on-demand` сами
по себе не означают отсутствие частей.

## Поиск, свойства и связи

```python
model_id = "d8ea8564-2e67-4f5f-98bc-a0f3f5ed54b8"
part_id = "8608d44f-b986-44a7-89a8-d1fd92dd2a65"

found = bim.search_elements(
    model_id,
    [Condition("Name", "Defined")],
    part_ids=[part_id],
    max_results=10,
)
print("Найдено:", found["Count"], "Усечено:", found["Truncated"])
elements = found["Elements"]
if elements:
    properties = bim.load_properties(model_id, elements)
    relations = bim.get_relations(model_id, elements,
                                 include_parent=True, include_children=True)
    print(properties)
    print(relations)
```

GUID здесь служат примером из УПС. Их доступность зависит от зарегистрированного
воркера. Можно передать GUID части вместо `model_id` без `part_ids`. Для нескольких
частей передавайте GUID общей модели и `part_ids`. Части разных моделей нельзя
объединять в одном запросе. Неизвестный GUID разрешает сам worker.

Ручная ссылка на элемент: `Element(part_id, element_id)`. Методы также принимают
словари `{"ModelPartId": "...", "ElementId": "..."}` из результата поиска.
`property_names=["Name", "GlobalId"]` ограничивает свойства. Без этого параметра
запрашиваются все доступные свойства. Проверяйте `FailedElementIds` и `Warnings`.
У связей проверяйте общий `Truncated` и `ChildrenTruncated` каждого элемента.

Условие имеет параметры `name`, `operator`, `value`, `category`, `data_type`.
Например, `Condition("Name", "Contains", "Pipe", data_type="String")`.
Условия соединяются через `logical_operator="And"` или `"Or"`.
Операторы SDK: `Equal`, `NotEqual`, `Defined`, `NotDefined`, `Contains`,
`NotContains`, `Wildcard`, `Regexp`, `Greater`, `Less`, `GreaterOrEqual`,
`LessOrEqual`. Поддержка зависит от типа свойства и индекса. Сравнения порядка
для строк не следует использовать как способ постраничного чтения.

## Файлы и Excel

```python
file = FileReference("ea19314c-35f4-4691-89da-d678c237314b")
bim.download_file(file, "SubSystemsStatus.xlsx")
excel = bim.read_excel(file, sheet_names=["TDSheet"],
                      include_formulas=True, include_empty_cells=False)
print(excel)
```

Поддерживаются три ссылки:

```python
# GUID объекта исходного файла.
file = FileReference(source_object_id)
# Документ и GUID физического IFile.
file = FileReference(document_id, file_id=physical_file_id)
# Документ и GUID Pilot-объекта исходного файла.
file = FileReference(document_id, file_id=source_object_id)
```

Дополнительно доступны `file_name` и `snapshot_created_at_utc`. Версия задаётся
временем снимка, например `"2026-08-03T12:00:00Z"`, а не отдельным VersionId.
При нескольких подходящих файлах нужно уточнить ссылку. Excel-операция работает
с `.xlsx`. Она возвращает исходную JSON-структуру API с типами ячеек, листами и
формулами. Клиент не подменяет её таблицей Pandas.

Скачивание потоковое, без Base64. Проверяются длина и SHA-256, если сервер передал
их в метаданных. Путь задаёте вы. Имя от сервера не используется как локальный
путь. Существующий файл сохраняется, если не передать `overwrite=True`.

## Задания, ожидание и большие результаты

```python
task = bim.submit("get-model-context", {})
print(task.id)                 # Сохраните ID для продолжения ожидания.
print(task.status())
task.wait(timeout=1800)
result = task.result()
metadata = task.metadata()

# Получить большой JSON сразу на диск, без его загрузки целиком в RAM.
task.download("catalog.json", decompress=True)

# После перезапуска блокнота:
task = bim.task(saved_task_id)
task.wait()
result = task.result()

# Запросить отмену при необходимости:
# task.cancel()
```

`submit` принимает любую операцию API. `run(operation, payload, ...)` объединяет
отправку, ожидание и получение JSON. Для бинарного результата используйте
`submit`, затем `wait` и `download`.

Параметры отправки: `target_model_key`, `preferred_worker_instance_id`,
`required_capabilities`, `priority`, `can_retry`, `max_attempts`,
`expires_at_utc`, `client_request_id`, `protocol_version`. Необязательное время
должно содержать часовой пояс. Для `get-model-context` автоматически используется
capability `model-context`. Не задавайте версию протокола без необходимости.

По умолчанию ожидание задания — 1800 секунд, опрос — раз в 2 секунды, HTTP-таймаут
— 60 секунд. Настройки клиента: `task_timeout`, `poll_interval`, `http_timeout`.
Клиентский таймаут не меняет лимит выполнения на сервере. Проверка общего времени
ожидания выполняется между HTTP-запросами. Текущий запрос может завершиться позже.
`task.wait(cancel_on_timeout=True)` отправит отмену по истечении времени. По
умолчанию задание продолжает работать и его можно снова открыть по ID.

`on_status` получает словарь состояния при каждом опросе. У скачивания `progress`
получает `(downloaded_bytes, total_bytes)`.

`Task.result()` ограничен `max_json_bytes` клиента, по умолчанию 128 MiB.
Потоковое `Task.download()` не имеет этого ограничения. При `decompress=True`
gzip распаковывается после проверки длины и хеша исходных переданных байтов.

## Полная выгрузка и продолжение

В API нет обычной пагинации `search-elements`. Один ответ с `Truncated=true` не
является полной выгрузкой. `iter_search_elements` последовательно обходит явно
заданные части. Большие выборки делятся дополнительными условиями по IFC
`GlobalId`. Этот режим рассчитан на индексы, где `GlobalId` соответствует GUID
элемента. Если безопасно разделить результат нельзя, возникает
`IncompleteResult`, а не молчаливый пропуск элементов.

```python
from bim_bridge import Checkpoints, iter_search_elements, iter_properties, write_jsonl

checkpoint = Checkpoints(bim, "checkpoints/ups-run-001")
ids = iter_search_elements(bim, model_id, [part_id], checkpoints=checkpoint)
batches = iter_properties(bim, model_id, ids, batch_size=500,
                          checkpoints=checkpoint)
records = (element for batch in batches for element in batch["Elements"])
write_jsonl(records, "outputs/properties.jsonl")
```

Повторите тот же код с той же папкой checkpoints после прерывания. Готовые ответы
берутся с диска, незавершённое задание ожидается по сохранённому ID. Если сервер
уже удалил задание или результат, этот запрос создаётся заново. Для новой свежей
выгрузки используйте новую папку. Checkpoints не обеспечивает неизменяемый снимок
Pilot между запросами, поэтому не меняйте исходную модель в ходе выгрузки.
В одной папке должен работать только один процесс.

`iter_properties` выдаёт пакеты свойств. `iter_relations` выдаёт пакеты связей и
делит пакет, если достигнут общий лимит детей. В строгом режиме по умолчанию
пропуски, предупреждения и усечения вызывают `IncompleteResult`. Режим
`strict=False` разрешает частичные пакеты свойств и связей, которые нужно
проверять самостоятельно. `iter_search_elements` не скрывает неполноту.

Генератор может успеть выдать часть записей перед ошибкой. `write_jsonl` сначала
пишет временный файл и публикует его только после успешного завершения генератора.
На локальном диске публикация атомарна. На Google Drive без поддержки hard link
используется копирование с запретом перезаписи. Оно не атомарно. При обычном
исключении неполная копия удаляется. При аварийном отключении среды это не
гарантируется. Для важных выгрузок пишите в `/content`, затем переносите готовый
файл в Drive. Checkpoints также занимают место на диске и содержат данные модели.
Обычный `/content` очищается при удалении среды Colab. Для продолжения между
сессиями сохраните checkpoints на постоянном диске.

Для полной выгрузки в один файл используйте `examples/export_properties.py`.
Клиент не снимает лимиты RAM, диска или размера результата worker. Размер пакета
можно уменьшить. `max_queries` ограничивает число поисковых запросов одной
выгрузки, по умолчанию 10000. При достижении лимита выгрузка считается неполной.

## Ошибки

```python
from bim_bridge import BimBridgeError, TaskTimeout, IncompleteResult

try:
    result = bim.model_context()
except TaskTimeout as error:
    print("Продолжить ожидание задания:", error.task_id)
except IncompleteResult as error:
    print("Неполная выгрузка:", error)
except BimBridgeError as error:
    print(error.code, error.status_code, error.retryable)
```

`TaskFailed` содержит ошибку worker. `IncompleteResult.result` содержит проблемный
ответ. HTTP 401 означает отказ в доступе, 404 — отсутствие, 409 — результат ещё
не готов, 410 — результат истёк, 413 — слишком большой запрос или результат.
Сохраняются коды сервера, включая ограничения ресурсов и недоступность модели.

Безопасные GET повторяются при временных сетевых ошибках и HTTP 429/502/503/504.
POST не повторяется вслепую. При неясном исходе отправки сохраните
`error.client_request_id` и повторите тот же запрос с этим идентификатором.
Дедупликация действует, пока менеджер хранит задание. Checkpoints делает это
автоматически. Не запускайте новую полную выгрузку после каждого таймаута.

## Соответствие API

| REST или операция | Python |
| --- | --- |
| GET /health | `bim.health()` |
| GET /workers | `bim.workers()` |
| POST /tasks | `bim.submit()` |
| GET /tasks/{id} | `task.status()` |
| DELETE /tasks/{id} | `task.cancel()` |
| GET /tasks/{id}/result | `task.result()` |
| GET /tasks/{id}/result/metadata | `task.metadata()` |
| GET /tasks/{id}/result/content | `task.download()` |
| get-model-context / model-context | `bim.model_context()` |
| search-elements | `bim.search_elements()` |
| load-element-properties | `bim.load_properties()` |
| get-element-relations | `bim.get_relations()` |
| download-pilot-file | `bim.download_file()` |
| read-excel-content | `bim.read_excel()` |
| echo / delay / generate-test-result / fail-test | `bim.diagnostic(operation, ...)` |

Полные имена `get_model_context`, `load_element_properties`,
`get_element_relations`, `download_pilot_file`, `read_excel_content` доступны как
синонимы коротких методов. Диагностика требует включённой capability `diagnostics`
у worker. `resolve-model-key` — внутренняя capability, а не отдельная операция.
Регистрация worker, heartbeat и загрузка chunks относятся к внутреннему транспорту
Pilot. Это не потребительские REST-маршруты внешнего Python-приложения.

## Локальная установка и тесты

Из папки проекта:

```text
python -m pip install -e .
python -m unittest discover -s tests -v
```

Тесты используют локальный HTTP-сервер и поддельный worker. Они не обращаются к
рабочему Pilot и не требуют токенов. Для сборки пакета:

```text
python -m pip wheel . --no-deps --wheel-dir dist
```

Публикация в GitHub не выполняется автоматически. Перед публикацией проверьте
состав файлов. В `.gitignore` исключены секреты, выгрузки и checkpoints.
