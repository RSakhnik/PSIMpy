# Python-клиент BIM Bridge

Модули для обращения к BIM Workers Manager из Python и Google Colab.
Python 3.10 или новее.

Выпуск 0.1.3 добавляет запросы пространственных данных и потоковую выгрузку мешей.

## Подключение в Colab

```python
%pip install --upgrade --no-cache-dir "git+https://github.com/RSakhnik/PSIMpy.git"
```

Для файловой выгрузки мешей и таблиц установите дополнительные зависимости:

```python
%pip install --upgrade --no-cache-dir "pilot-bim-bridge[streaming,pandas] @ git+https://github.com/RSakhnik/PSIMpy.git"
```

Если пакет уже импортирован, после обновления перезапустите сеанс Colab.

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
| get-element-spatial-data | `bim.get_spatial_data()` |
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

## Пространственные данные

```python
from getpass import getpass
from bim_bridge import BimBridge, Element

bim = BimBridge(token=getpass("Токен BIM Bridge: "))
element = Element(part_id, element_id)
result = bim.get_spatial_data(
    model_id, [element],
    include_descendants=True,
    include_meshes=False,
)
print(result["Elements"][0]["BoundsMin"])
```

Синоним — `get_element_spatial_data`. Capability выбирается автоматически.
В прямом запросе допустимо от 1 до 100 исходных элементов. По умолчанию читается
только сам элемент, без потомков и массивов мешей. `model_version_utc` позволяет
запросить определённый снимок. Параметры ожидания и отмены такие же, как у других методов.

Числа остаются в масштабе SDK. Единицы не назначаются. `CoordinateSpace=sdk-placement`
не означает геодезическую привязку. `SceneCoordinatesVerified` сохраняется как вернул worker.
Отсутствующая собственная геометрия (`no-geometry`) — нормальный результат.
У доменного элемента геометрия может быть у потомков: для неё нужен `include_descendants=True`.

### Пакеты и один снимок

```python
from bim_bridge import ExportSnapshot, Checkpoints, iter_spatial_data

snapshot = ExportSnapshot()
checkpoints = Checkpoints(bim, "checkpoints/spatial")
for batch in iter_spatial_data(
    bim, model_id, elements,
    include_descendants=True,
    snapshot=snapshot,
    checkpoints=checkpoints,
):
    for item in batch["Elements"]:
        print(item["Element"], item["BoundsCenter"])
```

`elements` может быть итератором поиска. Версия из его элементов передаётся дальше.
Без неё первый ответ фиксирует снимок. При смене снимка выгрузка останавливается.
По умолчанию пакет содержит 20 исходных элементов без мешей или 5 с мешами.
`batch_size` позволяет выбрать размер от 1 до 100. Результат пакета сохраняет всю
структуру, включая `MeshDefinitions`. Не объединяйте все меши модели в один список.

`strict=True` проверяет исходные идентификаторы, State, FailedElementIds, ElementErrors,
Warnings и Truncated. Частичный ответ вызывает IncompleteResult. При `strict=False`
ответ возвращается для анализа, но не считается полноценным завершённым кэшем.

Только ошибка завершённого задания worker `RESULT_TOO_LARGE` делит пакет пополам.
Для одного слишком большого исходного элемента ошибка передаётся вызывающему коду.
Сетевые ошибки и локальный предел JSON не вызывают повторных SDK-заданий.
По умолчанию новые пространственные функции задают `can_retry=False`, `max_attempts=1`.
Повторы блоков передачи worker от этого не отключаются.

### Меши непосредственно в файлы

Из локальной папки проекта установите необязательные зависимости:

```text
python -m pip install ".[streaming,pandas]"
```

В Colab загрузите wheel 0.1.3 и установите его:

```python
%pip install "./pilot_bim_bridge-0.1.3-py3-none-any.whl[streaming,pandas]"
```

Если bim_bridge уже был импортирован до обновления, перезапустите ядро.
Для файловой выгрузки достаточно extra `streaming`. pandas нужен только для таблиц.
Обычный PSIMpy и методы габаритов не требуют ни ijson, ни pandas, ни NumPy.

```python
from bim_bridge import export_spatial_data

for file in export_spatial_data(
    bim, model_id, elements,
    "outputs/spatial-run-01",
    include_descendants=True,
    include_meshes=True,
):
    print(file["Path"], file["Complete"], file["ModelVersionUtc"])
```

Весь JSON скачивается на диск. Проверяются транспортные SHA-256 и длина, затем JSON
проверяется потоково через ijson. Массивы мешей не собираются в Python-списки.
После проверки публикуются файл и запись в manifest.json. Меш-пакет самодостаточен:
не удаляйте из него MeshDefinitions и не смешивайте определения разных пакетов или снимков.

Один каталог соответствует одному серверу, токену, модели, набору флагов,
размеру пакета и снимку. Для свежего снимка или других параметров выберите новый каталог.
Не редактируйте manifest.json и .checkpoints вручную. Запускайте только одну выгрузку
в каталог. Если прекращаете итерацию досрочно, закройте генератор через `close()`
или используйте `contextlib.closing` для немедленного освобождения блокировки.

Состояние сохраняется автоматически. После прерывания повторите тот же вызов:
он продолжит по TaskId. ClientRequestId записывается до Submit, поэтому ручное
продолжение после потери ответа использует прежнюю идентичность запроса.
Готовые файлы проверяются по локальному хешу и данным снимка перед повторным использованием.
Если готовый файл повреждён, он скачивается заново из того же задания.
Истёкшее на сервере задание не пересоздаётся файловым режимом молча: ошибка передаётся
пользователю. Для новой попытки можно выбрать отдельный каталог.

Checkpoints, если передан в export_spatial_data, используется для хранения TaskId
и ClientRequestId. Файлы и манифест остаются в указанном каталоге выгрузки.
При strict=False неполный файл сохраняется с Complete=False. Он не используется
как готовый кэш при следующем запуске. В нём нельзя молча считать габариты полными.

### Вершины и DataFrame

```python
from bim_bridge import iter_placed_vertices

result = bim.get_spatial_data(model_id, [element], include_meshes=True)
for x, y, z in iter_placed_vertices(result, element):
    pass  # Здесь выполняется ваш расчёт. Масштаб чисел не меняется.
```

Функция применяет ModelPlacement к локальным вершинам каждого экземпляра.
Она работает без NumPy. Normals и индексы остаются в исходном JSON.

```python
from bim_bridge.dataframe import spatial_result_to_dataframe

table = spatial_result_to_dataframe(result)
display(table)
```

Таблица содержит одну строку на исходный элемент. В ней есть идентификаторы, State,
Scope, счётчики и BoundsMinX/Y/Z, BoundsMaxX/Y/Z, BoundsCenterX/Y/Z.
Отсутствующие габариты остаются пустыми, а не нулевыми. Мешей в таблице нет.
Снимок, CoordinateSpace и MatrixConvention доступны в `table.attrs["spatial"]`.
