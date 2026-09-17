"""Pandas-обёртки для результатов BIM Bridge.

Модуль не меняет формат низкоуровневого API. Он преобразует результаты
поиска/свойств и read-excel-content в pandas.DataFrame, чтобы после запроса
можно было сразу переходить к анализу.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - зависит от окружения
    raise ImportError(
        "Для bim_bridge.dataframe нужен pandas. "
        "Установите пакет с extra 'pandas' или отдельно: pip install pandas."
    ) from exc

from .bulk import iter_properties
from .errors import IncompleteResult


_ID_COLUMNS = ["ModelPartId", "ElementId"]


def search_result_to_dataframe(result: Mapping[str, Any]) -> pd.DataFrame:
    """Преобразовать сырой ответ search-elements в DataFrame с GUID элементов."""
    elements = result.get("Elements") or []
    frame = pd.DataFrame(elements)

    if frame.empty:
        frame = pd.DataFrame(columns=_ID_COLUMNS)
    else:
        columns = [name for name in _ID_COLUMNS if name in frame.columns]
        rest = [name for name in frame.columns if name not in columns]
        frame = frame.loc[:, columns + rest]

    frame.attrs["search"] = {
        key: result.get(key)
        for key in ("ModelKey", "TargetModelPartIds", "Count", "Truncated")
        if key in result
    }
    return frame


def properties_result_to_dataframe(
    result: Mapping[str, Any],
    *,
    include_category: bool = True,
    column_separator: str = ".",
) -> pd.DataFrame:
    """Преобразовать load-element-properties в широкую таблицу.

    Одна строка соответствует одному BIM-элементу. По умолчанию имя свойства
    имеет вид ``Category.Property``. Это защищает от большей части коллизий,
    когда одинаковые имена встречаются в разных наборах свойств.

    Единицы измерения сохраняются в ``df.attrs["units"]``.
    """
    rows: list[dict[str, Any]] = []
    units: dict[str, Any] = {}

    for element in result.get("Elements") or []:
        row: dict[str, Any] = {
            "ModelPartId": element.get("ModelPartId"),
            "ElementId": element.get("ElementId"),
        }
        local_counts: dict[str, int] = {}

        for property_set in element.get("PropertySets") or []:
            category = property_set.get("Category") or ""
            for prop in property_set.get("Properties") or []:
                name = prop.get("Name") or ""
                if include_category and category:
                    base = f"{category}{column_separator}{name}"
                else:
                    base = name

                # В Pilot теоретически возможны повторяющиеся свойства даже
                # внутри одного набора. Не уничтожаем данные молча.
                count = local_counts.get(base, 0) + 1
                local_counts[base] = count
                column = base if count == 1 else f"{base}#{count}"

                row[column] = prop.get("Value")
                unit = prop.get("Unit")
                if unit not in (None, ""):
                    units.setdefault(column, unit)

        rows.append(row)

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=_ID_COLUMNS)
    else:
        id_columns = [name for name in _ID_COLUMNS if name in frame.columns]
        value_columns = [name for name in frame.columns if name not in id_columns]
        frame = frame.loc[:, id_columns + value_columns]

    frame.attrs["units"] = units
    frame.attrs["warnings"] = list(result.get("Warnings") or [])
    frame.attrs["failed_element_ids"] = list(result.get("FailedElementIds") or [])
    if "ModelKey" in result:
        frame.attrs["model_key"] = result.get("ModelKey")
    return frame


def search_dataframe(
    client,
    model_key: str,
    conditions: Sequence[Any],
    *,
    part_ids: Sequence[str] | None = None,
    logical_operator: str = "And",
    max_results: int = 10_000,
    property_names: Sequence[str] | None = None,
    property_batch_size: int = 500,
    include_properties: bool = True,
    include_category: bool = True,
    column_separator: str = ".",
    allow_truncated: bool = False,
    strict: bool = True,
    **options,
) -> pd.DataFrame:
    """Выполнить поиск и вернуть готовый DataFrame.

    Если ``include_properties=True`` (по умолчанию), после поиска свойства
    найденных элементов загружаются пакетами и разворачиваются в колонки.

    По умолчанию усечённый search-elements считается ошибкой, потому что
    анализ неполной выборки без явного согласия пользователя слишком легко
    принять за анализ полной модели. Для осознанной выборки первых N элементов
    передайте ``allow_truncated=True``.
    """
    search = client.search_elements(
        model_key,
        conditions,
        part_ids=part_ids,
        logical_operator=logical_operator,
        max_results=max_results,
        **options,
    )

    if search.get("Truncated") and not allow_truncated:
        raise IncompleteResult(
            "Поиск усечён лимитом MaxResults. Увеличьте лимит, сузьте условие "
            "или явно передайте allow_truncated=True.",
            search,
        )

    if not include_properties:
        return search_result_to_dataframe(search)

    elements = search.get("Elements") or []
    if not elements:
        frame = pd.DataFrame(columns=_ID_COLUMNS)
        frame.attrs["search"] = dict(search)
        return frame

    combined: dict[str, Any] = {
        "ModelKey": search.get("ModelKey") or model_key,
        "Elements": [],
        "FailedElementIds": [],
        "Warnings": [],
    }

    for batch in iter_properties(
        client,
        model_key,
        elements,
        batch_size=property_batch_size,
        property_names=property_names,
        strict=strict,
        **options,
    ):
        combined["Elements"].extend(batch.get("Elements") or [])
        combined["FailedElementIds"].extend(batch.get("FailedElementIds") or [])
        combined["Warnings"].extend(batch.get("Warnings") or [])

    frame = properties_result_to_dataframe(
        combined,
        include_category=include_category,
        column_separator=column_separator,
    )
    frame.attrs["search"] = {
        key: search.get(key)
        for key in ("ModelKey", "TargetModelPartIds", "Count", "Truncated")
        if key in search
    }
    return frame


def excel_result_to_dataframes(
    result: Mapping[str, Any],
    *,
    header: int | None = 0,
    parse_dates: bool = True,
    use_formulas: bool = False,
) -> dict[str, pd.DataFrame]:
    """Преобразовать сырой read-excel-content в DataFrame для каждого листа.

    Возвращается ``dict[имя_листа, DataFrame]``. Пустые клетки между значениями
    восстанавливаются как ``None`` по RowNumber/ColumnNumber.
    """
    frames: dict[str, pd.DataFrame] = {}

    for sheet in result.get("Sheets") or []:
        name = sheet.get("Name") or "Sheet"
        matrix = _sheet_matrix(
            sheet,
            parse_dates=parse_dates,
            use_formulas=use_formulas,
        )
        frame = _matrix_to_dataframe(matrix, header=header)
        frame.attrs["sheet_name"] = name
        frame.attrs["used_range"] = sheet.get("UsedRange")
        frame.attrs["source_file"] = result.get("FileName")
        frame.attrs["object_id"] = result.get("ObjectId")
        frame.attrs["warnings"] = list(result.get("Warnings") or [])
        frames[name] = frame

    return frames


def read_excel_dataframe(
    client,
    reference,
    *,
    sheet_name: str | int | Sequence[str | int] | None = 0,
    header: int | None = 0,
    parse_dates: bool = True,
    use_formulas: bool = False,
    include_empty_cells: bool = False,
    **options,
):
    """Прочитать Excel через BIM Bridge и вернуть pandas-объект.

    Поведение ``sheet_name`` близко к ``pandas.read_excel``:

    - ``0`` (по умолчанию) -> первый лист как ``DataFrame``;
    - строка -> указанный лист как ``DataFrame``;
    - список -> ``dict[str, DataFrame]``;
    - ``None`` -> все листы как ``dict[str, DataFrame]``.

    Excel разбирается на worker через ClosedXML, поэтому ``openpyxl`` локально
    для этой операции не требуется.
    """
    requested_names = _requested_sheet_names(sheet_name)
    raw = client.read_excel(
        reference,
        sheet_names=requested_names,
        include_formulas=use_formulas,
        include_empty_cells=include_empty_cells,
        **options,
    )
    frames = excel_result_to_dataframes(
        raw,
        header=header,
        parse_dates=parse_dates,
        use_formulas=use_formulas,
    )
    return _select_sheets(frames, sheet_name)


def _sheet_matrix(
    sheet: Mapping[str, Any],
    *,
    parse_dates: bool,
    use_formulas: bool,
) -> list[list[Any]]:
    cells: list[tuple[int, int, Any]] = []

    for row in sheet.get("Rows") or []:
        row_number = int(row.get("RowNumber") or 0)
        for cell in row.get("Cells") or []:
            column_number = int(cell.get("ColumnNumber") or 0)
            if row_number <= 0 or column_number <= 0:
                continue
            cells.append(
                (
                    row_number,
                    column_number,
                    _cell_value(cell, parse_dates=parse_dates, use_formulas=use_formulas),
                )
            )

    if not cells:
        return []

    min_row = min(row for row, _, _ in cells)
    max_row = max(row for row, _, _ in cells)
    min_col = min(col for _, col, _ in cells)
    max_col = max(col for _, col, _ in cells)

    matrix = [
        [None for _ in range(max_col - min_col + 1)]
        for _ in range(max_row - min_row + 1)
    ]
    for row, col, value in cells:
        matrix[row - min_row][col - min_col] = value
    return matrix


def _cell_value(
    cell: Mapping[str, Any],
    *,
    parse_dates: bool,
    use_formulas: bool,
):
    formula = cell.get("Formula")
    if use_formulas and formula not in (None, ""):
        formula = str(formula)
        return formula if formula.startswith("=") else f"={formula}"

    value = cell.get("Value")
    if parse_dates and value is not None and str(cell.get("ValueType", "")).casefold() == "datetime":
        try:
            return pd.Timestamp(value)
        except (TypeError, ValueError):
            return value
    return value


def _matrix_to_dataframe(matrix: list[list[Any]], *, header: int | None) -> pd.DataFrame:
    if not matrix:
        return pd.DataFrame()

    if header is None:
        return pd.DataFrame(matrix)
    if not isinstance(header, int) or header < 0:
        raise ValueError("header должен быть неотрицательным int или None")
    if header >= len(matrix):
        raise ValueError("header выходит за пределы данных листа")

    columns = _deduplicate_headers(matrix[header])
    data = matrix[header + 1 :]
    return pd.DataFrame(data, columns=columns)


def _deduplicate_headers(values: Sequence[Any]) -> list[Any]:
    result: list[Any] = []
    seen: dict[Any, int] = {}

    for index, value in enumerate(values):
        if value is None or value == "":
            base: Any = f"Unnamed: {index}"
        else:
            try:
                hash(value)
                base = value
            except TypeError:
                base = str(value)

        count = seen.get(base, 0)
        seen[base] = count + 1
        result.append(base if count == 0 else f"{base}.{count}")

    return result


def _requested_sheet_names(
    sheet_name: str | int | Sequence[str | int] | None,
) -> list[str] | None:
    if isinstance(sheet_name, str):
        return [sheet_name]
    if isinstance(sheet_name, Sequence) and not isinstance(sheet_name, (str, bytes)):
        values = list(sheet_name)
        if values and all(isinstance(value, str) for value in values):
            return values
    # Индекс листа worker не принимает, поэтому для int/None читаем список листов.
    return None


def _select_sheets(
    frames: Mapping[str, pd.DataFrame],
    sheet_name: str | int | Sequence[str | int] | None,
):
    names = list(frames)

    if sheet_name is None:
        return dict(frames)
    if isinstance(sheet_name, str):
        return frames[_resolve_sheet_name(names, sheet_name)]
    if isinstance(sheet_name, int):
        return frames[names[sheet_name]]
    if isinstance(sheet_name, Sequence) and not isinstance(sheet_name, (str, bytes)):
        selected: dict[str, pd.DataFrame] = {}
        for selector in sheet_name:
            if isinstance(selector, int):
                name = names[selector]
            elif isinstance(selector, str):
                name = _resolve_sheet_name(names, selector)
            else:
                raise TypeError("Элементы sheet_name должны быть str или int")
            selected[name] = frames[name]
        return selected
    raise TypeError("sheet_name должен быть str, int, последовательностью или None")


def _resolve_sheet_name(names: Iterable[str], requested: str) -> str:
    for name in names:
        if name.casefold() == requested.casefold():
            return name
    raise KeyError(f"Лист {requested!r} отсутствует в ответе")


__all__ = [
    "search_result_to_dataframe",
    "properties_result_to_dataframe",
    "search_dataframe",
    "excel_result_to_dataframes",
    "read_excel_dataframe",
]
