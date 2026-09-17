import unittest

import pandas as pd

from bim_bridge import Condition
from bim_bridge.dataframe import (
    excel_result_to_dataframes,
    properties_result_to_dataframe,
    read_excel_dataframe,
    search_dataframe,
)


MODEL = "11111111-1111-4111-8111-111111111111"
PART = "22222222-2222-4222-8222-222222222222"
ELEMENT1 = "33333333-3333-4333-8333-333333333331"
ELEMENT2 = "33333333-3333-4333-8333-333333333332"


class FakeClient:
    def search_elements(self, model_key, conditions, **kwargs):
        return {
            "ModelKey": model_key,
            "TargetModelPartIds": kwargs.get("part_ids") or [],
            "Count": 2,
            "Truncated": False,
            "Elements": [
                {"ModelPartId": PART, "ElementId": ELEMENT1},
                {"ModelPartId": PART, "ElementId": ELEMENT2},
            ],
        }

    def run(self, operation, payload, **kwargs):
        if operation != "load-element-properties":
            raise AssertionError(operation)
        elements = []
        for item in payload["ElementIds"]:
            suffix = 1 if item["ElementId"] == ELEMENT1 else 2
            elements.append(
                {
                    **item,
                    "PropertySets": [
                        {
                            "Category": "Common_Properties",
                            "Type": "Common",
                            "Properties": [
                                {"Name": "Name", "Unit": None, "Value": f"Pipe-{suffix}"},
                                {"Name": "Mass", "Unit": "kg", "Value": 10.0 * suffix},
                            ],
                        }
                    ],
                }
            )
        return {
            "ModelKey": payload["TargetModelKey"],
            "Elements": elements,
            "FailedElementIds": [],
            "Warnings": [],
        }

    def read_excel(self, reference, **kwargs):
        return {
            "ObjectId": "file-object",
            "FileName": "sample.xlsx",
            "Warnings": [],
            "Sheets": [
                {
                    "Name": "TDSheet",
                    "UsedRange": "B2:D4",
                    "Rows": [
                        {
                            "RowNumber": 2,
                            "Cells": [
                                {"ColumnNumber": 2, "ValueType": "Text", "Value": "Tag", "Formula": None},
                                {"ColumnNumber": 3, "ValueType": "Text", "Value": "Mass", "Formula": None},
                                {"ColumnNumber": 4, "ValueType": "Text", "Value": "Date", "Formula": None},
                            ],
                        },
                        {
                            "RowNumber": 3,
                            "Cells": [
                                {"ColumnNumber": 2, "ValueType": "Text", "Value": "P-101", "Formula": None},
                                {"ColumnNumber": 3, "ValueType": "Number", "Value": 12.5, "Formula": None},
                                {"ColumnNumber": 4, "ValueType": "DateTime", "Value": "2026-09-18T00:00:00Z", "Formula": None},
                            ],
                        },
                        {
                            "RowNumber": 4,
                            "Cells": [
                                {"ColumnNumber": 2, "ValueType": "Text", "Value": "P-102", "Formula": None},
                                {"ColumnNumber": 4, "ValueType": "DateTime", "Value": "2026-09-19T00:00:00Z", "Formula": None},
                            ],
                        },
                    ],
                }
            ],
        }


class DataFrameTests(unittest.TestCase):
    def test_properties_result_to_dataframe(self):
        result = {
            "Elements": [
                {
                    "ModelPartId": "part",
                    "ElementId": "element",
                    "PropertySets": [
                        {
                            "Category": "Common_Properties",
                            "Properties": [
                                {"Name": "Name", "Unit": None, "Value": "P-101"},
                                {"Name": "Mass", "Unit": "kg", "Value": 12.5},
                            ],
                        }
                    ],
                }
            ],
            "Warnings": [],
            "FailedElementIds": [],
        }
        frame = properties_result_to_dataframe(result)
        self.assertEqual(frame.loc[0, "Common_Properties.Name"], "P-101")
        self.assertEqual(frame.attrs["units"]["Common_Properties.Mass"], "kg")

    def test_search_dataframe_loads_properties(self):
        frame = search_dataframe(
            FakeClient(),
            MODEL,
            [Condition("Name", "Defined")],
            part_ids=[PART],
        )
        self.assertEqual(len(frame), 2)
        self.assertEqual(list(frame["Common_Properties.Mass"]), [10.0, 20.0])
        self.assertFalse(frame.attrs["search"]["Truncated"])

    def test_excel_result_to_dataframe(self):
        raw = FakeClient().read_excel(None)
        frame = excel_result_to_dataframes(raw)["TDSheet"]
        self.assertEqual(list(frame.columns), ["Tag", "Mass", "Date"])
        self.assertEqual(frame.loc[0, "Tag"], "P-101")
        self.assertTrue(pd.isna(frame.loc[1, "Mass"]))
        self.assertIsInstance(frame.loc[0, "Date"], pd.Timestamp)

    def test_read_excel_dataframe_first_sheet(self):
        frame = read_excel_dataframe(FakeClient(), object())
        self.assertEqual(frame.shape, (2, 3))
        self.assertEqual(frame.attrs["sheet_name"], "TDSheet")


if __name__ == "__main__":
    unittest.main()
