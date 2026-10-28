from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID
import re

def guid(value):
    result = str(UUID(str(value)))
    if result == str(UUID(int=0)):
        raise ValueError("GUID не должен быть пустым")
    return result

def utc(value):
    if value is None:
        return None
    fraction = None
    if isinstance(value, str):
        match = re.search(r'[T ]\d{2}:\d{2}:\d{2}\.(\d+)', value)
        if match:
            fraction = match.group(1)
            if len(fraction) > 7:
                raise ValueError("Дата должна иметь не более 7 знаков долей секунды")
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt = value
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        raise ValueError("Нужна дата с часовым поясом, например 2026-08-03T12:00:00Z")
    converted = dt.astimezone(timezone.utc)
    # Python datetime has microseconds; Pilot DateTime has 100 ns ticks.
    # Keep the original seventh digit so a snapshot identity is not rounded.
    fraction = (fraction if fraction is not None else f"{converted.microsecond:06d}").rstrip('0')
    return converted.strftime('%Y-%m-%dT%H:%M:%S') + ('.' + fraction if fraction else '') + 'Z'

@dataclass(frozen=True)
class Element:
    model_part_id: str
    element_id: str
    def payload(self):
        return {"ModelPartId": guid(self.model_part_id), "ElementId": guid(self.element_id)}

def element_payload(value):
    if isinstance(value, Element):
        return value.payload()
    return Element(value["ModelPartId"], value["ElementId"]).payload()

@dataclass(frozen=True)
class Condition:
    name: str
    operator: str = "Defined"
    value: object = None
    category: str | None = None
    data_type: str = "String"
    def payload(self):
        return {"Name": self.name, "Operator": self.operator, "Value": self.value,
                "Category": self.category, "DataType": self.data_type}

@dataclass(frozen=True)
class FileReference:
    object_id: str
    file_id: str | None = None
    file_name: str | None = None
    snapshot_created_at_utc: datetime | str | None = None
    def payload(self):
        return {"ObjectId": guid(self.object_id), "FileId": guid(self.file_id) if self.file_id else None,
                "FileName": self.file_name, "SnapshotCreatedAtUtc": utc(self.snapshot_created_at_utc)}
