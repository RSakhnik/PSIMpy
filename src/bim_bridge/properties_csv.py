"""Streaming long-form CSV output for verified property batches."""
import csv
import json
import os
from pathlib import Path
import tempfile

from ._files import publish
from .errors import BimBridgeError, IncompleteResult


def write_properties_csv(batches, destination, *, overwrite=False):
    """Write ID/category/property/unit columns without retaining all rows."""
    destination = Path(destination)
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.properties-', suffix='.csv', dir=destination.parent)
    version = None
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['ModelPartId', 'ElementId',
                'ModelVersionUtc', 'Category', 'PropertyName', 'ValueType', 'Unit', 'Value'])
            writer.writeheader()
            for batch in batches:
                if batch.get('Truncated') or any(batch.get(name) for name in
                        ('FailedElementIds', 'ElementErrors', 'Warnings')):
                    raise IncompleteResult('Cannot publish incomplete property batch', batch)
                current = batch.get('ModelVersionUtc')
                if not current or batch.get('SnapshotVerified') is False:
                    raise IncompleteResult('Property batch did not confirm model version', batch)
                if version and current != version:
                    raise BimBridgeError('MODEL_CHANGED', 'Property batch version changed')
                version = current
                for element in batch.get('Elements') or []:
                    written = False
                    for section in element.get('PropertySets') or []:
                        for prop in section.get('Properties') or []:
                            written = True
                            value = prop.get('Value')
                            writer.writerow({'ModelPartId': element['ModelPartId'],
                                'ElementId': element['ElementId'], 'ModelVersionUtc': version,
                                'Category': section.get('Category'), 'PropertyName': prop.get('Name'),
                                'ValueType': prop.get('ValueType') or prop.get('Type') or type(value).__name__,
                                'Unit': prop.get('Unit'), 'Value': value if isinstance(value, (str, int, float, bool))
                                or value is None else json.dumps(value, ensure_ascii=False)})
                    if not written:
                        writer.writerow({'ModelPartId': element['ModelPartId'],
                                         'ElementId': element['ElementId'], 'ModelVersionUtc': version})
            stream.flush()
            os.fsync(stream.fileno())
        publish(temporary, destination, overwrite)
        return destination
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
