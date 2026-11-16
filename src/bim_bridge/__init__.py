from .client import BimBridge, Task
from .types import Condition, Element, FileReference
from .errors import BimBridgeError, TaskFailed, TaskTimeout, IncompleteResult
from .bulk import Checkpoints, ExportSnapshot, select_parts, iter_search_elements, iter_properties, iter_relations, write_jsonl
from .spatial import iter_spatial_data, export_spatial_data, iter_placed_vertices
from .pilot import build_pilot_adapter
from ._paging import iter_search_pages, iter_search_complete
from .exports import ExportArtifact
from .binary_spatial import SpatialArtifact, SpatialBufferSection
from .profile import ExportProfile
from .relations import walk_relations
from .properties_parallel import iter_properties_parallel
from .schema_discovery import discover_property_schemas
from .event_journal import EventJournal

__all__ = ["BimBridge", "Task", "Condition", "Element", "FileReference", "BimBridgeError",
           "TaskFailed", "TaskTimeout", "IncompleteResult", "Checkpoints", "ExportSnapshot", "select_parts",
           "iter_search_elements", "iter_properties", "iter_relations", "write_jsonl",
           "iter_spatial_data", "export_spatial_data", "iter_placed_vertices", "build_pilot_adapter", "iter_search_pages", "ExportArtifact", "SpatialArtifact", "SpatialBufferSection"]

from .parallel import ParallelExport, export_spatial_parallel
__all__ += ["ParallelExport", "export_spatial_parallel"]
__all__ += ["ExportProfile"]
__all__ += ["walk_relations"]
__all__ += ["iter_search_complete"]
__all__ += ["iter_properties_parallel"]
__all__ += ["discover_property_schemas"]
__all__ += ["EventJournal"]
