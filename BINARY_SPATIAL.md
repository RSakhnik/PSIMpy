# BBSPAT2 binary spatial artifact

Content type: `application/vnd.pilot.bim-spatial;version=2`. The file is not gzip
compressed, so individual arrays can be memory-mapped. No scale/unit conversion
is performed. Vertices and normals are the SDK's float values encoded as
little-endian IEEE754 float32; indices are little-endian uint32. IDs, placements,
bounds and metadata remain JSON. No glTF coordinate assumptions are introduced.

## Framing

The file starts with exactly eight ASCII bytes `BBSPAT2\n`. Each frame then has:

| Field | Encoding |
| --- | --- |
| Metadata byte length | uint32 little-endian |
| Binary byte length | uint64 little-endian |
| Metadata | UTF-8 JSON object, at most 1 MiB |
| Binary payload | zero or more bytes, at most 1 MiB per frame |

Normal production buffers contain at most 8190 scalars (32760 bytes), matching
bounded SDK cursor batches. The stream must end immediately after a `summary`.
An interrupted file without that summary is incomplete.

Frame `Type` values:

- `manifest`: Format=`bim-spatial`, FormatVersion=2, model/snapshot, flags,
  CoordinateSpace=`sdk-placement`, SceneCoordinatesVerified=false and matrix convention.
- `source-begin`: requested root Element and self/subtree Scope.
- `mesh-begin`: MeshDefinitionKey, ModelPartId, MeshId.
- `buffer`: MeshDefinitionKey, Field, DType (`<f4`/`<u4`), scalar Offset, scalar
  Count and SHA256 of the binary payload. Byte length is Count × 4.
- `mesh-end`: final scalar counts for Vertices, Normals, Indices and EdgeIndices.
- `instance`: actual Element, MeshDefinitionKey, ModelPlacement (16 JSON numbers).
- `error`: Element, Code and Message. A strict reader rejects these records.
- `source-end`: source Element, State, bounds and processing/instance counts.
- `summary`: snapshot, source/node/instance/mesh/failure counts and Complete.

Mesh keys combine part and mesh GUIDs. Definitions occur once across the whole
artifact, while instances retain each placement and element identity. Mesh keys
and traversal queues are indexed on disk during writing; the reader also uses a
small disk index when it needs cross-frame references. This bounds managed/Python
memory independently of the number of mesh definitions.

Matrices are row-major, column-vector, with translation in the last column.
Vertices and normals are mesh-local. ModelPlacement is applied only when the
caller requests placed vertices. Geodetic alignment and physical units are not
claimed by this format.

## Python

```python
artifact = bim.export_spatial(model_id, 'meshes.bbsp', part_ids=parts,
                              include_meshes=True, include_normals=False,
                              include_indices=True)
spatial = artifact.spatial()
info = spatial.validate()  # full artifact SHA + frame/reference/summary checks

for section in spatial.buffers(field='Vertices'):
    numbers = section.values()   # scalar iterator, base library only
    # Optional numpy extra; maps this block rather than loading the whole mesh.
    mapped = section.numpy(mmap=True)
    # Release a retained mapping before deleting/moving its source on Windows.
    mapped._mmap.close()

for x, y, z in spatial.iter_placed_vertices():
    pass
```

`iter_placed_vertices(strict=True)` validates before yielding. Use `strict=False`
only after a separate successful validation when avoiding another full scan.
Frames can be inspected with `frames()` without collecting large JSON arrays.
The complete artifact is authenticated by result metadata SHA-256; per-buffer
hashes allow local corruption detection during streaming inspection as well.
