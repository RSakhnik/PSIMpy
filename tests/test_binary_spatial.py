import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest

from bim_bridge import BimBridgeError, SpatialArtifact
from bim_bridge.binary_spatial import MAGIC, FRAME
from spatial_support import MODEL, PART, V1

ELEMENT={'ModelPartId':PART,'ElementId':MODEL}
MESH=PART+'/'+MODEL


def frame(stream,metadata,data=b''):
    raw=json.dumps(metadata).encode();stream.write(FRAME.pack(len(raw),len(data)));stream.write(raw);stream.write(data)


def artifact(path,*,empty=False):
    with open(path,'wb') as stream:
        stream.write(MAGIC)
        frame(stream,{'Type':'manifest','Format':'bim-spatial','FormatVersion':2,'ModelVersionUtc':V1,'IncludeMeshes':True})
        frame(stream,{'Type':'source-begin','Element':ELEMENT})
        if not empty:
            frame(stream,{'Type':'mesh-begin','MeshDefinitionKey':MESH,'ModelPartId':PART,'MeshId':MODEL})
            data=struct.pack('<ffffff',0,0,0,1.25,2.5,3.75)
            frame(stream,{'Type':'buffer','MeshDefinitionKey':MESH,'Field':'Vertices','DType':'<f4','Offset':0,'Count':6,'Sha256':hashlib.sha256(data).hexdigest()},data)
            frame(stream,{'Type':'mesh-end','MeshDefinitionKey':MESH,'Vertices':6,'Normals':0,'Indices':0,'EdgeIndices':0})
            frame(stream,{'Type':'instance','Element':ELEMENT,'MeshDefinitionKey':MESH,'ModelPlacement':[1,0,0,10,0,1,0,0,0,0,1,0,0,0,0,1]})
        frame(stream,{'Type':'source-end','Element':ELEMENT,'State':'no-geometry' if empty else 'complete'})
        frame(stream,{'Type':'summary','ModelVersionUtc':V1,'SourceCount':1,'InstanceCount':0 if empty else 1,'MeshCount':0 if empty else 1,'FailedCount':0,'Complete':True})


class BinarySpatialTests(unittest.TestCase):
    def test_float32_round_trip_and_placed_vertices(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'data';artifact(path)
            reader=SpatialArtifact(path);info=reader.validate()
            self.assertEqual(info['Summary']['MeshCount'],1)
            section=next(reader.buffers(field='Vertices'))
            self.assertEqual(list(section.values()),[0.,0.,0.,1.25,2.5,3.75])
            self.assertEqual(list(reader.iter_placed_vertices()),[(10.,0.,0.),(11.25,2.5,3.75)])
            try:import numpy
            except ImportError:return
            mapped=section.numpy()
            self.assertEqual(mapped.tolist(),[0.,0.,0.,1.25,2.5,3.75])
            mapped._mmap.close()
    def test_empty_geometry_and_corrupted_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'data';artifact(path,empty=True)
            self.assertEqual(SpatialArtifact(path).validate()['Summary']['InstanceCount'],0)
            self.assertEqual(list(SpatialArtifact(path).iter_placed_vertices()),[])
            artifact(path);reader=SpatialArtifact(path);section=next(reader.buffers())
            with path.open('r+b') as stream:stream.seek(section.byte_offset);stream.write(b'\xff\xff\xff\xff')
            with self.assertRaises(BimBridgeError):reader.validate()
    def test_missing_summary_truncated_metadata_and_changed_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'data';artifact(path)
            data=path.read_bytes()
            for broken in [data[:-2],data[:12],MAGIC+FRAME.pack(100,0)+b'{}']:
                path.write_bytes(broken)
                with self.assertRaises(BimBridgeError):SpatialArtifact(path).validate()
            artifact(path)
            with self.assertRaises(BimBridgeError):SpatialArtifact(path,expected_sha256='0'*64).validate()

    @unittest.skipUnless(os.environ.get('BIM_BRIDGE_INTEROP_ARTIFACT'),'Run .NET writer interop fixture first')
    def test_real_csharp_writer_binary_arrays_dedup_and_placements(self):
        path=Path(os.environ['BIM_BRIDGE_INTEROP_ARTIFACT'])
        reader=SpatialArtifact(path,expected_sha256=path.with_suffix('.sha256').read_text().strip())
        result=reader.validate()
        self.assertEqual(result['Summary']['SourceCount'],1)
        self.assertEqual(result['Summary']['MeshCount'],1)
        self.assertEqual(result['Summary']['InstanceCount'],2)
        self.assertEqual(len(list(reader.buffers(field='Vertices'))),1)
        self.assertEqual(list(next(reader.buffers(field='Indices')).values()),[0,1,2])
        points=list(reader.iter_placed_vertices())
        self.assertEqual(points,[(0.,0.,0.),(1.25,0.,0.),(0.,2.5,0.),(10.,0.,0.),(11.25,0.,0.),(10.,2.5,0.)])


if __name__=='__main__':unittest.main()
