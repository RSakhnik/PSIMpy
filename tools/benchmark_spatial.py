"""Offline JSON vs BBSPAT2 size/serialization/read diagnostics (synthetic data)."""
import argparse
from array import array
import hashlib
import json
from pathlib import Path
import sys
import time
import tracemalloc

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from bim_bridge.binary_spatial import MAGIC, FRAME, SpatialArtifact

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--vertices',type=int,default=100000)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
args.output.mkdir(parents=True,exist_ok=True)
model='11111111-1111-4111-8111-111111111111';part='22222222-2222-4222-8222-222222222222'
element={'ModelPartId':part,'ElementId':model};key=part+'/'+model
version='2026-01-01T00:00:00Z'
vertices=array('f',((i%1000)/3 for i in range(args.vertices*3)))
normals=array('f',(float(i%3==2) for i in range(args.vertices*3)))
indices=array('I',range(args.vertices//3*3))
legacy={'MeshDefinitions':[{'Key':key,'Vertices':list(vertices),'Normals':list(normals),'Indices':list(indices)}]}
json_path=args.output/'spatial.json';binary_path=args.output/'spatial.bbsp'


def measure(action):
    tracemalloc.start();start=time.perf_counter();action();elapsed=time.perf_counter()-start
    _,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    return {'ElapsedSeconds':elapsed,'PeakPythonBytes':peak}


def write_json():
    with json_path.open('w',encoding='utf-8') as stream:json.dump(legacy,stream,separators=(',',':'))


def write_binary():
    with binary_path.open('wb') as stream:
        stream.write(MAGIC)
        def frame(metadata,blob=b''):
            raw=json.dumps(metadata,separators=(',',':')).encode();stream.write(FRAME.pack(len(raw),len(blob)));stream.write(raw);stream.write(blob)
        frame({'Type':'manifest','Format':'bim-spatial','FormatVersion':2,'ModelVersionUtc':version,'IncludeMeshes':True})
        frame({'Type':'source-begin','Element':element})
        frame({'Type':'mesh-begin','MeshDefinitionKey':key,'ModelPartId':part,'MeshId':model})
        for field,values,dtype in [('Vertices',vertices,'<f4'),('Normals',normals,'<f4'),('Indices',indices,'<u4')]:
            for offset in range(0,len(values),8190):
                block=values[offset:offset+8190]
                if sys.byteorder!='little':block.byteswap()
                blob=block.tobytes()
                frame({'Type':'buffer','MeshDefinitionKey':key,'Field':field,'DType':dtype,'Offset':offset,'Count':len(block),'Sha256':hashlib.sha256(blob).hexdigest()},blob)
        frame({'Type':'mesh-end','MeshDefinitionKey':key,'Vertices':len(vertices),'Normals':len(normals),'Indices':len(indices),'EdgeIndices':0})
        frame({'Type':'instance','Element':element,'MeshDefinitionKey':key,'ModelPlacement':[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]})
        frame({'Type':'source-end','Element':element,'State':'complete'})
        frame({'Type':'summary','ModelVersionUtc':version,'SourceCount':1,'InstanceCount':1,'MeshCount':1,'FailedCount':0,'Complete':True})


def read_json():
    with json_path.open() as stream:json.load(stream)


report={'Vertices':args.vertices,'SourceArraysAllocatedBeforeMeasurement':True,
        'JsonWrite':measure(write_json),'BinaryWrite':measure(write_binary),
        'JsonRead':measure(read_json),'BinaryValidate':measure(lambda:SpatialArtifact(binary_path).validate()),
        'JsonBytes':json_path.stat().st_size,'BinaryBytes':binary_path.stat().st_size,
        'Note':'Local synthetic diagnostic, not Pilot SDK throughput; tracemalloc excludes native allocations.'}
(args.output/'spatial-benchmark.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report,indent=2))
