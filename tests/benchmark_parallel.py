"""Synthetic regression diagnostic: pull workers, bounded uploads and protocol downloads."""
import argparse
import json
import tempfile
import time
from pathlib import Path
from bim_bridge import ParallelExport
from parallel_support import Farm, elements
from spatial_support import MODEL, V1


def benchmark(shards=18, sdk=.1, upload=.03, download=.03):
    rows=[]
    for workers in (1,2,3):
        farm=Farm(workers,sdk=sdk,upload=upload,download=download/3)
        try:
            with tempfile.TemporaryDirectory() as directory:
                start=time.monotonic()
                with ParallelExport(farm.client(),MODEL,directory,element_ids=elements(shards),shard_size=1,
                                    model_version_utc=V1,poll_interval=.002) as result:
                    result.wait(60)
                    wall=time.monotonic()-start
                    rows.append(dict(Workers=workers,WallSeconds=wall,Shards=shards,SdkSecondsPerShard=sdk,
                        UploadSecondsPerShard=upload,DownloadSecondsPerShard=download,
                        SequentialStageSeconds=shards*(sdk+upload+download),
                        IdealSdkPipelineSeconds=((shards+workers-1)//workers)*sdk+upload+download,
                        Assignments=dict(farm.assignments),DownloadPeak=farm.max_downloads,
                        Complete=result.manifest['Complete']))
        finally:farm.close()
    for row in rows:row['Speedup']=rows[0]['WallSeconds']/row['WallSeconds']
    return rows


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sdk-seconds',type=float,default=.1);parser.add_argument('--shards',type=int,default=18)
    parser.add_argument('--upload-seconds',type=float,default=.03);parser.add_argument('--download-seconds',type=float,default=.03)
    args=parser.parse_args();rows=benchmark(args.shards,args.sdk_seconds,args.upload_seconds,args.download_seconds)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(rows,indent=2),encoding='utf-8');print(json.dumps(rows,indent=2))
    if not all(r['Complete'] for r in rows) or rows[2]['WallSeconds']>=rows[0]['WallSeconds']*.9:
        raise SystemExit('Synthetic scaling regression')
