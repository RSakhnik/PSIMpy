"""Measure offline profile construction and HTML rendering for synthetic shards."""
import argparse
import gc
import json
from pathlib import Path
from statistics import median
import tempfile
import time
import tracemalloc

from bim_bridge.profile import ExportProfile


def fixture(count):
    return dict(ExportSessionId='synthetic', ModelKey='fixture',
                ModelVersionUtc='2026-01-01T00:00:00Z', TotalWall=120.0,
                Complete=True, Shards=[dict(
                    ShardId=index, TaskId=f'task-{index}', State='READY',
                    WorkerInstanceId=f'worker-{index % 4}', ElementCount=1000,
                    ProcessingWallMs=1500.0, QueueWaitMs=20.0,
                    SpatialPerformance=dict(SourceElements=1000,
                                            SdkTraversalReadMs=400.0,
                                            SdkVerticesReadMs=300.0,
                                            MeshCacheHits=50, MeshCacheMisses=10,
                                            WorkerMeshCacheBytes=1024 * 1024),
                    Metadata=dict(TotalLength=1024 * 1024),
                    DownloadAttempts=[dict(Success=True, ChunkCalls=16,
                                           DownloadWallMs=120.0)],
                    StatusTimes=dict(CreatedAtUtc='2026-01-01T00:00:00Z',
                                     AssignedAtUtc='2026-01-01T00:00:01Z',
                                     CompletedAtUtc='2026-01-01T00:00:02Z'))
                    for index in range(count)])


def measure(manifest, repeats, output):
    build = []
    render = []
    ExportProfile(fixture(1)).save_html(output)
    for _ in range(repeats):
        gc.collect()
        started = time.perf_counter_ns()
        profile = ExportProfile(manifest)
        built = time.perf_counter_ns()
        profile.save_html(output)
        rendered = time.perf_counter_ns()
        build.append((built - started) / 1e6)
        render.append((rendered - built) / 1e6)
    gc.collect()
    tracemalloc.start()
    profile = ExportProfile(manifest)
    profile.save_html(output)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return dict(BuildMs=build, BuildMedianMs=median(build),
                HtmlMs=render, HtmlMedianMs=median(render),
                TracemallocPeakBytes=peak, HtmlBytes=output.stat().st_size,
                Attempts=len(profile.data['Attempts']), Stages=len(profile.data['Stages']))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shards', type=int, default=1000)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.shards < 1 or args.repeats < 1:
        parser.error('shards and repeats must be positive')
    manifest = fixture(args.shards)
    with tempfile.TemporaryDirectory() as directory:
        result = measure(manifest, args.repeats, Path(directory) / 'profile.html')
    result.update(Shards=args.shards, Repeats=args.repeats)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(payload, encoding='utf-8')
    print(payload)


if __name__ == '__main__':
    main()
