"""Compare scheduler revisions on identical in-process fake SDK workloads."""
import argparse
import importlib.util
import json
import statistics
import subprocess
import tempfile
import threading
import time
import tracemalloc
from unittest.mock import patch

from bim_bridge.parallel import ParallelExport
from parallel_support import Client, FakeTask, Farm, elements
from spatial_support import MODEL, V1


def baseline(revision):
    source = subprocess.check_output(['git', 'show', f'{revision}:src/bim_bridge/parallel.py'], encoding='utf-8')
    spec = importlib.util.spec_from_loader('bim_bridge._parallel_baseline', loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(compile(source, '<baseline parallel.py>', 'exec'), module.__dict__)
    return module.ParallelExport


def sample(export_class, scenario, *, shards=24):
    delay = .6 if scenario == 'delayed-status' else 0
    chunk_delay = .006 if scenario == 'slow-download' else .0002
    farm = Farm(3, sdk=.008, upload=.002, download=chunk_delay)
    lock = threading.Lock()
    pending = [0]; peak = [0]; submissions = []
    original_submit = Client.submit
    original_status = FakeTask.status
    original_metadata = FakeTask.metadata
    original_chunk = Client.rpc

    def submit(client, *args, **kwargs):
        with lock:
            pending[0] += 1; peak[0] = max(peak[0], pending[0])
            submissions.append(time.monotonic())
        try:
            return original_submit(client, *args, **kwargs)
        finally:
            with lock: pending[0] -= 1

    delayed = [False]; delay_window = [None,None]
    def status(task):
        with lock:
            pending[0] += 1; peak[0] = max(peak[0], pending[0])
        try:
            if delay and farm.jobs[task.id]['index'] == 0 and not delayed[0]:
                delayed[0] = True
                delay_window[0] = time.monotonic()
                time.sleep(delay)
                delay_window[1] = time.monotonic()
            return original_status(task)
        finally:
            with lock: pending[0] -= 1

    def tracked(call):
        def wrapper(*args,**kwargs):
            with lock:
                pending[0] += 1; peak[0] = max(peak[0],pending[0])
            try: return call(*args,**kwargs)
            finally:
                with lock: pending[0] -= 1
        return wrapper

    try:
        with tempfile.TemporaryDirectory() as directory, patch.object(Client, 'submit', submit), \
             patch.object(FakeTask, 'status', status), patch.object(FakeTask, 'metadata', tracked(original_metadata)), \
             patch.object(Client, 'rpc', tracked(original_chunk)):
            tracemalloc.start()
            start = time.monotonic()
            with export_class(farm.client(), MODEL, directory, element_ids=elements(shards),
                              model_version_utc=V1, shard_size=1, task_prefetch=3,
                              result_prefetch=4, download_workers=2, poll_interval=.003) as export:
                export.wait(30)
                wall = time.monotonic() - start
                manifest = export.manifest
            _, memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            gaps = [b-a for a,b in zip(submissions, submissions[1:])]
            during_delay = (sum(delay_window[0] < t < delay_window[1] for t in submissions)
                            if all(t is not None for t in delay_window) else None)
            return dict(WallSeconds=round(wall,3), ThroughputPerSecond=round(shards/wall,2),
                        MaxSubmitGapSeconds=round(max(gaps,default=0),3),
                        SubmittedDuringDelay=during_delay,
                        Submitted=len(submissions), MaxConcurrentRpc=peak[0],
                        MaxConcurrentDownloads=farm.max_downloads,
                        SchedulerWaitMs=round(manifest.get('SchedulerWaitWallMs',0),1),
                        RetryCount=sum(s['RetryCount'] for s in manifest['Shards']),
                        PeakTracedBytes=memory)
    finally:
        farm.close()


def compare(revision, repeats):
    prior = baseline(revision)
    rows=[]
    for scenario in ('healthy','delayed-status','slow-download'):
        for name, scheduler in (('baseline', prior), ('current', ParallelExport)):
            runs=[sample(scheduler,scenario) for _ in range(repeats)]
            rows.append(dict(Scenario=scenario,Implementation=name,Runs=runs,
                             MedianWallSeconds=statistics.median(r['WallSeconds'] for r in runs),
                             MedianSubmitGapSeconds=statistics.median(r['MaxSubmitGapSeconds'] for r in runs)))
    return rows


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline-revision',default='19a5aca')
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--output')
    args=parser.parse_args()
    rows=compare(args.baseline_revision,args.repeats)
    output=json.dumps(rows,indent=2)
    if args.output:
        from pathlib import Path
        Path(args.output).write_text(output,encoding='utf-8')
    print(output)
