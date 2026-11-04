"""Synchronize public protocol source copies in the three adjacent checkouts.

Run from any directory; --check is read-only and fails on drift.
"""
import argparse
from pathlib import Path
import sys

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check',action='store_true')
parser.add_argument('--manager', type=Path, help='Manager checkout root')
parser.add_argument('--worker', type=Path, help='AdvancedTools checkout root')
parser.add_argument('--python', type=Path, help='PSIMpy checkout root')
options=parser.parse_args()
root=Path(__file__).resolve().parents[2]
manager=options.manager or root/'PSIM_BIM_Workers_Manager'
worker=options.worker or root/'AdvancedTools-PSIM_ADMIN-'
python=options.python or root/'PSIMpy'
source=manager/'BIM Workerks Manager'/'src'/'Pilot.BimBridge.Contracts'
destinations=[worker/'AdvancedTools'/'BimBridge', python/'src'/'bim_bridge'/'_pilot_source']
different=[]
for name in ('Contracts.cs','ArtifactContracts.cs'):
    data=(source/name).read_bytes()
    for directory in destinations:
        target=directory/name
        if not target.exists() or target.read_bytes()!=data:
            if options.check:different.append(str(target))
            else:target.write_bytes(data)
if different:
    print('Protocol source drift:\n'+'\n'.join(different),file=sys.stderr)
    sys.exit(1)
