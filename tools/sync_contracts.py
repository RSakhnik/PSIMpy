"""Synchronize public protocol source copies in the three adjacent checkouts.

Run from any directory; --check is read-only and fails on drift.
"""
import argparse
from pathlib import Path
import sys

root=Path(__file__).resolve().parents[2]
source=root/'PSIM_BIM_Workers_Manager'/'BIM Workerks Manager'/'src'/'Pilot.BimBridge.Contracts'
destinations=[root/'AdvancedTools-PSIM_ADMIN-'/'AdvancedTools'/'BimBridge',
              root/'PSIMpy'/'src'/'bim_bridge'/'_pilot_source']
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check',action='store_true')
options=parser.parse_args()
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
