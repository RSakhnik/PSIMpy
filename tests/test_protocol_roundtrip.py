import os
from pathlib import Path
import tempfile
import unittest

from bim_bridge import BimBridge, BimBridgeError

MODEL='11111111-1111-4111-8111-111111111111'


@unittest.skipUnless(os.environ.get('BIM_BRIDGE_TEST_HARNESS'),'Build .NET protocol harness first')
class ProtocolRoundTripTests(unittest.TestCase):
    def test_real_manager_and_worker_roundtrip_with_upload_and_download_faults(self):
        for operation in ('elements','spatial'):
            with self.subTest(operation=operation),tempfile.TemporaryDirectory() as directory:
                previous=os.environ.get('BIM_BRIDGE_HARNESS_ROOT')
                os.environ['BIM_BRIDGE_HARNESS_ROOT']=directory
                client=BimBridge.from_pilot('offline-test/database','fake-user','fake-password',
                    adapter=os.environ['BIM_BRIDGE_TEST_HARNESS'],retries=0,timeout=30,poll_interval=.01)
                path=Path(directory)/('output.ndjson.gz' if operation=='elements' else 'output.bbsp')
                export=getattr(client,'export_'+operation)
                try:
                    with self.assertRaises(BimBridgeError) as caught:export(MODEL,path)
                    self.assertEqual(caught.exception.code,'PILOT_TRANSPORT_ERROR',str(caught.exception))
                    self.assertEqual(path.with_name(path.name+'.inprogress').stat().st_size,128)
                    client.close() # next call starts a new local harness process and Manager runtime
                    artifact=export(MODEL,path)
                    self.assertEqual((Path(directory)/'extraction-count').read_text(),'1')
                    if operation=='elements':self.assertEqual(len(list(artifact.iter_elements())),200)
                    else:self.assertEqual(artifact.spatial().validate()['Summary']['MeshCount'],1)
                    self.assertFalse(path.with_name(path.name+'.inprogress').exists())
                finally:
                    client.close()
                    if previous is None:os.environ.pop('BIM_BRIDGE_HARNESS_ROOT',None)
                    else:os.environ['BIM_BRIDGE_HARNESS_ROOT']=previous
