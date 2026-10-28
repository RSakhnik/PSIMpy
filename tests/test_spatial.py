import copy
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from bim_bridge import (BimBridge, BimBridgeError, Checkpoints, Element, ExportSnapshot,
                        IncompleteResult, TaskFailed, iter_spatial_data, iter_placed_vertices)
from spatial_support import FakeClient, MODEL, PART, V1, V2, elements, result_for

FIXTURES=Path(__file__).parent/'fixtures'


class SpatialClientTests(unittest.TestCase):
    def setUp(self):self.client=BimBridge('https://offline.invalid',token='fake')
    def test_simple_payload_alias_and_untouched_json(self):
        response={'unchanged':True};item=Element(PART,elements(1)[0]['ElementId'])
        with patch.object(self.client,'run',return_value=response) as run:
            result=self.client.get_element_spatial_data(MODEL,[item],include_descendants=True,include_meshes=True,
                model_version_utc=V1,timeout=5,cancel_on_timeout=True,preferred_worker_instance_id='worker')
        self.assertIs(response,result)
        args,options=run.call_args
        self.assertEqual(args[0],'get-element-spatial-data')
        self.assertEqual(args[1],{'TargetModelKey':MODEL,'ElementIds':[item.payload()],
            'IncludeDescendants':True,'IncludeMeshes':True,'ModelVersionUtc':V1})
        self.assertFalse(options['can_retry']);self.assertEqual(options['max_attempts'],1)
        self.assertTrue(options['cancel_on_timeout']);self.assertEqual(options['preferred_worker_instance_id'],'worker')
        with patch.object(self.client,'run') as run:
            self.client.get_spatial_data(MODEL,[item],can_retry=True,max_attempts=4)
            self.assertTrue(run.call_args.kwargs['can_retry']);self.assertEqual(run.call_args.kwargs['max_attempts'],4)
    def test_capabilities_for_generic_submit(self):
        for operation,cap in [('get-element-spatial-data','element-spatial-data'),('diagnose-element-spatial-data','spatial-diagnostics')]:
            with patch.object(self.client._http,'json',return_value={'TaskId':MODEL}) as request:
                self.client.submit(operation,{})
            self.assertEqual(request.call_args.args[2]['requiredCapabilities'],[cap])
    def test_input_validation_is_before_network_and_bounded(self):
        for value in [[],elements(101),[{'ModelPartId':PART,'ElementId':'bad'}]]:
            with self.subTest(value=str(value)[:40]),patch.object(self.client,'run') as run:
                with self.assertRaises((ValueError,TypeError)):self.client.get_spatial_data(MODEL,value)
                run.assert_not_called()
        with self.assertRaises(TypeError):self.client.get_spatial_data(MODEL,elements(1),include_meshes='False')
        consumed=[]
        def infinite():
            while True:consumed.append(1);yield elements(1)[0]
        with self.assertRaises(ValueError):self.client.get_spatial_data(MODEL,infinite())
        self.assertEqual(len(consumed),101)
    def test_search_version_is_inherited_and_conflicts_rejected(self):
        keys=[dict(elements(1)[0],ModelVersionUtc=V1)]
        with patch.object(self.client,'run') as run:
            self.client.get_spatial_data(MODEL,keys)
            self.assertEqual(run.call_args.args[1]['ModelVersionUtc'],V1)
        with self.assertRaises(BimBridgeError):self.client.get_spatial_data(MODEL,keys,model_version_utc=V2)
    def test_duplicate_ids_are_composite_and_keep_order(self):
        first=elements(1)[0];other=dict(first,ModelPartId=MODEL)
        with patch.object(self.client,'run') as run:
            self.client.get_spatial_data(MODEL,[first,other,first])
        self.assertEqual(run.call_args.args[1]['ElementIds'],[first,other])


class SpatialBulkTests(unittest.TestCase):
    def test_lazy_defaults_and_complete_packets(self):
        for meshes,size in [(False,20),(True,5)]:
            client=FakeClient();consumed=[]
            def source():
                for key in elements(41):consumed.append(1);yield key
            iterator=iter_spatial_data(client,MODEL,source(),include_meshes=meshes)
            self.assertFalse(consumed);next(iterator);self.assertEqual(len(consumed),size)
            list(iterator)
            self.assertEqual(len(client.calls),(41+size-1)//size)
            self.assertEqual(client.calls[1][1]['ModelVersionUtc'],V1)
    def test_batch_validation_empty_input_and_snapshot(self):
        for size in [0,-1,101,True,1.5]:
            with self.assertRaises(ValueError):list(iter_spatial_data(FakeClient(),MODEL,elements(1),batch_size=size))
        client=FakeClient();self.assertEqual(list(iter_spatial_data(client,MODEL,[])),[]);self.assertEqual(client.calls,[])
        keys=[dict(e,ModelVersionUtc=V1) for e in elements(2)]
        snapshot=ExportSnapshot()
        list(iter_spatial_data(client,MODEL,keys,batch_size=1,snapshot=snapshot))
        self.assertEqual(snapshot.version,V1);self.assertTrue(snapshot.verified)
    def test_worker_size_error_is_the_only_split_trigger(self):
        client=FakeClient();client.limit=2
        result=list(iter_spatial_data(client,MODEL,elements(5),batch_size=5))
        self.assertEqual([len(r['Elements']) for r in result],[2,1,2])
        self.assertTrue(all(call[1].get('ModelVersionUtc')==V1 for call in client.calls[2:]))
        for error in [TaskFailed('MODEL_CHANGED','changed'),BimBridgeError('RESULT_TOO_LARGE','local parser'),BimBridgeError('TRANSPORT_ERROR','network')]:
            client=FakeClient();client.error=error
            with self.assertRaises(BimBridgeError):list(iter_spatial_data(client,MODEL,elements(4)))
            self.assertEqual(len(client.calls),1)
        client=FakeClient();client.limit=0
        with self.assertRaises(TaskFailed):list(iter_spatial_data(client,MODEL,elements(1)))
    def test_strict_states_failures_warnings_and_missing_keys(self):
        for state in ('partial','failed'):
            client=FakeClient();client.state=state
            with self.assertRaises(IncompleteResult):list(iter_spatial_data(client,MODEL,elements(1)))
            self.assertEqual(len(list(iter_spatial_data(client,MODEL,elements(1),strict=False))),1)
        for field,value in [('FailedElementIds',[elements(1)[0]]),('ElementErrors',[{'Code':'TEST'}]),('Warnings',['test']),('Truncated',True),('Elements',[])]:
            client=FakeClient();client.transform=lambda r,field=field,value=value:dict(r,**{field:value})
            with self.subTest(field=field),self.assertRaises(IncompleteResult):list(iter_spatial_data(client,MODEL,elements(1)))
        client=FakeClient();client.transform=lambda r:dict(r,ModelVersionUtc=None)
        with self.assertRaises(IncompleteResult):list(iter_spatial_data(client,MODEL,elements(1),strict=False))
    def test_snapshot_change_stops_before_third_request(self):
        client=FakeClient();client.versions=[V1,V2]
        with self.assertRaises(BimBridgeError) as caught:list(iter_spatial_data(client,MODEL,elements(3),batch_size=1))
        self.assertEqual(caught.exception.code,'MODEL_CHANGED');self.assertEqual(len(client.calls),2)
    def test_mixed_input_versions_rejected_before_submit(self):
        client=FakeClient();keys=[dict(e,ModelVersionUtc=v) for e,v in zip(elements(2),(V1,V2))]
        with self.assertRaises(BimBridgeError):list(iter_spatial_data(client,MODEL,keys))
        self.assertFalse(client.calls)
    def test_checkpoints_separate_version_flags_and_partial_cache(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            checkpoints=Checkpoints(client,directory)
            def run(**kw):return list(iter_spatial_data(client,MODEL,elements(1),checkpoints=checkpoints,**kw))
            run(model_version_utc=V1);run(model_version_utc=V1);self.assertEqual(len(client.submits),1)
            run(model_version_utc=V2);run(model_version_utc=V2,include_meshes=True);run(model_version_utc=V2,include_descendants=True)
            self.assertEqual(len(client.submits),4)
            run();run();self.assertEqual(len(client.submits),6)
            client.state='partial'
            with self.assertRaises(IncompleteResult):run(model_version_utc=V2,include_descendants=True,include_meshes=True)
            client.state='no-geometry';run(model_version_utc=V2,include_descendants=True,include_meshes=True)
            self.assertEqual(len(client.submits),8)
            for path in checkpoints.directory.glob('*.json'):self.assertNotIn('not-a-real-secret',path.read_text())
    def test_corrupt_spatial_cache_can_refetch_same_completed_task(self):
        client=FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            cp=Checkpoints(client,directory)
            list(iter_spatial_data(client,MODEL,elements(1),model_version_utc=V1,checkpoints=cp))
            next(cp.directory.glob('*.result.json')).write_text('bad JSON')
            list(iter_spatial_data(client,MODEL,elements(1),model_version_utc=V1,checkpoints=cp))
            self.assertEqual(len(client.submits),1)


class SpatialAnalysisTests(unittest.TestCase):
    def test_live_sanitized_fixtures_reconstruct_bounds(self):
        for name in ('spatial_300.json','spatial_310.json'):
            result=json.loads((FIXTURES/name).read_text())
            for element in result['Elements']:
                vertices=list(iter_placed_vertices(result,element))
                self.assertTrue(vertices)
                self.assertEqual([min(p[i] for p in vertices) for i in range(3)],element['BoundsMin'])
                self.assertEqual([max(p[i] for p in vertices) for i in range(3)],element['BoundsMax'])
            self.assertNotIn('d8ea8564-2e67-4f5f-98bc-a0f3f5ed54b8',json.dumps(result))
    def test_selector_and_no_geometry(self):
        result=result_for(elements(1));self.assertEqual(list(iter_placed_vertices(result)),[])
        fixture=json.loads((FIXTURES/'spatial_300.json').read_text())
        key=fixture['Elements'][0]['Element']
        self.assertEqual(list(iter_placed_vertices(fixture,key)),list(iter_placed_vertices(fixture,Element(key['ModelPartId'],key['ElementId']))))
        with self.assertRaises(ValueError):list(iter_placed_vertices(fixture))
        del fixture['MeshDefinitions']
        with self.assertRaises(ValueError):list(iter_placed_vertices(fixture,key))
    def test_composite_definition_keys_and_no_unit_conversion(self):
        result=result_for(elements(1),state='complete');root=result['Elements'][0]
        root['Instances']=[{'MeshDefinitionKey':'part1/mesh','ModelPlacement':[1,0,0,12981000,0,1,0,0,0,0,1,0,0,0,0,1]},
                           {'MeshDefinitionKey':'part2/mesh','ModelPlacement':[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]}]
        result['MeshDefinitions']=[{'Key':'part1/mesh','Vertices':[2000,0,0]},{'Key':'part2/mesh','Vertices':[3,4,5]}]
        self.assertEqual(list(iter_placed_vertices(result)),[(12983000.,0.,0.),(3.,4.,5.)])
        root['Instances'][0]['ModelPlacement'][0]=float('nan')
        with self.assertRaises(BimBridgeError):list(iter_placed_vertices(result))
    def test_dataframe_only_has_scalar_bounds_and_metadata(self):
        from bim_bridge.dataframe import spatial_result_to_dataframe
        result=json.loads((FIXTURES/'spatial_300.json').read_text());frame=spatial_result_to_dataframe(result)
        self.assertEqual(len(frame),3);self.assertNotIn('MeshDefinitions',frame.columns)
        self.assertEqual(frame.iloc[0]['BoundsMinX'],result['Elements'][0]['BoundsMin'][0])
        self.assertEqual(frame.attrs['spatial']['ModelVersionUtc'],result['ModelVersionUtc'])
        empty=spatial_result_to_dataframe(result_for(elements(1)))
        self.assertTrue(empty[['BoundsMinX','BoundsMaxX','BoundsCenterX']].isna().all().all())
        self.assertFalse('units' in empty.attrs)


if __name__=='__main__':unittest.main()
