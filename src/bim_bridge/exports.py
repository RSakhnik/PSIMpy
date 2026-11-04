"""Worker-side bulk selection and durable artifact handles."""
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
import uuid
import time

from .bulk import ExportSnapshot, _atomic_json
from .errors import BimBridgeError, IncompleteResult
from .types import Condition, element_payload, guid, utc


@dataclass(frozen=True)
class ExportArtifact:
    path: Path
    task_id: str
    metadata: dict
    profile_data: dict = None

    def profile(self):
        from .profile import ExportProfile
        if self.profile_data is None:
            return ExportProfile(dict(Shards=[dict(ShardId=0, TaskId=self.task_id,
                State='READY', Metadata=self.metadata)]))
        return ExportProfile(self.profile_data)

    def spatial(self):
        from .binary_spatial import SpatialArtifact
        return SpatialArtifact(self.path,expected_sha256=self.metadata.get('Sha256'))

    def records(self):
        """Stream NDJSON records (manifest, element/error, summary)."""
        with self.path.open('rb') as raw:
            compressed=raw.read(2)==b'\x1f\x8b'
        opener=gzip.open if compressed else open
        with opener(self.path,'rt',encoding='utf-8') as stream:
            for line in stream:
                try:record=json.loads(line)
                except ValueError as error:raise BimBridgeError('INVALID_ARTIFACT','Повреждённая строка NDJSON') from error
                if not isinstance(record,dict):raise BimBridgeError('INVALID_ARTIFACT','Ожидалась NDJSON запись-объект')
                yield record

    def iter_elements(self, *, strict=True):
        """Bounded-memory element iteration. Consume fully to verify summary."""
        manifest=None;summary=None;count=0;errors=0;snapshot=ExportSnapshot()
        for record in self.records():
            kind=record.get('Type')
            if summary is not None:raise BimBridgeError('INVALID_ARTIFACT','Данные после summary')
            if manifest is None:
                if kind!='manifest' or record.get('Format')!='bim-elements' or record.get('FormatVersion')!=1:
                    raise BimBridgeError('INVALID_ARTIFACT','Неизвестный формат elements artifact')
                if not record.get('ModelVersionUtc'):raise IncompleteResult('Manifest не подтвердил snapshot',record)
                manifest=record;snapshot.accept(record);continue
            if kind=='element':count+=1;yield record
            elif kind=='error':
                errors+=1
                if strict:raise IncompleteResult('Экспорт содержит ошибку элемента',record)
            elif kind=='summary':summary=record;snapshot.accept(record)
            else:raise BimBridgeError('INVALID_ARTIFACT','Неизвестный тип NDJSON записи')
        if (summary is None or summary.get('ExportedCount')!=count or summary.get('FailedCount')!=errors
                or summary.get('SelectedCount')!=count+errors):
            raise IncompleteResult('NDJSON не подтвердил полную статистику',summary)
        if strict and (not summary.get('Complete') or errors):
            raise IncompleteResult('Экспорт неполный',summary)

    def iter_dataframes(self, *, chunk_size=10000, strict=True):
        from .dataframe import properties_result_to_dataframe
        if type(chunk_size) is not int or chunk_size<=0:raise ValueError('chunk_size должен быть положительным целым')
        rows=[]
        for record in self.iter_elements(strict=strict):
            rows.append(record)
            if len(rows)==chunk_size:
                yield properties_result_to_dataframe({'Elements':rows});rows=[]
        if rows:yield properties_result_to_dataframe({'Elements':rows})

    def to_dataframe(self, *, strict=True):
        import pandas as pd
        frames=list(self.iter_dataframes(strict=strict))
        return pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()


def _selection(model_key,part_ids,conditions,logical_operator,model_version_utc,element_ids=None):
    result={'TargetModelKey':guid(model_key),'Conditions':[c.payload() if isinstance(c,Condition) else c for c in conditions],
            'LogicalOperator':logical_operator}
    if part_ids is not None:result['ModelPartIds']=[guid(part) for part in part_ids]
    if element_ids is not None:result['ElementIds']=[element_payload(e) for e in element_ids]
    if model_version_utc is not None:result['ModelVersionUtc']=utc(model_version_utc)
    return result


def export_artifact(client,operation,payload,destination,*,timeout=None,on_status=None,progress=None,**options):
    started=time.perf_counter_ns()
    required=[operation]
    if operation=='export-spatial':required.append('spatial-binary-v2')
    destination=Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    state_path=destination.with_name(destination.name+'.export.json')
    identity=hashlib.sha256(json.dumps([client._http.base_url,client._http._token,operation,payload,options],sort_keys=True).encode()).hexdigest()
    if state_path.exists():
        try:state=json.loads(state_path.read_text(encoding='utf-8'))
        except (ValueError,OSError) as error:raise BimBridgeError('INTEGRITY_ERROR','Состояние export повреждено') from error
        if state.get('Identity')!=identity:raise BimBridgeError('EXPORT_CONFLICT','Путь принадлежит другому export; выберите новый путь')
    else:
        if destination.exists():raise FileExistsError(destination)
        state={'Identity':identity,'ClientRequestId':str(uuid.uuid4())}
    _atomic_json(state_path,state)
    if destination.is_file() and state.get('Metadata'):
        metadata=state['Metadata'];digest=hashlib.sha256()
        with destination.open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
        if destination.stat().st_size==metadata.get('TotalLength') and digest.hexdigest()==str(metadata.get('Sha256','')).lower():
            return ExportArtifact(destination,state['TaskId'],metadata,state.get('Profile'))
    for attempt in range(2):
        if not state.get('TaskId'):
            if not client.supports(operation,model_key=payload['TargetModelKey']):
                raise BimBridgeError('CAPABILITY_NOT_AVAILABLE',f'Worker не поддерживает {operation}')
            if 'artifact-upload-v2' not in client.health().get('Capabilities',[]):
                raise BimBridgeError('CAPABILITY_NOT_AVAILABLE','Manager не поддерживает durable artifact-upload-v2')
            # Keep ClientRequestId across an uncertain submit response.
            submit=dict(options);submit.setdefault('can_retry',False);submit.setdefault('max_attempts',1)
            task=client.submit(operation,payload,target_model_key=payload['TargetModelKey'],required_capabilities=required,
                               client_request_id=state['ClientRequestId'],**submit)
            state['TaskId']=task.id;_atomic_json(state_path,state)
        task=client.task(state['TaskId'])
        try:
            status=task.wait(timeout=timeout,on_status=on_status) or {}
            metadata=task.metadata()
        except BimBridgeError as error:
            missing=error.code in ('TASK_NOT_FOUND','RESULT_EXPIRED','EXPIRED') or error.status_code in (404,410)
            partial=destination.with_name(destination.name+'.inprogress')
            download_state=destination.with_name(destination.name+'.download.json')
            # A missing task may be resubmitted only before any result bytes or
            # immutable metadata exist. Never carry partial chunks to a new TaskId.
            pristine=not state.get('Metadata') and not destination.exists() and not partial.exists() and not download_state.exists()
            if attempt or not missing or not pristine:
                raise
            state.pop('TaskId',None)
            state['ClientRequestId']=str(uuid.uuid4())
            _atomic_json(state_path,state)
            continue
        break
    state['Metadata']=metadata;_atomic_json(state_path,state)
    if destination.is_file():
        digest=hashlib.sha256()
        with destination.open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
        if destination.stat().st_size==metadata.get('TotalLength') and digest.hexdigest()==str(metadata.get('Sha256','')).lower():
            return ExportArtifact(destination,task.id,metadata,state.get('Profile'))
    download_started=time.perf_counter_ns()
    transfer={}
    task.download(destination,overwrite=destination.exists(),progress=progress,resume=True,telemetry=transfer)
    download_ms=(time.perf_counter_ns()-download_started)/1e6
    previous=(state.get('Profile') or {}).get('TotalWall') or 0
    profile=dict(ExportSessionId=identity,ModelKey=payload['TargetModelKey'],
        ModelVersionUtc=payload.get('ModelVersionUtc'),
        Options={key:value for key,value in payload.items() if key not in ('ElementIds','Conditions','ModelPartIds')},
        ShardSize=None,
        TotalWall=previous+(time.perf_counter_ns()-started)/1e9,Complete=True,
        Shards=[dict(ShardId=0,TaskId=task.id,State='READY',ElementCount=len(payload.get('ElementIds') or []),
            ElementIdentity=hashlib.sha256(json.dumps([payload.get('ElementIds'),payload.get('Conditions'),
                payload.get('ModelPartIds')],sort_keys=True).encode()).hexdigest(),
            WorkerInstanceId=status.get('WorkerInstanceId'),ProcessingWallMs=status.get('ProcessingWallMs'),
            QueueWaitMs=status.get('QueueWaitMs'),
            AttemptQueueWaitMs=status.get('AttemptQueueWaitMs'),
            AssignmentId=status.get('AssignmentId'),
            RemoteAttemptNumber=status.get('AttemptNumber'),
            ManagerAttempts=status.get('PriorAttempts'),
            TaskAcceptanceWallMs=status.get('TaskAcceptanceWallMs'),
            AssignmentWallMs=status.get('AssignmentWallMs'),
            ResultPreparationWallMs=status.get('ResultPreparationWallMs'),
            HashVerificationMs=status.get('HashVerificationMs'),
            ArtifactPublicationMs=status.get('ArtifactPublicationMs'),
            UploadWallMs=status.get('UploadWallMs'),
            UploadRetryCount=status.get('UploadRetryCount'),
            UploadRetryWaitMs=status.get('UploadRetryWaitMs'),
            SpatialPerformance=status.get('SpatialPerformance'),DownloadWallMs=download_ms,
            **transfer,
            StatusTimes={key:status[key] for key in ('CreatedAtUtc','AssignedAtUtc','StartedAtUtc','PreparedAtUtc','CompletedAtUtc')
                         if status.get(key) is not None},Metadata=metadata)])
    state['Profile']=profile;_atomic_json(state_path,state)
    return ExportArtifact(destination,task.id,metadata,profile)


def export_elements(client,model_key,destination,*,part_ids=None,conditions=(),logical_operator='And',
                    property_names=None,include_identifiers=True,model_version_utc=None,**options):
    payload=_selection(model_key,part_ids,conditions,logical_operator,model_version_utc)
    payload.update(PropertyNames=property_names,IncludeIdentifiers=include_identifiers,Gzip=True)
    return export_artifact(client,'export-elements',payload,destination,**options)


def export_spatial(client,model_key,destination,*,part_ids=None,conditions=(),element_ids=None,logical_operator='And',
                   include_descendants=False,include_meshes=True,include_normals=False,include_indices=True,
                   include_edge_indices=False,model_version_utc=None,**options):
    payload=_selection(model_key,part_ids,conditions,logical_operator,model_version_utc,element_ids)
    payload.update(IncludeDescendants=include_descendants,IncludeMeshes=include_meshes,IncludeNormals=include_normals,
                   IncludeIndices=include_indices,IncludeEdgeIndices=include_edge_indices)
    return export_artifact(client,'export-spatial',payload,destination,**options)
