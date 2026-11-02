"""Высокоуровневый клиент и дескриптор задания."""
from dataclasses import dataclass
from pathlib import Path
import time
import uuid
from .errors import BimBridgeError, TaskFailed, TaskTimeout
from .transport import Transport
from .types import Condition, guid, utc, element_payload

CAPABILITIES = {"get-model-context": "model-context", "model-context": "model-context",
                "get-element-spatial-data": "element-spatial-data",
                "diagnose-element-spatial-data": "spatial-diagnostics",
                "echo": "diagnostics", "delay": "diagnostics",
                "generate-test-result": "diagnostics", "fail-test": "diagnostics"}
FAILED_STATES = {"FAILED", "CANCELED", "CANCELLED", "ABANDONED", "EXPIRED"}

@dataclass
class Task:
    client: "BimBridge"
    id: str
    client_request_id: str | None = None
    def status(self):
        return self.client._http.json("GET", f"/tasks/{self.id}")
    def cancel(self):
        return self.client._http.json("DELETE", f"/tasks/{self.id}")
    def wait(self, *, timeout=None, poll_interval=None, on_status=None, cancel_on_timeout=False):
        timeout = self.client.task_timeout if timeout is None else timeout
        interval = self.client.poll_interval if poll_interval is None else poll_interval
        if timeout <= 0 or interval <= 0:
            raise ValueError("timeout и poll_interval должны быть положительными")
        deadline = time.monotonic() + timeout
        while True:
            status = self.status()
            if on_status:
                on_status(status)
            state = status.get("State", "").upper()
            if state == "COMPLETED":
                return status
            if state in FAILED_STATES:
                error = status.get("TaskError") or {}
                raise TaskFailed(error.get("Code", state), str(error.get("Message", state)).replace(self.client._http._token, "[REDACTED]"),
                                 retryable=bool(error.get("IsRetryable")), task_id=self.id)
            if time.monotonic() >= deadline:
                if cancel_on_timeout:
                    self.cancel()
                raise TaskTimeout("CLIENT_TIMEOUT", "Ожидание завершено. Продолжить: client.task(id)", task_id=self.id)
            time.sleep(min(interval, max(0, deadline - time.monotonic())))
    def result(self):
        """Получить готовый JSON без ожидания. Бинарные файлы: download()."""
        return self.client._http.json("GET", f"/tasks/{self.id}/result")
    def metadata(self):
        return self.client._http.json("GET", f"/tasks/{self.id}/result/metadata")
    def download(self, destination, *, overwrite=False, decompress=False, progress=None, resume=True):
        return self.client._http.download(f"/tasks/{self.id}/result/content", destination, self.metadata(),
                                         overwrite=overwrite, decompress=decompress, progress=progress, resume=resume)

class BimBridge:
    @classmethod
    def from_pilot(cls, server_database, login, password, *, task_timeout=1800, poll_interval=2, **transport_options):
        """Pilot Custom API directly; suitable for Colab with .NET SDK/runtime."""
        from .pilot import PilotTransport
        if task_timeout<=0 or poll_interval<=0:raise ValueError('Таймаут и интервал должны быть положительными')
        instance=cls.__new__(cls)
        instance._http=PilotTransport(server_database,login,password,**transport_options)
        instance.task_timeout,instance.poll_interval=task_timeout,poll_interval
        return instance

    def close(self):
        close=getattr(self._http,'close',None)
        if close:close()

    def __init__(self, base_url="https://360pilot.ru/bim-bridge", *, token=None,
                 token_file=None, task_timeout=1800, poll_interval=2, http_timeout=60,
                 retries=2, auth="token", max_json_bytes=128*1024*1024):
        if token is not None and token_file is not None:
            raise ValueError("Задайте token или token_file, не оба")
        if token_file is not None:
            token = Path(token_file).read_text(encoding="utf-8-sig").strip()
        if task_timeout <= 0 or poll_interval <= 0:
            raise ValueError("Таймаут и интервал должны быть положительными")
        self._http = Transport(base_url, token, timeout=http_timeout, retries=retries, auth=auth, max_json_bytes=max_json_bytes)
        self.task_timeout, self.poll_interval = task_timeout, poll_interval
    def health(self):
        return self._http.json("GET", "/health")
    def export_elements(self, model_key, destination, **options):
        from .exports import export_elements
        return export_elements(self,model_key,destination,**options)
    def export_spatial(self, model_key, destination, **options):
        from .exports import export_spatial
        return export_spatial(self,model_key,destination,**options)
    def export_spatial_parallel(self, model_key, destination, **options):
        from .parallel import export_spatial_parallel
        return export_spatial_parallel(self, model_key, destination, **options)
    def _parallel_fork(self):
        instance=type(self).__new__(type(self))
        instance._http=self._http.fork()
        instance.task_timeout,instance.poll_interval=self.task_timeout,self.poll_interval
        return instance
    def statistics(self):
        return self._http.json("GET", "/statistics")
    def workers(self):
        return self._http.json("GET", "/workers")
    def supports(self, capability, *, model_key=None):
        workers=self.workers()
        if isinstance(workers,dict):workers=workers.get('Workers',[])
        key=guid(model_key) if model_key else None
        return any(capability in worker.get('Capabilities',[]) and
                   (key is None or key in [str(k).lower() for k in worker.get('AvailableModelKeys',[])]
                    or 'resolve-model-key' in worker.get('Capabilities',[]))
                   for worker in workers if worker.get('State') not in ('OFFLINE','UNREGISTERED'))
    def search_page(self, model_key, conditions=(), *, part_ids=None, logical_operator='And',
                    page_size=10000, cursor=None, model_version_utc=None, **options):
        if not self.supports('paged-search-v2',model_key=model_key):
            raise BimBridgeError('CAPABILITY_NOT_AVAILABLE','Worker не поддерживает paged-search-v2')
        payload={'TargetModelKey':guid(model_key),'Conditions':[c.payload() if isinstance(c,Condition) else c for c in conditions],
                 'LogicalOperator':logical_operator,'PageSize':page_size,'Cursor':cursor}
        if part_ids is not None:payload['TargetModelPartIds']=[guid(p) for p in part_ids]
        if model_version_utc is not None:payload['ModelVersionUtc']=utc(model_version_utc)
        return self.run('search-elements',payload,target_model_key=model_key,
                        required_capabilities=['paged-search-v2'],**options)
    def task(self, task_id):
        return Task(self, guid(task_id))
    def submit(self, operation, payload=None, *, target_model_key=None,
               preferred_worker_instance_id=None, required_capabilities=None,
               priority=0, can_retry=True, max_attempts=2, expires_at_utc=None,
               client_request_id=None, protocol_version=None):
        """POST не повторяется автоматически при сетевой ошибке."""
        request_id = guid(client_request_id) if client_request_id else str(uuid.uuid4())
        body = {"clientRequestId": request_id, "operationCode": operation,
                "payload": {} if payload is None else payload,
                "targetModelKey": guid(target_model_key) if target_model_key else None,
                "requiredCapabilities": required_capabilities or [CAPABILITIES.get(operation, operation)],
                "preferredWorkerInstanceId": preferred_worker_instance_id,
                "priority": priority, "canRetry": can_retry, "maxAttempts": max_attempts,
                "expiresAtUtc": utc(expires_at_utc)}
        if protocol_version:
            body["protocolVersion"] = protocol_version
        try:
            accepted = self._http.json("POST", "/tasks", body)
        except BimBridgeError as exc:
            exc.client_request_id = request_id
            raise
        return Task(self, guid(accepted["TaskId"]), request_id)
    def run(self, operation, payload=None, *, timeout=None, on_status=None,
            cancel_on_timeout=False, **submit_options):
        task = self.submit(operation, payload, **submit_options)
        task.wait(timeout=timeout, on_status=on_status, cancel_on_timeout=cancel_on_timeout)
        return task.result()
    def model_context(self, **options):
        return self.run("get-model-context", {}, **options)
    get_model_context = model_context
    def search_elements(self, model_key, conditions, *, part_ids=None,
                        logical_operator="And", max_results=10000, model_version_utc=None, **options):
        if not conditions:
            raise ValueError("Нужно условие. Для всех элементов используйте iter_search_elements")
        payload = {"TargetModelKey": guid(model_key),
                   "Conditions": [c.payload() if isinstance(c, Condition) else c for c in conditions],
                   "LogicalOperator": logical_operator, "MaxResults": max_results}
        if part_ids is not None:
            payload["TargetModelPartIds"] = [guid(p) for p in part_ids]
        if model_version_utc is not None:
            payload["ModelVersionUtc"] = utc(model_version_utc)
        return self.run("search-elements", payload, target_model_key=model_key, **options)
    def load_properties(self, model_key, elements, *, property_names=None, batch_size=100, model_version_utc=None, **options):
        payload = {"TargetModelKey": guid(model_key),
                        "ElementIds": [element_payload(e) for e in elements],
                        "PropertyNames": property_names, "BatchSize": batch_size}
        if model_version_utc is not None:
            payload["ModelVersionUtc"] = utc(model_version_utc)
        return self.run("load-element-properties", payload, target_model_key=model_key, **options)
    load_element_properties = load_properties
    def get_relations(self, model_key, elements, *, include_parent=True,
                      include_children=True, max_children=10000, model_version_utc=None, **options):
        if not (include_parent or include_children):
            raise ValueError("Нужно запросить родителя или дочерние элементы")
        payload = {"TargetModelKey": guid(model_key),
                        "ElementIds": [element_payload(e) for e in elements],
                        "IncludeParent": include_parent, "IncludeChildren": include_children,
                        "MaxChildren": max_children}
        if model_version_utc is not None:
            payload["ModelVersionUtc"] = utc(model_version_utc)
        return self.run("get-element-relations", payload, target_model_key=model_key, **options)
    get_element_relations = get_relations
    def get_spatial_data(self, model_key, elements, *, include_descendants=False,
                         include_meshes=False, model_version_utc=None, **options):
        """Габариты и опциональные меши в исходном масштабе SDK, до 100 элементов."""
        from .spatial import _payload, _options
        payload = _payload(model_key, elements, include_descendants, include_meshes, model_version_utc)
        return self.run("get-element-spatial-data", payload, target_model_key=guid(model_key), **_options(options))
    get_element_spatial_data = get_spatial_data
    def download_file(self, reference, destination, *, overwrite=False, timeout=None,
                      on_status=None, progress=None, **options):
        task = self.submit("download-pilot-file", {"FileReference": reference.payload()}, **options)
        task.wait(timeout=timeout, on_status=on_status)
        return task.download(destination, overwrite=overwrite, progress=progress)
    download_pilot_file = download_file
    def read_excel(self, reference, *, sheet_names=None, include_formulas=False,
                   include_empty_cells=False, **options):
        return self.run("read-excel-content", {"FileReference": reference.payload(),
                        "SheetNames": sheet_names, "IncludeFormulas": include_formulas,
                        "IncludeEmptyCells": include_empty_cells}, **options)
    read_excel_content = read_excel
    def diagnostic(self, operation="echo", *, message=None, delay_ms=0, size=0, **options):
        if operation not in {"echo", "delay", "generate-test-result", "fail-test"}:
            raise ValueError("Неизвестная диагностическая операция")
        return self.run(operation, {"Message": message, "DelayMilliseconds": delay_ms, "Size": size}, **options)
