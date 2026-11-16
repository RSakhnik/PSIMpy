"""HTTP без зависимостей. Redirect запрещён, чтобы не передать токен другому узлу."""
import gzip
import hashlib
import io
import json
import os
import random
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from .errors import BimBridgeError
from ._files import publish

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

class Transport:
    def __init__(self, base_url, token, *, timeout=60, retries=2, auth="token", max_json_bytes=128*1024*1024,
                 retry_base=1.0, retry_cap=5.0, retry_jitter=0.2):
        url = urllib.parse.urlsplit(base_url)
        if url.scheme not in ("https", "http") or not url.netloc or url.username or url.password or url.query or url.fragment:
            raise ValueError("base_url должен быть HTTP(S) URL без учётных данных, query и fragment")
        if not token or any(c in token for c in "\r\n\0"):
            raise ValueError("Нужен непустой API-токен без переводов строк")
        if timeout <= 0 or retries < 0 or max_json_bytes <= 0:
            raise ValueError("Некорректные лимиты HTTP")
        if retry_base <= 0 or retry_cap <= 0 or not 0 <= retry_jitter <= 1:
            raise ValueError("Некорректная политика повторов")
        if auth not in ("token", "bearer"):
            raise ValueError("auth: token или bearer")
        self.base_url, self._token = base_url.rstrip("/"), token
        self.timeout, self.retries, self.max_json_bytes = timeout, retries, max_json_bytes
        self.retry_base, self.retry_cap, self.retry_jitter = retry_base, retry_cap, retry_jitter
        self.max_chunk_bytes = 4 * 1024 * 1024
        self._headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
        self._headers["X-Api-Token" if auth == "token" else "Authorization"] = token if auth == "token" else "Bearer " + token
        self._opener = urllib.request.build_opener(_NoRedirect())

    def fork(self):
        forked = type(self)(self.base_url,self._token,timeout=self.timeout,retries=self.retries,
                          auth='token' if 'X-Api-Token' in self._headers else 'bearer',max_json_bytes=self.max_json_bytes,
                          retry_base=self.retry_base,retry_cap=self.retry_cap,retry_jitter=self.retry_jitter)
        forked.journal = getattr(self, 'journal', None)
        return forked

    def _retry_delay(self, attempt):
        base = min(self.retry_cap, self.retry_base * 2 ** attempt)
        return base * random.uniform(1 - self.retry_jitter, 1 + self.retry_jitter)

    def _error(self, response, status):
        try:
            data = json.loads(response.read(65536))
            error = data.get("Error") or data
            return BimBridgeError(error.get("Code", f"HTTP_{status}"),
                str(error.get("Message", "Ошибка API")).replace(self._token, "[REDACTED]"),
                status_code=status, retryable=bool(error.get("IsRetryable", status in (429, 502, 503, 504))))
        except (ValueError, AttributeError, TypeError):
            return BimBridgeError(f"HTTP_{status}", "Ответ сервера не содержит JSON ошибки",
                status_code=status, retryable=status in (429, 502, 503, 504))

    def open(self, method, path, payload=None, *, headers=None, operation_id=None):
        if not path.startswith("/") or path.startswith("//") or "?" in path or "#" in path:
            raise ValueError("Нужен относительный путь API")
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = dict(self._headers, **(headers or {}))
        if data is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
        attempts = self.retries + 1 if method == "GET" else 1
        operation_id = operation_id or str(uuid.uuid4())
        task_id = path.split('/')[2] if path.startswith('/tasks/') and len(path.split('/')) > 2 else None
        def record(event, began):
            event.update(OperationId=operation_id, Operation=method + ' ' + path,
                         TaskId=task_id, StartTime=began,
                         EndTime=datetime.now(timezone.utc).isoformat(),
                         ModelKey=payload.get('targetModelKey') or payload.get('TargetModelKey') if isinstance(payload, dict) else None,
                         ModelVersionUtc=payload.get('ModelVersionUtc') if isinstance(payload, dict) else None,
                         ClientRequestId=payload.get('clientRequestId') if isinstance(payload, dict) else None,
                         PayloadBytes=len(data) if data is not None else 0)
            recorder = getattr(self, 'telemetry', None)
            if recorder: recorder(dict(event))
            journal = getattr(self, 'journal', None)
            if journal: journal(event)
        for attempt in range(attempts):
            started = time.perf_counter_ns()
            began = datetime.now(timezone.utc).isoformat()
            error_code = None
            delay = self._retry_delay(attempt) if attempt + 1 < attempts else 0
            try:
                result = self._opener.open(urllib.request.Request(self.base_url + path, data, headers, method=method), timeout=self.timeout)
                record(dict(EventType='http_attempt', Attempt=attempt+1, RetryCount=attempt,
                            NetworkWallMs=(time.perf_counter_ns()-started)/1e6,
                            RetryWaitMs=0, Success=True, ErrorCode=None,
                            HttpStatus=getattr(result, 'status', None), Retryable=False), began)
                return result
            except urllib.error.HTTPError as exc:
                with exc:
                    error = self._error(exc, exc.code)
                error_code = error.code
                retry = exc.code in (429, 502, 503, 504) and attempt + 1 < attempts
                record(dict(EventType='http_attempt', Attempt=attempt+1, RetryCount=attempt,
                            NetworkWallMs=(time.perf_counter_ns()-started)/1e6,
                            RetryWaitMs=delay*1000 if retry else 0,
                            Success=False, ErrorCode=error_code,
                            HttpStatus=exc.code, Retryable=error.retryable), began)
                if exc.code not in (429, 502, 503, 504) or attempt + 1 == attempts:
                    raise error from None
            except (urllib.error.URLError, TimeoutError, OSError):
                record(dict(EventType='http_attempt', Attempt=attempt+1, RetryCount=attempt,
                            NetworkWallMs=(time.perf_counter_ns()-started)/1e6,
                            RetryWaitMs=delay*1000,
                            Success=False, ErrorCode='TRANSPORT_ERROR',
                            HttpStatus=None, Retryable=True), began)
                if attempt + 1 == attempts:
                    raise BimBridgeError("TRANSPORT_ERROR", "Соединение с BIM Bridge не выполнено",
                                         retryable=True,
                                         failure_kind='ambiguous' if method == 'POST' else 'temporary') from None
            time.sleep(delay)

    def json(self, method, path, payload=None):
        operation_id = str(uuid.uuid4())
        opened = False
        try:
            with self.open(method, path, payload, operation_id=operation_id) as response:
                opened = True
                raw = response.read(self.max_json_bytes + 1)
            if len(raw) > self.max_json_bytes:
                raise BimBridgeError("RESULT_TOO_LARGE", "Используйте Task.download() для большого результата")
            if raw.startswith(b"\x1f\x8b"):
                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as zipped:
                        raw = zipped.read(self.max_json_bytes + 1)
                except (OSError, EOFError):
                    raise BimBridgeError("INVALID_RESPONSE", "Повреждённый gzip JSON") from None
                if len(raw) > self.max_json_bytes:
                    raise BimBridgeError("RESULT_TOO_LARGE", "Распакованный JSON превышает лимит клиента")
            try:
                result = json.loads(raw.decode("utf-8-sig")) if raw else {}
            except (ValueError, UnicodeError):
                raise BimBridgeError("INVALID_RESPONSE", "Ожидался JSON. Для файла используйте download()") from None
            if isinstance(result, dict) and result.get("Error"):
                error = result["Error"]
                raise BimBridgeError(error.get("Code", "API_ERROR"), str(error.get("Message", "Ошибка API")).replace(self._token, "[REDACTED]"), retryable=bool(error.get("IsRetryable")))
        except BimBridgeError as error:
            journal = getattr(self, 'journal', None)
            if opened and journal:
                journal(dict(EventType='response_validation', OperationId=operation_id,
                             Operation=method + ' ' + path, Success=False,
                             ErrorCode=error.code, Retryable=error.retryable,
                             ClientRequestId=payload.get('clientRequestId') if isinstance(payload, dict) else None))
            raise
        return result

    def rpc(self, method, **arguments):
        if method != 'ReadResultChunk':
            raise BimBridgeError('INVALID_REQUEST', 'Unknown HTTP compatibility RPC')
        from .types import guid
        task_id=guid(arguments['TaskId']);index=arguments['ChunkIndex']
        if type(index) is not int or index < 0:raise ValueError('Invalid chunk index')
        return self.json('GET', f'/tasks/{task_id}/result/chunks/{index}')

    def download(self, path, destination, metadata, *, overwrite=False, decompress=False, progress=None, resume=False,
                 telemetry=None):
        if metadata.get('DownloadProtocol') == 'token-http-chunks-v2':
            from ._chunk_download import download_chunks
            from .types import guid
            parts=path.strip('/').split('/')
            if len(parts) not in (3,4) or parts[0]!='tasks' or parts[2]!='result' or (len(parts)==4 and parts[3]!='content'):raise ValueError('Invalid result path')
            return download_chunks(self,guid(parts[1]),destination,metadata,overwrite=overwrite,
                                   decompress=decompress,progress=progress,resume=resume,telemetry=telemetry)
        if resume:
            from ._download import download_resumable
            return download_resumable(self, path, destination, metadata, overwrite=overwrite,
                                      decompress=decompress, progress=progress)
        destination = Path(destination)
        if destination.exists() and not overwrite:
            raise FileExistsError(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(prefix=".bim-download-", dir=destination.parent)
        unpacked = None
        try:
            digest, count = hashlib.sha256(), 0
            with os.fdopen(fd, "wb") as target, self.open("GET", path) as source:
                for chunk in iter(lambda: source.read(1024*1024), b""):
                    target.write(chunk); digest.update(chunk); count += len(chunk)
                    if progress:
                        progress(count, metadata.get("TotalLength"))
            if metadata.get("TotalLength") is not None and count != metadata["TotalLength"]:
                raise BimBridgeError("INTEGRITY_ERROR", "Длина скачанного файла не совпала")
            expected = metadata.get("Sha256")
            if expected and digest.hexdigest().lower() != expected.lower():
                raise BimBridgeError("INTEGRITY_ERROR", "SHA-256 скачанного файла не совпал")
            if decompress and (metadata.get("ContentEncoding") or "").lower() == "gzip":
                fd, unpacked = tempfile.mkstemp(prefix=".bim-unpacked-", dir=destination.parent)
                with os.fdopen(fd, "wb") as target, gzip.open(temp, "rb") as source:
                    for chunk in iter(lambda: source.read(1024*1024), b""):
                        target.write(chunk)
                os.unlink(temp); temp, unpacked = unpacked, None
            publish(temp, destination, overwrite)
            return destination
        finally:
            for leftover in (temp, unpacked):
                if leftover and os.path.exists(leftover):
                    os.unlink(leftover)
