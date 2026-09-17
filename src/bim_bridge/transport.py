"""HTTP без зависимостей. Redirect запрещён, чтобы не передать токен другому узлу."""
import gzip
import hashlib
import io
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from .errors import BimBridgeError
from ._files import publish

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

class Transport:
    def __init__(self, base_url, token, *, timeout=60, retries=2, auth="token", max_json_bytes=128*1024*1024):
        url = urllib.parse.urlsplit(base_url)
        if url.scheme not in ("https", "http") or not url.netloc or url.username or url.password or url.query or url.fragment:
            raise ValueError("base_url должен быть HTTP(S) URL без учётных данных, query и fragment")
        if not token or any(c in token for c in "\r\n\0"):
            raise ValueError("Нужен непустой API-токен без переводов строк")
        if timeout <= 0 or retries < 0 or max_json_bytes <= 0:
            raise ValueError("Некорректные лимиты HTTP")
        if auth not in ("token", "bearer"):
            raise ValueError("auth: token или bearer")
        self.base_url, self._token = base_url.rstrip("/"), token
        self.timeout, self.retries, self.max_json_bytes = timeout, retries, max_json_bytes
        self._headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
        self._headers["X-Api-Token" if auth == "token" else "Authorization"] = token if auth == "token" else "Bearer " + token
        self._opener = urllib.request.build_opener(_NoRedirect())

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

    def open(self, method, path, payload=None):
        if not path.startswith("/") or path.startswith("//") or "?" in path or "#" in path:
            raise ValueError("Нужен относительный путь API")
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = dict(self._headers)
        if data is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
        attempts = self.retries + 1 if method == "GET" else 1
        for attempt in range(attempts):
            try:
                return self._opener.open(urllib.request.Request(self.base_url + path, data, headers, method=method), timeout=self.timeout)
            except urllib.error.HTTPError as exc:
                with exc:
                    error = self._error(exc, exc.code)
                if exc.code not in (429, 502, 503, 504) or attempt + 1 == attempts:
                    raise error from None
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt + 1 == attempts:
                    raise BimBridgeError("TRANSPORT_ERROR", "Соединение с BIM Bridge не выполнено", retryable=True) from None
            time.sleep(min(2 ** attempt, 5))

    def json(self, method, path, payload=None):
        with self.open(method, path, payload) as response:
            raw = response.read(self.max_json_bytes + 1)
        if len(raw) > self.max_json_bytes:
            raise BimBridgeError("RESULT_TOO_LARGE", "Используйте Task.download() для большого результата")
        if raw.startswith(b"\x1f\x8b"):
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as zipped:
                raw = zipped.read(self.max_json_bytes + 1)
            if len(raw) > self.max_json_bytes:
                raise BimBridgeError("RESULT_TOO_LARGE", "Распакованный JSON превышает лимит клиента")
        try:
            result = json.loads(raw.decode("utf-8-sig")) if raw else {}
        except (ValueError, UnicodeError):
            raise BimBridgeError("INVALID_RESPONSE", "Ожидался JSON. Для файла используйте download()") from None
        if isinstance(result, dict) and result.get("Error"):
            error = result["Error"]
            raise BimBridgeError(error.get("Code", "API_ERROR"), str(error.get("Message", "Ошибка API")).replace(self._token, "[REDACTED]"), retryable=bool(error.get("IsRetryable")))
        return result

    def download(self, path, destination, metadata, *, overwrite=False, decompress=False, progress=None):
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
