"""Direct Pilot Custom API transport via the local HttpPilotClient adapter.

The adapter is a child process speaking stdio, not a network service or facade.
The optional .NET SDK is required to build it; no Python/.NET interop dependency.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import threading
import time

from .errors import BimBridgeError


def build_pilot_adapter(*, dotnet='dotnet', directory=None):
    source=Path(__file__).parent/'_pilot_source'
    if not source.is_dir():
        # Editable source checkout compatibility.
        source=Path(__file__).resolve().parents[2]/'native'/'PilotAdapter'
    if not source.is_dir():
        raise BimBridgeError('PILOT_ADAPTER_MISSING','Исходники PilotAdapter отсутствуют в пакете')
    files=sorted(p for p in source.iterdir() if p.suffix in ('.cs','.csproj'))
    version=hashlib.sha256(b''.join(p.read_bytes() for p in files)).hexdigest()[:16]
    root=Path(directory) if directory else Path.home()/'.cache'/'psimpy'/'pilot-adapter'/version
    assembly=root/'out'/'PilotAdapter.dll'
    if assembly.is_file():return assembly
    if not shutil.which(dotnet):
        raise BimBridgeError('DOTNET_SDK_REQUIRED','Для Pilot Custom API установите .NET SDK 9, затем вызовите build_pilot_adapter()')
    sources=root/'src';sources.mkdir(parents=True,exist_ok=True)
    for path in files:shutil.copy2(path,sources/path.name)
    result=subprocess.run([dotnet,'publish',str(sources/'PilotAdapter.csproj'),'-o',str(root/'out'),'--verbosity','quiet'],
                          capture_output=True,text=True)
    if result.returncode:
        raise BimBridgeError('PILOT_ADAPTER_BUILD_FAILED','Не удалось собрать PilotAdapter: '+result.stdout[-4000:]+result.stderr[-4000:])
    return assembly


class PilotTransport:
    def __init__(self,server_database,login,password,*,adapter=None,dotnet='dotnet',timeout=60,retries=2,
                 max_json_bytes=128*1024*1024,max_chunk_bytes=4*1024*1024,enterprise_code=103):
        if not server_database or not login or not password:raise ValueError('Нужны адрес Pilot/database, login и password')
        if timeout<=0 or retries<0 or max_json_bytes<=0 or max_chunk_bytes<=0:raise ValueError('Некорректные лимиты транспорта')
        self.base_url='pilot://'+server_database.rstrip('/')
        self._token=login # identity only; only its hash is stored in checkpoints
        self.timeout,self.retries,self.max_json_bytes=timeout,retries,max_json_bytes
        self.max_chunk_bytes=max_chunk_bytes
        self._credentials={'ServerDatabase':server_database,'Login':login,'Password':password,'EnterpriseCode':enterprise_code}
        self._command=[dotnet,str(adapter or build_pilot_adapter(dotnet=dotnet))]
        self._lock=threading.Lock();self._process=None

    def _exchange(self,method,**arguments):
        process=self._process
        process.stdin.write(json.dumps(dict(arguments,Method=method))+'\n');process.stdin.flush()
        messages=queue.Queue()
        def read():
            try:messages.put(process.stdout.readline(max(self.max_json_bytes,2*self.max_chunk_bytes)+1))
            except BaseException as error:messages.put(error)
        threading.Thread(target=read,daemon=True).start()
        try: response=messages.get(timeout=self.timeout)
        except queue.Empty:
            self.close()
            raise BimBridgeError('PILOT_TRANSPORT_TIMEOUT','Pilot Custom API не ответил вовремя',retryable=True) from None
        if isinstance(response,BaseException):raise response
        if not response or not response.endswith('\n'):
            self.close()
            raise BimBridgeError('INVALID_RESPONSE','Локальный PilotAdapter завершился или превысил лимит ответа')
        try: result=json.loads(response)
        except ValueError:raise BimBridgeError('INVALID_RESPONSE','PilotAdapter вернул некорректный ответ') from None
        if isinstance(result,dict) and result.get('Error'):
            error=result['Error'];raise BimBridgeError(error.get('Code','PILOT_ERROR'),error.get('Message','Ошибка Pilot'),retryable=bool(error.get('IsRetryable')))
        return result

    def rpc(self,method,**arguments):
        # Submit is never blindly retried. ReadResultChunk is deterministic and
        # safe to repeat after a lost response or a fresh Pilot session.
        attempts=self.retries+1 if method in ('GetHealth','GetWorkers','GetTaskStatus','GetResultMetadata','ReadResultChunk') else 1
        with self._lock:
            for attempt in range(attempts):
                try:
                    if self._process is None or self._process.poll() is not None:
                        self._process=subprocess.Popen(self._command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                                       stderr=subprocess.DEVNULL,text=True,encoding='utf-8',bufsize=1)
                        self._exchange('Connect',**self._credentials)
                    return self._exchange(method,**arguments)
                except (OSError,BimBridgeError) as error:
                    self.close()
                    if isinstance(error,BimBridgeError) and not error.retryable:raise
                    if attempt+1==attempts:
                        if isinstance(error,BimBridgeError):raise
                        raise BimBridgeError('PILOT_TRANSPORT_ERROR','PilotAdapter недоступен',retryable=True) from None
                    time.sleep(min(2**attempt,5))

    def json(self,method,path,payload=None):
        # Internal compatibility adapter for the existing high-level client.
        # These path strings never leave this process as HTTP requests.
        if (method,path)==('GET','/health'):return self.rpc('GetHealth')
        if (method,path)==('GET','/workers'):return self.rpc('GetWorkers')
        if (method,path)==('POST','/tasks'):
            request=dict(payload)
            request['protocolVersion']=request.get('protocolVersion') or '1.0'
            request['payload']=base64.b64encode(json.dumps(request.get('payload') or {}).encode()).decode()
            request['payloadContentType']='application/json';request['payloadEncoding']='utf-8'
            return self.rpc('SubmitTask',Request=request)
        parts=path.strip('/').split('/')
        if len(parts)>=2 and parts[0]=='tasks':
            task_id=parts[1]
            if len(parts)==2:
                return self.rpc('CancelTask' if method=='DELETE' else 'GetTaskStatus',TaskId=task_id)
            metadata=self.rpc('GetResultMetadata',TaskId=task_id)
            if parts[2:]==['result','metadata']:return metadata
            if parts[2:]==['result']:
                if metadata['TotalLength']>self.max_json_bytes:raise BimBridgeError('RESULT_TOO_LARGE','Используйте Task.download()')
                import tempfile
                with tempfile.TemporaryDirectory() as directory:
                    target=self.download(path,Path(directory)/'result.json',metadata,decompress=True)
                    if target.stat().st_size>self.max_json_bytes:raise BimBridgeError('RESULT_TOO_LARGE','Распакованный JSON превышает лимит')
                    try:return json.loads(target.read_text(encoding='utf-8-sig'))
                    except (ValueError,UnicodeError):raise BimBridgeError('INVALID_RESPONSE','Для binary artifact используйте download()') from None
        raise BimBridgeError('INVALID_REQUEST','Неизвестный маршрут локального Pilot adapter')

    def download(self,path,destination,metadata,**options):
        from ._chunk_download import download_chunks
        return download_chunks(self,path.strip('/').split('/')[1],destination,metadata,**options)

    def close(self):
        process=self._process;self._process=None
        if process is not None:
            if process.poll() is None:process.kill()
            process.wait()
            process.stdin.close();process.stdout.close()
