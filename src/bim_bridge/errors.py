"""Ошибки транспорта, заданий и неполных выгрузок."""
class BimBridgeError(Exception):
    def __init__(self, code, message, *, status_code=None, retryable=False,
                 task_id=None, client_request_id=None):
        self.code, self.message = code, message
        self.status_code, self.retryable = status_code, retryable
        self.task_id, self.client_request_id = task_id, client_request_id
        super().__init__(f"{code}: {message}" + (f" [task={task_id}]" if task_id else ""))

class TaskTimeout(BimBridgeError):
    pass

class TaskFailed(BimBridgeError):
    pass

class IncompleteResult(BimBridgeError):
    def __init__(self, message, result=None):
        self.result = result
        super().__init__("INCOMPLETE_RESULT", message)
