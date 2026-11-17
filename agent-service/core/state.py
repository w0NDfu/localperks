from enum import StrEnum


class RunState(StrEnum):
    PLANNING = "planning"
    CALLING_MODEL = "calling_model"
    EXECUTING_TOOL = "executing_tool"
    COMPLETED = "completed"
    FAILED = "failed"


class AgentError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 502):
        super().__init__(message)
        self.code = code
        self.status_code = status_code

