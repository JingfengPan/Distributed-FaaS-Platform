from pydantic import BaseModel
import uuid


# Register function
class RegisterFn(BaseModel):
    name: str
    payload: str


class RegisterFnRep(BaseModel):
    function_id: uuid.UUID


# Execute function
class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str


class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID


# Task status
class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str


# Task result
class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str | None
