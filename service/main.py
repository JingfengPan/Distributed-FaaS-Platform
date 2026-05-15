from fastapi import FastAPI, HTTPException
import uuid
from models import (
    RegisterFn, RegisterFnRep,
    ExecuteFnReq, ExecuteFnRep,
    TaskStatusRep, TaskResultRep
)
from redis_client import (
    store_function, get_function,
    create_task, get_task
)
from starlette.responses import RedirectResponse
import base64
import re

app = FastAPI()


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


def is_valid_base64(s: str) -> bool:
    clean = re.sub(r"\s+", "", s)
    try:
        base64.b64decode(clean)
        return True
    except Exception:
        return False


@app.post("/register_function", response_model=RegisterFnRep)
def register_function(req: RegisterFn):
    if not is_valid_base64(req.payload):
        raise HTTPException(status_code=400, detail="Invalid function payload")
    fid = str(uuid.uuid4())
    store_function(fid, req.name, req.payload)
    return RegisterFnRep(function_id=fid)


@app.post("/execute_function", response_model=ExecuteFnRep)
def execute_function(req: ExecuteFnReq):
    fn = get_function(str(req.function_id))
    if fn is None:
        raise HTTPException(status_code=404, detail="Function not found")
    tid = str(uuid.uuid4())
    create_task(tid, str(req.function_id), fn["payload"], req.payload)
    return ExecuteFnRep(task_id=tid)


@app.get("/status/{task_id}", response_model=TaskStatusRep)
def get_status(task_id: str):
    task = get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskStatusRep(task_id=task["task_id"], status=task["status"])


@app.get("/result/{task_id}", response_model=TaskResultRep)
def get_result(task_id: str):
    task = get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskResultRep(
        task_id=task["task_id"], status=task["status"], result=task["result"])
