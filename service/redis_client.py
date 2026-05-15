import json
import redis
from enum import Enum

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TASK_CHANNEL = "tasks"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


class TaskStatus(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETED"
    FAILED = "FAILED"


def store_function(function_id: str, name: str, payload: str):
    r.set(f"fn:{function_id}", json.dumps({"name": name, "payload": payload}))


def get_function(function_id: str) -> dict | None:
    data = r.get(f"fn:{function_id}")
    return json.loads(data) if data else None


def create_task(task_id: str, function_id: str, fn_payload: str, param_payload: str):
    task = {
        "task_id":       task_id,
        "function_id":   function_id,
        "fn_payload":    fn_payload,
        "param_payload": param_payload,
        "status":        TaskStatus.QUEUED.value,
        "result":        None
    }
    r.set(f"task:{task_id}", json.dumps(task))
    r.publish(TASK_CHANNEL, task_id)


def get_task(task_id: str) -> dict | None:
    data = r.get(f"task:{task_id}")
    return json.loads(data) if data else None


def update_task_status(task_id: str, status: TaskStatus):
    task = get_task(task_id)
    if not task:
        return False
    task["status"] = status.value
    r.set(f"task:{task_id}", json.dumps(task))
    return True


def update_task_result(task_id: str, result: str, status: TaskStatus):
    task = get_task(task_id)
    if not task:
        return False
    task["status"] = status.value
    task["result"] = result
    r.set(f"task:{task_id}", json.dumps(task))
    return True


def mark_task_running(task_id: str):
    return update_task_status(task_id, TaskStatus.RUNNING)


def mark_task_complete(task_id: str, result: str):
    return update_task_result(task_id, result, TaskStatus.COMPLETE)


def mark_task_failed(task_id: str, result: str):
    return update_task_result(task_id, result, TaskStatus.FAILED)
