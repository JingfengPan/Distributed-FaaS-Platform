from service.serialize import deserialize, serialize
from service.redis_client import TaskStatus


def execute_task(task_id: str, ser_fn: str, ser_params: str):
    try:
        fn = deserialize(ser_fn)
    except Exception as e:
        err_payload = serialize(e)
        return task_id, TaskStatus.FAILED.value, err_payload

    try:
        args, kwargs = deserialize(ser_params)
    except Exception as e:
        err_payload = serialize(e)
        return task_id, TaskStatus.FAILED.value, err_payload

    try:
        result_obj = fn(*args, **kwargs)
        status = TaskStatus.COMPLETE
    except Exception as e:
        result_obj = e
        status = TaskStatus.FAILED

    ser_result = serialize(result_obj)

    return task_id, status.value, ser_result
