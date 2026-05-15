import os
import sys
import time
import uuid
import threading
import subprocess
import pytest
import socket
from service.redis_client import (
    r,
    create_task,
    get_task,
    store_function,
    TaskStatus
)
from service.serialize import serialize, deserialize
import task_dispatcher

PULL_PORT = 5001
PUSH_PORT = 5002


@pytest.fixture(scope="module", autouse=True)
def clear_redis():
    r.flushdb()
    yield
    r.flushdb()


@pytest.fixture(scope="module")
def start_pull_dispatcher():
    t = threading.Thread(
        target=task_dispatcher.dispatch_pull,
        args=(PULL_PORT,),
        daemon=True
    )
    t.start()
    time.sleep(0.5)


@pytest.fixture(scope="module")
def start_push_dispatcher():
    t = threading.Thread(
        target=task_dispatcher.dispatch_push,
        args=(PUSH_PORT,),
        daemon=True
    )
    t.start()
    time.sleep(0.5)


@pytest.fixture()
def one_pull_worker(start_pull_dispatcher):
    proc = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "1"
    ], cwd=os.getcwd())
    time.sleep(1.0)
    yield proc
    proc.terminate()
    proc.wait()


@pytest.fixture()
def one_push_worker(start_push_dispatcher):
    proc = subprocess.Popen([
        sys.executable, "-m", "worker_pool.push_worker",
        "-H", "localhost", "-p", str(PUSH_PORT), "-w", "1"
    ], cwd=os.getcwd())
    time.sleep(1.0)
    yield proc
    proc.terminate()
    proc.wait()


def test_pull_end_to_end(one_pull_worker):
    def double(x):
        return x * 2

    fid = str(uuid.uuid4())
    fn_ser = serialize(double)
    store_function(fid, "double", fn_ser)

    tid = str(uuid.uuid4())
    params = serialize(((25,), {}))
    create_task(tid, fid, fn_ser, params)

    for _ in range(20):
        task = get_task(tid)
        if task["status"] == TaskStatus.COMPLETE.value:
            assert deserialize(task["result"]) == 50
            return
        time.sleep(0.1)
    pytest.fail("Pull worker did not complete the task in time")


def test_push_end_to_end(one_push_worker):
    def double(x):
        return x * 2

    fid = str(uuid.uuid4())
    fn_ser = serialize(double)
    store_function(fid, "double", fn_ser)

    tid = str(uuid.uuid4())
    params = serialize(((25,), {}))
    create_task(tid, fid, fn_ser, params)

    for i in range(20):
        task = get_task(tid)
        if task["status"] == TaskStatus.COMPLETE.value:
            result = deserialize(task["result"])
            assert result == 50, f"Expected 50, got {result}"
            return
        elif task["status"] == TaskStatus.FAILED.value:
            pytest.fail(f"Task failed: {task['result']}")
        time.sleep(0.1)
    pytest.fail("Push worker did not complete the task in time")


def test_pull_fault_tolerance(start_pull_dispatcher):
    w1 = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "1"
    ], cwd=os.getcwd())
    w2 = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "1"
    ], cwd=os.getcwd())
    time.sleep(0.5)

    try:
        def long_task(x):
            import time
            time.sleep(x)
            return x

        fid = str(uuid.uuid4())
        fn_ser = serialize(long_task)
        store_function(fid, "long_task", fn_ser)

        tid = str(uuid.uuid4())
        params = serialize(((0.5,), {}))
        create_task(tid, fid, fn_ser, params)

        wid1 = f"{socket.gethostname()}-{w1.pid}"
        deadline = time.time() + 5
        while time.time() < deadline:
            if r.sismember(f"worker_tasks:{wid1}", tid):
                break
            time.sleep(0.1)
        else:
            pytest.fail("Worker1 did not pick up the task in time")

        w1.terminate()
        w1.wait()

        deadline = time.time() + task_dispatcher.HEARTBEAT_TIMEOUT * 2
        while time.time() < deadline:
            if r.hget("worker_heartbeats", wid1) is None:
                break
            time.sleep(0.1)
        else:
            pytest.fail("Dispatcher did not purge dead worker in time")

        for _ in range(50):
            task = get_task(tid)
            if task["status"] == TaskStatus.COMPLETE.value:
                assert deserialize(task["result"]) == 0.5
                break
            time.sleep(0.1)
        else:
            pytest.fail("Fault tolerance failed: task was not reassigned and completed")

    finally:
        for proc in (w1, w2):
            if proc.poll() is None:
                proc.terminate()
                proc.wait()


def test_dispatcher_task_state_transitions(start_pull_dispatcher):
    w1 = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "1"
    ], cwd=os.getcwd())
    time.sleep(0.5)

    try:
        def long_task(x):
            time.sleep(x)
            return x

        fid = str(uuid.uuid4())
        fn_ser = serialize(long_task)
        store_function(fid, "long_task", fn_ser)

        tid = str(uuid.uuid4())
        params = serialize(((0.5,), {}))
        create_task(tid, fid, fn_ser, params)

        deadline = time.time() + 5
        while time.time() < deadline:
            task = get_task(tid)
            if task["status"] == TaskStatus.RUNNING.value:
                break
            time.sleep(0.1)
        else:
            pytest.fail("Task did not transition to RUNNING state")

        deadline = time.time() + 5
        while time.time() < deadline:
            task = get_task(tid)
            if task["status"] == TaskStatus.COMPLETE.value:
                result = deserialize(task["result"])
                assert result == 0.5, f"Task result mismatch: expected 0.5, got {result}"
                break
            time.sleep(0.1)
        else:
            pytest.fail("Task did not transition to COMPLETE state")

    finally:
        if w1.poll() is None:
            w1.terminate()
            w1.wait()


def test_dispatcher_error_handling(start_pull_dispatcher):
    w1 = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "1"
    ], cwd=os.getcwd())
    time.sleep(0.5)

    try:
        def error_task():
            raise ValueError("Test error")

        fid = str(uuid.uuid4())
        fn_ser = serialize(error_task)
        store_function(fid, "error_task", fn_ser)

        tid = str(uuid.uuid4())
        params = serialize(((), {}))
        create_task(tid, fid, fn_ser, params)

        deadline = time.time() + 5
        while time.time() < deadline:
            task = get_task(tid)
            if task["status"] == TaskStatus.FAILED.value:
                error_msg = deserialize(task["result"])
                assert isinstance(error_msg, ValueError), "Error should be ValueError"
                assert str(error_msg) == "Test error", "Error message not preserved"
                break
            time.sleep(0.1)
        else:
            pytest.fail("Task did not transition to FAILED state")

    finally:
        if w1.poll() is None:
            w1.terminate()
            w1.wait()


def test_dispatcher_concurrent_tasks(start_pull_dispatcher):
    w1 = subprocess.Popen([
        sys.executable, "-m", "worker_pool.pull_worker",
        "-H", "localhost", "-p", str(PULL_PORT), "-w", "2"
    ], cwd=os.getcwd())
    time.sleep(0.5)

    try:
        def concurrent_task(x):
            time.sleep(0.1)
            return x

        fid = str(uuid.uuid4())
        fn_ser = serialize(concurrent_task)
        store_function(fid, "concurrent_task", fn_ser)

        task_ids = []
        for i in range(5):
            tid = str(uuid.uuid4())
            params = serialize(((i,), {}))
            create_task(tid, fid, fn_ser, params)
            task_ids.append(tid)

        deadline = time.time() + 10
        completed_tasks = set()
        while time.time() < deadline and len(completed_tasks) < len(task_ids):
            for tid in task_ids:
                if tid in completed_tasks:
                    continue
                task = get_task(tid)
                if task["status"] == TaskStatus.COMPLETE.value:
                    result = deserialize(task["result"])
                    expected = task_ids.index(tid)
                    assert result == expected, f"Task {tid} result mismatch: expected {expected}, got {result}"
                    completed_tasks.add(tid)
            time.sleep(0.1)

        assert len(completed_tasks) == len(task_ids), "Not all concurrent tasks were completed"

    finally:
        if w1.poll() is None:
            w1.terminate()
            w1.wait()
