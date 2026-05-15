import argparse
import threading
from queue import Queue
from multiprocessing import Pool
import zmq
import time
from service.redis_client import (
    r,
    TASK_CHANNEL,
    get_task,
    mark_task_running,
    mark_task_complete,
    mark_task_failed,
    TaskStatus,
    update_task_status
)
from worker_pool.executor import execute_task
from service.serialize import serialize, deserialize

HEARTBEAT_TIMEOUT = 3


def workers_monitor(workers: list[str]) -> tuple[list[str], list[str]]:
    now = time.time()
    raw = r.hgetall("worker_heartbeats")
    hb = {k: float(v) for k, v in raw.items()}
    
    alive = []
    dead = []
    for w in workers:
        if w in hb:
            if now - hb[w] < HEARTBEAT_TIMEOUT:
                alive.append(w)
            else:
                dead.append(w)
        else:
            alive.append(w)
    
    return alive, dead


def handle_task(task_id: str):
    try:
        mark_task_running(task_id)
        task = get_task(task_id)
        if not task:
            print(f"[local] Task {task_id} not found")
            return
        
        _, status, result = execute_task(
            task_id,
            task["fn_payload"],
            task["param_payload"]
        )
        
        if status == "COMPLETED":
            mark_task_complete(task_id, result)
            print(f"[local] Task {task_id} completed")
        else:
            mark_task_failed(task_id, result)
            print(f"[local] Task {task_id} failed: {result}")
    except Exception as e:
        print(f"[local] Error executing task {task_id}: {e}")
        mark_task_failed(task_id, str(e))


def dispatch_local(num_workers: int):
    pool = Pool(num_workers)
    pubsub = r.pubsub()
    pubsub.subscribe(TASK_CHANNEL)
    active_tasks = set()

    print(f"[local] Listening on Redis channel '{TASK_CHANNEL}' with {num_workers} workers")
    try:
        for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            task_id = msg["data"]
            if task_id in active_tasks:
                continue
            print(f"[local] Received task {task_id}")
            active_tasks.add(task_id)
            pool.apply_async(
                handle_task,
                args=(task_id,),
                callback=lambda _: active_tasks.discard(task_id),
                error_callback=lambda e: (print(f"[local] Pool error: {e}"), active_tasks.discard(task_id))
            )
    except Exception as e:
        print(f"[local] Fatal error: {e}")
        pool.terminate()
        pool.join()
        raise


def dispatch_pull(port: int):
    task_queue = Queue()
    active_tasks = set()

    def collector():
        sub = r.pubsub()
        sub.subscribe(TASK_CHANNEL)
        for msg in sub.listen():
            if msg["type"] == "message":
                task_id = msg["data"]
                if task_id not in active_tasks:
                    task_queue.put(task_id)

    threading.Thread(target=collector, daemon=True).start()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(f"tcp://*:{port}")

    workers = []

    print(f"[pull] REP socket bound to tcp://*:{port}")
    while True:
        alive_workers, dead_workers = workers_monitor(workers)
        for dead in dead_workers:
            for tid in r.smembers(f"worker_tasks:{dead}"):
                update_task_status(tid, TaskStatus.QUEUED)
                task_queue.put(tid)
                r.publish(TASK_CHANNEL, tid)
                active_tasks.discard(tid)
            r.delete(f"worker_tasks:{dead}")
            r.hdel("worker_heartbeats", dead)
        workers = alive_workers

        ready = sock.recv_string()
        msg = deserialize(ready)

        if msg.get("type") == "result":
            tid = msg["task_id"]
            status = msg["status"]
            res = msg["result"]
            wid = msg["worker_id"]

            if status == TaskStatus.COMPLETE.value:
                mark_task_complete(tid, res)
                print(f"[pull] Task COMPLETED -> {tid!r}")
            else:
                mark_task_failed(tid, res)
                print(f"[pull] Task COMPLETED -> {tid!r}")

            r.srem(f"worker_tasks:{wid}", tid)
            active_tasks.discard(tid)

            sock.send_string("")
            continue

        if msg.get("type") != "REQUEST_TASK":
            sock.send_string("")
            continue

        worker_id = msg["worker_id"]
        if worker_id not in workers:
            workers.append(worker_id)

        if task_queue.empty():
            sock.send_string("")
        else:
            task_id = task_queue.get()
            mark_task_running(task_id)
            for wid in workers:
                r.sadd(f"worker_tasks:{wid}", task_id)
            active_tasks.add(task_id)
            task = get_task(task_id)
            payload = {
                "task_id":       task_id,
                "fn_payload":    task["fn_payload"],
                "param_payload": task["param_payload"]
            }
            sock.send_string(serialize(payload))


def dispatch_push(port: int):
    task_queue = Queue()
    active_tasks = set()

    def collector():
        sub = r.pubsub()
        sub.subscribe(TASK_CHANNEL)
        for msg in sub.listen():
            if msg["type"] == "message":
                task_id = msg["data"]
                if task_id not in active_tasks:
                    task_queue.put(task_id)

    threading.Thread(target=collector, daemon=True).start()

    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    router.bind(f"tcp://*:{port}")

    workers = []
    idx = 0

    print(f"[push] ROUTER socket bound to tcp://*:{port}")
    while True:
        alive_workers, dead_workers = workers_monitor(workers)
        for dead in dead_workers:
            for tid in r.smembers(f"worker_tasks:{dead}"):
                update_task_status(tid, TaskStatus.QUEUED)
                r.publish(TASK_CHANNEL, tid)
                active_tasks.discard(tid)
            r.delete(f"worker_tasks:{dead}")
            r.hdel("worker_heartbeats", dead)
        workers = alive_workers

        try:
            frames = router.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again:
            frames = None

        if frames:
            if len(frames) == 3:
                identity, _empty, body = frames
            elif len(frames) == 2:
                identity, body = frames
            else:
                body = None

            if body:
                try:
                    msg = deserialize(body.decode())
                except Exception:
                    msg = {}

                if msg.get("type") == "register":
                    wid = msg["worker_id"]
                    if isinstance(wid, bytes):
                        wid = wid.decode()
                    if wid not in workers:
                        workers.append(wid)
                        r.hset("worker_heartbeats", wid, time.time())

                elif msg.get("type") == "result":
                    wid = msg.get("worker_id", identity.decode())
                    tid = msg["task_id"]
                    status = msg["status"]
                    res = msg["result"]

                    if status == TaskStatus.COMPLETE.value:
                        mark_task_complete(tid, res)
                        print(f"[push] Task COMPLETED -> {tid!r}")
                    else:
                        mark_task_failed(tid, res)
                        print(f"[push] Task FAILED -> {tid!r}")

                    r.srem(f"worker_tasks:{wid}", tid)
                    active_tasks.discard(tid)

        if workers and not task_queue.empty():
            task_id = task_queue.get()
            
            wid = workers[idx]
            idx = (idx + 1) % len(workers)

            mark_task_running(task_id)
            r.sadd(f"worker_tasks:{wid}", task_id)
            active_tasks.add(task_id)

            task = get_task(task_id)
            payload = {
                "type":          "task",
                "task_id":       task_id,
                "fn_payload":    task["fn_payload"],
                "param_payload": task["param_payload"],
            }
            router.send_multipart([
                wid.encode(),
                b"",
                serialize(payload).encode()
            ])


def main():
    parser = argparse.ArgumentParser(
        description="MPCSFaaS Task Dispatcher (modes: local, pull, push)"
    )
    parser.add_argument(
        "-m", "--mode",
        choices=["local", "pull", "push"],
        required=True,
        help="Dispatch mode"
    )
    parser.add_argument(
        "-p", "--port",
        type=int,
        help="ZMQ port (required for pull and push)"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=1,
        help="Number of processes (only for local mode)"
    )

    args = parser.parse_args()

    if args.mode == "local":
        dispatch_local(args.workers)
    elif args.mode in ("pull", "push"):
        if not args.port:
            parser.error("-p/--port is required for pull and push modes")
        if args.mode == "pull":
            dispatch_pull(args.port)
        else:
            dispatch_push(args.port)
    else:
        parser.error("Unknown mode")


if __name__ == "__main__":
    main()
