import argparse
import time
import threading
import zmq
import sys
import os
import socket
from service.redis_client import r
from service.serialize import serialize, deserialize
try:
    from .executor import execute_task
except ImportError:
    from executor import execute_task
if sys.platform.startswith("win"):
    from multiprocessing.dummy import Pool
else:
    from multiprocessing import Pool

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

HEARTBEAT_INTERVAL = 1
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"


def start_heartbeat(interval=HEARTBEAT_INTERVAL):
    def hb():
        while True:
            try:
                r.hset("worker_heartbeats", WORKER_ID, time.time())
            except Exception as e:
                print(f"[pull_worker] Heartbeat error: {e}")
            time.sleep(interval)
    threading.Thread(target=hb, daemon=True).start()


def pull_worker(host: str, port: int, num_procs: int, poll_interval: float = 0.01):
    try:
        start_heartbeat()
        
        ctx = zmq.Context()
        socket = ctx.socket(zmq.REQ)
        socket.connect(f"tcp://{host}:{port}")
        print(f"[pull_worker] Connected to dispatcher at tcp://{host}:{port}")

        lock = threading.Lock()
        pool = Pool(processes=num_procs)
        print(f"[pull_worker] Pool with {num_procs} processes ready")

        def request_task() -> str:
            try:
                msg = serialize({"type": "REQUEST_TASK", "worker_id": WORKER_ID})
                with lock:
                    socket.send_string(msg)
                    return socket.recv_string()
            except zmq.error.ZMQError as e:
                print(f"[pull_worker] ZMQ error in request_task: {e}")
                return ""
            except Exception as e:
                print(f"[pull_worker] Error in request_task: {e}")
                return ""

        def send_result(res):
            try:
                with lock:
                    socket.send_string(serialize({
                        "type":      "result",
                        "worker_id": WORKER_ID,
                        "task_id":   res[0],
                        "status":    res[1],
                        "result":    res[2]
                    }))
                    _ = socket.recv_string()
            except zmq.error.ZMQError as e:
                print(f"[pull_worker] ZMQ error in send_result: {e}")
            except Exception as e:
                print(f"[pull_worker] Error in send_result: {e}")

        while True:
            try:
                reply = request_task()
                if not reply:
                    time.sleep(poll_interval)
                    continue

                try:
                    payload = deserialize(reply)
                except Exception as e:
                    print(f"[pull_worker] Error deserializing task: {e}")
                    continue

                if not all(k in payload for k in ["task_id", "fn_payload", "param_payload"]):
                    print(f"[pull_worker] Invalid task payload: {payload}")
                    continue

                task_id = payload["task_id"]
                ser_fn = payload["fn_payload"]
                ser_params = payload["param_payload"]

                pool.apply_async(
                    execute_task,
                    args=(task_id, ser_fn, ser_params),
                    callback=send_result,
                    error_callback=lambda e: print(f"[pull_worker] Task execution error: {e}")
                )
            except Exception as e:
                print(f"[pull_worker] Main loop error: {e}")
                time.sleep(poll_interval)

    except Exception as e:
        print(f"[pull_worker] Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MPCSFaaS Pull Worker")
    parser.add_argument("-H", "--host", default="localhost", help="Dispatcher host")
    parser.add_argument("-p", "--port", type=int, required=True, help="Dispatcher REP port")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Number of processes in pool")
    args = parser.parse_args()

    pull_worker(args.host, args.port, args.workers)
