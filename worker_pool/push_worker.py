import argparse
import threading
import zmq
import time
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
                print(f"[push_worker] Heartbeat error: {e}")
            time.sleep(interval)
    threading.Thread(target=hb, daemon=True).start()


def push_worker(host: str, port: int, num_procs: int):
    try:
        start_heartbeat()

        ctx = zmq.Context()
        socket = ctx.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, WORKER_ID.encode())
        socket.setsockopt(zmq.RCVTIMEO, 1000)
        socket.connect(f"tcp://{host}:{port}")

        try:
            reg_msg = {
                "type": "register", 
                "worker_id": WORKER_ID,
                "capacity": num_procs
            }
            socket.send_string(serialize(reg_msg))
            print(f"[push_worker] Registered with dispatcher at tcp://{host}:{port} (capacity: {num_procs})")
        except Exception as e:
            print(f"[push_worker] Registration error: {e}")
            sys.exit(1)

        lock = threading.Lock()
        pool = Pool(processes=num_procs)
        print(f"[push_worker] Pool with {num_procs} processes ready")

        def send_result(res):
            try:
                with lock:
                    socket.send_string(serialize({
                        "type":    "result",
                        "task_id": res[0],
                        "status":  res[1],
                        "result":  res[2],
                        "worker_id": WORKER_ID
                    }))
            except zmq.error.ZMQError as e:
                print(f"[push_worker] ZMQ error in send_result: {e}")
            except Exception as e:
                print(f"[push_worker] Error in send_result: {e}")

        while True:
            try:
                msg_str = socket.recv_string()
                if not msg_str:
                    continue
                    
                try:
                    payload = deserialize(msg_str)
                except Exception as e:
                    print(f"[push_worker] Error deserializing message: {e}")
                    continue

                if payload.get("type") != "task":
                    continue

                if not all(k in payload for k in ["task_id", "fn_payload", "param_payload"]):
                    print(f"[push_worker] Invalid task payload: {payload}")
                    continue

                task_id = payload["task_id"]
                ser_fn = payload["fn_payload"]
                ser_params = payload["param_payload"]

                pool.apply_async(
                    execute_task,
                    args=(task_id, ser_fn, ser_params),
                    callback=send_result,
                    error_callback=lambda e: print(f"[push_worker] Task execution error: {e}")
                )
            except zmq.error.Again:
                continue
            except Exception as e:
                print(f"[push_worker] Main loop error: {e}")
                time.sleep(1)

    except Exception as e:
        print(f"[push_worker] Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MPCSFaaS Push Worker")
    parser.add_argument("-H", "--host", default="localhost", help="Dispatcher host")
    parser.add_argument("-p", "--port", type=int, required=True, help="Dispatcher ROUTER port")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Number of processes in pool")
    args = parser.parse_args()

    push_worker(args.host, args.port, args.workers)
