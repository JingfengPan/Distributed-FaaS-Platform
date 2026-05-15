import csv
import os
import sys
import time
import statistics
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
session = requests.Session()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)
from service.serialize import serialize

# ── CONFIG ────────────────────────────────────────────────────────────────────

API = "http://127.0.0.1:8000"
PROCS_PER_WORKER = 4
PULL_PORT = 5001
PUSH_PORT = 5002
STARTUP_WAIT = 2.0

MODES = ["local", "pull", "push"]
WORKER_COUNTS = [1, 2, 4, 8, 16]
TASKS_PER_W = 5
FUNCTIONS = ["nop", "sleep"]
SLEEP_SEC = 0.1

TOKENS = {}

# ── TASK FUNCTIONS ─────────────────────────────────────────────────────────────


def nop():
    return None


def sleep_task(sec: float):
    import time
    time.sleep(sec)
    return sec


# ── REGISTER ───────────────────────────────────────────────────────────────────


def register_functions():
    if TOKENS:
        return
    for name, fn in [("nop", nop), ("sleep", sleep_task)]:
        resp = session.post(f"{API}/register_function", json={
            "name": name,
            "payload": serialize(fn)
        })
        resp.raise_for_status()
        TOKENS[name] = resp.json()["function_id"]


# ── INVOKE ─────────────────────────────────────────────────────────────────────


def invoke_tasks(fn_key: str, total: int):
    def worker(arg):
        try:
            api_start = time.monotonic()

            if fn_key == "nop":
                payload = serialize(((), {}))
            else:
                payload = serialize(((arg,), {}))
                
            r1 = session.post(f"{API}/execute_function", json={
                "function_id": TOKENS[fn_key],
                "payload": payload
            })
            r1.raise_for_status()
            api_latency = time.monotonic() - api_start
            
            tid = r1.json()["task_id"]
            exec_start = time.monotonic()
            task_completed = False
            task_failed = False
            
            while not (task_completed or task_failed):
                try:
                    r2 = session.get(f"{API}/result/{tid}")
                    r2.raise_for_status()
                    status = r2.json()["status"]
                    if status == "COMPLETED":
                        task_completed = True
                        break
                    elif status == "FAILED":
                        task_failed = True
                        break
                    time.sleep(0.1)
                except Exception as e:
                    time.sleep(0.1)
            
            exec_latency = time.monotonic() - exec_start
            
            if task_failed:
                return None
            elif not task_completed:
                return None
            
            return {
                'api_latency': api_latency,
                'exec_latency': exec_latency
            }
        except Exception as e:
            print(f"Error in worker: {e}")
            return None

    args = [(SLEEP_SEC if fn_key == "sleep" else 0.0) for _ in range(total)]
    latencies = []
    with ThreadPoolExecutor(max_workers=total) as exe:
        futures = [exe.submit(worker, a) for a in args]
        for f in as_completed(futures):
            result = f.result()
            if result is not None:
                latencies.append(result)
    
    if not latencies:
        raise RuntimeError(f"No tasks completed successfully for {fn_key}")
    
    return latencies


# ── PROCESS ORCHESTRATION ──────────────────────────────────────────────────────


def start_for_mode(mode: str):
    procs = []
    dispatcher_path = os.path.join(BASE_DIR, "task_dispatcher.py")
    pull_worker_path = os.path.join(BASE_DIR, "worker_pool", "pull_worker.py")
    push_worker_path = os.path.join(BASE_DIR, "worker_pool", "push_worker.py")
    
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR
    
    try:
        if mode == "local":
            procs.append(subprocess.Popen([
                "python", dispatcher_path,
                "-m", "local", "-w", str(PROCS_PER_WORKER)
            ], cwd=BASE_DIR, env=env))
        elif mode == "pull":
            procs.append(subprocess.Popen([
                "python", dispatcher_path,
                "-m", "pull", "-p", str(PULL_PORT)
            ], cwd=BASE_DIR, env=env))
            procs.append(subprocess.Popen([
                "python", pull_worker_path,
                "-H", "localhost", "-p", str(PULL_PORT),
                "-w", str(PROCS_PER_WORKER)
            ], cwd=BASE_DIR, env=env))
        elif mode == "push":
            procs.append(subprocess.Popen([
                "python", dispatcher_path,
                "-m", "push", "-p", str(PUSH_PORT)
            ], cwd=BASE_DIR, env=env))
            procs.append(subprocess.Popen([
                "python", push_worker_path,
                "-H", "localhost", "-p", str(PUSH_PORT),
                "-w", str(PROCS_PER_WORKER)
            ], cwd=BASE_DIR, env=env))
        else:
            raise ValueError(f"unknown mode {mode}")

        time.sleep(STARTUP_WAIT)
        for i, proc in enumerate(procs):
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                error_msg = f"Process {i} failed to start. Exit code: {proc.returncode}"
                if stderr:
                    error_msg += f"\nError: {stderr.decode()}"
                raise RuntimeError(error_msg)
        
        return procs
    except Exception as e:
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        raise RuntimeError(f"Failed to start {mode} mode: {str(e)}")


def stop_procs(procs):
    for p in procs:
        p.terminate()
    time.sleep(1.0)


# ── BENCHMARK SWEEP ────────────────────────────────────────────────────────────


def run_all_and_dump():
    register_functions()
    fieldnames = [
        "mode", "workers", "fn", "tasks",
        "wall_clock", "throughput",
        "api_lat_mean", "exec_lat_mean"
    ]
    out = os.path.join(BASE_DIR, "benchmark", "benchmark_results.csv")
    with open(out, "w", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()

        for mode in MODES:
            print(f"\n=== MODE: {mode.upper()} ===")
            try:
                procs = start_for_mode(mode)
                print(f"Started {mode} mode")

                for w in WORKER_COUNTS:
                    total = w * TASKS_PER_W
                    print(f"Testing with {w} workers ({total} tasks)")
                    
                    try:
                        test_lats = invoke_tasks("nop", min(10, total))
                    except RuntimeError as e:
                        print(f"Test failed: {e}")
                        print("Skipping this worker count")
                        continue

                    for fn in FUNCTIONS:
                        try:
                            t0 = time.monotonic()
                            lats = invoke_tasks(fn, total)
                            t1 = time.monotonic()

                            wall = t1 - t0
                            thr = total / wall
                            
                            api_lats = [l['api_latency'] for l in lats]
                            exec_lats = [l['exec_latency'] for l in lats]
                            
                            writer.writerow({
                                "mode": mode,
                                "workers": w,
                                "fn": fn,
                                "tasks": total,
                                "wall_clock": round(wall, 4),
                                "throughput": round(thr, 4),
                                "api_lat_mean": round(statistics.mean(api_lats), 4),
                                "exec_lat_mean": round(statistics.mean(exec_lats), 4)
                            })
                            print(f"Completed {fn} with {w} workers: {round(thr, 2)} tasks/sec")
                        except RuntimeError as e:
                            print(f"Failed to complete {fn}: {e}")
                            continue
                    time.sleep(0.5)

            except Exception as e:
                print(f"Error in {mode} mode: {str(e)}")
            finally:
                if 'procs' in locals():
                    stop_procs(procs)

    print("\nSaved benchmark_results.csv")


if __name__ == "__main__":
    run_all_and_dump()
