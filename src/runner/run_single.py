#!/usr/bin/env python3
import argparse
import concurrent.futures as cf
import csv
from dataclasses import dataclass
import os
import random
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Dict, List, Tuple

import psutil

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    from metrics.instrumentation import get_last_duration_s, timed
    from metrics.prometheus_adapter import PrometheusConfig, PrometheusRunMetrics, query_k8s_utilization
except Exception:
    # Ray workers may import this module in an isolated path where sibling packages
    # are unavailable. Keep a local fallback so actor execution can still run.
    def timed(_metric_name=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                started = time.perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    wrapper.last_duration_s = time.perf_counter() - started

            wrapper.last_duration_s = 0.0
            return wrapper

        return decorator

    def get_last_duration_s(func):
        return float(getattr(func, "last_duration_s", 0.0))

    @dataclass
    class PrometheusConfig:
        enabled: bool = False
        port: int = 9108

    class PrometheusRunMetrics:
        def __init__(self, config: PrometheusConfig):
            self.enabled = config.enabled

        def record_task(self, row: dict) -> None:
            return None

        def record_run(self, mode: str) -> None:
            return None

    def query_k8s_utilization(prometheus_url: str, namespace: str, pod_regex: str):
        return None, None

CSV_PATH = Path("results/raw/raw_results.csv")
ACTOR_STATE_PATH = Path("results/raw/actor_states.csv")


def ensure_csv_header() -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "run_id", "mode", "n_agents", "n_tasks", "repeat_id", "seed", "task_id",
                "submit_ts", "start_ts", "end_ts", "task_duration_s", "sched_overhead_ms",
                "cpu_avg_pct", "gpu_avg_pct", "failed", "recovery_time_s",
            ])


def write_row(row: dict) -> None:
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row["run_id"], row["mode"], row["n_agents"], row["n_tasks"], row["repeat_id"], row["seed"], row["task_id"],
            f'{row["submit_ts"]:.9f}', f'{row["start_ts"]:.9f}', f'{row["end_ts"]:.9f}',
            f'{row["task_duration_s"]:.9f}', f'{row["sched_overhead_ms"]:.3f}',
            f'{row.get("cpu_avg_pct", 0.0):.3f}', f'{row.get("gpu_avg_pct", 0.0):.3f}',
            row.get("failed", 0), f'{row.get("recovery_time_s", 0.0):.9f}',
        ])


def ensure_actor_state_header() -> None:
    ACTOR_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not ACTOR_STATE_PATH.exists():
        with ACTOR_STATE_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "run_id", "backend", "actor_index", "tasks_executed", "snapshot_ts",
            ])


def write_actor_snapshots(run_id: str, backend: str, snapshots: List[dict]) -> None:
    ensure_actor_state_header()
    now = time.time()
    with ACTOR_STATE_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for snap in snapshots:
            w.writerow([
                run_id,
                backend,
                snap.get("actor_index", -1),
                snap.get("tasks_executed", 0),
                f"{now:.6f}",
            ])


@timed("synthetic_work")
def synthetic_work(units: int) -> float:
    x = 0.0
    for i in range(units):
        x += (i % 97) * 0.000001
    return x


def sample_gpu_pct() -> float:
    try:
        cmd = [
            "nvidia-smi",
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ]
        out = subprocess.check_output(cmd, text=True, timeout=1.0).strip()
        if not out:
            return 0.0
        vals = [float(line.strip()) for line in out.splitlines() if line.strip()]
        return sum(vals) / len(vals) if vals else 0.0
    except Exception:
        return 0.0


def should_inject_failure(task_id: int, n_tasks: int, threshold: int) -> bool:
    return n_tasks >= threshold and task_id == 0


def execute_local_attempt(units: int, inject_failure: bool) -> Dict[str, float]:
    start_ts = time.perf_counter()
    cpu_before = psutil.cpu_percent(interval=None)
    gpu_before = sample_gpu_pct()

    failed = 0
    try:
        if inject_failure:
            raise RuntimeError("injected failure")
        synthetic_work(units)
    except Exception:
        failed = 1

    end_ts = time.perf_counter()
    cpu_after = psutil.cpu_percent(interval=None)
    gpu_after = sample_gpu_pct()
    return {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "work_duration_s": get_last_duration_s(synthetic_work),
        "cpu_avg_pct": (cpu_before + cpu_after) / 2.0,
        "gpu_avg_pct": (gpu_before + gpu_after) / 2.0,
        "failed": failed,
    }


def worker_attempt(units: int, inject_failure: bool) -> Dict[str, float]:
    return execute_local_attempt(units=units, inject_failure=inject_failure)


class RayWorkerActor:
    def __init__(self, actor_index: int):
        self.actor_index = actor_index
        self.tasks_executed = 0

    def run_attempt(self, units: int, inject_failure: bool):
        self.tasks_executed += 1
        return worker_attempt(units=units, inject_failure=inject_failure)

    def snapshot(self):
        return {
            "actor_index": self.actor_index,
            "tasks_executed": self.tasks_executed,
        }


def run_baseline(args, run_id: str) -> List[dict]:
    rows = []
    for task_id in range(args.n_tasks):
        submit_ts = time.perf_counter()
        first_start = None
        final_end = None
        cpu_samples = []
        gpu_samples = []
        first_failure_ts = None
        failed = 1

        for attempt in range(args.max_retries + 1):
            inject = should_inject_failure(task_id, args.n_tasks, args.failure_injection_threshold) and attempt == 0
            result = execute_local_attempt(args.units, inject_failure=inject)

            if first_start is None:
                first_start = result["start_ts"]
            final_end = result["end_ts"]
            cpu_samples.append(result["cpu_avg_pct"])
            gpu_samples.append(result["gpu_avg_pct"])

            if result["failed"] == 0:
                failed = 0
                break

            if first_failure_ts is None:
                first_failure_ts = result["end_ts"]

        recovery_time_s = 0.0
        if first_failure_ts is not None and failed == 0 and final_end is not None:
            recovery_time_s = max(0.0, final_end - first_failure_ts)

        rows.append({
            "run_id": run_id,
            "mode": args.mode,
            "n_agents": args.n_agents,
            "n_tasks": args.n_tasks,
            "repeat_id": args.repeat_id,
            "seed": args.seed,
            "task_id": str(task_id),
            "submit_ts": submit_ts,
            "start_ts": first_start if first_start is not None else submit_ts,
            "end_ts": final_end if final_end is not None else submit_ts,
            "task_duration_s": (final_end - first_start) if (final_end and first_start) else 0.0,
            "sched_overhead_ms": ((first_start - submit_ts) * 1000.0) if first_start else 0.0,
            "cpu_avg_pct": sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0,
            "gpu_avg_pct": sum(gpu_samples) / len(gpu_samples) if gpu_samples else 0.0,
            "failed": failed,
            "recovery_time_s": recovery_time_s,
        })
    return rows


def run_ray(args, run_id: str) -> List[dict]:
    try:
        import ray  # type: ignore
        return run_ray_native(args, run_id, ray)
    except Exception as exc:
        print(f"[WARN] ray native execution failed, fallback to process_pool: {exc}")
        print(traceback.format_exc(limit=1))
        return run_ray_fallback(args, run_id)


def run_ray_native(args, run_id: str, ray) -> List[dict]:
    try:
        RemoteWorkerActor = ray.remote(RayWorkerActor)

        ctx = ray.init(
            ignore_reinit_error=True,
            num_cpus=args.n_agents,
            include_dashboard=not args.disable_dashboard,
            dashboard_port=args.dashboard_port,
            log_to_driver=False,
            runtime_env={
                "env_vars": {
                    "PYTHONPATH": str(SRC_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")
                }
            },
        )

        dashboard_url = getattr(ctx, "dashboard_url", "") or getattr(getattr(ctx, "address_info", {}), "get", lambda _: "")("webui_url")
        if dashboard_url:
            print(f"RAY_DASHBOARD={dashboard_url}")

        actors = [RemoteWorkerActor.remote(i) for i in range(args.n_agents)]
        state = {}
        pending = {}

        for task_id in range(args.n_tasks):
            submit_ts = time.perf_counter()
            inject = should_inject_failure(task_id, args.n_tasks, args.failure_injection_threshold)
            actor_idx = task_id % len(actors)
            ref = actors[actor_idx].run_attempt.remote(args.units, inject)
            pending[ref] = (task_id, actor_idx)
            state[task_id] = {
                "submit_ts": submit_ts,
                "first_start": None,
                "final_end": None,
                "cpu_samples": [],
                "gpu_samples": [],
                "first_failure_ts": None,
                "failed": 1,
                "attempt": 0,
            }

        while pending:
            ready, _ = ray.wait(list(pending.keys()), num_returns=1)
            done_ref = ready[0]
            task_id, actor_idx = pending.pop(done_ref)
            result = ray.get(done_ref)
            st = state[task_id]

            if st["first_start"] is None:
                st["first_start"] = result["start_ts"]
            st["final_end"] = result["end_ts"]
            st["cpu_samples"].append(result["cpu_avg_pct"])
            st["gpu_samples"].append(result["gpu_avg_pct"])

            if result["failed"] == 0:
                st["failed"] = 0
                continue

            if st["first_failure_ts"] is None:
                st["first_failure_ts"] = result["end_ts"]

            if st["attempt"] < args.max_retries:
                st["attempt"] += 1
                retry_ref = actors[actor_idx].run_attempt.remote(args.units, False)
                pending[retry_ref] = (task_id, actor_idx)

        actor_snapshots = ray.get([a.snapshot.remote() for a in actors])
        write_actor_snapshots(run_id, "ray", actor_snapshots)
        return rows_from_state(args, run_id, state)
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass


def run_ray_fallback(args, run_id: str) -> List[dict]:
    state = {}
    pending = {}
    worker_task_counts = [0 for _ in range(args.n_agents)]

    with cf.ProcessPoolExecutor(max_workers=args.n_agents) as ex:
        for task_id in range(args.n_tasks):
            submit_ts = time.perf_counter()
            inject = should_inject_failure(task_id, args.n_tasks, args.failure_injection_threshold)
            worker_idx = task_id % args.n_agents
            fut = ex.submit(worker_attempt, args.units, inject)
            pending[fut] = (task_id, worker_idx)
            worker_task_counts[worker_idx] += 1
            state[task_id] = {
                "submit_ts": submit_ts,
                "first_start": None,
                "final_end": None,
                "cpu_samples": [],
                "gpu_samples": [],
                "first_failure_ts": None,
                "failed": 1,
                "attempt": 0,
            }

        while pending:
            done, _ = cf.wait(set(pending.keys()), return_when=cf.FIRST_COMPLETED)
            for fut in done:
                task_id, worker_idx = pending.pop(fut)
                result = fut.result()
                st = state[task_id]

                if st["first_start"] is None:
                    st["first_start"] = result["start_ts"]
                st["final_end"] = result["end_ts"]
                st["cpu_samples"].append(result["cpu_avg_pct"])
                st["gpu_samples"].append(result["gpu_avg_pct"])

                if result["failed"] == 0:
                    st["failed"] = 0
                    continue

                if st["first_failure_ts"] is None:
                    st["first_failure_ts"] = result["end_ts"]

                if st["attempt"] < args.max_retries:
                    st["attempt"] += 1
                    retry_fut = ex.submit(worker_attempt, args.units, False)
                    pending[retry_fut] = (task_id, worker_idx)
                    worker_task_counts[worker_idx] += 1

    write_actor_snapshots(
        run_id,
        "process_pool",
        [{"actor_index": i, "tasks_executed": worker_task_counts[i]} for i in range(args.n_agents)],
    )
    return rows_from_state(args, run_id, state)


def rows_from_state(args, run_id: str, state: dict) -> List[dict]:
    rows = []
    for task_id in range(args.n_tasks):
        st = state[task_id]
        first_start = st["first_start"] if st["first_start"] is not None else st["submit_ts"]
        final_end = st["final_end"] if st["final_end"] is not None else st["submit_ts"]

        recovery_time_s = 0.0
        if st["first_failure_ts"] is not None and st["failed"] == 0:
            recovery_time_s = max(0.0, final_end - st["first_failure_ts"])

        rows.append({
            "run_id": run_id,
            "mode": args.mode,
            "n_agents": args.n_agents,
            "n_tasks": args.n_tasks,
            "repeat_id": args.repeat_id,
            "seed": args.seed,
            "task_id": str(task_id),
            "submit_ts": st["submit_ts"],
            "start_ts": first_start,
            "end_ts": final_end,
            "task_duration_s": max(0.0, final_end - first_start),
            "sched_overhead_ms": max(0.0, (first_start - st["submit_ts"]) * 1000.0),
            "cpu_avg_pct": sum(st["cpu_samples"]) / len(st["cpu_samples"]) if st["cpu_samples"] else 0.0,
            "gpu_avg_pct": sum(st["gpu_samples"]) / len(st["gpu_samples"]) if st["gpu_samples"] else 0.0,
            "failed": st["failed"],
            "recovery_time_s": recovery_time_s,
        })
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["ray", "baseline"], required=True)
    p.add_argument("--n-agents", type=int, required=True)
    p.add_argument("--n-tasks", "--tasks", dest="n_tasks", type=int, default=50)
    p.add_argument("--repeat-id", type=int, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--units", type=int, default=200000)
    p.add_argument("--max-retries", type=int, default=1)
    p.add_argument("--failure-injection-threshold", type=int, default=200)
    p.add_argument("--disable-dashboard", action="store_true")
    p.add_argument("--dashboard-port", type=int, default=8265)
    p.add_argument("--disable-prometheus-export", action="store_true")
    p.add_argument("--prometheus-port", type=int, default=9108)
    p.add_argument("--prometheus-url", type=str, default="")
    p.add_argument("--k8s-namespace", type=str, default="default")
    p.add_argument("--k8s-pod-regex", type=str, default=".*")
    args = p.parse_args()

    random.seed(args.seed)
    psutil.cpu_percent(interval=None)

    prom = PrometheusRunMetrics(
        PrometheusConfig(
            enabled=not args.disable_prometheus_export,
            port=args.prometheus_port,
        )
    )
    if not args.disable_prometheus_export:
        print(f"PROMETHEUS_EXPORTER=http://127.0.0.1:{args.prometheus_port}/metrics")

    ensure_csv_header()
    run_id = f'{args.mode}_A{args.n_agents}_T{args.n_tasks}_R{args.repeat_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}'

    if args.mode == "baseline":
        rows = run_baseline(args, run_id)
    else:
        rows = run_ray(args, run_id)

    for row in rows:
        write_row(row)
        prom.record_task(row)
    prom.record_run(args.mode)

    if args.prometheus_url:
        prom_cpu, prom_gpu = query_k8s_utilization(
            prometheus_url=args.prometheus_url,
            namespace=args.k8s_namespace,
            pod_regex=args.k8s_pod_regex,
        )
        if prom_cpu is not None:
            print(f"K8S_PROM_CPU_AVG_PCT={prom_cpu:.3f}")
        if prom_gpu is not None:
            print(f"K8S_PROM_GPU_AVG_PCT={prom_gpu:.3f}")

    print(f"RUN_ID={run_id}")
    print("OK")


if __name__ == "__main__":
    main()
