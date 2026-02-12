#!/usr/bin/env python3
import argparse
import csv
import os
import time
import uuid
from pathlib import Path

CSV_PATH = Path("results/raw/raw_results.csv")

def ensure_csv_header():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "run_id","mode","n_agents","repeat_id","seed","task_id",
                "submit_ts","start_ts","end_ts","task_duration_s","sched_overhead_ms",
                "cpu_avg_pct","gpu_avg_pct","failed","recovery_time_s"
            ])

def write_row(row: dict):
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row["run_id"], row["mode"], row["n_agents"], row["repeat_id"], row["seed"], row["task_id"],
            f'{row["submit_ts"]:.9f}', f'{row["start_ts"]:.9f}', f'{row["end_ts"]:.9f}',
            f'{row["task_duration_s"]:.9f}', f'{row["sched_overhead_ms"]:.3f}',
            row.get("cpu_avg_pct",""), row.get("gpu_avg_pct",""),
            row.get("failed",0), row.get("recovery_time_s","")
        ])

def synthetic_work(units: int):
    # простая compute-bound нагрузка без зависимостей
    x = 0.0
    for i in range(units):
        x += (i % 97) * 0.000001
    return x

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["ray","baseline"], required=True)
    p.add_argument("--n-agents", type=int, required=True)
    p.add_argument("--repeat-id", type=int, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--tasks", type=int, default=50)          # сколько задач в прогоне
    p.add_argument("--units", type=int, default=2_000_00)    # тяжесть одной задачи
    args = p.parse_args()

    ensure_csv_header()

    run_id = f'{args.mode}_N{args.n_agents}_R{args.repeat_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}'

    # baseline: последовательно
    # ray: пока тоже последовательно (заглушка) — B потом заменит на ray.remote
    for t in range(args.tasks):
        task_id = str(t)

        submit_ts = time.perf_counter()
        # имитация очереди/планирования
        time.sleep(0.001)

        start_ts = time.perf_counter()
        try:
            synthetic_work(args.units)
            failed = 0
        except Exception:
            failed = 1
        end_ts = time.perf_counter()

        task_duration_s = end_ts - start_ts
        sched_overhead_ms = (start_ts - submit_ts) * 1000.0

        write_row({
            "run_id": run_id,
            "mode": args.mode,
            "n_agents": args.n_agents,
            "repeat_id": args.repeat_id,
            "seed": args.seed,
            "task_id": task_id,
            "submit_ts": submit_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "task_duration_s": task_duration_s,
            "sched_overhead_ms": sched_overhead_ms,
            "cpu_avg_pct": "",
            "gpu_avg_pct": "",
            "failed": failed,
            "recovery_time_s": ""
        })

    print(f"RUN_ID={run_id}")
    print("OK")

if __name__ == "__main__":
    main()
