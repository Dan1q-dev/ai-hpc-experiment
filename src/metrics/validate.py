#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
import sys
from collections import defaultdict

CSV_PATH = Path("results/raw/raw_results.csv")
DEFAULT_MODES = ["baseline", "ray"]
DEFAULT_TASKS = [10, 20, 50, 100, 200, 500, 1000]
DEFAULT_REPEATS = 5

def die(msg):
    print(f"[VALIDATE] FAIL: {msg}")
    sys.exit(1)

def parse_int_list(raw: str):
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--modes", default="baseline,ray")
    parser.add_argument("--tasks", default="10,20,50,100,200,500,1000")
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    args = parser.parse_args()

    expected_modes = [x.strip() for x in args.modes.split(",") if x.strip()] or DEFAULT_MODES
    expected_tasks = parse_int_list(args.tasks) or DEFAULT_TASKS
    expected_repeats = args.repeats

    if not CSV_PATH.exists():
        die("raw_results.csv not found. Run experiments first.")

    rows = []
    with CSV_PATH.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    if not rows:
        die("raw_results.csv is empty")

    required = [
        "run_id", "mode", "n_agents", "n_tasks", "repeat_id", "seed", "task_id",
        "submit_ts", "start_ts", "end_ts", "task_duration_s", "sched_overhead_ms",
        "cpu_avg_pct", "gpu_avg_pct", "failed", "recovery_time_s",
    ]

    combo_to_runs = defaultdict(set)
    run_to_mode_tasks = {}
    run_to_rows = defaultdict(int)

    for i, row in enumerate(rows):
        for key in required:
            if row.get(key, "") == "":
                die(f"row {i}: missing {key}")

        run_id = row["run_id"]
        mode = row["mode"]
        n_tasks = int(row["n_tasks"])
        repeat_id = int(row["repeat_id"])
        n_agents = int(row["n_agents"])
        task_id = int(row["task_id"])

        submit_ts = float(row["submit_ts"])
        start_ts = float(row["start_ts"])
        end_ts = float(row["end_ts"])
        task_duration = float(row["task_duration_s"])
        sched_overhead = float(row["sched_overhead_ms"])
        cpu_avg = float(row["cpu_avg_pct"])
        gpu_avg = float(row["gpu_avg_pct"])
        failed = int(row["failed"])
        recovery_time = float(row["recovery_time_s"])

        if start_ts < submit_ts:
            die(f"row {i}: start_ts < submit_ts")
        if end_ts < start_ts:
            die(f"row {i}: end_ts < start_ts")
        if task_duration < 0:
            die(f"row {i}: task_duration_s < 0")
        if sched_overhead < 0:
            die(f"row {i}: sched_overhead_ms < 0")
        if failed not in (0, 1):
            die(f"row {i}: failed must be 0 or 1")
        if recovery_time < 0:
            die(f"row {i}: recovery_time_s < 0")
        if n_agents < 1:
            die(f"row {i}: n_agents must be >= 1")
        if n_tasks < 1:
            die(f"row {i}: n_tasks must be >= 1")
        if task_id < 0:
            die(f"row {i}: task_id must be >= 0")
        if cpu_avg < 0 or cpu_avg > 100:
            die(f"row {i}: cpu_avg_pct must be in [0, 100]")
        if gpu_avg < 0 or gpu_avg > 100:
            die(f"row {i}: gpu_avg_pct must be in [0, 100]")

        combo_to_runs[(mode, n_tasks)].add(run_id)
        run_to_rows[run_id] += 1

        if run_id in run_to_mode_tasks:
            if run_to_mode_tasks[run_id] != (mode, n_tasks, repeat_id):
                die(f"run_id {run_id}: inconsistent mode/n_tasks/repeat_id")
        else:
            run_to_mode_tasks[run_id] = (mode, n_tasks, repeat_id)

    for run_id, count in run_to_rows.items():
        n_tasks = run_to_mode_tasks[run_id][1]
        if count != n_tasks:
            die(f"run_id {run_id}: expected {n_tasks} rows, got {count}")

    for mode in expected_modes:
        for n_tasks in expected_tasks:
            runs = combo_to_runs.get((mode, n_tasks), set())
            if len(runs) != expected_repeats:
                die(
                    f"matrix incomplete for mode={mode}, n_tasks={n_tasks}: "
                    f"expected {expected_repeats} runs, got {len(runs)}"
                )

    print(
        f"[VALIDATE] OK. Rows={len(rows)}; "
        f"Runs={len(run_to_mode_tasks)}; "
        f"Modes={','.join(expected_modes)}; "
        f"Tasks={expected_tasks}; Repeats={expected_repeats}"
    )

if __name__ == "__main__":
    main()
