import csv
import sys
from pathlib import Path

CSV_PATH = Path("results/raw/raw_results.csv")
HEADER = [
    "run_id", "mode", "n_agents", "n_tasks", "repeat_id", "seed", "task_id",
    "submit_ts", "start_ts", "end_ts", "task_duration_s", "sched_overhead_ms",
    "cpu_avg_pct", "gpu_avg_pct", "failed", "recovery_time_s",
]

def ensure_header():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)

def write_row(row):
    ensure_header()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

if __name__ == "__main__":
    if len(sys.argv) != 17:
        raise SystemExit("Usage: collect_metrics.py <16 values matching METRICS_SPEC header>")
    row = [
        sys.argv[1],   # run_id
        sys.argv[2],   # mode
        int(sys.argv[3]),   # n_agents
        int(sys.argv[4]),   # n_tasks
        int(sys.argv[5]),   # repeat_id
        int(sys.argv[6]),   # seed
        int(sys.argv[7]),   # task_id
        float(sys.argv[8]), # submit_ts
        float(sys.argv[9]), # start_ts
        float(sys.argv[10]),# end_ts
        float(sys.argv[11]),# task_duration_s
        float(sys.argv[12]),# sched_overhead_ms
        float(sys.argv[13]),# cpu_avg_pct
        float(sys.argv[14]),# gpu_avg_pct
        int(sys.argv[15]),  # failed
        float(sys.argv[16]) # recovery_time_s
    ]
    write_row(row)
