import csv
import sys
from datetime import datetime

def write_row(row):
    with open("raw_results.csv", "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# запуск:
# python3 collect_metrics.py run01 ray 4 1 42 17 1708593104 1708593105 1708593110 5.0 100 57 0 0
#                 1     2    3 4 5 6  7          8          9       10 11 12 13 14

if __name__ == "__main__":
    row = [
        sys.argv[1],   # run_id
        sys.argv[2],   # mode
        int(sys.argv[3]),   # n_agents
        int(sys.argv[4]),   # repeat_id
        int(sys.argv[5]),   # seed
        int(sys.argv[6]),   # task_id
        float(sys.argv[7]), # submit_ts
        float(sys.argv[8]), # start_ts
        float(sys.argv[9]), # end_ts
        float(sys.argv[10]),# task_duration_s
        float(sys.argv[11]),# sched_overhead_ms
        float(sys.argv[12]),# cpu_avg_pct
        float(sys.argv[13]),# gpu_avg_pct
        int(sys.argv[14]),  # failed
        float(sys.argv[15]) # recovery_time_s
    ]
    write_row(row)
