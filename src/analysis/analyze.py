#!/usr/bin/env python3
import csv
from collections import defaultdict
from pathlib import Path
import statistics as st

RAW = Path("results/raw/raw_results.csv")
OUT = Path("results/aggregated/aggregated_results.csv")

def median(xs):
    return st.median(xs) if xs else float("nan")

def p95(xs):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = int(round(0.95 * (len(xs)-1)))
    return xs[k]

def main():
    if not RAW.exists():
        print("No raw_results.csv. Run experiments first.")
        return

    # group by run_id to compute TTS
    by_run = defaultdict(list)
    meta = {}
    with RAW.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            run_id = row["run_id"]
            by_run[run_id].append(row)
            meta[run_id] = (row["mode"], int(row["n_agents"]))

    run_tts = []  # (mode, n, tts)
    run_over_p95 = []  # (mode, n, p95 overhead within run)
    for run_id, rows in by_run.items():
        submit_min = min(float(x["submit_ts"]) for x in rows)
        end_max = max(float(x["end_ts"]) for x in rows)
        tts = end_max - submit_min

        overheads = [float(x["sched_overhead_ms"]) for x in rows]
        over95 = p95(overheads)

        mode, n = meta[run_id]
        run_tts.append((mode, n, tts))
        run_over_p95.append((mode, n, over95))

    # aggregate by (mode, n)
    agg_tts = defaultdict(list)
    agg_over95 = defaultdict(list)
    for mode, n, tts in run_tts:
        agg_tts[(mode,n)].append(tts)
    for mode, n, over95 in run_over_p95:
        agg_over95[(mode,n)].append(over95)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["mode","n_agents","tts_median_s","tts_runs","overhead_p95_median_ms"])
        for (mode,n) in sorted(agg_tts.keys()):
            w.writerow([
                mode, n,
                f"{median(agg_tts[(mode,n)]):.6f}",
                len(agg_tts[(mode,n)]),
                f"{median(agg_over95[(mode,n)]):.3f}",
            ])

    print(f"Wrote {OUT}")

if __name__ == "__main__":
    main()
