#!/usr/bin/env python3
from pathlib import Path
import numpy as np
import pandas as pd

RAW = Path("results/raw/raw_results.csv")
OUT = Path("results/aggregated/aggregated_results.csv")
OUT_SUMMARY = Path("results/aggregated/summary_tts.csv")
OUT_SPEEDUP = Path("results/aggregated/speedup_efficiency.csv")

def ci95(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    n = len(values)
    if n <= 1:
        return 0.0
    return 1.96 * values.std(ddof=1) / np.sqrt(n)

def main():
    if not RAW.exists():
        print("No raw_results.csv. Run experiments first.")
        return

    df = pd.read_csv(RAW)
    required = {
        "run_id", "mode", "n_agents", "n_tasks", "repeat_id", "task_id",
        "submit_ts", "start_ts", "end_ts", "sched_overhead_ms",
        "cpu_avg_pct", "gpu_avg_pct", "failed", "recovery_time_s",
    }
    missing = required - set(df.columns)
    if missing:
        print(f"raw_results.csv missing columns: {sorted(missing)}")
        return

    numeric_cols = [
        "n_agents", "n_tasks", "repeat_id", "task_id",
        "submit_ts", "start_ts", "end_ts", "sched_overhead_ms",
        "cpu_avg_pct", "gpu_avg_pct", "failed", "recovery_time_s",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["run_id", "mode", "n_tasks", "submit_ts", "end_ts"])

    run_metrics = (
        df.groupby(["run_id", "mode", "n_tasks", "n_agents"], as_index=False)
        .agg(
            repeat_id=("repeat_id", "first"),
            submit_min=("submit_ts", "min"),
            end_max=("end_ts", "max"),
            overhead_p50_ms=("sched_overhead_ms", lambda x: x.quantile(0.5)),
            overhead_p95_ms=("sched_overhead_ms", lambda x: x.quantile(0.95)),
            cpu_avg_pct=("cpu_avg_pct", "mean"),
            gpu_avg_pct=("gpu_avg_pct", "mean"),
            recovery_time_mean_s=("recovery_time_s", "mean"),
            failed_tasks=("failed", "sum"),
        )
    )
    run_metrics["tts_s"] = run_metrics["end_max"] - run_metrics["submit_min"]

    summary = (
        run_metrics.groupby(["mode", "n_tasks"], as_index=False)
        .agg(
            runs=("run_id", "count"),
            n_agents=("n_agents", "median"),
            tts_mean_s=("tts_s", "mean"),
            tts_median_s=("tts_s", "median"),
            tts_ci95_s=("tts_s", ci95),
            overhead_p50_median_ms=("overhead_p50_ms", "median"),
            overhead_p95_median_ms=("overhead_p95_ms", "median"),
            cpu_avg_pct=("cpu_avg_pct", "mean"),
            gpu_avg_pct=("gpu_avg_pct", "mean"),
            recovery_time_mean_s=("recovery_time_mean_s", "mean"),
            failed_tasks_total=("failed_tasks", "sum"),
        )
    )

    base = (
        summary[summary["mode"] == "baseline"][["n_tasks", "tts_median_s"]]
        .rename(columns={"tts_median_s": "tts_baseline_s"})
    )
    ray = (
        summary[summary["mode"] == "ray"][["n_tasks", "tts_median_s", "n_agents"]]
        .rename(columns={"tts_median_s": "tts_ray_s", "n_agents": "ray_agents"})
    )
    speedup = pd.merge(base, ray, on="n_tasks", how="inner")
    speedup["speedup"] = speedup["tts_baseline_s"] / speedup["tts_ray_s"]
    speedup["efficiency"] = speedup["speedup"] / speedup["ray_agents"].clip(lower=1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.sort_values(["mode", "n_tasks"]).to_csv(OUT, index=False)
    run_metrics.sort_values(["mode", "n_tasks", "repeat_id"]).to_csv(OUT_SUMMARY, index=False)
    speedup.sort_values(["n_tasks"]).to_csv(OUT_SPEEDUP, index=False)

    print(f"Wrote {OUT}")
    print(f"Wrote {OUT_SUMMARY}")
    print(f"Wrote {OUT_SPEEDUP}")

if __name__ == "__main__":
    main()
