#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

RAW_PRIMARY = Path("results/raw/raw_results.csv")
OUT_FIG_DIR = Path("results/figures")
OUT_AGG_DIR = Path("results/aggregated")
OUT_FIG_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_PATH = OUT_AGG_DIR / "aggregated_results.csv"
SPEEDUP_PATH = OUT_AGG_DIR / "speedup_efficiency.csv"


def load_raw() -> pd.DataFrame:
    if not RAW_PRIMARY.exists():
        raise SystemExit("Не найдено: results/raw/raw_results.csv")
    return pd.read_csv(RAW_PRIMARY)


def main():
    summary = pd.read_csv(SUMMARY_PATH) if SUMMARY_PATH.exists() else None
    speedup = pd.read_csv(SPEEDUP_PATH) if SPEEDUP_PATH.exists() else None
    raw = load_raw()

    if summary is None:
        raise SystemExit("Не найдено aggregated_results.csv. Сначала запустите analyze.py")

    plt.figure(figsize=(8, 5))
    for mode in ["ray", "baseline"]:
        part = summary[summary["mode"] == mode].sort_values("n_tasks")
        if not part.empty:
            plt.plot(part["n_tasks"], part["tts_median_s"], marker="o", label=mode)
            plt.fill_between(
                part["n_tasks"],
                part["tts_median_s"] - part["tts_ci95_s"],
                part["tts_median_s"] + part["tts_ci95_s"],
                alpha=0.2,
            )
    plt.title("TTS vs Task Count")
    plt.xlabel("n_tasks")
    plt.ylabel("TTS median (s)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(OUT_FIG_DIR / "tts_vs_n.png", dpi=300, bbox_inches="tight")
    plt.close()

    if speedup is not None and not speedup.empty:
        s = speedup.sort_values("n_tasks")

        plt.figure(figsize=(8, 5))
        plt.plot(s["n_tasks"], s["speedup"], marker="o")
        plt.title("Speedup vs Task Count")
        plt.xlabel("n_tasks")
        plt.ylabel("Speedup (baseline / ray)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_FIG_DIR / "speedup_vs_n.png", dpi=300, bbox_inches="tight")
        plt.close()

        plt.figure(figsize=(8, 5))
        plt.plot(s["n_tasks"], s["efficiency"], marker="o")
        plt.title("Efficiency vs Task Count")
        plt.xlabel("n_tasks")
        plt.ylabel("Efficiency")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_FIG_DIR / "efficiency_vs_n.png", dpi=300, bbox_inches="tight")
        plt.close()
    else:
        for stale in ["speedup_vs_n.png", "efficiency_vs_n.png"]:
            p = OUT_FIG_DIR / stale
            if p.exists():
                p.unlink()

    util = (
        summary.groupby("mode", as_index=False)
        .agg(cpu_avg_pct=("cpu_avg_pct", "mean"), gpu_avg_pct=("gpu_avg_pct", "mean"))
    )
    plt.figure(figsize=(8, 5))
    x = range(len(util))
    plt.bar([i - 0.15 for i in x], util["cpu_avg_pct"], width=0.3, label="CPU")
    plt.bar([i + 0.15 for i in x], util["gpu_avg_pct"], width=0.3, label="GPU")
    plt.xticks(list(x), util["mode"])
    plt.ylabel("Utilization (%)")
    plt.title("Average Resource Utilization")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_FIG_DIR / "resource_utilization.png", dpi=300, bbox_inches="tight")
    plt.close()

    rec = (
        raw.groupby(["mode", "n_tasks"], as_index=False)
        .agg(recovery_time_mean_s=("recovery_time_s", "mean"))
        .sort_values(["mode", "n_tasks"])
    )
    plt.figure(figsize=(8, 5))
    for mode in ["ray", "baseline"]:
        part = rec[rec["mode"] == mode]
        if not part.empty:
            plt.plot(part["n_tasks"], part["recovery_time_mean_s"], marker="o", label=mode)
    plt.title("Failure Recovery vs Task Count")
    plt.xlabel("n_tasks")
    plt.ylabel("Recovery time mean (s)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_FIG_DIR / "recovery_vs_n.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Plots written to: {OUT_FIG_DIR.resolve()}")


if __name__ == "__main__":
    main()
