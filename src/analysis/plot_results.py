import glob
import os
import pandas as pd
import matplotlib.pyplot as plt

CPU_COUNT = 8  # nproc показал 8

# 1) читаем все CSV
files = sorted(glob.glob("results/raw_*.csv"))
if not files:
    raise SystemExit("Не найдено файлов results/raw_*.csv")

df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

# 2) TTS для каждого прогона = max(end_ts) - min(submit_ts) по run_id+mode+n_tasks
run_tts = (
    df.groupby(["run_id", "mode", "n_tasks"], as_index=False)
      .agg(submit_min=("submit_ts", "min"), end_max=("end_ts", "max"))
)
run_tts["tts"] = run_tts["end_max"] - run_tts["submit_min"]

# 3) сводка по N и mode (среднее + медиана)
summary = (
    run_tts.groupby(["mode", "n_tasks"], as_index=False)
           .agg(tts_mean=("tts", "mean"), tts_median=("tts", "median"), runs=("tts", "count"))
)

# 4) Speedup и Efficiency (по среднему TTS)
ray = summary[summary["mode"] == "ray"][["n_tasks", "tts_mean"]].rename(columns={"tts_mean": "ray_tts"})
base = summary[summary["mode"] == "baseline"][["n_tasks", "tts_mean"]].rename(columns={"tts_mean": "base_tts"})
merged = pd.merge(ray, base, on="n_tasks", how="inner")
merged["speedup"] = merged["base_tts"] / merged["ray_tts"]
merged["efficiency"] = merged["speedup"] / CPU_COUNT

#График 1: TTS vs N 
plt.figure()
for mode in ["ray", "baseline"]:
    part = summary[summary["mode"] == mode].sort_values("n_tasks")
    plt.plot(part["n_tasks"], part["tts_mean"], marker="o", label=mode)

plt.title("Time-to-Solution (TTS) vs Number of Tasks")
plt.xlabel("N tasks")
plt.ylabel("TTS (seconds)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("tts_vs_n.png", dpi=300, bbox_inches="tight")
plt.show()

#График 2: Speedup vs N
plt.figure()
m = merged.sort_values("n_tasks")
plt.plot(m["n_tasks"], m["speedup"], marker="o")
plt.title("Speedup vs Number of Tasks")
plt.xlabel("N tasks")
plt.ylabel("Speedup (baseline / ray)")
plt.grid(True)
plt.tight_layout()
plt.savefig("speedup_vs_n.png", dpi=300, bbox_inches="tight")
plt.show()

#График 3: Efficiency vs N
plt.figure()
plt.plot(m["n_tasks"], m["efficiency"], marker="o")
plt.title(f"Parallel Efficiency vs Number of Tasks (CPU={CPU_COUNT})")
plt.xlabel("N tasks")
plt.ylabel("Efficiency (Speedup / CPU)")
plt.grid(True)
plt.tight_layout()
plt.savefig("efficiency_vs_n.png", dpi=300, bbox_inches="tight")
plt.show()

print("\n=== Summary (mean TTS) ===")
print(summary.sort_values(["mode", "n_tasks"]).to_string(index=False))

print("\n=== Speedup/Efficiency (from mean TTS) ===")
print(m[["n_tasks", "ray_tts", "base_tts", "speedup", "efficiency"]].to_string(index=False))
