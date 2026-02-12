#!/usr/bin/env python3
from pathlib import Path
import glob
import pandas as pd
import matplotlib.pyplot as plt

# Для efficiency лучше использовать число "рабочих" исполнителей Ray.
# Пока оставим константу, потом можно подтягивать из конфига.
CPU_COUNT = 8

RAW_PRIMARY = Path("results/raw/raw_results.csv")
RAW_GLOB = "results/raw_*.csv"

OUT_FIG_DIR = Path("results/figures")
OUT_AGG_DIR = Path("results/aggregated")
OUT_FIG_DIR.mkdir(parents=True, exist_ok=True)
OUT_AGG_DIR.mkdir(parents=True, exist_ok=True)


def load_raw() -> pd.DataFrame:
    files = []
    if RAW_PRIMARY.exists():
        files = [str(RAW_PRIMARY)]
    else:
        files = sorted(glob.glob(RAW_GLOB))

    if not files:
        raise SystemExit(
            "Не найдено входных файлов. Ожидалось: "
            "'results/raw/raw_results.csv' или шаблон 'results/raw_*.csv'"
        )

    df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    return df


def resolve_n_column(df: pd.DataFrame) -> str:
    # Поддержка обоих вариантов схемы
    if "n_agents" in df.columns:
        return "n_agents"
    if "n_tasks" in df.columns:
        return "n_tasks"
    raise SystemExit("В CSV нет ни 'n_agents', ни 'n_tasks'.")


def main():
    df = load_raw()

    required = {"run_id", "mode", "submit_ts", "end_ts"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"В CSV не хватает обязательных колонок: {sorted(missing)}")

    n_col = resolve_n_column(df)

    # Приводим типы
    for c in ["submit_ts", "end_ts"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["run_id", "mode", n_col, "submit_ts", "end_ts"]).copy()

    # TTS по run: max(end_ts)-min(submit_ts)
    run_tts = (
        df.groupby(["run_id", "mode", n_col], as_index=False)
          .agg(submit_min=("submit_ts", "min"),
               end_max=("end_ts", "max"))
    )
    run_tts["tts"] = run_tts["end_max"] - run_tts["submit_min"]

    # Сводка по mode и N
    summary = (
        run_tts.groupby(["mode", n_col], as_index=False)
               .agg(
                   tts_mean=("tts", "mean"),
                   tts_median=("tts", "median"),
                   runs=("tts", "count")
               )
    )

    # speedup / efficiency
    ray = (
        summary[summary["mode"] == "ray"][[n_col, "tts_mean"]]
        .rename(columns={"tts_mean": "ray_tts"})
    )
    base = (
        summary[summary["mode"] == "baseline"][[n_col, "tts_mean"]]
        .rename(columns={"tts_mean": "base_tts"})
    )

    merged = pd.merge(ray, base, on=n_col, how="inner")
    merged["speedup"] = merged["base_tts"] / merged["ray_tts"]
    merged["efficiency"] = merged["speedup"] / CPU_COUNT

    # ---- График 1: TTS vs N ----
    plt.figure()
    for mode in ["ray", "baseline"]:
        part = summary[summary["mode"] == mode].sort_values(n_col)
        if not part.empty:
            plt.plot(part[n_col], part["tts_mean"], marker="o", label=mode)

    plt.title("Time-to-Solution (TTS) vs N")
    plt.xlabel(n_col)
    plt.ylabel("TTS (seconds)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(OUT_FIG_DIR / "tts_vs_n.png", dpi=300, bbox_inches="tight")
    plt.close()

    # ---- График 2: Speedup vs N ----
    if not merged.empty:
        m = merged.sort_values(n_col)

        plt.figure()
        plt.plot(m[n_col], m["speedup"], marker="o")
        plt.title("Speedup vs N")
        plt.xlabel(n_col)
        plt.ylabel("Speedup (baseline / ray)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_FIG_DIR / "speedup_vs_n.png", dpi=300, bbox_inches="tight")
        plt.close()

        # ---- График 3: Efficiency vs N ----
        plt.figure()
        plt.plot(m[n_col], m["efficiency"], marker="o")
        plt.title(f"Parallel Efficiency vs N (CPU={CPU_COUNT})")
        plt.xlabel(n_col)
        plt.ylabel("Efficiency (Speedup / CPU)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_FIG_DIR / "efficiency_vs_n.png", dpi=300, bbox_inches="tight")
        plt.close()
    else:
        print("Предупреждение: не удалось посчитать speedup/efficiency (нет пересечения ray и baseline).")
        m = pd.DataFrame(columns=[n_col, "ray_tts", "base_tts", "speedup", "efficiency"])

    # Сохранение таблиц
    summary.to_csv(OUT_AGG_DIR / "summary_tts.csv", index=False)
    m.to_csv(OUT_AGG_DIR / "speedup_efficiency.csv", index=False)

    print("\n=== Summary (mean/median TTS) ===")
    print(summary.sort_values(["mode", n_col]).to_string(index=False))

    print("\n=== Speedup / Efficiency ===")
    if not m.empty:
        print(m[[n_col, "ray_tts", "base_tts", "speedup", "efficiency"]].to_string(index=False))
    else:
        print("Нет данных для вывода speedup/efficiency.")

    print(f"\nГотово. Графики: {OUT_FIG_DIR.resolve()}")
    print(f"Таблицы: {OUT_AGG_DIR.resolve()}")


if __name__ == "__main__":
    main()

