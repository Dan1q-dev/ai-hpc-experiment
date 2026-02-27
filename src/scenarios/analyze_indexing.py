#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DEFAULT = Path("results/scenario1/raw/indexing_raw.csv")
OUT_AGG_DEFAULT = Path("results/scenario1/aggregated/indexing_aggregated.csv")
OUT_SPEEDUP_DEFAULT = Path("results/scenario1/aggregated/indexing_speedup.csv")
OUT_STAGE_DEFAULT = Path("results/scenario1/aggregated/indexing_stage_breakdown.csv")


def ci95(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    n = len(vals)
    if n <= 1:
        return 0.0
    return 1.96 * vals.std(ddof=1) / np.sqrt(n)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=RAW_DEFAULT)
    p.add_argument("--out-agg", type=Path, default=OUT_AGG_DEFAULT)
    p.add_argument("--out-speedup", type=Path, default=OUT_SPEEDUP_DEFAULT)
    p.add_argument("--out-stage", type=Path, default=OUT_STAGE_DEFAULT)
    args = p.parse_args()

    if not args.input.exists():
        raise SystemExit(f"raw file not found: {args.input}")

    df = pd.read_csv(args.input)
    if df.empty:
        raise SystemExit("raw indexing CSV is empty")

    for col, default in [
        ("embedding_backend", "synthetic"),
        ("embedding_model", "n/a"),
        ("vector_backend", "mock"),
        ("qdrant_location", "n/a"),
    ]:
        if col not in df.columns:
            df[col] = default

    numeric_cols = [
        "dataset_size",
        "workers",
        "repeat_id",
        "parse_wall_s",
        "embed_wall_s",
        "upsert_wall_s",
        "total_wall_s",
        "parse_sum_s",
        "embed_sum_s",
        "upsert_sum_s",
        "points_total",
        "chunks_total",
        "docs_total",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    group_cols = ["mode", "embedding_backend", "embedding_model", "vector_backend", "qdrant_location", "dataset_size", "workers"]
    agg = (
        df.groupby(group_cols, as_index=False)
        .agg(
            runs=("run_id", "count"),
            total_mean_s=("total_wall_s", "mean"),
            total_median_s=("total_wall_s", "median"),
            total_ci95_s=("total_wall_s", ci95),
            parse_median_s=("parse_wall_s", "median"),
            embed_median_s=("embed_wall_s", "median"),
            upsert_median_s=("upsert_wall_s", "median"),
            points_mean=("points_total", "mean"),
            chunks_mean=("chunks_total", "mean"),
            docs_mean=("docs_total", "mean"),
            backends=("backend", lambda s: ";".join(sorted(set(str(x) for x in s)))),
        )
    )

    baseline = (
        agg[(agg["mode"] == "baseline") & (agg["workers"] == 1)][
            ["embedding_backend", "embedding_model", "vector_backend", "qdrant_location", "dataset_size", "total_median_s"]
        ]
        .rename(columns={"total_median_s": "baseline_total_median_s"})
    )

    speedup = pd.merge(
        agg[agg["mode"] == "ray"].copy(),
        baseline,
        on=["embedding_backend", "embedding_model", "vector_backend", "qdrant_location", "dataset_size"],
        how="left",
    )
    speedup["speedup"] = speedup["baseline_total_median_s"] / speedup["total_median_s"]
    speedup["efficiency"] = speedup["speedup"] / speedup["workers"].clip(lower=1)

    stage = (
        agg[
            [
                "mode",
                "embedding_backend",
                "embedding_model",
                "vector_backend",
                "qdrant_location",
                "dataset_size",
                "workers",
                "parse_median_s",
                "embed_median_s",
                "upsert_median_s",
                "total_median_s",
            ]
        ]
        .sort_values(["mode", "embedding_backend", "vector_backend", "dataset_size", "workers"])
        .reset_index(drop=True)
    )

    args.out_agg.parent.mkdir(parents=True, exist_ok=True)
    agg.sort_values(["mode", "embedding_backend", "vector_backend", "dataset_size", "workers"]).to_csv(args.out_agg, index=False)
    speedup.sort_values(["embedding_backend", "vector_backend", "dataset_size", "workers"]).to_csv(args.out_speedup, index=False)
    stage.to_csv(args.out_stage, index=False)

    print(f"Wrote {args.out_agg}")
    print(f"Wrote {args.out_speedup}")
    print(f"Wrote {args.out_stage}")


if __name__ == "__main__":
    main()
