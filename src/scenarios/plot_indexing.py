#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

AGG_DEFAULT = Path("results/scenario1/aggregated/indexing_aggregated.csv")
SPEEDUP_DEFAULT = Path("results/scenario1/aggregated/indexing_speedup.csv")
FIG_DIR_DEFAULT = Path("results/scenario1/figures")


def pick_profile(df: pd.DataFrame) -> dict:
    profile_cols = ["embedding_backend", "embedding_model", "vector_backend", "qdrant_location"]
    for col, default in [
        ("embedding_backend", "synthetic"),
        ("embedding_model", "n/a"),
        ("vector_backend", "mock"),
        ("qdrant_location", "n/a"),
    ]:
        if col not in df.columns:
            df[col] = default

    counts = (
        df.groupby(profile_cols, as_index=False)
        .size()
        .rename(columns={"size": "rows"})
        .sort_values("rows", ascending=False)
    )
    top = counts.iloc[0]
    return {k: top[k] for k in profile_cols}


def filter_profile(df: pd.DataFrame, profile: dict) -> pd.DataFrame:
    out = df.copy()
    for k, v in profile.items():
        if k in out.columns:
            out = out[out[k] == v]
    return out



def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--agg", type=Path, default=AGG_DEFAULT)
    p.add_argument("--speedup", type=Path, default=SPEEDUP_DEFAULT)
    p.add_argument("--fig-dir", type=Path, default=FIG_DIR_DEFAULT)
    args = p.parse_args()

    if not args.agg.exists():
        raise SystemExit(f"aggregated file not found: {args.agg}")

    agg = pd.read_csv(args.agg)
    speedup = pd.read_csv(args.speedup) if args.speedup.exists() else pd.DataFrame()
    if agg.empty:
        raise SystemExit(f"aggregated file is empty: {args.agg}")

    args.fig_dir.mkdir(parents=True, exist_ok=True)

    profile = pick_profile(agg)
    agg_profile = filter_profile(agg, profile)
    speedup_profile = filter_profile(speedup, profile) if not speedup.empty else speedup

    ray_agg = agg_profile[agg_profile["mode"] == "ray"].copy()
    profile_label = (
        f"embed={profile.get('embedding_backend')} "
        f"vector={profile.get('vector_backend')}"
    )

    plt.figure(figsize=(9, 5.5))
    for ds in sorted(ray_agg["dataset_size"].dropna().unique()):
        part = ray_agg[ray_agg["dataset_size"] == ds].sort_values("workers")
        plt.plot(part["workers"], part["total_median_s"], marker="o", label=f"dataset={int(ds):,}")
    plt.title(f"Scenario 1: Indexing Time vs Worker Count (Ray, {profile_label})")
    plt.xlabel("Workers")
    plt.ylabel("Total median time (s)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.fig_dir / "indexing_total_vs_workers.png", dpi=300, bbox_inches="tight")
    plt.close()

    if not speedup_profile.empty:
        plt.figure(figsize=(9, 5.5))
        for ds in sorted(speedup_profile["dataset_size"].dropna().unique()):
            part = speedup_profile[speedup_profile["dataset_size"] == ds].sort_values("workers")
            plt.plot(part["workers"], part["speedup"], marker="o", label=f"dataset={int(ds):,}")
        plt.axhline(1.0, color="gray", linestyle="--", linewidth=1)
        plt.title(f"Scenario 1: Speedup vs Worker Count ({profile_label})")
        plt.xlabel("Workers")
        plt.ylabel("Speedup (baseline / ray)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(args.fig_dir / "indexing_speedup_vs_workers.png", dpi=300, bbox_inches="tight")
        plt.close()

    if not ray_agg.empty:
        top_ds = int(ray_agg["dataset_size"].max())
        stage = ray_agg[ray_agg["dataset_size"] == top_ds].sort_values("workers")

        plt.figure(figsize=(9, 5.5))
        x = stage["workers"].astype(int)
        parse = stage["parse_median_s"]
        embed = stage["embed_median_s"]
        upsert = stage["upsert_median_s"]

        plt.bar(x, parse, label="parse/chunk")
        plt.bar(x, embed, bottom=parse, label="embedding")
        plt.bar(x, upsert, bottom=parse + embed, label="upsert")
        plt.title(f"Scenario 1: Stage Breakdown @ dataset={top_ds:,} (Ray, {profile_label})")
        plt.xlabel("Workers")
        plt.ylabel("Median stage time (s)")
        plt.grid(True, axis="y")
        plt.legend()
        plt.tight_layout()
        plt.savefig(args.fig_dir / "indexing_stage_breakdown.png", dpi=300, bbox_inches="tight")
        plt.close()

    print(f"Plots written to: {args.fig_dir}")


if __name__ == "__main__":
    main()
