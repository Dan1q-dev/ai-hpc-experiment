#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

RAW_DEFAULT = Path("results/scenario1/raw/indexing_raw.csv")


REQUIRED_COLS = {
    "run_id",
    "mode",
    "dataset_size",
    "workers",
    "repeat_id",
    "seed",
    "batch_docs",
    "embedding_backend",
    "embedding_model",
    "embedding_dim",
    "embedding_intensity",
    "template_pool_size",
    "max_chunks_per_doc",
    "vector_backend",
    "qdrant_location",
    "backend",
    "docs_total",
    "chunks_total",
    "points_total",
    "parse_wall_s",
    "embed_wall_s",
    "upsert_wall_s",
    "total_wall_s",
    "parse_sum_s",
    "embed_sum_s",
    "upsert_sum_s",
    "checksum",
    "ray_address",
    "timestamp_utc",
}


def die(msg: str) -> None:
    print(f"[INDEXING_VALIDATE] FAIL: {msg}")
    sys.exit(1)


def parse_int_csv(raw: str):
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=RAW_DEFAULT)
    p.add_argument("--dataset-sizes", default="100000,1000000,10000000")
    p.add_argument("--workers", default="1,2,4,8")
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--skip-baseline", action="store_true")
    args = p.parse_args()

    if not args.input.exists():
        die(f"file not found: {args.input}")

    df = pd.read_csv(args.input)
    if df.empty:
        die("raw CSV is empty")

    missing_cols = REQUIRED_COLS - set(df.columns)
    if missing_cols:
        die(f"missing columns: {sorted(missing_cols)}")

    dataset_sizes = parse_int_csv(args.dataset_sizes)
    workers = parse_int_csv(args.workers)

    for col in [
        "dataset_size",
        "workers",
        "repeat_id",
        "docs_total",
        "chunks_total",
        "points_total",
        "parse_wall_s",
        "embed_wall_s",
        "upsert_wall_s",
        "total_wall_s",
        "parse_sum_s",
        "embed_sum_s",
        "upsert_sum_s",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df["run_id"].duplicated().any():
        die("run_id must be unique")

    if df[["parse_wall_s", "embed_wall_s", "upsert_wall_s", "total_wall_s"]].lt(0).any().any():
        die("negative timings detected")

    if (df["points_total"] <= 0).any():
        die("points_total must be > 0")

    if (df["docs_total"] != df["dataset_size"]).any():
        die("docs_total must equal dataset_size for each run")

    if not df["embedding_backend"].isin({"synthetic", "pretrained"}).all():
        die("embedding_backend must be synthetic|pretrained")

    if not df["vector_backend"].isin({"mock", "qdrant"}).all():
        die("vector_backend must be mock|qdrant")

    expected = []
    for ds in dataset_sizes:
        for repeat in range(1, args.repeats + 1):
            if not args.skip_baseline:
                expected.append(("baseline", ds, 1, repeat))
            for w in workers:
                expected.append(("ray", ds, w, repeat))

    seen = set(
        zip(
            df["mode"].astype(str),
            df["dataset_size"].astype(int),
            df["workers"].astype(int),
            df["repeat_id"].astype(int),
        )
    )

    missing = [item for item in expected if item not in seen]
    if missing:
        preview = ", ".join(str(x) for x in missing[:5])
        die(f"matrix incomplete: missing {len(missing)} points; sample={preview}")

    print(
        f"[INDEXING_VALIDATE] OK rows={len(df)} expected_points={len(expected)} "
        f"datasets={dataset_sizes} workers={workers} repeats={args.repeats}"
    )


if __name__ == "__main__":
    main()
