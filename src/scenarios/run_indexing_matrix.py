#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
from typing import List, Set, Tuple

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scenarios.indexing_benchmark import (
    BenchmarkConfig,
    RAW_DEFAULT,
    append_row,
    ensure_csv_header,
    run_once,
    shutdown_ray_session,
)


def parse_int_csv(raw: str) -> List[int]:
    out = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset-sizes", default="100000,1000000,10000000")
    p.add_argument("--workers", default="1,2,4,8")
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--seed-base", type=int, default=42)
    p.add_argument("--batch-docs", type=int, default=20000)
    p.add_argument("--embedding-backend", choices=["synthetic", "pretrained"], default="synthetic")
    p.add_argument("--embedding-model", default="BAAI/bge-small-en-v1.5")
    p.add_argument("--embedding-dim", type=int, default=32)
    p.add_argument("--embedding-intensity", type=int, default=1)
    p.add_argument("--template-pool-size", type=int, default=2048)
    p.add_argument("--max-chunks-per-doc", type=int, default=2)
    p.add_argument("--vector-backend", choices=["mock", "qdrant"], default="mock")
    p.add_argument("--qdrant-location", default=":memory:")
    p.add_argument("--ray-address", default="")
    p.add_argument("--ray-warmup", action="store_true", default=True)
    p.add_argument("--no-ray-warmup", dest="ray_warmup", action="store_false")
    p.add_argument("--skip-baseline", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--clean", action="store_true")
    p.add_argument("--output", type=Path, default=RAW_DEFAULT)
    return p.parse_args()


def run_and_store(cfg: BenchmarkConfig, output: Path) -> None:
    row = run_once(cfg)
    append_row(output, row)
    print(
        f"[MATRIX] mode={cfg.mode} dataset={cfg.dataset_size} workers={cfg.workers} "
        f"repeat={cfg.repeat_id}/{cfg.seed} total={float(row['total_wall_s']):.3f}s backend={row['backend']}"
    )


def load_completed_points(path: Path) -> Set[Tuple[str, int, int, int]]:
    done: Set[Tuple[str, int, int, int]] = set()
    if not path.exists():
        return done

    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                key = (
                    str(row["mode"]),
                    int(row["dataset_size"]),
                    int(row["workers"]),
                    int(row["repeat_id"]),
                )
            except Exception:
                continue
            done.add(key)
    return done


def main() -> None:
    args = parse_args()

    dataset_sizes = parse_int_csv(args.dataset_sizes)
    workers = parse_int_csv(args.workers)
    if not dataset_sizes:
        raise SystemExit("--dataset-sizes is empty")
    if not workers:
        raise SystemExit("--workers is empty")
    if args.repeats <= 0:
        raise SystemExit("--repeats must be > 0")

    if args.clean and args.resume:
        raise SystemExit("--clean and --resume cannot be used together")

    if args.clean and args.output.exists():
        args.output.unlink()

    ensure_csv_header(args.output)
    completed = load_completed_points(args.output) if args.resume else set()

    total_runs = len(dataset_sizes) * args.repeats * len(workers)
    if not args.skip_baseline:
        total_runs += len(dataset_sizes) * args.repeats

    print(
        f"[MATRIX] Starting indexing scenario: datasets={dataset_sizes} workers={workers} repeats={args.repeats} "
        f"skip_baseline={args.skip_baseline} resume={args.resume} completed={len(completed)} total_runs={total_runs} "
        f"embed={args.embedding_backend} vector={args.vector_backend}"
    )

    if args.ray_warmup:
        warmup_cfg = BenchmarkConfig(
            mode="ray",
            dataset_size=min(1000, dataset_sizes[0]),
            workers=max(workers),
            repeat_id=0,
            seed=args.seed_base,
            batch_docs=min(args.batch_docs, 1000),
            embedding_backend=args.embedding_backend,
            embedding_model=args.embedding_model,
            embedding_dim=args.embedding_dim,
            embedding_intensity=args.embedding_intensity,
            template_pool_size=args.template_pool_size,
            max_chunks_per_doc=args.max_chunks_per_doc,
            vector_backend=args.vector_backend,
            qdrant_location=args.qdrant_location,
            ray_address=args.ray_address,
        )
        warmup_row = run_once(warmup_cfg)
        print(
            f"[MATRIX] Ray warmup done: workers={warmup_cfg.workers} "
            f"backend={warmup_row['backend']} total={float(warmup_row['total_wall_s']):.3f}s"
        )

    done = 0
    try:
        for ds in dataset_sizes:
            for repeat_id in range(1, args.repeats + 1):
                seed = args.seed_base + repeat_id
                if not args.skip_baseline:
                    key = ("baseline", ds, 1, repeat_id)
                    if args.resume and key in completed:
                        done += 1
                        print(f"[MATRIX] Skip completed point: {key}")
                        print(f"[MATRIX] Progress: {done}/{total_runs}")
                    else:
                        cfg = BenchmarkConfig(
                            mode="baseline",
                            dataset_size=ds,
                            workers=1,
                            repeat_id=repeat_id,
                            seed=seed,
                            batch_docs=args.batch_docs,
                            embedding_backend=args.embedding_backend,
                            embedding_model=args.embedding_model,
                            embedding_dim=args.embedding_dim,
                            embedding_intensity=args.embedding_intensity,
                            template_pool_size=args.template_pool_size,
                            max_chunks_per_doc=args.max_chunks_per_doc,
                            vector_backend=args.vector_backend,
                            qdrant_location=args.qdrant_location,
                            ray_address="",
                        )
                        run_and_store(cfg, output=args.output)
                        completed.add(key)
                        done += 1
                        print(f"[MATRIX] Progress: {done}/{total_runs}")

                for w in workers:
                    key = ("ray", ds, w, repeat_id)
                    if args.resume and key in completed:
                        done += 1
                        print(f"[MATRIX] Skip completed point: {key}")
                        print(f"[MATRIX] Progress: {done}/{total_runs}")
                        continue
                    cfg = BenchmarkConfig(
                        mode="ray",
                        dataset_size=ds,
                        workers=w,
                        repeat_id=repeat_id,
                        seed=seed,
                        batch_docs=args.batch_docs,
                        embedding_backend=args.embedding_backend,
                        embedding_model=args.embedding_model,
                        embedding_dim=args.embedding_dim,
                        embedding_intensity=args.embedding_intensity,
                        template_pool_size=args.template_pool_size,
                        max_chunks_per_doc=args.max_chunks_per_doc,
                        vector_backend=args.vector_backend,
                        qdrant_location=args.qdrant_location,
                        ray_address=args.ray_address,
                    )
                    run_and_store(cfg, output=args.output)
                    completed.add(key)
                    done += 1
                    print(f"[MATRIX] Progress: {done}/{total_runs}")
    finally:
        shutdown_ray_session()

    print(f"[MATRIX] Completed. Output: {args.output}")


if __name__ == "__main__":
    main()
