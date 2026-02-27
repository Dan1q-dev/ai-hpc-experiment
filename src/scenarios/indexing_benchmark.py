#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import time
import traceback
from typing import Dict, List, Optional, Sequence, Tuple
import uuid

import numpy as np

RAW_DEFAULT = Path("results/scenario1/raw/indexing_raw.csv")
SRC_ROOT = Path(__file__).resolve().parents[1]
_RAY_SESSION_KEY: Optional[str] = None

# In-process caches to avoid reloading model/table per batch.
_FASTEMBED_MODEL_CACHE: Dict[str, object] = {}
_TEMPLATE_EMB_CACHE: Dict[Tuple[str, int, int], np.ndarray] = {}

CSV_HEADER = [
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
]


@dataclass
class BenchmarkConfig:
    mode: str
    dataset_size: int
    workers: int
    repeat_id: int
    seed: int = 42
    batch_docs: int = 20000
    embedding_backend: str = "synthetic"  # synthetic|pretrained
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 32
    embedding_intensity: int = 1
    template_pool_size: int = 2048
    max_chunks_per_doc: int = 2
    vector_backend: str = "mock"  # mock|qdrant
    qdrant_location: str = ":memory:"
    ray_address: str = ""


class MockVectorStore:
    def __init__(self) -> None:
        self.points_total = 0
        self.checksum = 0.0

    def upsert(self, ids: np.ndarray, embeddings: np.ndarray) -> float:
        # Cheap deterministic reduction to mimic sink-side work.
        self.points_total += int(ids.shape[0])
        batch_checksum = float(np.sum(embeddings[:, 0], dtype=np.float64)) if embeddings.size else 0.0
        self.checksum += batch_checksum
        return batch_checksum

    def close(self) -> None:
        return


class QdrantVectorStore:
    def __init__(self, *, dim: int, location: str, collection_name: str) -> None:
        try:
            from qdrant_client import QdrantClient  # type: ignore
            from qdrant_client import models as qmodels  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "qdrant-client is required for vector_backend=qdrant. "
                "Install requirements or set VECTOR_BACKEND=mock."
            ) from exc

        self._models = qmodels
        self.collection_name = collection_name
        self.points_total = 0
        self.checksum = 0.0

        if location.startswith("http://") or location.startswith("https://"):
            self.client = QdrantClient(url=location)
        elif location and location != ":memory:":
            # File-system local mode (qdrant embedded).
            self.client = QdrantClient(path=location)
        else:
            self.client = QdrantClient(path=":memory:")

        try:
            if self.client.collection_exists(collection_name=self.collection_name):
                self.client.delete_collection(collection_name=self.collection_name)
        except Exception:
            # Older/newer client compatibility.
            try:
                self.client.delete_collection(collection_name=self.collection_name)
            except Exception:
                pass

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=qmodels.VectorParams(size=dim, distance=qmodels.Distance.COSINE),
            optimizers_config=qmodels.OptimizersConfigDiff(indexing_threshold=0),
        )

    def upsert(self, ids: np.ndarray, embeddings: np.ndarray) -> float:
        if embeddings.size == 0:
            return 0.0

        payload = self._models.Batch(
            ids=[int(x) for x in ids.tolist()],
            vectors=embeddings.tolist(),
        )
        self.client.upsert(collection_name=self.collection_name, points=payload, wait=True)

        self.points_total += int(ids.shape[0])
        batch_checksum = float(np.sum(embeddings[:, 0], dtype=np.float64))
        self.checksum += batch_checksum
        return batch_checksum

    def close(self) -> None:
        try:
            self.client.delete_collection(collection_name=self.collection_name)
        except Exception:
            pass
        try:
            self.client.close()
        except Exception:
            pass


def ensure_csv_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)


def append_row(path: Path, row: Dict[str, object]) -> None:
    ensure_csv_header(path)
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writerow(row)


def split_ranges(total: int, shards: int) -> List[Tuple[int, int]]:
    shards = max(1, int(shards))
    step = int(math.ceil(total / shards))
    out = []
    for i in range(shards):
        start = i * step
        end = min(total, start + step)
        if start >= end:
            continue
        out.append((start, end))
    return out


def parse_chunk_ids(doc_ids: np.ndarray, max_chunks_per_doc: int, seed: int) -> np.ndarray:
    if max_chunks_per_doc <= 1:
        return doc_ids

    max_chunks_per_doc = int(max_chunks_per_doc)
    reps = 1 + ((doc_ids + seed) % max_chunks_per_doc)

    # Build unique point ids per chunk: doc_id * max_chunks + chunk_offset.
    repeated_base = np.repeat(doc_ids * max_chunks_per_doc, reps)
    group_starts = np.repeat(np.cumsum(reps) - reps, reps)
    offsets = np.arange(int(reps.sum()), dtype=np.int64) - group_starts
    return repeated_base + offsets


def generate_synthetic_embeddings(
    chunk_ids: np.ndarray,
    embedding_dim: int,
    intensity: int,
    seed: int,
) -> np.ndarray:
    if chunk_ids.size == 0:
        return np.empty((0, embedding_dim), dtype=np.float32)

    ids = chunk_ids.astype(np.float32)[:, None]
    basis = (np.arange(embedding_dim, dtype=np.float32)[None, :] + 1.0)
    emb = np.sin((ids + float(seed)) * 0.0007 * basis)

    for _ in range(max(1, intensity) - 1):
        emb = np.tanh(emb * 1.0007 + 0.0003)

    norms = np.linalg.norm(emb, axis=1, keepdims=True) + 1e-9
    emb = emb / norms
    return emb.astype(np.float32, copy=False)


def template_text(template_id: int) -> str:
    topics = [
        "ray autoscaling",
        "kubernetes orchestration",
        "agent scheduling",
        "distributed retrieval",
        "vector indexing",
        "failure recovery",
        "resource utilization",
        "parallel pipelines",
    ]
    domain = topics[template_id % len(topics)]
    return (
        f"template-{template_id}: synthetic benchmark document about {domain}. "
        f"This text is used for deterministic embedding generation in scenario1."
    )


def _load_fastembed_model(model_name: str):
    model = _FASTEMBED_MODEL_CACHE.get(model_name)
    if model is not None:
        return model

    try:
        from fastembed import TextEmbedding  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "fastembed is required for embedding_backend=pretrained. "
            "Install requirements or set EMBEDDING_BACKEND=synthetic."
        ) from exc

    model = TextEmbedding(model_name=model_name)
    _FASTEMBED_MODEL_CACHE[model_name] = model
    return model


def _precompute_template_embeddings(model_name: str, template_pool_size: int, seed: int) -> np.ndarray:
    key = (model_name, template_pool_size, seed)
    table = _TEMPLATE_EMB_CACHE.get(key)
    if table is not None:
        return table

    model = _load_fastembed_model(model_name)
    texts = [template_text((seed + i) % template_pool_size) for i in range(template_pool_size)]
    vectors = list(model.embed(texts, batch_size=256))
    table = np.asarray(vectors, dtype=np.float32)
    norms = np.linalg.norm(table, axis=1, keepdims=True) + 1e-9
    table = table / norms
    _TEMPLATE_EMB_CACHE[key] = table
    return table


def generate_pretrained_embeddings(
    chunk_ids: np.ndarray,
    model_name: str,
    template_pool_size: int,
    seed: int,
) -> np.ndarray:
    if chunk_ids.size == 0:
        return np.empty((0, 0), dtype=np.float32)

    table = _precompute_template_embeddings(model_name, template_pool_size, seed)
    idx = np.mod(chunk_ids + seed, template_pool_size).astype(np.int64, copy=False)
    return table[idx]


def create_vector_store(
    vector_backend: str,
    *,
    embedding_dim: int,
    qdrant_location: str,
    collection_name: str,
):
    if vector_backend == "mock":
        return MockVectorStore()
    if vector_backend == "qdrant":
        return QdrantVectorStore(dim=embedding_dim, location=qdrant_location, collection_name=collection_name)
    raise ValueError(f"unsupported vector_backend={vector_backend}")


def process_shard(
    start: int,
    end: int,
    *,
    batch_docs: int,
    embedding_backend: str,
    embedding_model: str,
    embedding_dim: int,
    embedding_intensity: int,
    template_pool_size: int,
    max_chunks_per_doc: int,
    vector_backend: str,
    qdrant_location: str,
    collection_name: str,
    seed: int,
) -> Dict[str, float]:
    parse_s = 0.0
    embed_s = 0.0
    upsert_s = 0.0

    docs_total = 0
    chunks_total = 0
    checksum_total = 0.0
    eff_dim = 0

    store = create_vector_store(
        vector_backend,
        embedding_dim=embedding_dim,
        qdrant_location=qdrant_location,
        collection_name=collection_name,
    )

    shard_started = time.perf_counter()
    try:
        for left in range(start, end, batch_docs):
            right = min(end, left + batch_docs)
            doc_ids = np.arange(left, right, dtype=np.int64)
            docs_total += int(doc_ids.shape[0])

            t0 = time.perf_counter()
            chunk_ids = parse_chunk_ids(doc_ids, max_chunks_per_doc=max_chunks_per_doc, seed=seed)
            parse_s += time.perf_counter() - t0
            chunks_total += int(chunk_ids.shape[0])

            t0 = time.perf_counter()
            if embedding_backend == "synthetic":
                embeddings = generate_synthetic_embeddings(
                    chunk_ids,
                    embedding_dim=embedding_dim,
                    intensity=embedding_intensity,
                    seed=seed,
                )
            elif embedding_backend == "pretrained":
                embeddings = generate_pretrained_embeddings(
                    chunk_ids,
                    model_name=embedding_model,
                    template_pool_size=template_pool_size,
                    seed=seed,
                )
            else:
                raise ValueError(f"unsupported embedding_backend={embedding_backend}")

            embed_s += time.perf_counter() - t0
            if embeddings.ndim == 2 and embeddings.shape[1] > 0:
                eff_dim = int(embeddings.shape[1])

            t0 = time.perf_counter()
            checksum_total += store.upsert(chunk_ids, embeddings)
            upsert_s += time.perf_counter() - t0
    finally:
        store.close()

    shard_total_s = time.perf_counter() - shard_started

    return {
        "docs_total": float(docs_total),
        "chunks_total": float(chunks_total),
        "points_total": float(getattr(store, "points_total", chunks_total)),
        "parse_s": parse_s,
        "embed_s": embed_s,
        "upsert_s": upsert_s,
        "total_s": shard_total_s,
        "checksum": checksum_total,
        "embedding_dim": float(eff_dim or embedding_dim),
    }


def process_shard_remote(*args, **kwargs) -> Dict[str, float]:
    return process_shard(*args, **kwargs)


def run_baseline(cfg: BenchmarkConfig) -> Tuple[str, List[Dict[str, float]], float]:
    started = time.perf_counter()
    run_token = uuid.uuid4().hex[:10]
    shard_result = process_shard(
        0,
        cfg.dataset_size,
        batch_docs=cfg.batch_docs,
        embedding_backend=cfg.embedding_backend,
        embedding_model=cfg.embedding_model,
        embedding_dim=cfg.embedding_dim,
        embedding_intensity=cfg.embedding_intensity,
        template_pool_size=cfg.template_pool_size,
        max_chunks_per_doc=cfg.max_chunks_per_doc,
        vector_backend=cfg.vector_backend,
        qdrant_location=cfg.qdrant_location,
        collection_name=f"s1_{cfg.mode}_{cfg.repeat_id}_{run_token}_0",
        seed=cfg.seed,
    )
    return "baseline", [shard_result], time.perf_counter() - started


def run_ray_native(cfg: BenchmarkConfig) -> Tuple[str, List[Dict[str, float]], float]:
    import ray  # type: ignore

    global _RAY_SESSION_KEY

    session_key = cfg.ray_address.strip() or "local-default"
    if ray.is_initialized() and _RAY_SESSION_KEY != session_key:
        ray.shutdown()
        _RAY_SESSION_KEY = None

    if not ray.is_initialized():
        py_path = str(SRC_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")
        init_kwargs: Dict[str, object] = {
            "ignore_reinit_error": True,
            "log_to_driver": False,
            "runtime_env": {
                "env_vars": {
                    "PYTHONPATH": py_path,
                }
            },
        }
        if cfg.ray_address:
            init_kwargs["address"] = cfg.ray_address
            init_kwargs["runtime_env"] = {
                "working_dir": str(SRC_ROOT),
                "excludes": ["__pycache__"],
                "env_vars": {
                    "PYTHONPATH": py_path,
                },
            }
        else:
            init_kwargs["num_cpus"] = max(cfg.workers, int(os.cpu_count() or cfg.workers))

        ray.init(**init_kwargs)
        _RAY_SESSION_KEY = session_key

    started = time.perf_counter()
    RemoteFn = ray.remote(process_shard_remote)
    refs = []
    run_token = uuid.uuid4().hex[:10]
    for idx, (start, end) in enumerate(split_ranges(cfg.dataset_size, cfg.workers)):
        refs.append(
            RemoteFn.remote(
                start,
                end,
                batch_docs=cfg.batch_docs,
                embedding_backend=cfg.embedding_backend,
                embedding_model=cfg.embedding_model,
                embedding_dim=cfg.embedding_dim,
                embedding_intensity=cfg.embedding_intensity,
                template_pool_size=cfg.template_pool_size,
                max_chunks_per_doc=cfg.max_chunks_per_doc,
                vector_backend=cfg.vector_backend,
                qdrant_location=cfg.qdrant_location,
                collection_name=f"s1_{cfg.mode}_{cfg.repeat_id}_{run_token}_{idx}",
                seed=cfg.seed,
            )
        )
    shard_results = ray.get(refs)
    wall_total = time.perf_counter() - started
    return "ray_native", shard_results, wall_total


def run_ray_fallback(cfg: BenchmarkConfig) -> Tuple[str, List[Dict[str, float]], float]:
    import concurrent.futures as cf

    started = time.perf_counter()
    shard_results: List[Dict[str, float]] = []
    with cf.ProcessPoolExecutor(max_workers=cfg.workers) as ex:
        futures = []
        run_token = uuid.uuid4().hex[:10]
        for idx, (start, end) in enumerate(split_ranges(cfg.dataset_size, cfg.workers)):
            futures.append(
                ex.submit(
                    process_shard,
                    start,
                    end,
                    batch_docs=cfg.batch_docs,
                    embedding_backend=cfg.embedding_backend,
                    embedding_model=cfg.embedding_model,
                    embedding_dim=cfg.embedding_dim,
                    embedding_intensity=cfg.embedding_intensity,
                    template_pool_size=cfg.template_pool_size,
                    max_chunks_per_doc=cfg.max_chunks_per_doc,
                    vector_backend=cfg.vector_backend,
                    qdrant_location=cfg.qdrant_location,
                    collection_name=f"s1_{cfg.mode}_{cfg.repeat_id}_{run_token}_{idx}",
                    seed=cfg.seed,
                )
            )
        for fut in cf.as_completed(futures):
            shard_results.append(fut.result())
    wall_total = time.perf_counter() - started
    return "process_pool", shard_results, wall_total


def run_ray(cfg: BenchmarkConfig) -> Tuple[str, List[Dict[str, float]], float]:
    try:
        return run_ray_native(cfg)
    except Exception as exc:
        if isinstance(exc, ModuleNotFoundError):
            print("[WARN] ray is not installed in current interpreter, fallback to process pool.")
        else:
            print(f"[WARN] ray native execution failed, fallback to process pool: {exc}")
            print(traceback.format_exc(limit=1))
        return run_ray_fallback(cfg)


def shutdown_ray_session() -> None:
    global _RAY_SESSION_KEY
    try:
        import ray  # type: ignore

        if ray.is_initialized():
            ray.shutdown()
    except Exception:
        pass
    _RAY_SESSION_KEY = None


def aggregate_run(
    cfg: BenchmarkConfig,
    backend: str,
    shard_results: Sequence[Dict[str, float]],
    total_wall_s: float,
) -> Dict[str, object]:
    parse_list = [float(r.get("parse_s", 0.0)) for r in shard_results]
    embed_list = [float(r.get("embed_s", 0.0)) for r in shard_results]
    upsert_list = [float(r.get("upsert_s", 0.0)) for r in shard_results]

    docs_total = int(sum(float(r.get("docs_total", 0.0)) for r in shard_results))
    chunks_total = int(sum(float(r.get("chunks_total", 0.0)) for r in shard_results))
    points_total = int(sum(float(r.get("points_total", 0.0)) for r in shard_results))
    checksum = float(sum(float(r.get("checksum", 0.0)) for r in shard_results))
    eff_dim = int(max(float(r.get("embedding_dim", 0.0)) for r in shard_results)) if shard_results else cfg.embedding_dim

    run_id = (
        f"{cfg.mode}_DS{cfg.dataset_size}_W{cfg.workers}_"
        f"R{cfg.repeat_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    )

    return {
        "run_id": run_id,
        "mode": cfg.mode,
        "dataset_size": cfg.dataset_size,
        "workers": cfg.workers,
        "repeat_id": cfg.repeat_id,
        "seed": cfg.seed,
        "batch_docs": cfg.batch_docs,
        "embedding_backend": cfg.embedding_backend,
        "embedding_model": cfg.embedding_model,
        "embedding_dim": eff_dim,
        "embedding_intensity": cfg.embedding_intensity,
        "template_pool_size": cfg.template_pool_size,
        "max_chunks_per_doc": cfg.max_chunks_per_doc,
        "vector_backend": cfg.vector_backend,
        "qdrant_location": cfg.qdrant_location,
        "backend": backend,
        "docs_total": docs_total,
        "chunks_total": chunks_total,
        "points_total": points_total,
        "parse_wall_s": max(parse_list) if parse_list else 0.0,
        "embed_wall_s": max(embed_list) if embed_list else 0.0,
        "upsert_wall_s": max(upsert_list) if upsert_list else 0.0,
        "total_wall_s": total_wall_s,
        "parse_sum_s": sum(parse_list),
        "embed_sum_s": sum(embed_list),
        "upsert_sum_s": sum(upsert_list),
        "checksum": checksum,
        "ray_address": cfg.ray_address,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def run_once(cfg: BenchmarkConfig) -> Dict[str, object]:
    if cfg.mode not in {"baseline", "ray"}:
        raise ValueError("mode must be baseline or ray")
    if cfg.dataset_size <= 0:
        raise ValueError("dataset_size must be > 0")
    if cfg.workers <= 0:
        raise ValueError("workers must be > 0")
    if cfg.batch_docs <= 0:
        raise ValueError("batch_docs must be > 0")
    if cfg.embedding_dim <= 0:
        raise ValueError("embedding_dim must be > 0")
    if cfg.embedding_intensity <= 0:
        raise ValueError("embedding_intensity must be > 0")
    if cfg.template_pool_size <= 0:
        raise ValueError("template_pool_size must be > 0")
    if cfg.max_chunks_per_doc <= 0:
        raise ValueError("max_chunks_per_doc must be > 0")
    if cfg.embedding_backend not in {"synthetic", "pretrained"}:
        raise ValueError("embedding_backend must be synthetic or pretrained")
    if cfg.vector_backend not in {"mock", "qdrant"}:
        raise ValueError("vector_backend must be mock or qdrant")

    if cfg.mode == "baseline":
        backend, shards, total = run_baseline(cfg)
    else:
        backend, shards, total = run_ray(cfg)

    return aggregate_run(cfg, backend=backend, shard_results=shards, total_wall_s=total)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["baseline", "ray"], required=True)
    p.add_argument("--dataset-size", type=int, required=True)
    p.add_argument("--workers", type=int, required=True)
    p.add_argument("--repeat-id", type=int, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch-docs", type=int, default=20000)
    p.add_argument("--embedding-backend", choices=["synthetic", "pretrained"], default="synthetic")
    p.add_argument("--embedding-model", type=str, default="BAAI/bge-small-en-v1.5")
    p.add_argument("--embedding-dim", type=int, default=32)
    p.add_argument("--embedding-intensity", type=int, default=1)
    p.add_argument("--template-pool-size", type=int, default=2048)
    p.add_argument("--max-chunks-per-doc", type=int, default=2)
    p.add_argument("--vector-backend", choices=["mock", "qdrant"], default="mock")
    p.add_argument("--qdrant-location", type=str, default=":memory:")
    p.add_argument("--ray-address", type=str, default=os.getenv("RAY_ADDRESS", ""))
    p.add_argument("--output", type=Path, default=RAW_DEFAULT)
    p.add_argument("--no-write", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = BenchmarkConfig(
        mode=args.mode,
        dataset_size=args.dataset_size,
        workers=args.workers,
        repeat_id=args.repeat_id,
        seed=args.seed,
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

    row = run_once(cfg)
    if not args.no_write:
        append_row(args.output, row)

    print(
        "[INDEXING] "
        f"mode={row['mode']} dataset={row['dataset_size']} workers={row['workers']} repeat={row['repeat_id']} "
        f"backend={row['backend']} embed={row['embedding_backend']} vector={row['vector_backend']} "
        f"total={float(row['total_wall_s']):.3f}s parse={float(row['parse_wall_s']):.3f}s "
        f"embed_s={float(row['embed_wall_s']):.3f}s upsert={float(row['upsert_wall_s']):.3f}s"
    )
    print(f"RUN_ID={row['run_id']}")
    print("OK")


if __name__ == "__main__":
    main()
