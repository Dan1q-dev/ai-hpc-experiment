# SCENARIO 1: Indexing Scalability

## Objective
Measure how indexing throughput scales with Ray when workload grows from
`100K` to `10M` documents and worker count changes from `1` to `8`.

## Matrix
- `dataset_size`: `100000, 1000000, 10000000`
- `workers`: `1,2,4,8`
- `repeats`: `5`
- `modes`: `baseline` (workers=1), `ray` (workers in matrix)

Expected runs:
- baseline: `3 dataset sizes x 5 repeats = 15`
- ray: `3 dataset sizes x 4 workers x 5 repeats = 60`
- total: `75`

## Stages and KPIs
Per run, pipeline tracks:
- `parse_wall_s`: parse/chunk stage wall time
- `embed_wall_s`: embedding generation stage wall time
- `upsert_wall_s`: vector upsert stage wall time
- `total_wall_s`: end-to-end wall time
- `speedup`: `baseline_total_median / ray_total_median`
- `efficiency`: `speedup / workers`

## Commands
Smoke:
```bash
bash scripts/run_indexing_scenario.sh smoke
```

Full:
```bash
bash scripts/run_indexing_scenario.sh full
```

Strict stack smoke (`pretrained + qdrant`):
```bash
bash scripts/run_indexing_scenario_strict.sh smoke
```

Strict stack full:
```bash
bash scripts/run_indexing_scenario_strict.sh full
```

Slurm + Apptainer templates (if Slurm is available):
```bash
# configure env vars first
export SIF_IMAGE=/path/to/scalable-agentic-rag.sif
export SHARED_DIR=/shared/path/ray

# submit head + N worker nodes
bash slurm/submit_cluster.sh 4
```

Files:
- `slurm/ray_head.sh`
- `slurm/ray_worker.sh`
- `slurm/submit_cluster.sh`

## Artifacts
- Raw: `results/scenario1/raw/indexing_raw.csv`
- Aggregated: `results/scenario1/aggregated/indexing_aggregated.csv`
- Speedup: `results/scenario1/aggregated/indexing_speedup.csv`
- Stage breakdown: `results/scenario1/aggregated/indexing_stage_breakdown.csv`
- Figures: `results/scenario1/figures/indexing_total_vs_workers.png`
- Figures: `results/scenario1/figures/indexing_speedup_vs_workers.png`
- Figures: `results/scenario1/figures/indexing_stage_breakdown.png`

## Notes
- `ray` mode uses native Ray when available, otherwise falls back to
  process-pool backend and marks backend in CSV.
- `embedding_backend=pretrained` uses FastEmbed ONNX model
  (`embedding_model`) with deterministic text-template embedding cache.
- `vector_backend=qdrant` uses Qdrant upsert path (`qdrant_location` in CSV).
- For external cluster execution, set `RAY_ADDRESS=auto` before run.
