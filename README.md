# AI-HPC Experiment (Scenario V: Autoscaling)

## Goal
Quantitatively evaluate benefits of Ray-based orchestration for agent workloads
against a sequential baseline under dynamic growth of task count.

## Experiment Scope
- Scenario V only: autoscaling stress from 10 to 1000 tasks.
- Modes: `baseline` and `ray`.
- Repeats per point: 5.
- Same synthetic compute workload for both modes.

## KPIs
- Time-to-Solution (TTS).
- Speedup (`TTS_baseline / TTS_ray`) for the same task count.
- Scheduling overhead (p50 / p95).
- Resource utilization (CPU / GPU average).
- Failure recovery time when failure injection is enabled.

## Observability
- Prometheus scrape endpoint is exposed by runner on `127.0.0.1:9108/metrics`.
- Ray Dashboard is enabled in ray mode by default.
- Actor state snapshots are saved to `results/raw/actor_states.csv`.
- Custom timing decorators live in `src/metrics/instrumentation.py`.

## Repo Layout
- `docs/` protocol and metric specification.
- `scripts/` orchestration scripts.
- `src/runner/` single experiment run logic.
- `src/metrics/` validation helpers.
- `src/analysis/` aggregation and plotting.
- `results/` generated experiment outputs.

## Quick Start
Recommended Python for full `ray` flow: `3.10-3.12`.
If `ray` is unavailable (for example on Python 3.13), `--mode ray` falls back
to local process-based parallel execution so the pipeline can still run end-to-end.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip -r requirements.txt
make all
```

Windows PowerShell:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -U pip -r requirements.txt
python src/runner/run_single.py --mode baseline --n-tasks 10 --repeat-id 1 --seed 42
python src/runner/run_single.py --mode ray --n-tasks 10 --repeat-id 1 --seed 42 --n-agents 4
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

## Full Experiment
```bash
bash scripts/run_experiments.sh full
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

## Scenario 1: Indexing Scalability (Ray Data/Indexing Pipeline)
Target matrix:
- Dataset sizes: `100000, 1000000, 10000000`
- Ray workers: `1,2,4,8`
- Repeats: `5`
- Control group: `baseline` (workers=1)

Run smoke (quick verification):
```bash
bash scripts/run_indexing_scenario.sh smoke
# or: make scenario1-smoke
```

Run full matrix:
```bash
bash scripts/run_indexing_scenario.sh full
# or: make scenario1-full
```

Strict stack smoke (pretrained embeddings + Qdrant vector DB):
```bash
bash scripts/run_indexing_scenario_strict.sh smoke
# or: make scenario1-strict-smoke
```

Strict stack full matrix (CPU-only hosts: long run):
```bash
bash scripts/run_indexing_scenario_strict.sh full
# or: make scenario1-strict-full
```

Strict stack defaults:
- embedding model: `BAAI/bge-small-en-v1.5` (FastEmbed ONNX)
- vector DB: `qdrant` (`QDRANT_LOCATION=:memory:` by default)

Containerized Scenario 1 run:
```bash
bash scripts/docker_run.sh scenario1-smoke
bash scripts/docker_run.sh scenario1-full
```

Scenario 1 outputs:
- Raw runs: `results/scenario1/raw/indexing_raw.csv`
- Aggregated: `results/scenario1/aggregated/indexing_aggregated.csv`
- Speedup: `results/scenario1/aggregated/indexing_speedup.csv`
- Stage breakdown: `results/scenario1/aggregated/indexing_stage_breakdown.csv`
- Figures: `results/scenario1/figures/*.png`

Slurm templates for Ray cluster bootstrap:
- `slurm/ray_head.sh`
- `slurm/ray_worker.sh`
- `slurm/submit_cluster.sh`

## Local Slurm (Docker, no sudo)
This repository includes a reproducible single-node Slurm profile that runs in Docker.
Use it when host-level `slurmctld/slurmd` setup is unavailable.

Bring Slurm up:
```bash
bash scripts/slurm_local_up.sh
# or: make slurm-up
```

Check cluster:
```bash
bash scripts/slurm_local_status.sh
# or: make slurm-status
```

Run smoke parallel job (`sbatch` + `srun -n 4`):
```bash
bash scripts/slurm_local_submit_test.sh
# or: make slurm-test
```

Run arbitrary Slurm command inside container:
```bash
bash scripts/slurm_local_exec.sh "scontrol ping && sinfo -N"
```

Stop/remove local Slurm:
```bash
bash scripts/slurm_local_down.sh
# or: make slurm-down
```

## Containerized Run (Docker)
Container image defaults:
- Python image: `python:3.9-slim-bookworm`
- Ray version: `2.46.0` (aligned with KubeRay cluster used in this project)

Build image:
```bash
bash scripts/docker_run.sh build
# or: make docker-build
```

Smoke run inside container:
```bash
bash scripts/docker_run.sh smoke
# or: make docker-smoke
```

Full matrix + validation + analysis + plots inside container:
```bash
bash scripts/docker_run.sh full
# or: make docker-full
```

Open presentation page from container:
```bash
bash scripts/docker_run.sh present
# http://localhost:8031/presentation/
```

If port `8031` is busy:
```bash
PRESENTATION_PORT=8041 bash scripts/docker_run.sh present
# http://localhost:8041/presentation/
```

Stop presentation container:
```bash
bash scripts/docker_run.sh stop-present
```

Optional: run against external KubeRay/Prometheus (passed into container):
```bash
export RAY_ADDRESS=auto
export RAY_AGENTS=2
export PROMETHEUS_URL=http://prometheus-server.monitoring.svc.cluster.local
export K8S_NAMESPACE=default
export K8S_POD_REGEX='autoscale-exp-.*'
bash scripts/docker_run.sh full
```

Notes:
- `scripts/docker_run.sh` uses `--network host` for experiment runs to simplify
  access to local K8s/KubeRay networking on Ubuntu.
- Result files are written to host-mounted `results/`.
- First image build can be long because Ray + scientific stack is downloaded once.
- If Docker BuildKit/buildx is unavailable, script automatically falls back to
  legacy `docker build`.
- You can override image build args:
  `PYTHON_IMAGE=python:3.10-slim-bookworm RAY_VERSION=2.46.0 bash scripts/docker_run.sh build`

Optional: run the containerized full experiment as a Kubernetes Job:
```bash
# Build local image first
bash scripts/docker_run.sh build

# (k3s) import image into containerd runtime used by cluster
docker save ai-hpc-experiment:local | sudo k3s ctr images import -

# Ensure PVC exists (or edit configs/experiment-job.yaml)
kubectl get pvc ai-hpc-results-pvc -n default

# Run job
kubectl apply -f configs/experiment-job.yaml
kubectl logs -f job/ai-hpc-full-run -n default
```

Full experiment against an existing KubeRay cluster (ray mode on cluster, baseline local):
```bash
export RAY_ADDRESS=auto
export RAY_AGENTS=2
export PROMETHEUS_URL=http://prometheus-server.monitoring.svc.cluster.local
export K8S_NAMESPACE=default
export K8S_POD_REGEX='raycluster-autoscaler-.*'
bash scripts/run_experiments.sh full
```

Windows PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_experiments.ps1 -Mode full
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

Run protocol and KPI definitions are documented in:
- `docs/EXPERIMENT_PROTOCOL.md`
- `docs/METRICS_SPEC.md`
- `docs/SCENARIO1_INDEXING.md`

Optional K8s Prometheus pull example:
```bash
python src/runner/run_single.py \
  --mode ray \
  --n-agents 4 \
  --n-tasks 100 \
  --repeat-id 1 \
  --prometheus-url http://prometheus.monitoring.svc:9090 \
  --k8s-namespace default \
  --k8s-pod-regex "ray-.*"
```
