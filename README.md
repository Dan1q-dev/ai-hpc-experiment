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

