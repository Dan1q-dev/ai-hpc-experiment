# EXPERIMENT_PROTOCOL.md

## Scenario
Scenario V: Autoscaling.

## Objective
Measure how Ray improves orchestration efficiency for containerized AI-HPC tasks
when workload size grows from small to large batches.

## Matrix
- Modes: `baseline`, `ray`
- Task counts (`n_tasks`): `10, 20, 50, 100, 200, 500, 1000`
- Repeats: `1..5` for every `(mode, n_tasks)` point
- Seed policy: `seed = 42 + repeat_id` (fixed and reproducible)
- Default parallelism for ray (`n_agents`): `4`
- Failure injection for recovery KPI: enabled for `n_tasks >= 200`

Expected run count:
- `2 modes x 7 task points x 5 repeats = 70 runs`

## Controlled Variables
- Same compute workload function and `units` in both modes.
- Same machine/cluster resources for baseline and ray.
- No code changes between repeated runs.

## Collected KPIs
- `TTS` per run: `max(end_ts) - min(submit_ts)`
- `Speedup(n_tasks)`: median `TTS_baseline / TTS_ray`
- `Scheduling overhead`: p50/p95 of `start_ts - submit_ts`
- `CPU/GPU utilization`: average percentages over run
- `Failure recovery time`: retry completion time after injected failure

## Observability Tooling
- Prometheus exporter from runner: `http://127.0.0.1:9108/metrics` during each run.
- Optional Prometheus API integration for K8s metrics:
  `--prometheus-url <URL> --k8s-namespace <NS> --k8s-pod-regex <REGEX>`.
- Ray Dashboard enabled by default in ray mode (port 8265 unless changed).
- Actor state snapshots are exported to `results/raw/actor_states.csv`.
- Function timing uses custom Python decorators from `src/metrics/instrumentation.py`.

## Execution
Smoke:
```bash
bash scripts/run_experiments.sh smoke
```

Full:
```bash
bash scripts/run_experiments.sh full
```

Post-processing:
```bash
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

## Output Artifacts
- Raw per-task records: `results/raw/raw_results.csv`
- Aggregated KPI table: `results/aggregated/aggregated_results.csv`
- Summary tables: `results/aggregated/*.csv`
- Figures: `results/figures/*.png`
