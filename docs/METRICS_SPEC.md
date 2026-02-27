# METRICS_SPEC.md

## Raw Data Schema (`results/raw/raw_results.csv`)
- `run_id`: unique run identifier.
- `mode`: `baseline` or `ray`.
- `n_agents`: configured worker/actor parallelism (for baseline can be `1`).
- `n_tasks`: total number of tasks in this run (autoscaling axis).
- `repeat_id`: repeat index (`1..5`).
- `seed`: deterministic seed used for run.
- `task_id`: task index inside run.
- `submit_ts`: timestamp right before task submission.
- `start_ts`: timestamp when execution actually starts.
- `end_ts`: timestamp when execution completes (success or final failure).
- `task_duration_s`: `end_ts - start_ts`.
- `sched_overhead_ms`: `(start_ts - submit_ts) * 1000`.
- `cpu_avg_pct`: average CPU utilization sampled for the task window.
- `gpu_avg_pct`: average GPU utilization sampled for the task window (`0` if unavailable).
- `failed`: `0` or `1` after retries.
- `recovery_time_s`: time between first failure and successful retry completion (`0` if no recovery).

## Actor State Schema (`results/raw/actor_states.csv`)
- `run_id`: run identifier.
- `backend`: `ray` or `process_pool`.
- `actor_index`: actor/worker index.
- `tasks_executed`: number of attempts executed by this actor.
- `snapshot_ts`: unix timestamp when snapshot was captured.

## Aggregation Formulas
- `TTS(run_id) = max(end_ts) - min(submit_ts)`.
- `Speedup(n_tasks) = median(TTS_baseline) / median(TTS_ray)` for same `n_tasks`.
- `Efficiency(n_tasks) = Speedup(n_tasks) / n_agents_ray`.
- `Scheduling overhead p50/p95`: quantiles over task-level overheads.

## Reliability Semantics
- Failure injection may force one task to fail on first attempt.
- Recovery is measured only when retry succeeds.
- If task stays failed after max retries, `failed=1` and `recovery_time_s=0`.

## Prometheus
- Runner exports metrics for scrape on `127.0.0.1:9108/metrics` by default.
- Optional K8s Prometheus pull is supported via:
  `--prometheus-url`, `--k8s-namespace`, `--k8s-pod-regex`.
- If K8s Prometheus pull returns values in `ray` mode, those values are written
  into `cpu_avg_pct` / `gpu_avg_pct` for that run's rows.
