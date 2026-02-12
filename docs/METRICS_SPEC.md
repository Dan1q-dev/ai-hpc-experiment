# Metrics Spec (raw_results.csv)

CSV path: results/raw/raw_results.csv

## Columns
- run_id: unique run identifier (string)
- mode: "ray" | "baseline"
- n_agents: number of agents (int)
- repeat_id: repeat index (int)
- seed: RNG seed (int)
- task_id: unique task id inside run (string/int)
- submit_ts: perf_counter timestamp when task submitted (float, seconds)
- start_ts: perf_counter timestamp when task started (float, seconds)
- end_ts: perf_counter timestamp when task finished (float, seconds)
- task_duration_s: end_ts - start_ts (float, seconds)
- sched_overhead_ms: (start_ts - submit_ts) * 1000 (float, ms)
- cpu_avg_pct: optional, average CPU utilization during task/run (float)
- gpu_avg_pct: optional, average GPU utilization during task/run (float)
- failed: 0/1 whether task failed (int)
- recovery_time_s: optional, failure recovery time if injected (float)

## Run-level TTS
TTS(run_id) = max(end_ts) - min(submit_ts)
