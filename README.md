# AI-HPC Experiment (Ray + KubeRay + Slurm)

## Goal
Evaluate scalability and orchestration efficiency for agent-based AI workloads when scaling from 1 to 16 agents.

## Team Roles
- A (Infra): environment + run scripts + reproducibility
- B (Metrics): telemetry + raw data collection
- C (Analysis): aggregation + plots + report

## Project Structure
- `docs/` — protocol and metric specs
- `scripts/` — orchestration scripts
- `src/runner/` — run logic
- `src/metrics/` — validation and metric helpers
- `src/analysis/` — analysis/plots
- `results/` — raw, aggregated, figures

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip -r requirements.txt
bash scripts/run_experiments.sh
python src/metrics/validate.py
python src/analysis/analyze.py

nano docs/EXPERIMENT_PROTOCOL.md
# Experiment Protocol (Scenario V: Autoscaling)

## Goal
Quantitatively evaluate Ray/KubeRay autoscaling benefits for agent workloads vs baseline (no Ray).

## Scenarios
We implement Scenario V (Autoscaling): increase agents from 1 to 16.

## Parameters (Fixed)
- N agents: 1, 2, 4, 8, 12, 16
- Repeats per point: 5
- Modes: ray, baseline
- Seed: 42
- Workload: fixed synthetic compute task (same size across runs)

## KPIs
- TTS (Time-to-Solution)
- Speedup
- Efficiency
- Scheduling Overhead (p50/p95)
- CPU/GPU utilization (optional if available)
- Failure recovery time (optional injection at N=8 and N=16)

## Data outputs
- Raw per-task metrics: results/raw/raw_results.csv
- Aggregated metrics: results/aggregated/aggregated_results.csv
- Figures: results/figures/

