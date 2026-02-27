#!/bin/bash
set -euo pipefail

WORKER_NODES="${1:-2}"

echo "[SLURM] submit head"
HEAD_JOB_ID="$(sbatch --parsable slurm/ray_head.sh)"
echo "[SLURM] head job: ${HEAD_JOB_ID}"

echo "[SLURM] submit workers: ${WORKER_NODES} nodes"
WORKER_JOB_ID="$(sbatch --parsable --dependency=after:${HEAD_JOB_ID} --nodes=${WORKER_NODES} slurm/ray_worker.sh)"
echo "[SLURM] worker job: ${WORKER_JOB_ID}"

echo "[SLURM] monitor: squeue -j ${HEAD_JOB_ID},${WORKER_JOB_ID}"
