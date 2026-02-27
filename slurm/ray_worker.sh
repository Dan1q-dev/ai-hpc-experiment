#!/bin/bash
#SBATCH --job-name=ray_worker
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=04:00:00
#SBATCH --output=ray_worker_%j.out
#SBATCH --error=ray_worker_%j.err
#SBATCH --partition=gpu
#SBATCH --gres=gpu:4

set -euo pipefail

module load apptainer

SIF_IMAGE="${SIF_IMAGE:-/path/to/scalable-agentic-rag.sif}"
SHARED_DIR="${SHARED_DIR:-$PWD/slurm_shared}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-600}"

INFO_FILE="${SHARED_DIR}/ray_head_info.env"
ELAPSED=0
while [[ ! -f "${INFO_FILE}" ]]; do
  sleep 5
  ELAPSED=$((ELAPSED + 5))
  if [[ "${ELAPSED}" -ge "${WAIT_TIMEOUT_SEC}" ]]; then
    echo "[RAY_WORKER] timeout waiting for ${INFO_FILE}" >&2
    exit 1
  fi
done

# shellcheck disable=SC1090
source "${INFO_FILE}"

echo "[RAY_WORKER] node=${HOSTNAME} joining ${RAY_HEAD_IP}:${RAY_HEAD_PORT}"

apptainer exec --nv "${SIF_IMAGE}" bash -lc "
ray start \
  --address=${RAY_HEAD_IP}:${RAY_HEAD_PORT} \
  --num-cpus=${SLURM_CPUS_PER_TASK} \
  --num-gpus=${SLURM_GPUS_ON_NODE:-0} \
  --block
"
