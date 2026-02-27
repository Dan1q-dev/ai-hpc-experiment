#!/bin/bash
#SBATCH --job-name=ray_head
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=ray_head_%j.out
#SBATCH --error=ray_head_%j.err
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1

set -euo pipefail

module load apptainer

SIF_IMAGE="${SIF_IMAGE:-/path/to/scalable-agentic-rag.sif}"
SHARED_DIR="${SHARED_DIR:-$PWD/slurm_shared}"
RAY_HEAD_PORT="${RAY_HEAD_PORT:-6379}"
RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-8265}"

mkdir -p "${SHARED_DIR}"
HEAD_IP="$(hostname -I | awk '{print $1}')"

cat > "${SHARED_DIR}/ray_head_info.env" <<ENV
RAY_HEAD_IP=${HEAD_IP}
RAY_HEAD_PORT=${RAY_HEAD_PORT}
RAY_DASHBOARD_PORT=${RAY_DASHBOARD_PORT}
ENV

echo "[RAY_HEAD] node=${HOSTNAME} ip=${HEAD_IP}"
echo "[RAY_HEAD] info file: ${SHARED_DIR}/ray_head_info.env"

apptainer exec --nv "${SIF_IMAGE}" bash -lc "
ray start \
  --head \
  --node-ip-address=${HEAD_IP} \
  --port=${RAY_HEAD_PORT} \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=${RAY_DASHBOARD_PORT} \
  --num-cpus=${SLURM_CPUS_PER_TASK} \
  --block
"
