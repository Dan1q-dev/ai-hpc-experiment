#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${SLURM_IMAGE:-ai-hpc-slurm-local:latest}"
CONTAINER="${SLURM_CONTAINER:-ai-hpc-slurm-local}"

echo "[SLURM] building image ${IMAGE}"
docker build -t "${IMAGE}" "${ROOT_DIR}/slurm/docker"

if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER}"; then
  echo "[SLURM] removing existing container ${CONTAINER}"
  docker rm -f "${CONTAINER}" >/dev/null
fi

echo "[SLURM] starting container ${CONTAINER}"
docker run -d \
  --name "${CONTAINER}" \
  --hostname slurm-local \
  --privileged \
  -v "${ROOT_DIR}:/workspace" \
  -w /workspace \
  -p 6817:6817 \
  -p 6818:6818 \
  "${IMAGE}" >/dev/null

sleep 3

echo "[SLURM] cluster status"
docker exec "${CONTAINER}" bash -lc "sinfo"
