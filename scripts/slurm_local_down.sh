#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${SLURM_CONTAINER:-ai-hpc-slurm-local}"
if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER}"; then
  docker rm -f "${CONTAINER}" >/dev/null
  echo "[SLURM] container removed: ${CONTAINER}"
else
  echo "[SLURM] container not found: ${CONTAINER}"
fi
