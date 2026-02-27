#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${SLURM_CONTAINER:-ai-hpc-slurm-local}"

docker ps --filter "name=^${CONTAINER}$"
docker exec "${CONTAINER}" bash -lc "sinfo && squeue"
