#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${SLURM_CONTAINER:-ai-hpc-slurm-local}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 '<command>'" >&2
  exit 1
fi

docker exec "${CONTAINER}" bash -lc "$*"
