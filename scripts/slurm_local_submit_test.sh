#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${SLURM_CONTAINER:-ai-hpc-slurm-local}"

docker exec "${CONTAINER}" bash -lc '
cat > /tmp/slurm_smoke.sbatch <<"JOB"
#!/bin/bash
#SBATCH --job-name=slurm-smoke
#SBATCH --partition=debug
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --output=/tmp/slurm-smoke-%j.out

hostname
scontrol show hostname "$SLURM_JOB_NODELIST"
srun -n 4 bash -lc "echo rank=\$SLURM_PROCID local=\$SLURM_LOCALID host=\$(hostname)"
JOB

JOB_ID=$(sbatch --parsable /tmp/slurm_smoke.sbatch)
echo "[SLURM] submitted job ${JOB_ID}"
for _ in $(seq 1 30); do
  STATE=$(squeue -h -j "${JOB_ID}" -o "%T" || true)
  if [[ -z "${STATE}" ]]; then
    break
  fi
  sleep 1
done

echo "[SLURM] job output"
cat "/tmp/slurm-smoke-${JOB_ID}.out"
'
