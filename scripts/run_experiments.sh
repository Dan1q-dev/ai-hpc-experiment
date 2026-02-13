#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
PYTHON_BIN="${PYTHON_BIN:-python}"
OUT_CSV="results/raw/raw_results.csv"

mkdir -p results/raw

if ! "${PYTHON_BIN}" -c "import ray" >/dev/null 2>&1; then
  echo "[WARN] ray is not available. 'ray' mode will run with local process fallback."
fi

if [[ "${MODE}" == "smoke" ]]; then
  TASK_POINTS=(10 50)
  REPEATS=2
  UNITS=5000
else
  TASK_POINTS=(10 20 50 100 200 500 1000)
  REPEATS=5
  UNITS=20000
fi

if [[ -f "${OUT_CSV}" ]]; then
  rm -f "${OUT_CSV}"
fi

for N_TASKS in "${TASK_POINTS[@]}"; do
  for REPEAT_ID in $(seq 1 "${REPEATS}"); do
    SEED=$((42 + REPEAT_ID))

    echo "[RUN] baseline n_tasks=${N_TASKS} repeat=${REPEAT_ID}"
    "${PYTHON_BIN}" src/runner/run_single.py \
      --mode baseline \
      --n-agents 1 \
      --n-tasks "${N_TASKS}" \
      --repeat-id "${REPEAT_ID}" \
      --seed "${SEED}" \
      --units "${UNITS}" \
      --failure-injection-threshold 200 \
      --max-retries 1

    echo "[RUN] ray n_tasks=${N_TASKS} repeat=${REPEAT_ID}"
    "${PYTHON_BIN}" src/runner/run_single.py \
      --mode ray \
      --n-agents 4 \
      --n-tasks "${N_TASKS}" \
      --repeat-id "${REPEAT_ID}" \
      --seed "${SEED}" \
      --units "${UNITS}" \
      --failure-injection-threshold 200 \
      --max-retries 1
  done
done

echo "[RUN] complete: ${OUT_CSV}"
