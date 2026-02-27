#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
PYTHON_BIN="${PYTHON_BIN:-python}"
OUT_CSV="results/raw/raw_results.csv"
RAY_ADDRESS="${RAY_ADDRESS:-}"
PROMETHEUS_URL="${PROMETHEUS_URL:-}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"
K8S_POD_REGEX="${K8S_POD_REGEX:-raycluster-autoscaler-.*}"
RAY_AGENTS="${RAY_AGENTS:-4}"
export RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0

mkdir -p results/raw

# Prefer python3 automatically on systems where "python" is unavailable.
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  if [[ "${PYTHON_BIN}" == "python" ]] && command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
    echo "[CFG] python not found; fallback to ${PYTHON_BIN}"
  else
    echo "[ERR] Python interpreter '${PYTHON_BIN}' not found." >&2
    echo "[HINT] Set PYTHON_BIN explicitly, e.g. PYTHON_BIN=python3.12" >&2
    exit 1
  fi
fi

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

RAY_CONN_ARGS=()
if [[ -n "${RAY_ADDRESS}" ]]; then
  RAY_CONN_ARGS+=(--ray-address "${RAY_ADDRESS}")
fi

RAY_PROM_ARGS=()
if [[ -n "${PROMETHEUS_URL}" ]]; then
  RAY_PROM_ARGS+=(--prometheus-url "${PROMETHEUS_URL}" --k8s-namespace "${K8S_NAMESPACE}" --k8s-pod-regex "${K8S_POD_REGEX}")
fi

if [[ -n "${RAY_ADDRESS}" ]]; then
  echo "[CFG] ray mode will use external cluster: ${RAY_ADDRESS}"
fi
if [[ -n "${PROMETHEUS_URL}" ]]; then
  echo "[CFG] ray mode will query K8s Prometheus: ${PROMETHEUS_URL} (ns=${K8S_NAMESPACE}, pod_regex=${K8S_POD_REGEX})"
fi
echo "[CFG] ray mode agents: ${RAY_AGENTS}"

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
      --n-agents "${RAY_AGENTS}" \
      --n-tasks "${N_TASKS}" \
      --repeat-id "${REPEAT_ID}" \
      --seed "${SEED}" \
      --units "${UNITS}" \
      --failure-injection-threshold 200 \
      --max-retries 1 \
      "${RAY_CONN_ARGS[@]}" \
      "${RAY_PROM_ARGS[@]}"
  done
done

echo "[RUN] complete: ${OUT_CSV}"
