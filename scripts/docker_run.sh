#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-help}"
MODE="${MODE:-full}"
IMAGE_NAME="${IMAGE_NAME:-ai-hpc-experiment:local}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UID_VAL="${UID:-$(id -u)}"
GID_VAL="${GID:-$(id -g)}"
PRESENTATION_CONTAINER="${PRESENTATION_CONTAINER:-ai-hpc-presentation}"
PRESENTATION_PORT="${PRESENTATION_PORT:-8031}"

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/docker_run.sh build
  bash scripts/docker_run.sh smoke
  bash scripts/docker_run.sh full
  bash scripts/docker_run.sh scenario1-smoke
  bash scripts/docker_run.sh scenario1-full
  bash scripts/docker_run.sh scenario1-strict-smoke
  bash scripts/docker_run.sh scenario1-strict-full
  bash scripts/docker_run.sh analyze
  bash scripts/docker_run.sh present
  bash scripts/docker_run.sh stop-present
  bash scripts/docker_run.sh shell

Environment overrides (optional):
  IMAGE_NAME
  PYTHON_IMAGE, RAY_VERSION
  PRESENTATION_PORT, PRESENTATION_CONTAINER
  RAY_ADDRESS, RAY_AGENTS, PROMETHEUS_URL, K8S_NAMESPACE, K8S_POD_REGEX
  UID, GID
USAGE
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "[ERR] docker command not found." >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "[ERR] docker daemon is not available. Start Docker and retry." >&2
    exit 1
  fi
}

build_image() {
  require_docker
  local build_args=(
    --build-arg PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.9-slim-bookworm}" \
    --build-arg RAY_VERSION="${RAY_VERSION:-2.46.0}" \
    -t "${IMAGE_NAME}" \
    -f "${PROJECT_ROOT}/Dockerfile" \
    "${PROJECT_ROOT}"
  )

  if DOCKER_BUILDKIT=1 docker build "${build_args[@]}"; then
    return
  fi

  echo "[WARN] BuildKit build failed, fallback to legacy docker build." >&2
  docker build "${build_args[@]}"
}

ensure_image() {
  require_docker
  if ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
    echo "[CFG] image ${IMAGE_NAME} not found; building..."
    build_image
  fi
}

run_experiment_mode() {
  local mode="$1"
  ensure_image
  docker run --rm \
    --network host \
    -u "${UID_VAL}:${GID_VAL}" \
    -v "${PROJECT_ROOT}:/workspace" \
    -w /workspace \
    -e PYTHON_BIN=python3 \
    -e MODE="${mode}" \
    -e RAY_ADDRESS="${RAY_ADDRESS:-}" \
    -e RAY_AGENTS="${RAY_AGENTS:-2}" \
    -e PROMETHEUS_URL="${PROMETHEUS_URL:-}" \
    -e K8S_NAMESPACE="${K8S_NAMESPACE:-default}" \
    -e K8S_POD_REGEX="${K8S_POD_REGEX:-autoscale-exp-.*}" \
    "${IMAGE_NAME}" \
    bash -lc "bash scripts/run_experiments.sh ${mode} && python3 src/metrics/validate.py && python3 src/analysis/analyze.py && python3 src/analysis/plot_results.py"
}

run_analyze() {
  ensure_image
  docker run --rm \
    --network host \
    -u "${UID_VAL}:${GID_VAL}" \
    -v "${PROJECT_ROOT}:/workspace" \
    -w /workspace \
    -e PYTHON_BIN=python3 \
    "${IMAGE_NAME}" \
    bash -lc "python3 src/metrics/validate.py && python3 src/analysis/analyze.py && python3 src/analysis/plot_results.py"
}

run_scenario1() {
  local mode="$1"
  ensure_image
  docker run --rm \
    --network host \
    -u "${UID_VAL}:${GID_VAL}" \
    -v "${PROJECT_ROOT}:/workspace" \
    -w /workspace \
    -e PYTHON_BIN=python3 \
    -e RAY_ADDRESS="${RAY_ADDRESS:-}" \
    -e EMBEDDING_BACKEND="${EMBEDDING_BACKEND:-synthetic}" \
    -e EMBEDDING_MODEL="${EMBEDDING_MODEL:-BAAI/bge-small-en-v1.5}" \
    -e VECTOR_BACKEND="${VECTOR_BACKEND:-mock}" \
    -e TEMPLATE_POOL_SIZE="${TEMPLATE_POOL_SIZE:-2048}" \
    -e QDRANT_LOCATION="${QDRANT_LOCATION:-:memory:}" \
    "${IMAGE_NAME}" \
    bash -lc "bash scripts/run_indexing_scenario.sh ${mode}"
}

start_presentation() {
  ensure_image
  docker rm -f "${PRESENTATION_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${PRESENTATION_CONTAINER}" \
    -u "${UID_VAL}:${GID_VAL}" \
    -v "${PROJECT_ROOT}:/workspace" \
    -w /workspace \
    -p "${PRESENTATION_PORT}:8031" \
    "${IMAGE_NAME}" \
    python3 -m http.server 8031 >/dev/null
  echo "Presentation: http://localhost:${PRESENTATION_PORT}/presentation/"
}

stop_presentation() {
  require_docker
  docker rm -f "${PRESENTATION_CONTAINER}" >/dev/null 2>&1 || true
  echo "Stopped: ${PRESENTATION_CONTAINER}"
}

open_shell() {
  ensure_image
  docker run --rm -it \
    --network host \
    -u "${UID_VAL}:${GID_VAL}" \
    -v "${PROJECT_ROOT}:/workspace" \
    -w /workspace \
    -e PYTHON_BIN=python3 \
    -e RAY_ADDRESS="${RAY_ADDRESS:-}" \
    -e RAY_AGENTS="${RAY_AGENTS:-2}" \
    -e PROMETHEUS_URL="${PROMETHEUS_URL:-}" \
    -e K8S_NAMESPACE="${K8S_NAMESPACE:-default}" \
    -e K8S_POD_REGEX="${K8S_POD_REGEX:-autoscale-exp-.*}" \
    "${IMAGE_NAME}" \
    bash
}

case "${ACTION}" in
  build) build_image ;;
  smoke) run_experiment_mode smoke ;;
  full) run_experiment_mode full ;;
  scenario1-smoke) run_scenario1 smoke ;;
  scenario1-full) run_scenario1 full ;;
  scenario1-strict-smoke) EMBEDDING_BACKEND=pretrained VECTOR_BACKEND=qdrant run_scenario1 smoke ;;
  scenario1-strict-full) EMBEDDING_BACKEND=pretrained VECTOR_BACKEND=qdrant run_scenario1 full ;;
  analyze) run_analyze ;;
  present) start_presentation ;;
  stop-present) stop_presentation ;;
  shell) open_shell ;;
  *)
    usage
    exit 1
    ;;
esac
