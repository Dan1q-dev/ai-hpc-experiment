#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi
RAY_ADDRESS="${RAY_ADDRESS:-}"
EMBEDDING_BACKEND="${EMBEDDING_BACKEND:-synthetic}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-BAAI/bge-small-en-v1.5}"
VECTOR_BACKEND="${VECTOR_BACKEND:-mock}"
TEMPLATE_POOL_SIZE="${TEMPLATE_POOL_SIZE:-2048}"
QDRANT_LOCATION="${QDRANT_LOCATION:-:memory:}"

OUT_RAW="results/scenario1/raw/indexing_raw.csv"
OUT_AGG_DIR="results/scenario1/aggregated"
OUT_FIG_DIR="results/scenario1/figures"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "[ERR] Python interpreter not found: ${PYTHON_BIN}" >&2
  exit 1
fi

IMPORTS="import numpy, pandas, matplotlib"
if [[ "${EMBEDDING_BACKEND}" == "pretrained" ]]; then
  IMPORTS="${IMPORTS}; import fastembed"
fi
if [[ "${VECTOR_BACKEND}" == "qdrant" ]]; then
  IMPORTS="${IMPORTS}; import qdrant_client"
fi

if ! "${PYTHON_BIN}" -c "${IMPORTS}" >/dev/null 2>&1; then
  echo "[ERR] Missing required Python packages for scenario1 in ${PYTHON_BIN}." >&2
  echo "[HINT] Activate venv and install requirements: python3.12 -m venv .venv && source .venv/bin/activate && pip install -U pip -r requirements.txt" >&2
  exit 1
fi

if [[ "${MODE}" == "smoke" ]]; then
  DATASET_SIZES="10000,50000"
  WORKERS="1,2"
  REPEATS=2
  BATCH_DOCS="10000"
  EMB_DIM="24"
  EMB_INTENSITY="1"
  MAX_CHUNKS="2"
else
  DATASET_SIZES="100000,1000000,10000000"
  WORKERS="1,2,4,8"
  REPEATS=5
  BATCH_DOCS="20000"
  EMB_DIM="32"
  EMB_INTENSITY="1"
  MAX_CHUNKS="2"
fi

mkdir -p "$(dirname "${OUT_RAW}")" "${OUT_AGG_DIR}" "${OUT_FIG_DIR}"
rm -f "${OUT_RAW}"
rm -f "${OUT_AGG_DIR}"/*.csv
rm -f "${OUT_FIG_DIR}"/*.png

echo "[SCENARIO1] mode=${MODE} datasets=${DATASET_SIZES} workers=${WORKERS} repeats=${REPEATS}"
echo "[SCENARIO1] embedding_backend=${EMBEDDING_BACKEND} embedding_model=${EMBEDDING_MODEL}"
echo "[SCENARIO1] vector_backend=${VECTOR_BACKEND} qdrant_location=${QDRANT_LOCATION}"

"${PYTHON_BIN}" src/scenarios/run_indexing_matrix.py \
  --dataset-sizes "${DATASET_SIZES}" \
  --workers "${WORKERS}" \
  --repeats "${REPEATS}" \
  --batch-docs "${BATCH_DOCS}" \
  --embedding-backend "${EMBEDDING_BACKEND}" \
  --embedding-model "${EMBEDDING_MODEL}" \
  --embedding-dim "${EMB_DIM}" \
  --embedding-intensity "${EMB_INTENSITY}" \
  --template-pool-size "${TEMPLATE_POOL_SIZE}" \
  --max-chunks-per-doc "${MAX_CHUNKS}" \
  --vector-backend "${VECTOR_BACKEND}" \
  --qdrant-location "${QDRANT_LOCATION}" \
  --ray-address "${RAY_ADDRESS}" \
  --clean \
  --output "${OUT_RAW}"

"${PYTHON_BIN}" src/scenarios/validate_indexing.py \
  --input "${OUT_RAW}" \
  --dataset-sizes "${DATASET_SIZES}" \
  --workers "${WORKERS}" \
  --repeats "${REPEATS}"

"${PYTHON_BIN}" src/scenarios/analyze_indexing.py \
  --input "${OUT_RAW}" \
  --out-agg "${OUT_AGG_DIR}/indexing_aggregated.csv" \
  --out-speedup "${OUT_AGG_DIR}/indexing_speedup.csv" \
  --out-stage "${OUT_AGG_DIR}/indexing_stage_breakdown.csv"

"${PYTHON_BIN}" src/scenarios/plot_indexing.py \
  --agg "${OUT_AGG_DIR}/indexing_aggregated.csv" \
  --speedup "${OUT_AGG_DIR}/indexing_speedup.csv" \
  --fig-dir "${OUT_FIG_DIR}"

echo "[SCENARIO1] complete"
echo "[SCENARIO1] raw: ${OUT_RAW}"
echo "[SCENARIO1] aggregated: ${OUT_AGG_DIR}"
echo "[SCENARIO1] figures: ${OUT_FIG_DIR}"
