#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-smoke}"

export EMBEDDING_BACKEND="${EMBEDDING_BACKEND:-pretrained}"
export EMBEDDING_MODEL="${EMBEDDING_MODEL:-BAAI/bge-small-en-v1.5}"
export VECTOR_BACKEND="${VECTOR_BACKEND:-qdrant}"
export TEMPLATE_POOL_SIZE="${TEMPLATE_POOL_SIZE:-2048}"
export QDRANT_LOCATION="${QDRANT_LOCATION:-:memory:}"

bash scripts/run_indexing_scenario.sh "${MODE}"
