ARG PYTHON_IMAGE=python:3.9-slim-bookworm
FROM ${PYTHON_IMAGE}

ARG RAY_VERSION=2.46.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && grep -v '^ray' requirements.txt > /tmp/requirements-no-ray.txt \
    && pip install -r /tmp/requirements-no-ray.txt \
    && pip install "ray[default]==${RAY_VERSION}"

COPY . .

RUN mkdir -p results/raw results/aggregated results/figures

CMD ["bash", "-lc", "bash scripts/run_experiments.sh smoke && python3 src/metrics/validate.py && python3 src/analysis/analyze.py && python3 src/analysis/plot_results.py"]
