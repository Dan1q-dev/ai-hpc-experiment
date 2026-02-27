#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server


@dataclass
class PrometheusConfig:
    enabled: bool = True
    port: int = 9108


class PrometheusRunMetrics:
    def __init__(self, config: PrometheusConfig):
        self.enabled = config.enabled
        if self.enabled:
            try:
                start_http_server(config.port)
            except OSError:
                self.enabled = False

        self._runs_total = Counter(
            "ai_hpc_runs_total",
            "Total number of completed runs",
            ["mode"],
        )
        self._task_duration = Histogram(
            "ai_hpc_task_duration_seconds",
            "Task duration in seconds",
            ["mode"],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
        )
        self._sched_overhead = Histogram(
            "ai_hpc_sched_overhead_ms",
            "Scheduling overhead in milliseconds",
            ["mode"],
            buckets=(0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 20000),
        )
        self._task_failures = Counter(
            "ai_hpc_task_failures_total",
            "Number of failed tasks after retries",
            ["mode"],
        )
        self._recovery_time = Histogram(
            "ai_hpc_recovery_time_seconds",
            "Failure recovery time in seconds",
            ["mode"],
            buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
        )
        self._cpu_gauge = Gauge(
            "ai_hpc_cpu_avg_pct",
            "Average CPU utilization percent",
            ["mode"],
        )
        self._gpu_gauge = Gauge(
            "ai_hpc_gpu_avg_pct",
            "Average GPU utilization percent",
            ["mode"],
        )

    def record_task(self, row: dict) -> None:
        if not self.enabled:
            return
        mode = row["mode"]
        self._task_duration.labels(mode=mode).observe(float(row.get("task_duration_s", 0.0)))
        self._sched_overhead.labels(mode=mode).observe(float(row.get("sched_overhead_ms", 0.0)))
        self._cpu_gauge.labels(mode=mode).set(float(row.get("cpu_avg_pct", 0.0)))
        self._gpu_gauge.labels(mode=mode).set(float(row.get("gpu_avg_pct", 0.0)))
        if int(row.get("failed", 0)) == 1:
            self._task_failures.labels(mode=mode).inc()
        rec = float(row.get("recovery_time_s", 0.0))
        if rec > 0:
            self._recovery_time.labels(mode=mode).observe(rec)

    def record_run(self, mode: str) -> None:
        if not self.enabled:
            return
        self._runs_total.labels(mode=mode).inc()


def _query_instant(prometheus_url: str, query: str, timeout_s: float = 4.0) -> Optional[float]:
    url = f"{prometheus_url.rstrip('/')}/api/v1/query"
    try:
        res = requests.get(url, params={"query": query}, timeout=timeout_s)
        res.raise_for_status()
        payload = res.json()
        if payload.get("status") != "success":
            return None
        result = payload.get("data", {}).get("result", [])
        if not result:
            return None
        value = result[0].get("value", [None, None])[1]
        return float(value)
    except Exception:
        return None


def query_k8s_utilization(
    prometheus_url: str,
    namespace: str,
    pod_regex: str,
) -> Tuple[Optional[float], Optional[float]]:
    cpu_query = (
        f'avg(rate(container_cpu_usage_seconds_total{{namespace="{namespace}",pod=~"{pod_regex}",container!=""}}[5m])) * 100'
    )
    gpu_query = (
        f'avg(container_accelerator_duty_cycle{{namespace="{namespace}",pod=~"{pod_regex}"}})'
    )
    cpu = _query_instant(prometheus_url, cpu_query)
    if cpu is None:
        # Fallback for clusters where cAdvisor pod metrics are not scraped.
        cpu = _query_instant(
            prometheus_url,
            '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
        )
    gpu = _query_instant(prometheus_url, gpu_query)
    return cpu, gpu
