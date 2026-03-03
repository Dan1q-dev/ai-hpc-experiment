# AI-HPC Experiment (Сценарий V: Автомасштабирование)

## Цель
Количественно оценить преимущества оркестрации на базе Ray для агентных нагрузок
по сравнению с последовательным baseline при динамическом росте числа задач.

## Область эксперимента
- Только Scenario V: стресс автоскейлинга от 10 до 1000 задач.
- Режимы: `baseline` и `ray`.
- Повторов на точку: 5.
- Одинаковая синтетическая вычислительная нагрузка для обоих режимов.

## KPI
- Time-to-Solution (TTS).
- Ускорение (`TTS_baseline / TTS_ray`) для одинакового числа задач.
- Накладные расходы планирования (p50 / p95).
- Утилизация ресурсов (средние CPU / GPU).
- Время восстановления после отказа, когда включена инъекция отказов.

## Наблюдаемость
- Endpoint Prometheus для scrape публикуется раннером на `127.0.0.1:9108/metrics`.
- Ray Dashboard по умолчанию включен в режиме ray.
- Снимки состояния акторов сохраняются в `results/raw/actor_states.csv`.
- Кастомные декораторы таймингов находятся в `src/metrics/instrumentation.py`.

## Структура репозитория
- `docs/` протокол эксперимента и спецификация метрик.
- `scripts/` скрипты оркестрации.
- `src/runner/` логика одиночного запуска эксперимента.
- `src/metrics/` вспомогательные инструменты валидации.
- `src/analysis/` агрегация и построение графиков.
- `results/` сгенерированные результаты экспериментов.

## Быстрый старт
Рекомендуемая версия Python для полного `ray`-потока: `3.10-3.12`.
Если `ray` недоступен (например, на Python 3.13), `--mode ray` переключается
на локальное параллельное выполнение через процессы, чтобы пайплайн всё равно
выполнялся end-to-end.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip -r requirements.txt
make all
```

Windows PowerShell:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -U pip -r requirements.txt
python src/runner/run_single.py --mode baseline --n-tasks 10 --repeat-id 1 --seed 42
python src/runner/run_single.py --mode ray --n-tasks 10 --repeat-id 1 --seed 42 --n-agents 4
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

## Полный эксперимент
```bash
bash scripts/run_experiments.sh full
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

## Сценарий 1: Масштабируемость индексации (Ray Data/Indexing Pipeline)
Целевая матрица:
- Размеры датасета: `100000, 1000000, 10000000`
- Ray workers: `1,2,4,8`
- Повторы: `5`
- Контрольная группа: `baseline` (workers=1)

Запуск smoke (быстрая проверка):
```bash
bash scripts/run_indexing_scenario.sh smoke
# или: make scenario1-smoke
```

Запуск полной матрицы:
```bash
bash scripts/run_indexing_scenario.sh full
# или: make scenario1-full
```

Smoke для strict-стека (pretrained embeddings + Qdrant vector DB):
```bash
bash scripts/run_indexing_scenario_strict.sh smoke
# или: make scenario1-strict-smoke
```

Полная матрица strict-стека (хосты только с CPU: долгий прогон):
```bash
bash scripts/run_indexing_scenario_strict.sh full
# или: make scenario1-strict-full
```

Параметры strict-стека по умолчанию:
- embedding модель: `BAAI/bge-small-en-v1.5` (FastEmbed ONNX)
- векторная БД: `qdrant` (`QDRANT_LOCATION=:memory:` по умолчанию)

Контейнерный запуск Scenario 1:
```bash
bash scripts/docker_run.sh scenario1-smoke
bash scripts/docker_run.sh scenario1-full
```

Артефакты Scenario 1:
- Сырые прогоны: `results/scenario1/raw/indexing_raw.csv`
- Агрегированные: `results/scenario1/aggregated/indexing_aggregated.csv`
- Speedup: `results/scenario1/aggregated/indexing_speedup.csv`
- Разбивка по стадиям: `results/scenario1/aggregated/indexing_stage_breakdown.csv`
- Графики: `results/scenario1/figures/*.png`

Slurm-шаблоны для bootstrap Ray-кластера:
- `slurm/ray_head.sh`
- `slurm/ray_worker.sh`
- `slurm/submit_cluster.sh`

Примечания по исполнению Scenario 1:
- `ray` mode использует `Ray Data` (`map_batches`) как основной путь.
- Цепочка fallback для устойчивости: `ray_data` -> `ray_native` -> `process_pool`.
- Для `Ray Data` нужен `pyarrow` (входит в `requirements.txt`).
- Фактически использованный backend записывается в колонку `backend` в
  `results/scenario1/raw/indexing_raw.csv`.

## Локальный Slurm (Docker, без sudo)
В репозитории есть воспроизводимый однузловой профиль Slurm, который запускается в Docker.
Используйте его, если на хосте недоступна настройка `slurmctld/slurmd`.

Поднять Slurm:
```bash
bash scripts/slurm_local_up.sh
# или: make slurm-up
```

Проверить кластер:
```bash
bash scripts/slurm_local_status.sh
# или: make slurm-status
```

Запустить smoke-параллельную задачу (`sbatch` + `srun -n 4`):
```bash
bash scripts/slurm_local_submit_test.sh
# или: make slurm-test
```

Выполнить произвольную команду Slurm внутри контейнера:
```bash
bash scripts/slurm_local_exec.sh "scontrol ping && sinfo -N"
```

Остановить/удалить локальный Slurm:
```bash
bash scripts/slurm_local_down.sh
# или: make slurm-down
```

## Контейнерный запуск (Docker)
Параметры контейнерного образа по умолчанию:
- Python-образ: `python:3.9-slim-bookworm`
- Версия Ray: `2.46.0` (синхронизирована с KubeRay-кластером этого проекта)

Собрать образ:
```bash
bash scripts/docker_run.sh build
# или: make docker-build
```

Smoke-запуск внутри контейнера:
```bash
bash scripts/docker_run.sh smoke
# или: make docker-smoke
```

Полная матрица + валидация + анализ + графики внутри контейнера:
```bash
bash scripts/docker_run.sh full
# или: make docker-full
```

Открыть страницу презентации из контейнера:
```bash
bash scripts/docker_run.sh present
# http://localhost:8031/presentation/
```

Если порт `8031` занят:
```bash
PRESENTATION_PORT=8041 bash scripts/docker_run.sh present
# http://localhost:8041/presentation/
```

Остановить контейнер презентации:
```bash
bash scripts/docker_run.sh stop-present
```

Опционально: запуск с внешним KubeRay/Prometheus (передаются в контейнер):
```bash
export RAY_ADDRESS=auto
export RAY_AGENTS=2
export PROMETHEUS_URL=http://prometheus-server.monitoring.svc.cluster.local
export K8S_NAMESPACE=default
export K8S_POD_REGEX='autoscale-exp-.*'
bash scripts/docker_run.sh full
```

Примечания:
- `scripts/docker_run.sh` использует `--network host` для запусков эксперимента,
  чтобы упростить доступ к локальной сети K8s/KubeRay в Ubuntu.
- Файлы результатов пишутся в примонтированный с хоста `results/`.
- Первая сборка образа может быть долгой, так как стек Ray + scientific
  загружается один раз.
- Если Docker BuildKit/buildx недоступен, скрипт автоматически переключается
  на классический `docker build`.
- Можно переопределить build-аргументы образа:
  `PYTHON_IMAGE=python:3.10-slim-bookworm RAY_VERSION=2.46.0 bash scripts/docker_run.sh build`

Опционально: запуск полного контейнерного эксперимента как Kubernetes Job:
```bash
# Сначала соберите локальный образ
bash scripts/docker_run.sh build

# (k3s) импортируйте образ в containerd runtime кластера
docker save ai-hpc-experiment:local | sudo k3s ctr images import -

# Убедитесь, что PVC существует (или отредактируйте configs/experiment-job.yaml)
kubectl get pvc ai-hpc-results-pvc -n default

# Запустите job
kubectl apply -f configs/experiment-job.yaml
kubectl logs -f job/ai-hpc-full-run -n default
```

Полный эксперимент на существующем KubeRay-кластере (ray mode в кластере, baseline локально):
```bash
export RAY_ADDRESS=auto
export RAY_AGENTS=2
export PROMETHEUS_URL=http://prometheus-server.monitoring.svc.cluster.local
export K8S_NAMESPACE=default
export K8S_POD_REGEX='raycluster-autoscaler-.*'
bash scripts/run_experiments.sh full
```

Windows PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_experiments.ps1 -Mode full
python src/metrics/validate.py
python src/analysis/analyze.py
python src/analysis/plot_results.py
```

Описание протокола запусков и KPI находится в:
- `docs/EXPERIMENT_PROTOCOL.md`
- `docs/METRICS_SPEC.md`
- `docs/SCENARIO1_INDEXING.md`

Пример опционального pull метрик из K8s Prometheus:
```bash
python src/runner/run_single.py \
  --mode ray \
  --n-agents 4 \
  --n-tasks 100 \
  --repeat-id 1 \
  --prometheus-url http://prometheus.monitoring.svc:9090 \
  --k8s-namespace default \
  --k8s-pod-regex "ray-.*"
```
