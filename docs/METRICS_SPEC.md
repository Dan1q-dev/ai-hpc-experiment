# METRICS_SPEC.md
Описание всех метрик, собираемых в эксперименте

## Структура данных raw_results.csv

- run_id — уникальный идентификатор прогона
- mode — режим эксперимента (ray / baseline)
- n_agents — количество агентов в прогоне
- repeat_id — номер повторения (1..5)
- seed — фиксированный seed
- task_id — ID задачи внутри прогона
- submit_ts — время отправки задачи
- start_ts — время начала выполнения
- end_ts — время завершения
- task_duration_s = end_ts - start_ts
- sched_overhead_ms = (start_ts - submit_ts) * 1000
- cpu_avg_pct — средняя загрузка CPU
- gpu_avg_pct — средняя загрузка GPU
- failed — 0/1
- recovery_time_s — если был отказ

## Формулы

TTS(run_id) = max(end_ts) - min(submit_ts)

Speedup(N) = TTS(baseline_N) / TTS(ray_N)
