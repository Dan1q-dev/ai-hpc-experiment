PYTHON ?= python

.PHONY: smoke full validate analyze plot all clean docker-build docker-smoke docker-full docker-present docker-stop-present scenario1-smoke scenario1-full scenario1-strict-smoke scenario1-strict-full docker-scenario1-smoke docker-scenario1-full slurm-up slurm-status slurm-test slurm-down

smoke:
	bash scripts/run_experiments.sh smoke

full:
	bash scripts/run_experiments.sh full

validate:
	$(PYTHON) src/metrics/validate.py

analyze:
	$(PYTHON) src/analysis/analyze.py

plot:
	$(PYTHON) src/analysis/plot_results.py

all: smoke validate analyze plot

clean:
	rm -rf results/raw results/figures
	rm -f results/aggregated/aggregated_results.csv
	rm -f results/aggregated/summary_tts.csv
	rm -f results/aggregated/speedup_efficiency.csv

docker-build:
	bash scripts/docker_run.sh build

docker-smoke:
	bash scripts/docker_run.sh smoke

docker-full:
	bash scripts/docker_run.sh full

docker-present:
	bash scripts/docker_run.sh present

docker-stop-present:
	bash scripts/docker_run.sh stop-present

scenario1-smoke:
	bash scripts/run_indexing_scenario.sh smoke

scenario1-full:
	bash scripts/run_indexing_scenario.sh full

scenario1-strict-smoke:
	bash scripts/run_indexing_scenario_strict.sh smoke

scenario1-strict-full:
	bash scripts/run_indexing_scenario_strict.sh full

docker-scenario1-smoke:
	bash scripts/docker_run.sh scenario1-smoke

docker-scenario1-full:
	bash scripts/docker_run.sh scenario1-full

slurm-up:
	bash scripts/slurm_local_up.sh

slurm-status:
	bash scripts/slurm_local_status.sh

slurm-test:
	bash scripts/slurm_local_submit_test.sh

slurm-down:
	bash scripts/slurm_local_down.sh
