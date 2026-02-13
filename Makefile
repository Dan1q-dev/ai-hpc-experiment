PYTHON ?= python

.PHONY: smoke full validate analyze plot all clean

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
