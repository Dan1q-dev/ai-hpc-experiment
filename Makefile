run-smoke:
	python src/runner/run_single.py --mode baseline --n-agents 1 --repeat-id 1 --seed 42 --tasks 10 --units 50000
	python src/runner/run_single.py --mode ray --n-agents 1 --repeat-id 1 --seed 42 --tasks 10 --units 50000

validate:
	python src/metrics/validate.py

analyze:
	python src/analysis/analyze.py

all: run-smoke validate analyze
