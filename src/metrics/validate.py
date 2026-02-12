#!/usr/bin/env python3
import csv
from pathlib import Path
import sys

CSV_PATH = Path("results/raw/raw_results.csv")

def die(msg):
    print(f"[VALIDATE] FAIL: {msg}")
    sys.exit(1)

def main():
    if not CSV_PATH.exists():
        die("raw_results.csv not found. Run experiments first.")

    rows = []
    with CSV_PATH.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    if not rows:
        die("raw_results.csv is empty")

    for i, row in enumerate(rows[:2000]):  # проверяем первые 2000 строк
        for key in ["run_id","mode","n_agents","repeat_id","task_id","submit_ts","start_ts","end_ts"]:
            if row.get(key, "") == "":
                die(f"row {i}: missing {key}")

        submit_ts = float(row["submit_ts"])
        start_ts = float(row["start_ts"])
        end_ts = float(row["end_ts"])

        if start_ts < submit_ts:
            die(f"row {i}: start_ts < submit_ts")
        if end_ts < start_ts:
            die(f"row {i}: end_ts < start_ts")

    print(f"[VALIDATE] OK. Rows: {len(rows)}")

if __name__ == "__main__":
    main()
