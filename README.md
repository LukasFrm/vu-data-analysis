# Vilnius road traffic and weather

This repository contains Parts 1 and 2 of a Vilnius University data-analysis
project. It documents dataset selection and quality analysis, then implements
the data-collection, processing and baseline-modelling workflow. The research
subject is Vilnius **road traffic**, with weather used as contemporaneous
predictive information.

See [`FILE_TREE.md`](FILE_TREE.md) for an annotated inventory of the project files.

## Data period

- JUDU traffic: 2025-09-01 through 2026-09-30
- Open-Meteo weather: the same period
- Traffic target: published hourly `count` for each measurement point

## Run

Run these commands from the `vu-data-analysis/` source folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python download_weather.py
python prepare_part2_data.py
python benchmark_part2.py
```

The validated JUDU archive is already included. To reacquire it, run
`python download_judu_traffic.py --force` before the preparation step.

Generated outputs:

- `data/processed/judu_traffic_weather_hourly.parquet`
- `reports/coverage_report.csv`
- `reports/coverage_by_month.csv`
- `reports/benchmark_metrics.csv`
- `reports/benchmark_run.json`

Included field documentation:

- `reports/data_dictionary.md`

The train/test split is chronological. Results describe predictive performance and association, not causal effects of weather.
