# Project file tree

```text
vu-data-analysis/
├── .gitignore                                      # Excludes local environments, caches and LaTeX build files.
├── README.md                                       # Project overview and reproduction instructions.
├── FILE_TREE.md                                    # Annotated inventory of the submitted repository.
├── requirements.txt                                # Python dependencies for the data and modelling pipeline.
├── download_judu_traffic.py                        # Downloads and validates monthly JUDU traffic records.
├── download_weather.py                             # Downloads hourly Vilnius weather and handles local time.
├── prepare_part2_data.py                           # Cleans traffic, joins weather and creates quality summaries.
├── benchmark_part2.py                              # Runs chronological baseline and Ridge models.
├── data/
│   ├── raw/
│   │   ├── judu/
│   │   │   └── judu_transporto_srautai_2025-09_to_2026-09.zip
│   │   │                                               # Validated monthly JUDU source archive.
│   │   └── weather/
│   │       ├── open_meteo_vilnius_2025-09-01_2026-09-30.json
│   │       │                                           # Original Open-Meteo response.
│   │       └── open_meteo_vilnius_metadata.json        # Weather request and processing metadata.
│   └── processed/
│       └── judu_traffic_weather_hourly.parquet          # Final joined modelling table.
└── reports/
    ├── data_dictionary.md                           # Field definitions, units and time-handling notes.
    ├── coverage_report.csv                          # Overall data-quality summary.
    ├── coverage_by_month.csv                        # Monthly observation and detector coverage.
    ├── benchmark_metrics.csv                        # MAE, RMSE and R-squared benchmark results.
    └── benchmark_run.json                           # Train/test periods and model configuration.
```

The submitted source tree contains the Part 1 and Part 2 data, code and
reproducibility material. Report documents and their private generation files,
local virtual environments, caches, temporary renders and superseded datasets
are excluded.
