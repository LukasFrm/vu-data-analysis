#!/usr/bin/env python3
"""Run chronological traffic-flow baselines and Ridge benchmarks."""

from __future__ import annotations

import gc
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "processed" / "judu_traffic_weather_hourly.parquet"
METRICS_PATH = BASE_DIR / "reports" / "benchmark_metrics.csv"
RUN_METADATA_PATH = BASE_DIR / "reports" / "benchmark_run.json"

SPLIT_TIMESTAMP = pd.Timestamp("2026-07-01 00:00:00")
RIDGE_ALPHA = 10.0

CATEGORICAL_TIME_SITE = ["measurementpoint", "time_slot", "month"]
WEATHER_NUMERIC = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "cloud_cover",
    "wind_speed_10m",
]
WEATHER_CATEGORICAL = ["weather_code"]


def metrics_row(model: str, y_true: pd.Series, predictions: np.ndarray) -> dict:
    return {
        "model": model,
        "test_rows": len(y_true),
        "mae": mean_absolute_error(y_true, predictions),
        "rmse": float(np.sqrt(mean_squared_error(y_true, predictions))),
        "r2": r2_score(y_true, predictions),
    }


def historical_mean_predictions(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    global_mean = float(train["vehicle_count"].mean())
    slot_means = train.groupby(
        ["measurementpoint", "time_slot"], observed=True
    )["vehicle_count"].mean()
    point_hour_means = train.groupby(
        ["measurementpoint", "hour"], observed=True
    )["vehicle_count"].mean()
    point_means = train.groupby("measurementpoint", observed=True)[
        "vehicle_count"
    ].mean()

    slot_index = pd.MultiIndex.from_frame(test[["measurementpoint", "time_slot"]])
    point_hour_index = pd.MultiIndex.from_frame(test[["measurementpoint", "hour"]])

    predictions = slot_means.reindex(slot_index).to_numpy()
    fallback = point_hour_means.reindex(point_hour_index).to_numpy()
    predictions = np.where(np.isnan(predictions), fallback, predictions)
    fallback = point_means.reindex(test["measurementpoint"]).to_numpy()
    predictions = np.where(np.isnan(predictions), fallback, predictions)
    return np.where(np.isnan(predictions), global_mean, predictions)


def ridge_pipeline(categorical: list[str], numeric: list[str]) -> Pipeline:
    transformers = [
        (
            "categorical",
            OneHotEncoder(handle_unknown="ignore", dtype=np.float32),
            categorical,
        )
    ]
    if numeric:
        numeric_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler(with_mean=False)),
            ]
        )
        transformers.append(("numeric", numeric_pipeline, numeric))

    preprocessor = ColumnTransformer(
        transformers,
        sparse_threshold=0.3,
    )
    model = Ridge(
        alpha=RIDGE_ALPHA,
        solver="lsqr",
        max_iter=300,
        tol=1e-4,
    )
    return Pipeline([("preprocessor", preprocessor), ("model", model)])


def fit_and_score(
    model_name: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    categorical: list[str],
    numeric: list[str],
) -> tuple[dict, float]:
    features = categorical + numeric
    pipeline = ridge_pipeline(categorical, numeric)
    start = time.perf_counter()
    print(f"Fitting {model_name} on {len(train):,} rows...")
    pipeline.fit(train[features], train["vehicle_count"])
    predictions = pipeline.predict(test[features])
    elapsed = time.perf_counter() - start
    result = metrics_row(model_name, test["vehicle_count"], predictions)
    result["fit_and_predict_seconds"] = elapsed
    del pipeline, predictions
    gc.collect()
    return result, elapsed


def main() -> None:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Missing merged dataset: {DATA_PATH}. Run prepare_part2_data.py first."
        )
    print("Reading merged detector-hour dataset...")
    data = pd.read_parquet(DATA_PATH)
    data = data.loc[
        data["vehicle_count"].notna() & data["model_eligible"]
    ].copy()

    train = data.loc[data["timestamp_local"] < SPLIT_TIMESTAMP].copy()
    test = data.loc[data["timestamp_local"] >= SPLIT_TIMESTAMP].copy()
    common_points = np.intersect1d(
        train["measurementpoint"].unique(),
        test["measurementpoint"].unique(),
    )
    train = train.loc[train["measurementpoint"].isin(common_points)].copy()
    test = test.loc[test["measurementpoint"].isin(common_points)].copy()
    del data
    gc.collect()

    if train.empty or test.empty:
        raise AssertionError("Chronological train/test split produced an empty partition.")
    if train["timestamp_local"].max() >= test["timestamp_local"].min():
        raise AssertionError("Training and test periods overlap.")

    for column in CATEGORICAL_TIME_SITE + WEATHER_CATEGORICAL:
        train[column] = train[column].astype("string")
        test[column] = test[column].astype("string")

    results = []
    global_mean = float(train["vehicle_count"].mean())
    constant_predictions = np.full(len(test), global_mean)
    constant_result = metrics_row(
        "global_train_mean", test["vehicle_count"], constant_predictions
    )
    constant_result["fit_and_predict_seconds"] = 0.0
    results.append(constant_result)
    del constant_predictions

    start = time.perf_counter()
    historical_predictions = historical_mean_predictions(train, test)
    historical_result = metrics_row(
        "measurementpoint_weekday_hour_mean",
        test["vehicle_count"],
        historical_predictions,
    )
    historical_result["fit_and_predict_seconds"] = time.perf_counter() - start
    results.append(historical_result)
    del historical_predictions
    gc.collect()

    ridge_time, _ = fit_and_score(
        "ridge_time_site",
        train,
        test,
        CATEGORICAL_TIME_SITE,
        [],
    )
    results.append(ridge_time)

    ridge_weather, _ = fit_and_score(
        "ridge_time_site_weather",
        train,
        test,
        CATEGORICAL_TIME_SITE + WEATHER_CATEGORICAL,
        WEATHER_NUMERIC,
    )
    results.append(ridge_weather)

    metrics = pd.DataFrame(results)
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(METRICS_PATH, index=False)

    metadata = {
        "split_timestamp": str(SPLIT_TIMESTAMP),
        "train_start": str(train["timestamp_local"].min()),
        "train_end": str(train["timestamp_local"].max()),
        "test_start": str(test["timestamp_local"].min()),
        "test_end": str(test["timestamp_local"].max()),
        "train_rows": len(train),
        "test_rows": len(test),
        "stable_measurement_points": len(common_points),
        "ridge_alpha": RIDGE_ALPHA,
        "categorical_features": CATEGORICAL_TIME_SITE + WEATHER_CATEGORICAL,
        "weather_numeric_features": WEATHER_NUMERIC,
    }
    RUN_METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("\nBenchmark results:")
    print(metrics.to_string(index=False))
    print(f"\nSaved metrics: {METRICS_PATH}")


if __name__ == "__main__":
    main()
