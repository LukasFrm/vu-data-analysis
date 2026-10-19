#!/usr/bin/env python3
"""Convert the JUDU archive to Parquet and merge it with Vilnius weather."""

from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


BASE_DIR = Path(__file__).resolve().parent
RAW_ARCHIVE = (
    BASE_DIR
    / "data"
    / "raw"
    / "judu"
    / "judu_transporto_srautai_2025-09_to_2026-09.zip"
)
WEATHER_PATH = BASE_DIR / "data" / "processed" / "vilnius_weather_hourly.parquet"
TRAFFIC_PATH = BASE_DIR / "data" / "processed" / "judu_traffic_hourly.parquet"
MERGED_PATH = BASE_DIR / "data" / "processed" / "judu_traffic_weather_hourly.parquet"
COVERAGE_PATH = BASE_DIR / "reports" / "coverage_report.csv"
COVERAGE_MONTH_PATH = BASE_DIR / "reports" / "coverage_by_month.csv"

EXPECTED_ROWS = 7_008_072
EXPECTED_START = pd.Timestamp("2025-09-01 00:00:00")
EXPECTED_END = pd.Timestamp("2026-09-30 23:00:00")


def load_point_lookup(archive: zipfile.ZipFile) -> pd.DataFrame:
    with archive.open("measurement_points.csv") as source:
        lookup = pd.read_csv(source)
    lookup["measurementpoint"] = pd.to_numeric(
        lookup["measurementpoint"], errors="raise"
    ).astype("int32")
    lookup = lookup.drop_duplicates("measurementpoint", keep="last")
    return lookup[["measurementpoint", "segment_name_latest"]]


def prepare_month(raw: pd.DataFrame, lookup: pd.DataFrame, member: str) -> pd.DataFrame:
    required = {
        "id",
        "measurementpoint",
        "count",
        "datefrom",
        "dateto",
        "hourfrom",
        "hourto",
        "divisor",
        "datefrom_ms",
        "dateto_ms",
    }
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"{member}: missing columns {sorted(missing)}")

    frame = pd.DataFrame(
        {
            "record_id": pd.to_numeric(raw["id"], errors="raise").astype("int64"),
            "measurementpoint": pd.to_numeric(
                raw["measurementpoint"], errors="raise"
            ).astype("int32"),
            "vehicle_count": pd.to_numeric(raw["count"], errors="coerce").astype(
                "Int32"
            ),
            "timestamp_local": pd.to_datetime(raw["datefrom"], errors="raise"),
            "hourfrom": pd.to_numeric(raw["hourfrom"], errors="coerce").astype(
                "Int8"
            ),
            "hourto": pd.to_numeric(raw["hourto"], errors="coerce").astype("Int8"),
            "divisor": pd.to_numeric(raw["divisor"], errors="coerce").astype("Int16"),
            "datefrom_ms": pd.to_numeric(raw["datefrom_ms"], errors="coerce").astype(
                "Int64"
            ),
        }
    )
    frame = frame.merge(lookup, on="measurementpoint", how="left", validate="many_to_one")
    frame = frame.rename(columns={"segment_name_latest": "segment_name"})

    if frame["record_id"].duplicated().any():
        raise ValueError(f"{member}: duplicate ArcGIS record IDs")
    if frame["vehicle_count"].isna().any():
        raise ValueError(f"{member}: missing vehicle counts")
    if frame["vehicle_count"].lt(0).any():
        raise ValueError(f"{member}: negative vehicle counts")

    frame["segment_name"] = frame["segment_name"].astype("string")
    duplicate_mask = frame.duplicated(
        ["measurementpoint", "timestamp_local"], keep=False
    )
    if duplicate_mask.any():
        frame = (
            frame.groupby(
                ["measurementpoint", "timestamp_local"],
                as_index=False,
                sort=False,
                dropna=False,
            )
            .agg(
                record_id=("record_id", "min"),
                vehicle_count=("vehicle_count", "mean"),
                hourfrom=("hourfrom", "first"),
                hourto=("hourto", "first"),
                divisor=("divisor", "max"),
                datefrom_ms=("datefrom_ms", "first"),
                segment_name=("segment_name", "first"),
                source_rows_aggregated=("record_id", "size"),
            )
        )
    else:
        frame["source_rows_aggregated"] = np.int8(1)

    frame["vehicle_count"] = frame["vehicle_count"].astype("float32")
    frame["source_rows_aggregated"] = frame["source_rows_aggregated"].astype("int8")
    frame = frame[
        [
            "record_id",
            "measurementpoint",
            "vehicle_count",
            "timestamp_local",
            "hourfrom",
            "hourto",
            "divisor",
            "datefrom_ms",
            "segment_name",
            "source_rows_aggregated",
        ]
    ]
    return frame.sort_values(["timestamp_local", "measurementpoint"])


def write_traffic_parquet() -> dict:
    if not RAW_ARCHIVE.exists():
        raise FileNotFoundError(f"Missing JUDU archive: {RAW_ARCHIVE}")

    raw_rows = 0
    total_rows = 0
    point_ids: set[int] = set()
    minimum = None
    maximum = None
    missing_segment_rows = 0
    parquet_writer = None

    with zipfile.ZipFile(RAW_ARCHIVE) as archive:
        members = sorted(
            name
            for name in archive.namelist()
            if name.startswith("traffic_") and name.endswith(".csv")
        )
        if len(members) != 13:
            raise ValueError(f"Expected 13 monthly traffic files, found {len(members)}")
        lookup = load_point_lookup(archive)

        try:
            for member in tqdm(members, desc="Preparing JUDU months", unit="month"):
                with archive.open(member) as source:
                    raw = pd.read_csv(source, low_memory=False)
                raw_rows += len(raw)
                frame = prepare_month(raw, lookup, member)

                total_rows += len(frame)
                point_ids.update(frame["measurementpoint"].unique().tolist())
                month_min = frame["timestamp_local"].min()
                month_max = frame["timestamp_local"].max()
                minimum = month_min if minimum is None else min(minimum, month_min)
                maximum = month_max if maximum is None else max(maximum, month_max)
                missing_segment_rows += int(frame["segment_name"].isna().sum())

                table = pa.Table.from_pandas(frame, preserve_index=False)
                if parquet_writer is None:
                    TRAFFIC_PATH.parent.mkdir(parents=True, exist_ok=True)
                    parquet_writer = pq.ParquetWriter(
                        TRAFFIC_PATH,
                        table.schema,
                        compression="zstd",
                        use_dictionary=True,
                    )
                parquet_writer.write_table(table)
        finally:
            if parquet_writer is not None:
                parquet_writer.close()

    if raw_rows != EXPECTED_ROWS:
        raise AssertionError(f"Expected {EXPECTED_ROWS:,} raw rows, read {raw_rows:,}")
    if minimum != EXPECTED_START or maximum != EXPECTED_END:
        raise AssertionError(f"Unexpected traffic range: {minimum} to {maximum}")

    return {
        "traffic_raw_rows": raw_rows,
        "traffic_rows": total_rows,
        "traffic_detector_hours_aggregated_from_multiple_rows": raw_rows - total_rows,
        "traffic_measurement_points": len(point_ids),
        "traffic_start": str(minimum),
        "traffic_end": str(maximum),
        "traffic_missing_segment_name_rows": missing_segment_rows,
    }


def merge_weather(traffic_summary: dict) -> dict:
    if not WEATHER_PATH.exists():
        raise FileNotFoundError(
            f"Missing weather file: {WEATHER_PATH}. Run download_weather.py first."
        )

    print("Reading prepared traffic and weather...")
    traffic = pd.read_parquet(TRAFFIC_PATH)
    weather = pd.read_parquet(WEATHER_PATH)

    if traffic.duplicated(["measurementpoint", "timestamp_local"]).any():
        raise AssertionError("Duplicate detector-hour keys in prepared traffic.")
    if weather["timestamp_local"].duplicated().any():
        raise AssertionError("Duplicate timestamps in prepared weather.")

    merged = traffic.merge(
        weather,
        on="timestamp_local",
        how="left",
        validate="many_to_one",
    )
    merged["hour"] = merged["timestamp_local"].dt.hour.astype("int8")
    merged["weekday"] = merged["timestamp_local"].dt.dayofweek.astype("int8")
    merged["month"] = merged["timestamp_local"].dt.month.astype("int8")
    merged["time_slot"] = (merged["weekday"] * 24 + merged["hour"]).astype("int16")

    weather_columns = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "weather_code",
        "cloud_cover",
        "wind_speed_10m",
    ]
    missing_weather_rows = int(merged[weather_columns].isna().all(axis=1).sum())
    weather_match_pct = 100 * (1 - missing_weather_rows / len(merged))

    merged["is_mapped_road_point"] = (
        merged["segment_name"].notna()
        & ~merged["segment_name"].str.contains("mygtuk", case=False, na=False)
    )
    merged["count_plausible"] = merged["vehicle_count"].le(10_000)
    merged["model_eligible"] = (
        merged["is_mapped_road_point"]
        & merged["count_plausible"]
        & ~merged[weather_columns].isna().all(axis=1)
    )

    coverage_by_month = (
        merged.groupby(merged["timestamp_local"].dt.to_period("M"), observed=True)
        .agg(
            detector_hour_rows=("vehicle_count", "size"),
            measurement_points=("measurementpoint", "nunique"),
            distinct_days=(
                "timestamp_local",
                lambda values: values.dt.normalize().nunique(),
            ),
            model_eligible_rows=("model_eligible", "sum"),
            median_vehicle_count=("vehicle_count", "median"),
        )
        .reset_index(names="month")
    )
    coverage_by_month["month"] = coverage_by_month["month"].astype("string")
    coverage_by_month["model_eligible_rows"] = coverage_by_month[
        "model_eligible_rows"
    ].astype("int64")
    coverage_by_month.to_csv(COVERAGE_MONTH_PATH, index=False)

    merged.to_parquet(MERGED_PATH, index=False, compression="zstd")

    summary = {
        **traffic_summary,
        "weather_rows_after_dst_handling": len(weather),
        "weather_start": str(weather["timestamp_local"].min()),
        "weather_end": str(weather["timestamp_local"].max()),
        "weather_repeated_local_hours_collapsed": int(
            weather["weather_hours_aggregated"].gt(1).sum()
        ),
        "merged_rows": len(merged),
        "missing_weather_rows": missing_weather_rows,
        "weather_match_pct": round(weather_match_pct, 6),
        "excluded_nonroad_or_unmapped_rows": int(
            (~merged["is_mapped_road_point"]).sum()
        ),
        "excluded_implausible_count_rows_total": int(
            (~merged["count_plausible"]).sum()
        ),
        "excluded_implausible_count_rows_among_mapped_roads": int(
            (merged["is_mapped_road_point"] & ~merged["count_plausible"]).sum()
        ),
        "model_eligible_rows": int(merged["model_eligible"].sum()),
        "rain_traffic_rows": int(merged["is_rain"].fillna(False).sum()),
        "snow_traffic_rows": int(merged["is_snow"].fillna(False).sum()),
        "freezing_traffic_rows": int(merged["is_freezing"].fillna(False).sum()),
        "vehicle_count_min": int(merged["vehicle_count"].min()),
        "vehicle_count_median": float(merged["vehicle_count"].median()),
        "vehicle_count_mean": float(merged["vehicle_count"].mean()),
        "vehicle_count_max": int(merged["vehicle_count"].max()),
    }

    COVERAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([summary]).to_csv(COVERAGE_PATH, index=False)
    return summary


def main() -> None:
    print("Preparing the JUDU traffic archive...")
    traffic_summary = write_traffic_parquet()
    print("Merging traffic with weather...")
    summary = merge_weather(traffic_summary)

    print(f"Saved merged dataset: {MERGED_PATH}")
    print(f"Saved coverage report: {COVERAGE_PATH}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    TRAFFIC_PATH.unlink(missing_ok=True)
    WEATHER_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
