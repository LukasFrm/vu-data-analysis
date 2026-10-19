#!/usr/bin/env python3
"""Download and prepare hourly historical weather for Vilnius.

Open-Meteo epoch timestamps are converted with the Europe/Vilnius timezone.
This makes the spring DST gap explicit. The repeated autumn local-clock hour is
collapsed to one row because JUDU publishes only one row for that clock hour.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw" / "weather"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_JSON_PATH = RAW_DIR / "open_meteo_vilnius_2025-09-01_2026-09-30.json"
METADATA_PATH = RAW_DIR / "open_meteo_vilnius_metadata.json"
OUTPUT_PATH = PROCESSED_DIR / "vilnius_weather_hourly.parquet"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 54.6872
LONGITUDE = 25.2797
TIMEZONE = "Europe/Vilnius"
START_DATE = "2025-09-01"
END_DATE = "2026-09-30"

HOURLY_VARIABLES = [
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

SUM_ON_REPEATED_HOUR = ["precipitation", "rain", "snowfall"]
MEAN_ON_REPEATED_HOUR = [
    "temperature_2m",
    "relative_humidity_2m",
    "snow_depth",
    "cloud_cover",
    "wind_speed_10m",
]


def download_json() -> tuple[dict, str]:
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": TIMEZONE,
        "timeformat": "unixtime",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "VU-data-analysis-weather/1.0"},
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        payload = json.load(response)
    if "hourly" not in payload:
        raise RuntimeError(f"Open-Meteo response has no hourly data: {payload}")
    return payload, url


def prepare_weather(payload: dict) -> pd.DataFrame:
    hourly = pd.DataFrame(payload["hourly"])
    hourly["timestamp_utc"] = pd.to_datetime(
        hourly.pop("time"), unit="s", utc=True
    )
    hourly["timestamp_local"] = (
        hourly["timestamp_utc"]
        .dt.tz_convert(TIMEZONE)
        .dt.tz_localize(None)
    )

    aggregation = {column: "mean" for column in MEAN_ON_REPEATED_HOUR}
    aggregation.update({column: "sum" for column in SUM_ON_REPEATED_HOUR})
    aggregation["weather_code"] = "max"
    aggregation["timestamp_utc"] = "min"

    group_sizes = (
        hourly.groupby("timestamp_local", sort=True)
        .size()
        .rename("weather_hours_aggregated")
    )
    prepared = (
        hourly.groupby("timestamp_local", as_index=False, sort=True)
        .agg(aggregation)
        .merge(group_sizes, on="timestamp_local", how="left", validate="one_to_one")
    )

    prepared["weather_code"] = prepared["weather_code"].astype("Int16")
    prepared["weather_hours_aggregated"] = prepared[
        "weather_hours_aggregated"
    ].astype("int8")
    prepared["is_rain"] = prepared["rain"].fillna(0).gt(0)
    prepared["is_snow"] = prepared["snowfall"].fillna(0).gt(0)
    prepared["is_freezing"] = prepared["temperature_2m"].le(0)

    if prepared["timestamp_local"].duplicated().any():
        raise AssertionError("Weather timestamps are not unique after DST aggregation.")
    if not prepared["timestamp_local"].is_monotonic_increasing:
        raise AssertionError("Weather timestamps are not sorted.")
    return prepared


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Downloading Open-Meteo historical weather...")
    payload, request_url = download_json()
    RAW_JSON_PATH.write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    weather = prepare_weather(payload)
    weather.to_parquet(OUTPUT_PATH, index=False, compression="zstd")

    metadata = {
        "source": "Open-Meteo Historical Weather API",
        "documentation": "https://open-meteo.com/en/docs/historical-weather-api",
        "request_url": request_url,
        "requested_coordinates": {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
        },
        "returned_coordinates": {
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "elevation": payload.get("elevation"),
        },
        "timezone": TIMEZONE,
        "period": {"start": START_DATE, "end": END_DATE},
        "hourly_units": payload.get("hourly_units", {}),
        "dst_method": (
            "Requested Unix time, converted from UTC to Europe/Vilnius. "
            "Repeated autumn local hour: sums for precipitation/rain/snowfall, "
            "means for continuous variables, maximum weather code."
        ),
        "prepared_rows": len(weather),
        "repeated_local_hours_collapsed": int(
            weather["weather_hours_aggregated"].gt(1).sum()
        ),
    }
    METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Saved raw response: {RAW_JSON_PATH}")
    print(f"Saved prepared weather: {OUTPUT_PATH}")
    print(f"Rows: {len(weather):,}")
    print(
        "Local range:",
        weather["timestamp_local"].min(),
        "to",
        weather["timestamp_local"].max(),
    )
    print(
        "Repeated local hours collapsed:",
        int(weather["weather_hours_aggregated"].gt(1).sum()),
    )


if __name__ == "__main__":
    main()
