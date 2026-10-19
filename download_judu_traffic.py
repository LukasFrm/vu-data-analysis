#!/usr/bin/env python3
"""Download historical JUDU detector-hour records from the public ArcGIS service.

The checked-in workflow already has a validated archive for 2025-09 through
2026-09. This script documents and reproduces that acquisition when a refresh is
needed. The ArcGIS service is slow, so completed monthly files are requested in
parallel and packaged only after exact row-count validation.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


TABLE_URL = (
    "https://arcgis.sisp.lt/arcgis/rest/services/"
    "VIS/Traffic_Volumes/MapServer/3/query"
)
SEGMENT_URL = (
    "https://arcgis.sisp.lt/arcgis/rest/services/"
    "VIS/Traffic_Volumes/MapServer/0/query"
)
DEFAULT_PAGE_SIZE = 50_000


def request_json(url: str, params: dict, attempts: int = 6) -> dict:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={
            "User-Agent": "VU-data-analysis-JUDU/1.0",
            "Accept-Encoding": "gzip",
        },
    )
    last_error = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                body = response.read()
                if response.headers.get("Content-Encoding", "").lower() == "gzip":
                    body = gzip.decompress(body)
            payload = json.loads(body)
            if "error" in payload:
                raise RuntimeError(json.dumps(payload["error"], ensure_ascii=False))
            return payload
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(2**attempt, 20))
    raise RuntimeError(f"Request failed after {attempts} attempts: {last_error}")


def next_month(month: str) -> str:
    year, number = map(int, month.split("-"))
    if number == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{number + 1:02d}"


def months_between(start: str, end: str) -> list[str]:
    result = []
    month = start
    while month <= end:
        result.append(month)
        month = next_month(month)
    return result


def month_where(month: str) -> str:
    following = next_month(month)
    return (
        f"datefrom >= DATE '{month}-01 00:00:00' "
        f"AND datefrom < DATE '{following}-01 00:00:00'"
    )


def naive_iso(epoch_ms: int | None) -> str:
    if epoch_ms is None:
        return ""
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def download_month(month: str, directory: Path, page_size: int) -> tuple[str, int, Path]:
    where = month_where(month)
    expected = request_json(
        TABLE_URL,
        {"where": where, "returnCountOnly": "true", "f": "json"},
    )["count"]
    path = directory / f"traffic_{month}.csv"
    fields = [
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
    ]
    seen_ids: set[int] = set()
    written = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for offset in range(0, expected, page_size):
            payload = request_json(
                TABLE_URL,
                {
                    "where": where,
                    "outFields": (
                        "id,measurementpoint,count,datefrom,dateto,"
                        "hourfrom,hourto,divisor"
                    ),
                    "returnGeometry": "false",
                    "orderByFields": "id ASC",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                    "f": "json",
                },
            )
            features = payload.get("features", [])
            if not features:
                raise RuntimeError(f"{month}: empty page at offset {offset}")
            for feature in features:
                row = feature["attributes"]
                record_id = int(row["id"])
                if record_id in seen_ids:
                    raise RuntimeError(f"{month}: duplicate ArcGIS id {record_id}")
                seen_ids.add(record_id)
                datefrom_ms = row.get("datefrom")
                dateto_ms = row.get("dateto")
                writer.writerow(
                    {
                        "id": record_id,
                        "measurementpoint": row.get("measurementpoint"),
                        "count": row.get("count"),
                        "datefrom": naive_iso(datefrom_ms),
                        "dateto": naive_iso(dateto_ms),
                        "hourfrom": row.get("hourfrom"),
                        "hourto": row.get("hourto"),
                        "divisor": row.get("divisor"),
                        "datefrom_ms": datefrom_ms,
                        "dateto_ms": dateto_ms,
                    }
                )
            written += len(features)
    if written != expected or len(seen_ids) != expected:
        raise RuntimeError(
            f"{month}: expected {expected:,}; wrote {written:,}; "
            f"unique ids {len(seen_ids):,}"
        )
    return month, expected, path


def write_measurement_points(months: list[str], directory: Path) -> Path:
    clauses = []
    for month in months:
        year, number = map(int, month.split("-"))
        clauses.append(f"(year = {year} AND month = {number})")
    where = " OR ".join(clauses)
    payload = request_json(
        SEGMENT_URL,
        {
            "where": where,
            "outFields": "measurementpoint,segment_name,year,month",
            "returnGeometry": "false",
            "orderByFields": "year ASC,month ASC,measurementpoint ASC",
            "resultRecordCount": 50_000,
            "f": "json",
        },
    )
    rows = [feature["attributes"] for feature in payload.get("features", [])]
    grouped: dict[int, dict] = {}
    for row in rows:
        point = int(row["measurementpoint"])
        period = f"{int(row['year']):04d}-{int(row['month']):02d}"
        name = row.get("segment_name") or ""
        entry = grouped.setdefault(point, {"periods": [], "names": []})
        entry["periods"].append(period)
        if name and name not in entry["names"]:
            entry["names"].append(name)
        entry["latest_name"] = name

    path = directory / "measurement_points.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "measurementpoint",
                "segment_name_latest",
                "first_month_available",
                "last_month_available",
                "segment_name_variants",
            ]
        )
        for point in sorted(grouped):
            entry = grouped[point]
            writer.writerow(
                [
                    point,
                    entry.get("latest_name", ""),
                    min(entry["periods"]),
                    max(entry["periods"]),
                    " | ".join(entry["names"]),
                ]
            )
    return path


def build_archive(paths: list[Path], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".zip.part")
    with zipfile.ZipFile(
        temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as archive:
        for path in sorted(paths, key=lambda item: item.name):
            archive.write(path, arcname=path.name)
    os.replace(temporary, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-month", default="2025-09", help="YYYY-MM")
    parser.add_argument("--end-month", default="2026-09", help="YYYY-MM")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            Path(__file__).resolve().parent
            / "data"
            / "raw"
            / "judu"
            / "judu_transporto_srautai_2025-09_to_2026-09.zip"
        ),
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.output.exists() and not args.force:
        raise FileExistsError(
            f"Output already exists: {args.output}. Use --force to replace it."
        )
    months = months_between(args.start_month, args.end_month)
    if not months or args.start_month > args.end_month:
        raise ValueError("Invalid month range")

    with tempfile.TemporaryDirectory(prefix="judu-download-") as temp:
        staging = Path(temp)
        month_counts = {}
        monthly_paths = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(download_month, month, staging, args.page_size): month
                for month in months
            }
            for future in as_completed(futures):
                month, count, path = future.result()
                month_counts[month] = count
                monthly_paths.append(path)
                print(f"Downloaded {month}: {count:,} rows", flush=True)

        lookup_path = write_measurement_points(months, staging)
        readme_path = staging / "README.txt"
        readme_path.write_text(
            "JUDU historical traffic-flow export\n"
            f"Period: {args.start_month} through {args.end_month}\n"
            f"Rows: {sum(month_counts.values()):,}\n"
            f"Source: {TABLE_URL.removesuffix('/query')}\n",
            encoding="utf-8",
        )
        build_archive(monthly_paths + [lookup_path, readme_path], args.output)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
