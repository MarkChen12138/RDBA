#!/usr/bin/env python3
"""
Mapper for profiling Yahoo Finance extracts (Mark Chen).

Reads CSV rows and emits ticker-level partial statistics that will be
aggregated by the reducer:
    - row counts
    - min/max/avg for the primary price column (close/value)
    - volume stats (when available)
    - first/last trading date
    - missing value counts per column
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from typing import Dict, Iterable, Tuple

VALUE_PRIORITY = ("close", "value", "adj_close", "open", "high", "low")

MISSING_SENTINELS = {"", "na", "n/a", ".", "null", "none"}


def parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = raw.strip()
    if text.lower() in MISSING_SENTINELS:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_row(row: Iterable[str], header: Iterable[str]) -> Dict[str, str]:
    row_values = list(row)
    values: Dict[str, str] = {}
    for idx, col in enumerate(header):
        values[col] = row_values[idx].strip() if idx < len(row_values) else ""
    return values


def pick_value(record: Dict[str, str]) -> Tuple[str | None, float | None]:
    for column in VALUE_PRIORITY:
        val = parse_float(record.get(column))
        if val is not None:
            return column, val
    return None, None


def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def emit(payload: Dict[str, object], ticker: str) -> None:
    print(f"{ticker}\t{json.dumps(payload, separators=(',', ':'))}")


def main() -> None:
    reader = csv.reader(sys.stdin)
    header: list[str] | None = None

    for row in reader:
        if not row:
            continue

        first_cell = row[0].strip().lower()
        if first_cell == "date":
            header = [cell.strip().lower() for cell in row]
            continue

        if not header:
            continue

        record = normalize_row(row, header)

        ticker = record.get("ticker", "").strip()
        if not ticker:
            continue

        trade_date = normalize_date(record.get("date"))
        if not trade_date:
            continue

        series_label = record.get("series_label", "").strip() or "unspecified"

        value_column, value = pick_value(record)
        volume = parse_float(record.get("volume"))

        missing = {}
        for col, raw in record.items():
            if raw.strip().lower() in MISSING_SENTINELS:
                missing[col] = 1

        payload = {
            "count": 1,
            "series_labels": {series_label: 1},
            "value_sum": value if value is not None else 0.0,
            "value_count": 1 if value is not None else 0,
            "value_min": value,
            "value_max": value,
            "value_sources": {value_column: 1} if value_column else {},
            "volume_sum": volume if volume is not None else 0.0,
            "volume_count": 1 if volume is not None else 0,
            "volume_min": volume,
            "volume_max": volume,
            "date_min": trade_date,
            "date_max": trade_date,
            "missing": missing,
        }

        emit(payload, ticker)


if __name__ == "__main__":
    main()
