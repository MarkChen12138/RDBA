#!/usr/bin/env python3
"""
Mapper that prepares Yahoo Finance rows for downstream cleaning.

It normalizes field names, parses numeric values, tags the dataset type
(equity/rate/vix), and emits JSON payloads keyed by
`ticker|date|series_label` for deduping in the reducer.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from typing import Dict, Iterable

VALUE_PRIORITY = ("close", "value", "adj_close", "open", "high", "low")


def parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text or text.lower() in {"na", "n/a", "null", "none", "."}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def normalize_date(text: str | None) -> str | None:
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return datetime.strptime(stripped[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def detect_dataset_type(columns: Iterable[str]) -> str:
    cols = set(columns)
    if {"open", "high", "low", "close"}.issubset(cols):
        return "equity"
    if "value" in cols and "open" not in cols:
        return "rate"
    return "generic"


def normalize_row(row: Iterable[str], header: Iterable[str]) -> Dict[str, str]:
    values = list(row)
    normalized = [col.strip().lower() for col in header]
    data: Dict[str, str] = {}
    for idx, column in enumerate(normalized):
        data[column] = values[idx].strip() if idx < len(values) else ""
    return data


def pick_value(record: Dict[str, str]) -> float | None:
    for column in VALUE_PRIORITY:
        val = parse_float(record.get(column))
        if val is not None:
            return val
    return None


def main() -> None:
    reader = csv.reader(sys.stdin)
    header: list[str] | None = None
    dataset_type = "generic"

    for row in reader:
        if not row:
            continue

        first_cell = row[0].strip().lower()
        if first_cell == "date":
            header = row
            dataset_type = detect_dataset_type(col.strip().lower() for col in row)
            continue

        if not header:
            continue

        record = normalize_row(row, header)
        trade_date = normalize_date(record.get("date"))
        if not trade_date:
            continue

        ticker = record.get("ticker", "").strip()
        if not ticker:
            continue

        series_label = record.get("series_label", "").strip() or "unspecified"

        clean = {
            "date": trade_date,
            "ticker": ticker.upper(),
            "series_label": series_label,
            "dataset_type": dataset_type,
            "open": parse_float(record.get("open")),
            "high": parse_float(record.get("high")),
            "low": parse_float(record.get("low")),
            "close": parse_float(record.get("close")),
            "adj_close": parse_float(record.get("adj_close")),
            "value": pick_value(record),
            "volume": parse_float(record.get("volume")),
        }

        quality_notes = []
        if clean["value"] is None:
            quality_notes.append("missing_primary_value")
        if dataset_type == "equity":
            for field in ("open", "high", "low", "close"):
                if clean[field] is None:
                    quality_notes.append(f"missing_{field}")
        clean["quality_notes"] = ";".join(sorted(set(quality_notes))) if quality_notes else "ok"

        key = f"{clean['ticker']}|{clean['date']}|{clean['series_label']}"
        print(f"{key}\t{json.dumps(clean, separators=(',', ':'))}")


if __name__ == "__main__":
    main()
