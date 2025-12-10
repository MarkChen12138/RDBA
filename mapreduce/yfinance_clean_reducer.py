#!/usr/bin/env python3
"""
Reducer that finalizes cleaned Yahoo Finance rows.

The reducer deduplicates on (ticker, date, series_label) and keeps the
best-quality record, then writes normalized CSV rows.
"""

from __future__ import annotations

import json
import sys
from typing import Dict, Optional

OUTPUT_COLUMNS = [
    "date",
    "ticker",
    "series_label",
    "dataset_type",
    "value",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "quality_notes",
]

NUMERIC_FIELDS = ["value", "open", "high", "low", "close", "adj_close", "volume"]


def record_score(record: Dict[str, object]) -> tuple[int, int]:
    quality_bonus = 1 if record.get("quality_notes") == "ok" else 0
    numeric_count = sum(1 for field in NUMERIC_FIELDS if record.get(field) is not None)
    return quality_bonus, numeric_count


def pick_record(existing: Optional[Dict[str, object]], incoming: Dict[str, object]) -> Dict[str, object]:
    if existing is None:
        return incoming
    if record_score(incoming) > record_score(existing):
        return incoming
    return existing


def format_number(value: Optional[float]) -> str:
    if value is None:
        return ""
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.8f}".rstrip("0").rstrip(".")
    return str(value)


def emit(record: Dict[str, object], header_printed: bool) -> bool:
    if not header_printed:
        print(",".join(OUTPUT_COLUMNS))
        header_printed = True
    row = []
    for column in OUTPUT_COLUMNS:
        if column in NUMERIC_FIELDS:
            row.append(format_number(record.get(column)))
        else:
            row.append(str(record.get(column, "") or ""))
    print(",".join(row))
    return header_printed


def main() -> None:
    current_key: str | None = None
    best_record: Dict[str, object] | None = None
    header_printed = False

    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            key, raw = line.rstrip("\n").split("\t", 1)
        except ValueError:
            continue
        record = json.loads(raw)

        if current_key is None:
            current_key = key
            best_record = record
            continue

        if key != current_key:
            if best_record:
                header_printed = emit(best_record, header_printed)
            current_key = key
            best_record = record
        else:
            best_record = pick_record(best_record, record)

    if current_key is not None and best_record is not None:
        emit(best_record, header_printed)


if __name__ == "__main__":
    main()
