"""
Yahoo Finance data source.

Downloads supplementary market data (Fed futures, Treasury yields, etc.)
that relates to Federal Reserve policy outlook.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd
import yfinance as yf

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/yfinance")

# Ticker → friendly name used for filenames.
TICKERS: Dict[str, str] = {
    "ZQ=F": "fed_funds_futures",  # 30-Day Fed Funds Futures (CME)
    "^IRX": "3m_treasury_bill",  # 13 Week Treasury Bill Yield
    "^FVX": "5y_treasury_yield",  # 5 Year Treasury Yield
    "^TNX": "10y_treasury_yield",  # 10 Year Treasury Yield
}

# Pull roughly two years of history by default.
DEFAULT_START = date.today() - timedelta(days=730)
DEFAULT_END = None  # None = up to latest available
INTERVAL = "1d"  # Supported values: 1d, 1h, 5m, etc.


def to_safe_name(name: str) -> str:
    """Convert a name to a safe filename."""
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)


def export_data() -> None:
    """Fetch and export Yahoo Finance data."""
    ensure_dir(OUTPUT_DIR)

    print("\n=== Fetching Yahoo Finance data ===")
    metadata = {
        "start": DEFAULT_START.isoformat(),
        "end": DEFAULT_END.isoformat() if DEFAULT_END else None,
        "interval": INTERVAL,
        "tickers": TICKERS,
    }

    metadata_path = OUTPUT_DIR / "yfinance_metadata.json"
    write_json(metadata_path, metadata)
    print(f"Saved metadata to {metadata_path}")

    for ticker, label in TICKERS.items():
        print(f"- {ticker}")
        df = yf.download(
            ticker,
            start=DEFAULT_START.isoformat(),
            end=DEFAULT_END.isoformat() if DEFAULT_END else None,
            interval=INTERVAL,
            progress=False,
            auto_adjust=False,
        )

        if df.empty:
            print("  → no data returned")
            continue

        df = df.rename_axis("date")
        df["ticker"] = ticker
        df["series_label"] = label

        output_file = OUTPUT_DIR / f"yfinance_{to_safe_name(label)}.csv"
        ensure_dir(output_file.parent)
        df.to_csv(output_file, index=True)
        print(f"  → saved {len(df):,} rows to {output_file}")

