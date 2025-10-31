from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/kalshi")

MARKET_TICKERS: Dict[str, str] = {
    # ticker: output label
    "KXFEDDECISION-24DEC-C26": "cut_gt_25bps",
    "KXFEDDECISION-24DEC-C25": "cut_25bps",
    "KXFEDDECISION-24DEC-H0": "maintain",
}

REQUEST_TIMEOUT = 30
BATCH_SIZE = 1000
MAX_RECORDS = 200_000


def fetch_trades(
    ticker: str,
    max_records: Optional[int] = MAX_RECORDS,
) -> pd.DataFrame:
    api_url = "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
    all_rows: List[Dict] = []
    cursor: Optional[str] = None

    while True:
        batch_limit = BATCH_SIZE
        if max_records is not None:
            remaining = max_records - len(all_rows)
            if remaining <= 0:
                break
            batch_limit = min(batch_limit, remaining)

        params = {"ticker": ticker, "limit": batch_limit}
        if cursor:
            params["cursor"] = cursor

        response = requests.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        trades: List[Dict] = payload.get("trades", [])
        cursor = payload.get("cursor")

        if not trades:
            break

        all_rows.extend(trades)

        if not cursor:
            break

        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "created_time" in df.columns:
        df["created_time_utc"] = pd.to_datetime(df["created_time"], utc=True)
        df["created_time_ny"] = df["created_time_utc"].dt.tz_convert("America/New_York")

    return df


def export_data() -> None:
    ensure_dir(OUTPUT_DIR)

    print("\n=== Fetching Kalshi trades ===")

    summary = []
    for ticker, label in MARKET_TICKERS.items():
        print(f"- {ticker}")
        df = fetch_trades(ticker)

        if df.empty:
            print("  → no trades found")
            continue

        df = df.copy()
        df["ticker"] = ticker
        df["market_label"] = label

        output_file = OUTPUT_DIR / f"kalshi_{label}.csv"
        safe_write_csv(df, output_file)

        summary.append({"ticker": ticker, "label": label, "rows": len(df)})

    metadata_path = OUTPUT_DIR / "kalshi_metadata.json"
    write_json(metadata_path, {"markets": summary})
    print(f"Saved metadata to {metadata_path}")
