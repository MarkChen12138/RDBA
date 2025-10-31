from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/polymarket")
EVENT_SLUG = "fed-interest-rates-december-2024"

MARKET_LABELS: Dict[str, str] = {
    # slug: output label
    "fed-decreases-interest-rates-by-25-bps-after-december-2024-meeting": "cut_25bps",
    "no-change-in-fed-interest-rates-after-december-2024-meeting": "maintain",
    "fed-decreases-interest-rates-by-50-bps-after-december-2024-meeting": "cut_gt_25bps",
    "fed-decreases-interest-rates-by-75-bps-after-december-2024-meeting": "cut_gt_25bps",
}

REQUEST_TIMEOUT = 30
BATCH_SIZE = 1000
MAX_RECORDS = 200_000


def fetch_event(slug: str) -> Dict:
    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_trades(
    condition_id: str,
    *,
    yes_only: bool = True,
    max_records: Optional[int] = MAX_RECORDS,
) -> pd.DataFrame:
    api_url = "https://data-api.polymarket.com/trades"
    all_rows: List[Dict] = []
    offset = 0
    yes_token_id: Optional[str] = None

    while True:
        batch_limit = BATCH_SIZE
        if max_records is not None:
            remaining = max_records - len(all_rows)
            if remaining <= 0:
                break
            batch_limit = min(batch_limit, remaining)

        params = {"market": condition_id, "limit": batch_limit, "offset": offset}
        response = requests.get(api_url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        page: List[Dict] = response.json()

        if not page:
            break

        if yes_only and yes_token_id is None:
            yes_token_id = _infer_yes_token_id(page)

        if yes_only and yes_token_id:
            page = [trade for trade in page if trade.get("token_id") == yes_token_id]

        all_rows.extend(page)
        offset += batch_limit

        if len(page) < batch_limit:
            break

        time.sleep(0.15)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "timestamp" in df.columns:
        df["utc_time"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["ny_time"] = df["utc_time"].dt.tz_convert("America/New_York")

    return df


def _infer_yes_token_id(trades: Iterable[Dict]) -> Optional[str]:
    for trade in trades:
        if trade.get("price", 0) > 0.5:
            return trade.get("token_id")
    return None


def export_data() -> None:
    ensure_dir(OUTPUT_DIR)

    print("\n=== Fetching Polymarket trades ===")
    event = fetch_event(EVENT_SLUG)
    markets = event.get("markets", [])

    if not markets:
        print("No markets returned for the Polymarket event.")
        return

    selected_markets = [
        market for market in markets if (market.get("slug") or "") in MARKET_LABELS
    ]

    if not selected_markets:
        print("No matching markets found in the Polymarket event.")
        return

    metadata_snapshot = [
        {
            "slug": m.get("slug"),
            "condition_id": m.get("conditionId"),
            "question": m.get("question"),
            "label": MARKET_LABELS.get(m.get("slug") or "", "unknown"),
        }
        for m in selected_markets
    ]
    metadata_path = OUTPUT_DIR / f"{EVENT_SLUG}_metadata.json"
    write_json(
        metadata_path,
        {"event_slug": EVENT_SLUG, "selected_markets": metadata_snapshot},
    )
    print(f"Saved event metadata to {metadata_path}")

    label_to_frames: Dict[str, List[pd.DataFrame]] = {}

    for market in selected_markets:
        slug = market.get("slug") or ""
        condition_id = market.get("conditionId")
        label = MARKET_LABELS[slug]

        if not condition_id:
            continue

        print(f"- {market.get('question', slug)}")
        df = fetch_trades(condition_id)

        if df.empty:
            print("  → no trades found")
            continue

        df = df.copy()
        df["market_slug"] = slug
        df["market_label"] = label
        df["market_question"] = market.get("question", "")
        label_to_frames.setdefault(label, []).append(df)

    if not label_to_frames:
        print("No trades downloaded for the selected Polymarket markets.")
        return

    for label, frames in label_to_frames.items():
        combined = pd.concat(frames, ignore_index=True)
        output_file = OUTPUT_DIR / f"polymarket_{label}.csv"
        safe_write_csv(combined, output_file)
