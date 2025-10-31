"""
FRED data source.

Downloads core macro series (CPI, Unemployment, GDP, FEDFUNDS, DGS10,
Target range upper/lower) used for Fed policy analysis.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import requests

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/fred")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
env_path = Path(__file__).resolve().parents[1] / ".env"

load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("FRED_API_KEY")
print("FRED_API_KEY =", API_KEY) 


# series_id -> friendly label (used in filenames & wide columns)
SERIES: Dict[str, str] = {
    "CPIAUCSL": "cpi_all_items",
    "UNRATE": "unemployment_rate",
    "GDPC1": "real_gdp",
    "FEDFUNDS": "effective_fed_funds_rate",
    "DGS10": "treasury_10y_yield",
    "DFEDTARU": "target_range_upper",
    "DFEDTARL": "target_range_lower",
}

# # 10 years/ can be modified
# DEFAULT_START = (date.today() - timedelta(days=365 * 10)).isoformat()
# DEFAULT_END: Optional[str] = None  # None = up to latest


def _request_series(series_id: str, start: str, end: Optional[str]) -> pd.DataFrame:
    """Call FRED observations API and return a tidy DataFrame."""
    if not API_KEY:
        raise RuntimeError(
            "Missing FRED_API_KEY. Set it in environment or .env file."
        )

    params = {
    "series_id": series_id,
    "api_key": API_KEY,
    "file_type": "json",
    }
    if start:
        params["observation_start"] = start
    if end:
        params["observation_end"] = end


    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    obs = pd.DataFrame(js.get("observations", []))
    if obs.empty:
        return obs

    # Normalize fields
    obs = obs.rename(columns={"date": "date_str"})
    obs["date"] = pd.to_datetime(obs["date_str"], utc=True).dt.date
    # FRED uses "." for missing values
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs["series_id"] = series_id
    return obs[["series_id", "date", "value"]].sort_values("date").reset_index(
        drop=True
    )


def _to_safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)


def export_data(
    series: Optional[Iterable[str]] = None,
    start: Optional[str] = None,  
    end: Optional[str] = None,
) -> None:

    """Fetch and export FRED series into CSVs and a wide CSV."""
    ensure_dir(OUTPUT_DIR)

    # metadata
    meta = {
        "start": start,
        "end": end,
        "series": series if series is not None else list(SERIES.keys()),
        "base_url": BASE_URL,
    }
    write_json(OUTPUT_DIR / "fred_metadata.json", meta)
    print("\n=== Fetching FRED data ===")
    print(f"Saved metadata to {OUTPUT_DIR / 'fred_metadata.json'}")

    frames = []
    series_ids = list(series) if series is not None else list(SERIES.keys())

    for sid in series_ids:
        label = SERIES.get(sid, sid.lower())
        print(f"- {sid} ({label})")
        df = _request_series(sid, start=start, end=end)
        if df.empty:
            print("  → no data returned")
            continue

        df["series_label"] = label
        # one CSV per series
        out_file = OUTPUT_DIR / f"fred_{_to_safe(label)}.csv"
        safe_write_csv(df, out_file)
        print(f"  → saved {len(df):,} rows to {out_file}")

        frames.append(df)

    # wide table (date as index, one column per series)
    # if frames:
    #     all_df = pd.concat(frames, ignore_index=True)
    #     wide = (
    #         all_df.pivot_table(index="date", columns="series_label", values="value")
    #         .sort_index()
    #         .reset_index()
    #     )
    #     wide_file = OUTPUT_DIR / "fred_wide.csv"
    #     safe_write_csv(wide, wide_file)
    #     print(f"  → wide table with {wide.shape[0]:,} rows, saved to {wide_file}")
    # else:
    #     print("No data fetched; skip wide table.")
