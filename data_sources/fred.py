from __future__ import annotations
import os
from pathlib import Path
from typing import Iterable, Optional
import pandas as pd
import requests
from dotenv import load_dotenv
import json
import time


# config
OUTPUT_DIR = Path("./market_data/fred")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES_LIST_URL = "https://api.stlouisfed.org/fred/category/series"
CATEGORY_CHILDREN_URL = "https://api.stlouisfed.org/fred/category/children"

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("FRED_API_KEY")

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def safe_write_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)

def write_json(path: Path, obj: dict):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def _to_safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)

def get_series_ids_from_category(category_id: int) -> list[str]:
    params = {
        "api_key": API_KEY,
        "file_type": "json",
        "category_id": category_id
    }
    r = requests.get(SERIES_LIST_URL, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    return [s["id"] for s in js.get("seriess", [])]


def get_all_series_recursive(category_id: int) -> list[str]:
    visited = set()
    all_series = []

    def dfs(cid: int):
        if cid in visited:
            return
        visited.add(cid)
        print(f"🔍 Searching category {cid}...")

        # series in this category
        series = get_series_ids_from_category(cid)
        all_series.extend(series)

        # children categories
        params = {
            "api_key": API_KEY,
            "file_type": "json",
            "category_id": cid
        }
        r = requests.get(CATEGORY_CHILDREN_URL, params=params, timeout=60)
        r.raise_for_status()
        js = r.json()
        subcats = js.get("categories", [])

        for subcat in subcats:
            dfs(subcat["id"])

    dfs(category_id)
    return all_series

def fetch_series_observations(series_id: str, start: str, end: Optional[str]) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": start,
    }
    if end:
        params["observation_end"] = end

    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()

    obs = pd.DataFrame(js.get("observations", []))
    if obs.empty:
        return obs

    obs = obs.rename(columns={"date": "date_str"})
    obs["date"] = pd.to_datetime(obs["date_str"], utc=True).dt.date
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs["series_id"] = series_id
    return obs[["series_id", "date", "value"]].sort_values("date").reset_index(drop=True)

def export_data(
    series: Optional[Iterable[str]] = None,
    category_id: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    ensure_dir(OUTPUT_DIR)


    if category_id is not None:
        print(f"\nRecursively fetching series from category_id={category_id}")
        series_ids = get_all_series_recursive(category_id)

    elif series is not None:
        series_ids = list(series)

    else:
        raise ValueError("You must specify either series list or category_id")
 
    # metadata
    meta = {
        "start": start,
        "end": end,
        "series": series_ids,
        "base_url": BASE_URL,
        "category_id": category_id,
    }
    write_json(OUTPUT_DIR / "fred_metadata.json", meta)
    print(f"Metadata saved to {OUTPUT_DIR / 'fred_metadata.json'}")

    # fetch & save
    for sid in series_ids:
        print(f"Fetching {sid}...")
        df = fetch_series_observations(sid, start=start, end=end)

        if df.empty:
            print(f"No data for {sid}")
        else:
            out_file = OUTPUT_DIR / f"fred_{_to_safe(sid)}.csv"
            safe_write_csv(df, out_file)
            print(f"Saved {len(df):,} rows to {out_file}")

        time.sleep(1)

    print(f"Total series found: {len(series_ids)}")
