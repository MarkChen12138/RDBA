"""Yahoo Finance download helper used by Mark for market signals."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
import yfinance as yf

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/yfinance")

# Grouped tickers → friendly labels. These align with the silver table
# expectations documented in README (equity, rates, VIX buckets).
EQUITY_TICKERS: Dict[str, str] = {
    "^GSPC": "sp500_index",
    "^IXIC": "nasdaq_composite",
    "^DJI": "dow_jones_index",
    "XLF": "financials_etf",
    "XLK": "technology_etf",
    "XLY": "consumer_discretionary_etf",
    "XLP": "consumer_staples_etf",
    "XLI": "industrial_etf",
    "XLE": "energy_etf",
    "XLU": "utilities_etf",
}

RATE_TICKERS: Dict[str, str] = {
    "ZQ=F": "fed_funds_futures",
    "^IRX": "3m_treasury_bill",
    "^FVX": "5y_treasury_yield",
    "^TNX": "10y_treasury_yield",
    "^TYX": "30y_treasury_yield",
}

VIX_TICKERS: Dict[str, str] = {
    "^VIX": "cboe_volatility_index",
    "^VVIX": "cboe_vvix_index",
}

# Pull roughly two years of history by default.
DEFAULT_START = date.today() - timedelta(days=730)
DEFAULT_END: Optional[date] = None  # None = up to latest available
INTERVAL = "1d"  # Supported values: 1d, 1h, 5m, etc.


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse MultiIndex columns (yfinance >=0.2.66) to a single level."""
    if isinstance(df.columns, pd.MultiIndex):
        flat_cols = []
        for col in df.columns.to_list():
            if isinstance(col, tuple):
                primary = col[0] if (col[0] not in (None, "")) else col[1]
                fallback = "_".join(str(part) for part in col if part)
                flat_cols.append(primary or fallback)
            else:
                flat_cols.append(col)
        df.columns = flat_cols
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase/underscore price column names for consistency."""
    return df.rename(columns=lambda c: str(c).strip().lower().replace(" ", "_"))


def _to_datestr(value: date | str | None) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.isoformat()


def _download_history(
    ticker: str,
    label: str,
    *,
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    """Download history for a ticker and attach metadata columns."""
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            auto_adjust=False,
        )
    except Exception as exc:  # pragma: no cover - network error surface only
        print(f"  → failed to download {ticker}: {exc}")
        return pd.DataFrame()

    if df.empty:
        return df

    df = df.rename_axis("date").reset_index()
    df = _flatten_columns(df)
    df = _normalize_columns(df)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    result = df.copy()
    result["ticker"] = ticker
    result["series_label"] = label
    return result


def _concat(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _build_equity_table(
    tickers: Dict[str, str],
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    rows = []
    for ticker, label in tickers.items():
        print(f"- {ticker} ({label})")
        df = _download_history(ticker, label, start=start, end=end, interval=interval)
        if df.empty:
            print("  → no data returned")
            continue

        cols = [
            "date",
            "ticker",
            "series_label",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
        ]
        rows.append(df.reindex(columns=cols))
        print(f"  → pulled {len(df):,} rows")

    combined = _concat(rows)
    if not combined.empty:
        combined.sort_values(["ticker", "date"], inplace=True)
    return combined


def _build_rates_table(
    tickers: Dict[str, str],
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    rows = []
    for ticker, label in tickers.items():
        print(f"- {ticker} ({label})")
        df = _download_history(ticker, label, start=start, end=end, interval=interval)
        if df.empty:
            print("  → no data returned")
            continue

        subset = df[["date", "ticker", "series_label", "close"]].rename(
            columns={"close": "value"}
        )
        rows.append(subset)
        print(f"  → pulled {len(df):,} rows")

    combined = _concat(rows)
    if not combined.empty:
        combined.sort_values(["ticker", "date"], inplace=True)
    return combined


def _build_vix_table(
    tickers: Dict[str, str],
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    return _build_equity_table(tickers, start, end, interval)


def export_data(
    *,
    start: date | str | None = DEFAULT_START,
    end: date | str | None = DEFAULT_END,
    interval: str = INTERVAL,
    equity_tickers: Optional[Dict[str, str]] = None,
    rate_tickers: Optional[Dict[str, str]] = None,
    vix_tickers: Optional[Dict[str, str]] = None,
) -> None:
    """Fetch equity, rates, and VIX series from Yahoo Finance."""

    ensure_dir(OUTPUT_DIR)

    equity_map = dict(equity_tickers or EQUITY_TICKERS)
    rate_map = dict(rate_tickers or RATE_TICKERS)
    vix_map = dict(vix_tickers or VIX_TICKERS)

    start_str = _to_datestr(start)
    end_str = _to_datestr(end)

    print("\n=== Fetching Yahoo Finance data ===")
    metadata = {
        "start": start_str,
        "end": end_str,
        "interval": interval,
        "equity_tickers": equity_map,
        "rate_tickers": rate_map,
        "vix_tickers": vix_map,
    }

    metadata_path = OUTPUT_DIR / "yfinance_metadata.json"
    write_json(metadata_path, metadata)
    print(f"Saved metadata to {metadata_path}")

    equity_df = _build_equity_table(equity_map, start_str, end_str, interval)
    if not equity_df.empty:
        safe_write_csv(equity_df, OUTPUT_DIR / "yfinance_equity.csv")
    else:
        print("No equity tickers returned data.")

    rates_df = _build_rates_table(rate_map, start_str, end_str, interval)
    if not rates_df.empty:
        safe_write_csv(rates_df, OUTPUT_DIR / "yfinance_rates.csv")
    else:
        print("No rate tickers returned data.")

    vix_df = _build_vix_table(vix_map, start_str, end_str, interval)
    if not vix_df.empty:
        safe_write_csv(vix_df, OUTPUT_DIR / "yfinance_vix.csv")
    else:
        print("No VIX tickers returned data.")
