"""
GDELT DOC 2.0 API data source.

Downloads news articles related to Federal Reserve policy decisions
using the GDELT DOC 2.0 API.

**Rate Limiting:**
GDELT API has rate limits. To avoid 429 errors:
- Use shorter timespan (e.g., "15min" or "1h" for incremental)
- Use fewer queries per run
- Wait at least 5 seconds between queries
- If you get rate limited, wait 10-30 minutes before retrying

**Data Pipeline:**
- Bronze: Raw CSV + metadata in market_data/gdelt/
- Silver: Cleaned Parquet in data/silver/gdelt/dt=YYYY-MM-DD/hour=HH/
- Gold: Feature aggregations in data/gold/gdelt_features/

**Usage:**
    from data_sources import gdelt

    # Default (incremental 15min)
    gdelt.export_data()

    # Backfill mode (longer time range, batched)
    gdelt.backfill_data(start_date="2024-01-01", end_date="2024-01-31")

    # Custom parameters
    gdelt.export_data(
        queries={"fed": '("Federal Reserve" OR FOMC)'},
        timespan="1h",
        maxrecords=250
    )

    # Process to silver/gold layers
    gdelt.process_to_silver()
    gdelt.compute_gold_features()
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from .utils import ensure_dir, safe_write_csv, write_json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output directories
OUTPUT_DIR = Path("./market_data/gdelt")
SILVER_DIR = Path("./data/silver/gdelt")
GOLD_DIR = Path("./data/gold/gdelt_features")

# GDELT DOC 2.0 API endpoint
API_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# =============================================================================
# DEFAULT QUERIES - Fed/FOMC Keywords
# =============================================================================
DEFAULT_QUERIES = {
    # Core Fed/FOMC policy queries
    "fed_fomc": (
        '("Federal Reserve" OR "FOMC" OR "Fed meeting" OR "Fed decision" '
        'OR "Federal Open Market Committee")'
    ),
    "rate_decision": (
        '("rate cut" OR "rate hike" OR "interest rate" OR "rate decision" '
        'OR "rate increase" OR "rate decrease" OR "basis points")'
    ),
    "fed_officials": (
        '("Jerome Powell" OR "Powell" OR "Fed Chair" OR "Fed chairman" '
        'OR "Fed governor" OR "Fed president")'
    ),
    "monetary_policy": (
        '("monetary policy" OR "quantitative easing" OR "QE" OR "tightening" '
        'OR "hawkish" OR "dovish" OR "fed funds rate")'
    ),
}

# Additional queries (can be enabled as needed)
ADDITIONAL_QUERIES = {
    "inflation_fed": (
        '("Federal Reserve" AND (inflation OR CPI OR PCE OR "price stability"))'
    ),
    "employment_fed": (
        '("Federal Reserve" AND (employment OR "labor market" OR unemployment OR jobs))'
    ),
    "treasury_market": (
        '("Treasury yield" OR "Treasury bond" OR "10-year yield" OR "2-year yield" '
        'OR "yield curve" OR "bond market")'
    ),
    "fed_balance_sheet": (
        '("Fed balance sheet" OR "quantitative tightening" OR "QT" '
        'OR "asset purchases" OR "Treasury holdings")'
    ),
}

# =============================================================================
# DEFAULT PARAMETERS
# =============================================================================
DEFAULT_PARAMS = {
    "mode": "artlist",        # Article list mode
    "maxrecords": 250,        # Maximum records to fetch per query
    "timespan": "15min",      # Default to incremental 15-minute pulls
    "sort": "datedesc",       # Sort by date descending
    "format": "json",         # JSON format
}

# Incremental/backfill presets
TIMESPAN_PRESETS = {
    "15min": "15min",         # Real-time incremental
    "1h": "1h",               # Hourly incremental
    "1d": "1d",               # Daily incremental
    "1w": "1w",               # Weekly
    "1month": "1month",       # Monthly
    "3months": "3months",     # Quarterly (use sparingly)
}

# Rate limiting configuration
RATE_LIMIT_CONFIG = {
    "initial_backoff_sec": 10,
    "max_backoff_sec": 120,
    "backoff_multiplier": 2,
    "between_query_wait_sec": 5,
    "max_retries": 5,
}


# =============================================================================
# FETCH FUNCTIONS
# =============================================================================

def fetch_articles(
    query: str,
    timespan: str = "15min",
    maxrecords: int = 250,
    max_retries: int = 5,
) -> Tuple[List[Dict], Dict[str, Any]]:
    """
    Fetch articles from GDELT DOC 2.0 API.

    Args:
        query: Search query string
        timespan: Time range (e.g., "15min", "1h", "1d", "1w")
        maxrecords: Maximum number of records to fetch
        max_retries: Maximum number of retry attempts

    Returns:
        Tuple of (articles list, request metadata dict)
    """
    params = {
        "query": query,
        "mode": DEFAULT_PARAMS["mode"],
        "maxrecords": maxrecords,
        "timespan": timespan,
        "sort": DEFAULT_PARAMS["sort"],
        "format": DEFAULT_PARAMS["format"],
    }

    request_metadata = {
        "query": query,
        "timespan": timespan,
        "maxrecords": maxrecords,
        "request_timestamp": datetime.utcnow().isoformat() + "Z",
        "api_url": API_BASE_URL,
        "params": params,
        "status": "pending",
        "attempts": 0,
        "articles_count": 0,
        "error": None,
    }

    backoff_sec = RATE_LIMIT_CONFIG["initial_backoff_sec"]

    for attempt in range(max_retries):
        request_metadata["attempts"] = attempt + 1

        try:
            logger.info(f"  Attempt {attempt + 1}/{max_retries}: Fetching articles...")
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            articles = data.get("articles", [])

            request_metadata["status"] = "success"
            request_metadata["articles_count"] = len(articles)
            request_metadata["response_timestamp"] = datetime.utcnow().isoformat() + "Z"

            if not articles:
                logger.warning(f"  No articles found for query: {query[:50]}...")
                request_metadata["status"] = "empty_result"
                return [], request_metadata

            logger.info(f"  Fetched {len(articles)} articles")
            return articles, request_metadata

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                # Rate limit - exponential backoff
                logger.warning(
                    f"  Rate limit (429). Waiting {backoff_sec}s before retry "
                    f"{attempt + 1}/{max_retries}..."
                )
                request_metadata["error"] = f"Rate limit 429 at attempt {attempt + 1}"

                if attempt < max_retries - 1:
                    import time
                    time.sleep(backoff_sec)
                    backoff_sec = min(
                        backoff_sec * RATE_LIMIT_CONFIG["backoff_multiplier"],
                        RATE_LIMIT_CONFIG["max_backoff_sec"]
                    )
                else:
                    logger.error(f"  Rate limit exceeded after {max_retries} attempts")
                    logger.info(
                        "  Tip: Try again later or use shorter timespan "
                        "(e.g., '15min' instead of '1h')"
                    )
                    request_metadata["status"] = "rate_limited"
                    return [], request_metadata
            else:
                status_code = e.response.status_code if e.response else "unknown"
                logger.warning(f"  HTTP error {status_code}: {e}")
                request_metadata["error"] = f"HTTP {status_code}: {str(e)}"
                request_metadata["status"] = "http_error"
                return [], request_metadata

        except requests.exceptions.Timeout as e:
            logger.warning(f"  Timeout on attempt {attempt + 1}: {e}")
            request_metadata["error"] = f"Timeout: {str(e)}"
            if attempt < max_retries - 1:
                import time
                time.sleep(3 ** attempt)
            else:
                request_metadata["status"] = "timeout"
                return [], request_metadata

        except requests.exceptions.RequestException as e:
            logger.warning(f"  Request error on attempt {attempt + 1}: {e}")
            request_metadata["error"] = f"Request error: {str(e)}"
            if attempt < max_retries - 1:
                import time
                time.sleep(3 ** attempt)
            else:
                request_metadata["status"] = "request_error"
                return [], request_metadata

        except Exception as e:
            logger.error(f"  Unexpected error: {e}")
            request_metadata["error"] = f"Unexpected: {str(e)}"
            request_metadata["status"] = "error"
            return [], request_metadata

    return [], request_metadata


def articles_to_dataframe(articles: List[Dict], query_label: str) -> pd.DataFrame:
    """
    Convert list of articles to a pandas DataFrame.

    Args:
        articles: List of article dictionaries from GDELT API
        query_label: Label for the query that generated these articles

    Returns:
        DataFrame with article data, including parsed timestamps and deduplication
    """
    if not articles:
        return pd.DataFrame()

    # Extract relevant fields
    records = []
    for article in articles:
        # Generate unique ID for deduplication
        url = article.get("url", "")
        seendate = article.get("seendate", "")
        unique_id = hashlib.md5(f"{url}|{seendate}".encode()).hexdigest()[:16]

        record = {
            "article_id": unique_id,
            "query_label": query_label,
            "url": url,
            "url_mobile": article.get("url_mobile", ""),
            "title": article.get("title", ""),
            "seendate": seendate,
            "domain": article.get("domain", ""),
            "language": article.get("language", ""),
            "sourcecountry": article.get("sourcecountry", ""),
            "socialimage": article.get("socialimage", ""),
            # Additional fields if available
            "tone": article.get("tone", None),
            "themes": article.get("themes", ""),
            "locations": article.get("locations", ""),
            "persons": article.get("persons", ""),
            "organizations": article.get("organizations", ""),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # Parse seendate to datetime (UTC)
    if "seendate" in df.columns and not df.empty:
        df["seendate_parsed"] = pd.to_datetime(
            df["seendate"],
            format="%Y%m%dT%H%M%SZ",
            errors="coerce",
            utc=True
        )

        # Add partition columns for silver layer
        df["dt"] = df["seendate_parsed"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["seendate_parsed"].dt.strftime("%H")

    return df


# =============================================================================
# BRONZE LAYER - Raw Data Export
# =============================================================================

def export_data(
    queries: Optional[Dict[str, str]] = None,
    timespan: str = "15min",
    maxrecords: int = 250,
    output_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Fetch and export GDELT news data to bronze layer.

    Args:
        queries: Dictionary of query_label -> query_string
        timespan: Time range for articles (e.g., "15min", "1h", "1d")
        maxrecords: Maximum records per query
        output_dir: Custom output directory (default: market_data/gdelt/)

    Returns:
        Combined DataFrame of all fetched articles
    """
    import time

    if output_dir is None:
        output_dir = OUTPUT_DIR
    ensure_dir(output_dir)

    logger.info("\n=== Fetching GDELT DOC 2.0 data (Bronze Layer) ===")
    logger.info(f"Timespan: {timespan}, Max records: {maxrecords}")

    if queries is None:
        queries = DEFAULT_QUERIES

    # Metadata for this fetch run
    fetch_timestamp = datetime.utcnow()
    metadata = {
        "fetch_timestamp": fetch_timestamp.isoformat() + "Z",
        "timespan": timespan,
        "maxrecords": maxrecords,
        "queries": queries,
        "api_base_url": API_BASE_URL,
        "query_results": {},
        "total_articles": 0,
        "unique_articles": 0,
        "status": "running",
    }

    # Save initial metadata
    metadata_path = output_dir / "gdelt_metadata.json"
    write_json(metadata_path, metadata)

    # Fetch data for each query
    all_articles = []
    total_queries = len(queries)

    for idx, (query_label, query_string) in enumerate(queries.items(), 1):
        logger.info(f"\n[{idx}/{total_queries}] Fetching: {query_label}")
        logger.info(f"   Query: {query_string[:80]}...")

        articles, req_metadata = fetch_articles(
            query=query_string,
            timespan=timespan,
            maxrecords=maxrecords,
        )

        metadata["query_results"][query_label] = req_metadata

        if articles:
            df = articles_to_dataframe(articles, query_label)

            if not df.empty:
                # Save individual query results
                timestamp_str = fetch_timestamp.strftime("%Y%m%d_%H%M%S")
                output_file = output_dir / f"gdelt_{query_label}_{timestamp_str}.csv"
                safe_write_csv(df, output_file)

                all_articles.append(df)

        # Rate limiting - wait between queries
        if idx < total_queries:
            wait_time = RATE_LIMIT_CONFIG["between_query_wait_sec"]
            logger.info(f"   Waiting {wait_time}s before next query...")
            time.sleep(wait_time)

    # Combine and deduplicate results
    if all_articles:
        combined_df = pd.concat(all_articles, ignore_index=True)

        # Deduplicate by article_id
        original_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=["article_id"], keep="first")
        dedup_count = len(combined_df)

        if original_count > dedup_count:
            logger.info(f"   Deduplicated: {original_count} -> {dedup_count} articles")

        # Sort by date
        if "seendate_parsed" in combined_df.columns:
            combined_df = combined_df.sort_values("seendate_parsed", ascending=False)

        # Save combined results
        timestamp_str = fetch_timestamp.strftime("%Y%m%d_%H%M%S")
        combined_output = output_dir / f"gdelt_combined_{timestamp_str}.csv"
        safe_write_csv(combined_df, combined_output)

        # Also save latest combined (for easy access)
        latest_output = output_dir / "gdelt_combined_latest.csv"
        safe_write_csv(combined_df, latest_output)

        # Update metadata
        metadata["total_articles"] = original_count
        metadata["unique_articles"] = dedup_count
        metadata["status"] = "success"

        # Statistics
        logger.info("\n=== Summary Statistics ===")
        logger.info(f"   Total articles fetched: {original_count:,}")
        logger.info(f"   Unique articles: {dedup_count:,}")
        logger.info(f"   Unique domains: {combined_df['domain'].nunique():,}")
        logger.info(f"   Languages: {combined_df['language'].nunique()}")
        logger.info(f"   Countries: {combined_df['sourcecountry'].nunique()}")

        if "seendate_parsed" in combined_df.columns:
            date_range = combined_df["seendate_parsed"].dropna()
            if not date_range.empty:
                logger.info(
                    f"   Date range: {date_range.min()} to {date_range.max()}"
                )
    else:
        combined_df = pd.DataFrame()
        metadata["status"] = "no_data"
        logger.warning("\nNo articles fetched from any query")

    # Save final metadata
    write_json(metadata_path, metadata)
    logger.info(f"\nMetadata saved to {metadata_path}")

    return combined_df


# =============================================================================
# BACKFILL FUNCTION - Batch Historical Data
# =============================================================================

def backfill_data(
    start_date: str,
    end_date: str,
    queries: Optional[Dict[str, str]] = None,
    batch_hours: int = 24,
    maxrecords: int = 250,
) -> pd.DataFrame:
    """
    Backfill historical GDELT data in batches.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        queries: Dictionary of query_label -> query_string
        batch_hours: Hours per batch (default 24)
        maxrecords: Maximum records per query per batch

    Returns:
        Combined DataFrame of all backfilled articles
    """
    import time

    logger.info("\n=== GDELT Backfill Mode ===")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Batch size: {batch_hours} hours")

    if queries is None:
        queries = DEFAULT_QUERIES

    # Parse dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Calculate batches
    batches = []
    current_dt = start_dt
    while current_dt < end_dt:
        batch_end = min(current_dt + timedelta(hours=batch_hours), end_dt)
        batches.append((current_dt, batch_end))
        current_dt = batch_end

    logger.info(f"Total batches: {len(batches)}")

    all_results = []
    backfill_dir = OUTPUT_DIR / "backfill"
    ensure_dir(backfill_dir)

    for batch_idx, (batch_start, batch_end) in enumerate(batches, 1):
        logger.info(f"\n--- Batch {batch_idx}/{len(batches)} ---")
        logger.info(f"Period: {batch_start} to {batch_end}")

        # Calculate timespan for this batch
        hours_diff = int((batch_end - batch_start).total_seconds() / 3600)
        timespan = f"{hours_diff}h"

        # For GDELT, we need to add date filters to the query
        date_str = batch_start.strftime("%Y%m%d%H%M%S")

        for query_label, query_string in queries.items():
            # GDELT uses startdatetime and enddatetime params
            # But DOC 2.0 API uses timespan, so we adjust query
            logger.info(f"  Fetching: {query_label}")

            articles, req_metadata = fetch_articles(
                query=query_string,
                timespan=timespan,
                maxrecords=maxrecords,
            )

            if articles:
                df = articles_to_dataframe(articles, query_label)
                if not df.empty:
                    all_results.append(df)

            # Rate limiting
            time.sleep(RATE_LIMIT_CONFIG["between_query_wait_sec"])

        # Wait longer between batches
        if batch_idx < len(batches):
            logger.info("  Waiting 10s before next batch...")
            time.sleep(10)

    # Combine results
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["article_id"], keep="first")

        if "seendate_parsed" in combined_df.columns:
            combined_df = combined_df.sort_values("seendate_parsed", ascending=False)

        # Save backfill results
        output_file = backfill_dir / f"gdelt_backfill_{start_date}_{end_date}.csv"
        safe_write_csv(combined_df, output_file)

        logger.info(f"\nBackfill complete: {len(combined_df):,} unique articles")
        return combined_df

    logger.warning("\nNo articles fetched during backfill")
    return pd.DataFrame()


# =============================================================================
# SILVER LAYER - Data Cleaning & Transformation
# =============================================================================

def clean_articles_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize article DataFrame for silver layer.

    Args:
        df: Raw DataFrame from bronze layer

    Returns:
        Cleaned DataFrame ready for silver layer
    """
    if df.empty:
        return df

    # Make a copy
    df = df.copy()

    # 1. Handle null values
    string_cols = ["url", "title", "domain", "language", "sourcecountry",
                   "themes", "locations", "persons", "organizations"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    # 2. Parse seendate if not already parsed or is string type (from CSV)
    if "seendate_parsed" in df.columns:
        # Convert to datetime if it's a string (from CSV reload)
        if df["seendate_parsed"].dtype == "object":
            df["seendate_parsed"] = pd.to_datetime(
                df["seendate_parsed"],
                errors="coerce",
                utc=True
            )
    elif "seendate" in df.columns:
        df["seendate_parsed"] = pd.to_datetime(
            df["seendate"],
            format="%Y%m%dT%H%M%SZ",
            errors="coerce",
            utc=True
        )

    # 3. Ensure UTC timezone
    if "seendate_parsed" in df.columns and not df["seendate_parsed"].empty:
        try:
            if df["seendate_parsed"].dt.tz is None:
                df["seendate_parsed"] = df["seendate_parsed"].dt.tz_localize("UTC")
            elif str(df["seendate_parsed"].dt.tz) != "UTC":
                df["seendate_parsed"] = df["seendate_parsed"].dt.tz_convert("UTC")
        except Exception:
            # If timezone handling fails, try to parse again
            df["seendate_parsed"] = pd.to_datetime(
                df["seendate_parsed"], errors="coerce", utc=True
            )

    # 4. Add partition columns
    if "seendate_parsed" in df.columns:
        df["dt"] = df["seendate_parsed"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["seendate_parsed"].dt.strftime("%H")
        df["ts_utc"] = df["seendate_parsed"]  # Alias for cross-source joins

    # 5. Standardize language codes (lowercase)
    if "language" in df.columns:
        df["language"] = df["language"].str.lower().str.strip()

    # 6. Standardize country codes (uppercase)
    if "sourcecountry" in df.columns:
        df["sourcecountry"] = df["sourcecountry"].str.upper().str.strip()

    # 7. Clean domain (lowercase, strip)
    if "domain" in df.columns:
        df["domain"] = df["domain"].str.lower().str.strip()

    # 8. Handle tone (ensure numeric)
    if "tone" in df.columns:
        df["tone"] = pd.to_numeric(df["tone"], errors="coerce")

    # 9. Deduplicate
    df = df.drop_duplicates(subset=["article_id"], keep="first")

    # 10. Remove rows with invalid timestamps
    if "seendate_parsed" in df.columns:
        df = df.dropna(subset=["seendate_parsed"])

    return df


def process_to_silver(
    input_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
) -> Dict[str, int]:
    """
    Process bronze data to silver layer with partitioning.

    Args:
        input_path: Path to bronze CSV file (default: latest combined)
        output_dir: Output directory for silver data

    Returns:
        Dictionary with processing statistics
    """
    if output_dir is None:
        output_dir = SILVER_DIR

    logger.info("\n=== Processing to Silver Layer ===")

    # Find input file
    if input_path is None:
        input_path = OUTPUT_DIR / "gdelt_combined_latest.csv"

    if not input_path.exists():
        logger.warning(f"Input file not found: {input_path}")
        return {"status": "no_input", "records": 0}

    # Load bronze data
    logger.info(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    original_count = len(df)
    logger.info(f"Loaded {original_count:,} records")

    # Clean data
    df = clean_articles_df(df)
    cleaned_count = len(df)
    logger.info(f"After cleaning: {cleaned_count:,} records")

    if df.empty:
        logger.warning("No valid records after cleaning")
        return {"status": "empty_after_cleaning", "records": 0}

    # Write partitioned data
    ensure_dir(output_dir)
    partitions_written = 0

    for (dt, hour), group in df.groupby(["dt", "hour"]):
        partition_dir = output_dir / f"dt={dt}" / f"hour={hour}"
        ensure_dir(partition_dir)

        # Write as Parquet with Snappy compression
        parquet_path = partition_dir / "articles.parquet"
        group.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        partitions_written += 1

    logger.info(f"Written {partitions_written} partitions to {output_dir}")

    # Write silver metadata
    silver_metadata = {
        "process_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_file": str(input_path),
        "original_records": original_count,
        "cleaned_records": cleaned_count,
        "partitions_written": partitions_written,
        "date_range": {
            "min": df["dt"].min() if "dt" in df.columns else None,
            "max": df["dt"].max() if "dt" in df.columns else None,
        }
    }
    write_json(output_dir / "silver_metadata.json", silver_metadata)

    return {
        "status": "success",
        "original_records": original_count,
        "cleaned_records": cleaned_count,
        "partitions_written": partitions_written,
    }


# =============================================================================
# GOLD LAYER - Feature Engineering
# =============================================================================

def compute_gold_features(
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    window_minutes: int = 15,
) -> pd.DataFrame:
    """
    Compute gold layer features: news intensity and sentiment aggregations.

    Features computed per 15-minute window:
    - article_count: Number of articles
    - unique_domains: Number of unique news sources
    - avg_tone: Average sentiment tone
    - tone_std: Sentiment volatility
    - top_domains: Most active news domains
    - top_countries: Most active source countries
    - english_ratio: Ratio of English articles

    Args:
        input_dir: Silver data directory
        output_dir: Gold features output directory
        window_minutes: Aggregation window in minutes (default: 15)

    Returns:
        DataFrame with computed features
    """
    if input_dir is None:
        input_dir = SILVER_DIR
    if output_dir is None:
        output_dir = GOLD_DIR

    logger.info("\n=== Computing Gold Layer Features ===")
    logger.info(f"Window size: {window_minutes} minutes")

    # Load all silver data
    dfs = []
    parquet_files = list(input_dir.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {input_dir}")
        # Try loading from bronze
        bronze_file = OUTPUT_DIR / "gdelt_combined_latest.csv"
        if bronze_file.exists():
            logger.info("Loading from bronze layer instead...")
            df = pd.read_csv(bronze_file)
            df = clean_articles_df(df)
            dfs.append(df)
        else:
            return pd.DataFrame()
    else:
        for pf in parquet_files:
            dfs.append(pd.read_parquet(pf))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(df):,} records for feature computation")

    # Ensure timestamp column
    if "ts_utc" not in df.columns:
        if "seendate_parsed" in df.columns:
            df["ts_utc"] = pd.to_datetime(df["seendate_parsed"], utc=True)
        else:
            logger.error("No timestamp column found")
            return pd.DataFrame()

    # Ensure ts_utc is datetime
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    # Create window column (floor to window_minutes)
    df["ts_window"] = df["ts_utc"].dt.floor(f"{window_minutes}min")

    # Aggregate features by window
    features = []

    for window, group in df.groupby("ts_window"):
        feature_row = {
            # Time key for cross-source joins (UTC aligned)
            "ts": window,
            "ts_utc": window,

            # Article count / intensity
            "article_count": len(group),
            "unique_domains": group["domain"].nunique(),
            "unique_countries": group["sourcecountry"].nunique(),

            # Language distribution
            "english_count": (group["language"] == "english").sum(),
            "english_ratio": (
                (group["language"] == "english").sum() / len(group)
                if len(group) > 0 else 0
            ),

            # Sentiment (tone) features
            "avg_tone": group["tone"].mean() if "tone" in group.columns else None,
            "tone_std": group["tone"].std() if "tone" in group.columns else None,
            "tone_min": group["tone"].min() if "tone" in group.columns else None,
            "tone_max": group["tone"].max() if "tone" in group.columns else None,

            # Top domains (most frequent)
            "top_domains": ",".join(
                group["domain"].value_counts().head(5).index.tolist()
            ),

            # Top countries
            "top_countries": ",".join(
                group["sourcecountry"].value_counts().head(5).index.tolist()
            ),

            # Query distribution
            "query_labels": ",".join(
                group["query_label"].unique().tolist()
                if "query_label" in group.columns else []
            ),
        }
        features.append(feature_row)

    features_df = pd.DataFrame(features)

    if features_df.empty:
        logger.warning("No features computed")
        return features_df

    # Sort by timestamp
    features_df = features_df.sort_values("ts").reset_index(drop=True)

    # Add derived features
    # News shock indicator (high activity spike)
    if len(features_df) > 1:
        rolling_mean = features_df["article_count"].rolling(4, min_periods=1).mean()
        rolling_std = features_df["article_count"].rolling(4, min_periods=1).std().fillna(1)
        features_df["news_shock"] = (
            (features_df["article_count"] - rolling_mean) / rolling_std.replace(0, 1)
        )
        features_df["news_shock"] = features_df["news_shock"].fillna(0)
    else:
        features_df["news_shock"] = 0

    # Add partition columns
    features_df["dt"] = features_df["ts"].dt.strftime("%Y-%m-%d")
    features_df["hour"] = features_df["ts"].dt.strftime("%H")

    # Save features
    ensure_dir(output_dir)

    # Save as Parquet
    parquet_path = output_dir / "gdelt_features.parquet"
    features_df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"Saved features to {parquet_path}")

    # Also save as CSV for easy inspection
    csv_path = output_dir / "gdelt_features.csv"
    features_df.to_csv(csv_path, index=False)
    logger.info(f"Saved features to {csv_path}")

    # Feature metadata
    feature_metadata = {
        "compute_timestamp": datetime.utcnow().isoformat() + "Z",
        "window_minutes": window_minutes,
        "total_windows": len(features_df),
        "total_articles": df["article_count"].sum() if "article_count" in df.columns else len(df),
        "date_range": {
            "min": str(features_df["ts"].min()),
            "max": str(features_df["ts"].max()),
        },
        "features": list(features_df.columns),
    }
    write_json(output_dir / "gold_metadata.json", feature_metadata)

    # Summary statistics
    logger.info("\n=== Gold Layer Summary ===")
    logger.info(f"Total time windows: {len(features_df)}")
    logger.info(f"Date range: {features_df['ts'].min()} to {features_df['ts'].max()}")
    logger.info(f"Avg articles per window: {features_df['article_count'].mean():.1f}")
    logger.info(f"Max articles in window: {features_df['article_count'].max()}")

    return features_df


# =============================================================================
# CROSS-SOURCE FUSION SUPPORT
# =============================================================================

def get_aligned_features(
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    window_minutes: int = 15,
) -> pd.DataFrame:
    """
    Get GDELT features aligned to UTC time windows for cross-source joining.

    This function returns features ready to join with other data sources
    (Polymarket, Kalshi, yfinance, FRED) using the 'ts' column as the join key.

    Args:
        start_ts: Start timestamp (UTC)
        end_ts: End timestamp (UTC)
        window_minutes: Time window for alignment (default: 15 minutes)

    Returns:
        DataFrame with ts column aligned to specified window
    """
    features_path = GOLD_DIR / "gdelt_features.parquet"

    if not features_path.exists():
        logger.warning("Gold features not found. Computing from available data...")
        features_df = compute_gold_features(window_minutes=window_minutes)
    else:
        features_df = pd.read_parquet(features_path)

    if features_df.empty:
        return features_df

    # Ensure ts is datetime with UTC
    features_df["ts"] = pd.to_datetime(features_df["ts"], utc=True)

    # Filter by time range if specified
    if start_ts is not None:
        if start_ts.tzinfo is None:
            start_ts = start_ts.replace(tzinfo=pd.Timestamp.now("UTC").tzinfo)
        features_df = features_df[features_df["ts"] >= start_ts]

    if end_ts is not None:
        if end_ts.tzinfo is None:
            end_ts = end_ts.replace(tzinfo=pd.Timestamp.now("UTC").tzinfo)
        features_df = features_df[features_df["ts"] <= end_ts]

    return features_df


def create_news_shock_features(
    features_df: pd.DataFrame,
    lookback_windows: List[int] = [1, 4, 12, 24],
) -> pd.DataFrame:
    """
    Create enhanced news shock features for model consumption.

    Args:
        features_df: Gold layer features DataFrame
        lookback_windows: List of lookback windows (in number of 15-min periods)

    Returns:
        DataFrame with additional shock features
    """
    if features_df.empty:
        return features_df

    df = features_df.copy()

    for lookback in lookback_windows:
        window_label = f"{lookback * 15}min" if lookback < 4 else f"{lookback // 4}h"

        # Rolling article count
        df[f"article_count_ma_{window_label}"] = (
            df["article_count"].rolling(lookback, min_periods=1).mean()
        )

        # News intensity change
        df[f"article_count_change_{window_label}"] = (
            df["article_count"] - df[f"article_count_ma_{window_label}"]
        )

        # Tone momentum (if available)
        if "avg_tone" in df.columns:
            df[f"tone_ma_{window_label}"] = (
                df["avg_tone"].rolling(lookback, min_periods=1).mean()
            )
            df[f"tone_change_{window_label}"] = (
                df["avg_tone"] - df[f"tone_ma_{window_label}"]
            )

    return df


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def run_full_pipeline(
    timespan: str = "15min",
    maxrecords: int = 250,
    compute_features: bool = True,
) -> Dict[str, Any]:
    """
    Run the full GDELT pipeline: Bronze -> Silver -> Gold.

    Args:
        timespan: Time range for article fetch
        maxrecords: Maximum records per query
        compute_features: Whether to compute gold features

    Returns:
        Dictionary with pipeline results
    """
    results = {
        "bronze": None,
        "silver": None,
        "gold": None,
    }

    # Bronze: Fetch raw data
    logger.info("=" * 60)
    logger.info("STEP 1: Bronze Layer - Fetching Raw Data")
    logger.info("=" * 60)
    bronze_df = export_data(timespan=timespan, maxrecords=maxrecords)
    results["bronze"] = {
        "records": len(bronze_df),
        "status": "success" if not bronze_df.empty else "no_data"
    }

    if bronze_df.empty:
        logger.warning("No data fetched. Pipeline stopped.")
        return results

    # Silver: Clean and partition
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Silver Layer - Cleaning & Partitioning")
    logger.info("=" * 60)
    silver_stats = process_to_silver()
    results["silver"] = silver_stats

    # Gold: Feature engineering
    if compute_features:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: Gold Layer - Feature Engineering")
        logger.info("=" * 60)
        gold_df = compute_gold_features()
        results["gold"] = {
            "windows": len(gold_df),
            "status": "success" if not gold_df.empty else "no_features"
        }

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)

    return results


# For backwards compatibility
def fetch_data(*args, **kwargs):
    """Alias for export_data for backwards compatibility."""
    return export_data(*args, **kwargs)
