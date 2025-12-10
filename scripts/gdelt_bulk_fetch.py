#!/usr/bin/env python3
"""
GDELT Bulk Data Fetcher - For GB-scale data collection

This script is designed to run on NYU HPC Dataproc for large-scale data collection.
It fetches months of GDELT news data related to Federal Reserve/FOMC.

Usage:
    # On NYU HPC:
    python scripts/gdelt_bulk_fetch.py --months 6

    # Local testing (smaller scale):
    python scripts/gdelt_bulk_fetch.py --days 7

    # Custom date range:
    python scripts/gdelt_bulk_fetch.py --start 2024-06-01 --end 2024-12-01

Estimated data sizes:
    - 1 week:   ~5,000-10,000 articles (~10-20 MB)
    - 1 month:  ~20,000-50,000 articles (~50-100 MB)
    - 3 months: ~100,000-200,000 articles (~200-500 MB)
    - 6 months: ~300,000-500,000 articles (~500 MB - 1 GB)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from data_sources import gdelt
from data_sources.utils import ensure_dir, write_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output directory for bulk data
BULK_OUTPUT_DIR = Path("./market_data/gdelt/bulk")

# Extended queries for comprehensive Fed/FOMC coverage
# Using simpler query patterns for better GDELT API compatibility
# NOTE: Avoid 'OR' patterns - they cause API errors
ALL_QUERIES = {
    # Core queries - simplified for reliability
    "federal_reserve": 'Federal Reserve',
    "fomc": 'FOMC',
    "rate_decision": 'interest rate decision',
    "rate_hike": 'rate hike',
    "rate_cut": 'rate cut',
    "powell": 'Jerome Powell',
    "fed_chair": 'Fed chairman',
    "monetary_policy": 'monetary policy',
    "inflation": 'inflation CPI',
    "treasury_yield": 'Treasury yield',
    "treasury_bond": 'Treasury bond',
    "central_bank": 'central bank policy',
    "economic_outlook": 'economic outlook growth',
    "quantitative": 'quantitative easing',
    "hawkish": 'hawkish dovish',
    "fed_statement": 'Fed statement',
}


def fetch_day_batch(
    date: datetime,
    queries: dict,
    maxrecords: int = 250,
    output_dir: Path = BULK_OUTPUT_DIR,
) -> dict:
    """
    Fetch all articles for a single day.

    Args:
        date: The date to fetch
        queries: Dictionary of query_label -> query_string
        maxrecords: Max records per query
        output_dir: Output directory

    Returns:
        Statistics dictionary
    """
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"\n{'='*60}")
    logger.info(f"Fetching data for {date_str}")
    logger.info(f"{'='*60}")

    day_dir = output_dir / f"dt={date_str}"
    ensure_dir(day_dir)

    all_articles = []
    stats = {
        "date": date_str,
        "queries": {},
        "total_fetched": 0,
        "unique_articles": 0,
        "status": "running",
    }

    for idx, (query_label, query_string) in enumerate(queries.items(), 1):
        logger.info(f"  [{idx}/{len(queries)}] {query_label}")

        try:
            articles, req_metadata = gdelt.fetch_articles(
                query=query_string,
                timespan="1d",  # Full day
                maxrecords=maxrecords,
            )

            stats["queries"][query_label] = {
                "count": len(articles),
                "status": req_metadata.get("status", "unknown"),
            }

            if articles:
                df = gdelt.articles_to_dataframe(articles, query_label)
                if not df.empty:
                    all_articles.append(df)
                    logger.info(f"      Fetched {len(df)} articles")
            else:
                logger.warning(f"      No articles")

        except Exception as e:
            logger.error(f"      Error: {e}")
            stats["queries"][query_label] = {"count": 0, "status": "error", "error": str(e)}

        # Rate limiting between queries
        if idx < len(queries):
            time.sleep(3)  # 3 seconds between queries

    # Combine and deduplicate
    if all_articles:
        combined_df = pd.concat(all_articles, ignore_index=True)
        stats["total_fetched"] = len(combined_df)

        # Deduplicate
        combined_df = combined_df.drop_duplicates(subset=["article_id"], keep="first")
        stats["unique_articles"] = len(combined_df)

        # Sort by date
        if "seendate_parsed" in combined_df.columns:
            combined_df = combined_df.sort_values("seendate_parsed", ascending=False)

        # Save daily file
        output_file = day_dir / "articles.csv"
        combined_df.to_csv(output_file, index=False)
        logger.info(f"  Saved {len(combined_df)} unique articles to {output_file}")

        stats["status"] = "success"
        stats["file"] = str(output_file)
    else:
        stats["status"] = "no_data"
        logger.warning(f"  No articles for {date_str}")

    # Save daily metadata
    meta_file = day_dir / "metadata.json"
    write_json(meta_file, stats)

    return stats


def bulk_fetch(
    start_date: datetime,
    end_date: datetime,
    queries: dict = None,
    maxrecords: int = 250,
    output_dir: Path = BULK_OUTPUT_DIR,
) -> dict:
    """
    Bulk fetch GDELT data for a date range.

    Args:
        start_date: Start date
        end_date: End date
        queries: Queries to use (default: ALL_QUERIES)
        maxrecords: Max records per query
        output_dir: Output directory

    Returns:
        Overall statistics
    """
    if queries is None:
        queries = ALL_QUERIES

    ensure_dir(output_dir)

    # Calculate days
    total_days = (end_date - start_date).days + 1

    logger.info("=" * 70)
    logger.info("GDELT BULK FETCH")
    logger.info("=" * 70)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total days: {total_days}")
    logger.info(f"Queries: {len(queries)}")
    logger.info(f"Max records per query: {maxrecords}")
    logger.info(f"Estimated max articles: {total_days * len(queries) * maxrecords:,}")
    logger.info("=" * 70)

    overall_stats = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "total_days": total_days,
        "queries_count": len(queries),
        "daily_stats": [],
        "total_articles": 0,
        "unique_articles": 0,
        "status": "running",
        "start_timestamp": datetime.utcnow().isoformat() + "Z",
    }

    # Fetch day by day
    current_date = start_date
    while current_date <= end_date:
        try:
            day_stats = fetch_day_batch(
                date=current_date,
                queries=queries,
                maxrecords=maxrecords,
                output_dir=output_dir,
            )
            overall_stats["daily_stats"].append(day_stats)
            overall_stats["total_articles"] += day_stats.get("total_fetched", 0)
            overall_stats["unique_articles"] += day_stats.get("unique_articles", 0)

        except Exception as e:
            logger.error(f"Error fetching {current_date}: {e}")
            overall_stats["daily_stats"].append({
                "date": current_date.strftime("%Y-%m-%d"),
                "status": "error",
                "error": str(e),
            })

        # Wait between days to avoid rate limiting
        current_date += timedelta(days=1)
        if current_date <= end_date:
            logger.info("Waiting 10s before next day...")
            time.sleep(10)

    # Final stats
    overall_stats["status"] = "completed"
    overall_stats["end_timestamp"] = datetime.utcnow().isoformat() + "Z"

    # Save overall metadata
    meta_file = output_dir / "bulk_metadata.json"
    write_json(meta_file, overall_stats)

    # Combine all daily files into one
    logger.info("\n" + "=" * 70)
    logger.info("COMBINING ALL DATA")
    logger.info("=" * 70)

    all_files = list(output_dir.glob("dt=*/articles.csv"))
    if all_files:
        dfs = [pd.read_csv(f) for f in all_files]
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.drop_duplicates(subset=["article_id"], keep="first")

        # Save combined file
        combined_file = output_dir / "gdelt_bulk_combined.csv"
        combined.to_csv(combined_file, index=False)

        # Calculate file size
        file_size_mb = combined_file.stat().st_size / (1024 * 1024)

        logger.info(f"Combined file: {combined_file}")
        logger.info(f"Total unique articles: {len(combined):,}")
        logger.info(f"File size: {file_size_mb:.1f} MB")

        overall_stats["combined_file"] = str(combined_file)
        overall_stats["combined_articles"] = len(combined)
        overall_stats["file_size_mb"] = round(file_size_mb, 2)

        # Update metadata
        write_json(meta_file, overall_stats)

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("BULK FETCH COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Days processed: {len(overall_stats['daily_stats'])}")
    logger.info(f"Total articles: {overall_stats['total_articles']:,}")
    logger.info(f"Unique articles: {overall_stats.get('combined_articles', overall_stats['unique_articles']):,}")
    if "file_size_mb" in overall_stats:
        logger.info(f"Data size: {overall_stats['file_size_mb']:.1f} MB")
    logger.info("=" * 70)

    return overall_stats


def main():
    parser = argparse.ArgumentParser(
        description="GDELT Bulk Data Fetcher for GB-scale collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/gdelt_bulk_fetch.py --months 6     # 6 months of data (~1 GB)
  python scripts/gdelt_bulk_fetch.py --months 3     # 3 months (~500 MB)
  python scripts/gdelt_bulk_fetch.py --days 30      # 30 days (~100 MB)
  python scripts/gdelt_bulk_fetch.py --days 7       # 7 days for testing (~20 MB)
  python scripts/gdelt_bulk_fetch.py --start 2024-06-01 --end 2024-12-01
        """,
    )

    parser.add_argument(
        "--months", type=int,
        help="Number of months to fetch (from today backwards)"
    )
    parser.add_argument(
        "--days", type=int,
        help="Number of days to fetch (from today backwards)"
    )
    parser.add_argument(
        "--start", type=str,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--maxrecords", type=int, default=250,
        help="Max records per query (default: 250)"
    )
    parser.add_argument(
        "--output", type=str, default=str(BULK_OUTPUT_DIR),
        help="Output directory"
    )
    parser.add_argument(
        "--core-only", action="store_true",
        help="Use only core queries (4 instead of 8)"
    )

    args = parser.parse_args()

    # Determine date range
    end_date = datetime.now()

    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    elif args.months:
        start_date = end_date - timedelta(days=args.months * 30)
    elif args.days:
        start_date = end_date - timedelta(days=args.days)
    else:
        # Default: 7 days for testing
        logger.info("No date range specified. Using default 7 days for testing.")
        start_date = end_date - timedelta(days=7)

    # Select queries
    if args.core_only:
        # Use simpler core set for faster fetching
        queries = {
            "federal_reserve": 'Federal Reserve',
            "interest_rate": 'interest rate',
            "fed_chair": 'Powell OR Fed Chair',
            "monetary": 'monetary policy',
        }
    else:
        queries = ALL_QUERIES

    # Run bulk fetch
    stats = bulk_fetch(
        start_date=start_date,
        end_date=end_date,
        queries=queries,
        maxrecords=args.maxrecords,
        output_dir=Path(args.output),
    )

    return 0 if stats["status"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
