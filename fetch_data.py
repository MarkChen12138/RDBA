#!/usr/bin/env python3
"""
Convenience wrapper to download data from various market data sources.

Usage examples:
    python fetch_data.py                    # all sources
    python fetch_data.py -p                 # Polymarket only
    python fetch_data.py -k                 # Kalshi only
    python fetch_data.py -y                 # Yahoo Finance only
    python fetch_data.py -g                 # GDELT news only (incremental 15min)
    python fetch_data.py -g --gdelt-timespan 1h    # GDELT with 1-hour window
    python fetch_data.py -g --gdelt-backfill 2024-01-01 2024-01-31  # GDELT backfill
    python fetch_data.py -g --gdelt-pipeline       # Full GDELT pipeline (bronze->silver->gold)
    python fetch_data.py -p -k              # Polymarket + Kalshi
    python fetch_data.py --list             # list available sources
    python fetch_data.py --source gdelt     # specific source by name
"""

from __future__ import annotations

import argparse

from data_sources import base, export_all, kalshi, polymarket, yfinance, gdelt, fred


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download market data from various sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Download from all sources
  %(prog)s -p -k              # Polymarket and Kalshi only
  %(prog)s -g                 # GDELT news only (incremental 15min)
  %(prog)s -g --gdelt-timespan 1h   # GDELT with 1-hour window
  %(prog)s -g --gdelt-backfill 2024-01-01 2024-01-31  # GDELT historical backfill
  %(prog)s -g --gdelt-pipeline      # Full GDELT pipeline (bronze->silver->gold)
  %(prog)s --source gdelt     # Specific source by name
  %(prog)s --list             # List available sources

GDELT Rate Limiting:
  - Default timespan is 15min for incremental pulls
  - Use 1h or longer for less frequent updates
  - If rate limited (429), wait 10-30 minutes before retrying
  - For backfill, data is fetched in batches with automatic delays
        """,
    )
    parser.add_argument(
        "--polymarket",
        "-p",
        action="store_true",
        help="Fetch Polymarket data only.",
    )
    parser.add_argument(
        "--kalshi",
        "-k",
        action="store_true",
        help="Fetch Kalshi data only.",
    )
    parser.add_argument(
        "--yfinance",
        "-y",
        action="store_true",
        help="Fetch Yahoo Finance data only.",
    )
    parser.add_argument(
        "--gdelt",
        "-g",
        action="store_true",
        help="Fetch GDELT news data only.",
    )
    # GDELT-specific options
    parser.add_argument(
        "--gdelt-timespan",
        type=str,
        default="15min",
        choices=["15min", "1h", "1d", "1w", "1month", "3months"],
        help="GDELT time window for incremental fetch (default: 15min).",
    )
    parser.add_argument(
        "--gdelt-maxrecords",
        type=int,
        default=250,
        help="Maximum records per GDELT query (default: 250).",
    )
    parser.add_argument(
        "--gdelt-backfill",
        nargs=2,
        metavar=("START_DATE", "END_DATE"),
        help="Backfill GDELT data for date range (YYYY-MM-DD YYYY-MM-DD).",
    )
    parser.add_argument(
        "--gdelt-pipeline",
        action="store_true",
        help="Run full GDELT pipeline: Bronze -> Silver -> Gold.",
    )
    parser.add_argument(
        "--gdelt-silver",
        action="store_true",
        help="Process existing GDELT bronze data to silver layer.",
    )
    parser.add_argument(
        "--gdelt-gold",
        action="store_true",
        help="Compute GDELT gold layer features from silver data.",
    )
    parser.add_argument(
        "--source",
        "-s",
        type=str,
        help="Fetch from a specific source by name (e.g., 'kalshi', 'polymarket', 'gdelt').",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all available data sources and exit.",
    )
    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Fetch from all sources (default if no specific sources are specified).",
    )
    parser.add_argument(
        "--fred",
        "-f",
        action="store_true",
        help="Fetch FRED macro series.")
    parser.add_argument(
        "--fred-series",
        type=str,
        help="Comma-separated FRED series ids.")
    parser.add_argument(
        "--fred-start",
        type=str,
        default=None,
        help="Observation start (YYYY-MM-DD).")
    parser.add_argument(
        "--fred-end",
        type=str,
        default=None,
        help="Observation end (YYYY-MM-DD).")
    parser.add_argument(
        "--fred-category",
        type=int,
        help="Fetch all FRED series under a given category_id (e.g., 22 = Interest Rates)."
    )

    args = parser.parse_args()

    # Handle --list
    if args.list:
        sources = base.list_available_sources()
        print("Available data sources:")
        for source in sources:
            print(f"  - {source}")
        return args

    # If no specific sources are selected, use all
    if not any([args.polymarket, args.kalshi, args.yfinance, args.gdelt, args.fred, args.source,
                args.gdelt_backfill, args.gdelt_pipeline, args.gdelt_silver, args.gdelt_gold]):
        args.all = True

    if args.all:
        args.polymarket = True
        args.kalshi = True
        args.yfinance = True
        args.gdelt = True
        args.fred = True

    return args


def main() -> None:
    args = parse_args()

    if args.list:
        return

    if args.source:
        # Fetch specific source by name
        try:
            module = base.get_data_source_module(args.source)
            module.export_data()
        except ImportError as e:
            print(f"Error: {e}")
            print(f"Available sources: {', '.join(base.list_available_sources())}")
            return
    else:
        # Use individual flags
        if args.polymarket:
            polymarket.export_data()
        if args.kalshi:
            kalshi.export_data()
        if args.yfinance:
            yfinance.export_data()

        # GDELT handling with extended options
        if args.gdelt or args.gdelt_backfill or args.gdelt_pipeline or args.gdelt_silver or args.gdelt_gold:
            if args.gdelt_backfill:
                # Backfill mode
                start_date, end_date = args.gdelt_backfill
                print(f"\n=== GDELT Backfill Mode ===")
                print(f"Date range: {start_date} to {end_date}")
                gdelt.backfill_data(
                    start_date=start_date,
                    end_date=end_date,
                    maxrecords=args.gdelt_maxrecords,
                )
            elif args.gdelt_pipeline:
                # Full pipeline mode
                print(f"\n=== GDELT Full Pipeline Mode ===")
                gdelt.run_full_pipeline(
                    timespan=args.gdelt_timespan,
                    maxrecords=args.gdelt_maxrecords,
                    compute_features=True,
                )
            elif args.gdelt_silver:
                # Silver layer only
                print(f"\n=== GDELT Silver Layer Processing ===")
                gdelt.process_to_silver()
            elif args.gdelt_gold:
                # Gold layer only
                print(f"\n=== GDELT Gold Layer Feature Computation ===")
                gdelt.compute_gold_features()
            else:
                # Standard incremental fetch (bronze only)
                gdelt.export_data(
                    timespan=args.gdelt_timespan,
                    maxrecords=args.gdelt_maxrecords,
                )

        if args.fred:
            if args.fred_category:
                fred.export_data(
                    category_id=args.fred_category,
                    start=args.fred_start,
                    end=args.fred_end
                )
            else:
                custom = (
                    [s.strip() for s in args.fred_series.split(",")]
                    if args.fred_series else None
                )
                fred.export_data(
                    series=custom,
                    start=args.fred_start,
                    end=args.fred_end
                )


    print("\nDone!")


if __name__ == "__main__":
    main()
