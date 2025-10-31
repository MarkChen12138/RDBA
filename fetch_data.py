#!/usr/bin/env python3
"""
Convenience wrapper to download data from various market data sources.

Usage examples:
    python fetch_data.py                    # all sources
    python fetch_data.py -p                 # Polymarket only
    python fetch_data.py -k                 # Kalshi only
    python fetch_data.py -y                 # Yahoo Finance only
    python fetch_data.py -p -k              # Polymarket + Kalshi
    python fetch_data.py --list             # list available sources
    python fetch_data.py --source kalshi    # specific source by name
"""

from __future__ import annotations

import argparse

from data_sources import base, export_all, kalshi, polymarket, yfinance


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download market data from various sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Download from all sources
  %(prog)s -p -k              # Polymarket and Kalshi only
  %(prog)s --source kalshi    # Specific source by name
  %(prog)s --list             # List available sources
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
        "--source",
        "-s",
        type=str,
        help="Fetch from a specific source by name (e.g., 'kalshi', 'polymarket').",
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
    
    args = parser.parse_args()
    
    # Handle --list
    if args.list:
        sources = base.list_available_sources()
        print("Available data sources:")
        for source in sources:
            print(f"  - {source}")
        return args
    
    # If no specific sources are selected, use all
    if not any([args.polymarket, args.kalshi, args.yfinance, args.source]):
        args.all = True
    
    if args.all:
        args.polymarket = True
        args.kalshi = True
        args.yfinance = True

    return args


def main() -> None:
    args = parse_args()
    
    if args.list:
        return

    sources_to_fetch = []

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

    print("\nDone!")


if __name__ == "__main__":
    main()

