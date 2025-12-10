#!/usr/bin/env python3
"""
GDELT Data Analysis Script

This script performs comprehensive analysis on collected GDELT news data
for the Fed/FOMC sentiment and market impact study.

Features:
1. Descriptive Statistics
2. Temporal Analysis
3. Sentiment Analysis
4. Topic Distribution
5. News Shock Detection
6. Cross-source Correlation (with market data)

Usage:
    python scripts/analyze_gdelt.py --input market_data/gdelt/bulk_6months
    python scripts/analyze_gdelt.py --input data/gold/gdelt_features
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_bronze_data(input_dir: Path) -> pd.DataFrame:
    """Load raw bronze layer data."""
    combined_file = input_dir / "gdelt_bulk_combined.csv"
    if combined_file.exists():
        logger.info(f"Loading combined file: {combined_file}")
        return pd.read_csv(combined_file)

    # Try to load from daily partitions
    daily_files = list(input_dir.glob("dt=*/articles.csv"))
    if daily_files:
        logger.info(f"Loading {len(daily_files)} daily files...")
        dfs = [pd.read_csv(f) for f in daily_files]
        return pd.concat(dfs, ignore_index=True)

    raise FileNotFoundError(f"No data found in {input_dir}")


def load_gold_data(input_dir: Path) -> pd.DataFrame:
    """Load gold layer feature data."""
    feature_file = input_dir / "gdelt_features.csv"
    if feature_file.exists():
        return pd.read_csv(feature_file)

    # Try parquet
    parquet_files = list(input_dir.glob("*.parquet"))
    if parquet_files:
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)

    raise FileNotFoundError(f"No feature data found in {input_dir}")


def analyze_descriptive_stats(df: pd.DataFrame) -> dict:
    """Generate descriptive statistics."""
    logger.info("Generating descriptive statistics...")

    stats = {
        "total_articles": len(df),
        "unique_articles": df["article_id"].nunique() if "article_id" in df.columns else len(df),
        "date_range": {},
        "columns": list(df.columns),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
    }

    # Date range
    if "seendate_parsed" in df.columns:
        df["seendate_parsed"] = pd.to_datetime(df["seendate_parsed"], errors="coerce")
        stats["date_range"] = {
            "start": str(df["seendate_parsed"].min()),
            "end": str(df["seendate_parsed"].max()),
            "days": (df["seendate_parsed"].max() - df["seendate_parsed"].min()).days,
        }

    # Domain distribution
    if "domain" in df.columns:
        domain_counts = df["domain"].value_counts()
        stats["top_domains"] = domain_counts.head(20).to_dict()
        stats["unique_domains"] = df["domain"].nunique()

    # Language distribution
    if "language" in df.columns:
        stats["language_distribution"] = df["language"].value_counts().to_dict()

    # Country distribution
    if "sourcecountry" in df.columns:
        country_counts = df["sourcecountry"].value_counts()
        stats["top_countries"] = country_counts.head(20).to_dict()
        stats["unique_countries"] = df["sourcecountry"].nunique()

    # Query label distribution
    if "query_label" in df.columns:
        stats["query_distribution"] = df["query_label"].value_counts().to_dict()

    return stats


def analyze_temporal_patterns(df: pd.DataFrame) -> dict:
    """Analyze temporal patterns in the data."""
    logger.info("Analyzing temporal patterns...")

    if "seendate_parsed" not in df.columns:
        return {"error": "No timestamp column found"}

    df["seendate_parsed"] = pd.to_datetime(df["seendate_parsed"], errors="coerce")
    df = df.dropna(subset=["seendate_parsed"])

    # Extract time components
    df["date"] = df["seendate_parsed"].dt.date
    df["hour"] = df["seendate_parsed"].dt.hour
    df["dayofweek"] = df["seendate_parsed"].dt.dayofweek
    df["weekday_name"] = df["seendate_parsed"].dt.day_name()

    temporal = {
        "daily_article_counts": {},
        "hourly_distribution": {},
        "weekday_distribution": {},
        "peak_hours": [],
        "peak_days": [],
    }

    # Daily counts
    daily_counts = df.groupby("date").size()
    temporal["daily_article_counts"] = {
        "mean": float(daily_counts.mean()),
        "std": float(daily_counts.std()),
        "min": int(daily_counts.min()),
        "max": int(daily_counts.max()),
        "total_days": len(daily_counts),
    }

    # Hourly distribution
    hourly_counts = df.groupby("hour").size()
    temporal["hourly_distribution"] = hourly_counts.to_dict()
    temporal["peak_hours"] = hourly_counts.nlargest(3).index.tolist()

    # Weekday distribution
    weekday_counts = df.groupby("weekday_name").size()
    temporal["weekday_distribution"] = weekday_counts.to_dict()

    # Identify high-activity periods (news shocks)
    daily_mean = daily_counts.mean()
    daily_std = daily_counts.std()
    high_activity_days = daily_counts[daily_counts > daily_mean + 2 * daily_std]
    temporal["high_activity_days"] = {
        str(date): int(count) for date, count in high_activity_days.items()
    }

    return temporal


def analyze_sentiment(df: pd.DataFrame) -> dict:
    """Analyze sentiment/tone in the data."""
    logger.info("Analyzing sentiment/tone...")

    if "tone" not in df.columns:
        return {"error": "No tone column found"}

    # Convert tone to numeric
    df["tone"] = pd.to_numeric(df["tone"], errors="coerce")
    df = df.dropna(subset=["tone"])

    sentiment = {
        "overall": {
            "mean": float(df["tone"].mean()),
            "std": float(df["tone"].std()),
            "median": float(df["tone"].median()),
            "min": float(df["tone"].min()),
            "max": float(df["tone"].max()),
        },
        "distribution": {
            "positive": int((df["tone"] > 0).sum()),
            "negative": int((df["tone"] < 0).sum()),
            "neutral": int((df["tone"] == 0).sum()),
        },
        "by_query": {},
        "extreme_articles": {
            "most_positive": [],
            "most_negative": [],
        },
    }

    # Sentiment by query
    if "query_label" in df.columns:
        sentiment_by_query = df.groupby("query_label")["tone"].agg(["mean", "std", "count"])
        sentiment["by_query"] = sentiment_by_query.to_dict("index")

    # Extreme articles
    most_positive = df.nlargest(5, "tone")[["title", "tone", "url"]].to_dict("records")
    most_negative = df.nsmallest(5, "tone")[["title", "tone", "url"]].to_dict("records")
    sentiment["extreme_articles"]["most_positive"] = most_positive
    sentiment["extreme_articles"]["most_negative"] = most_negative

    return sentiment


def analyze_topic_distribution(df: pd.DataFrame) -> dict:
    """Analyze topic/query distribution."""
    logger.info("Analyzing topic distribution...")

    topics = {
        "query_counts": {},
        "query_overlap": {},
        "temporal_query_trends": {},
    }

    if "query_label" in df.columns:
        # Query counts
        query_counts = df["query_label"].value_counts()
        topics["query_counts"] = query_counts.to_dict()

        # Temporal trends by query
        if "seendate_parsed" in df.columns:
            df["seendate_parsed"] = pd.to_datetime(df["seendate_parsed"], errors="coerce")
            df["date"] = df["seendate_parsed"].dt.date

            query_daily = df.groupby(["date", "query_label"]).size().unstack(fill_value=0)

            # Calculate correlations between queries
            if len(query_daily.columns) > 1:
                correlations = query_daily.corr()
                topics["query_correlations"] = correlations.to_dict()

    return topics


def detect_news_shocks(df: pd.DataFrame, window_hours: int = 1) -> dict:
    """Detect news shock events."""
    logger.info("Detecting news shocks...")

    if "seendate_parsed" not in df.columns:
        return {"error": "No timestamp column found"}

    df = df.copy()
    df["seendate_parsed"] = pd.to_datetime(df["seendate_parsed"], errors="coerce")
    df = df.dropna(subset=["seendate_parsed"])

    if df.empty:
        return {"error": "No valid timestamps found"}

    # Create hourly counts using groupby instead of resample
    df["hour_bucket"] = df["seendate_parsed"].dt.floor("h")
    hourly_counts = df.groupby("hour_bucket").size()

    if len(hourly_counts) < 2:
        return {"error": "Not enough data for shock detection"}

    # Calculate z-scores
    mean_count = hourly_counts.mean()
    std_count = hourly_counts.std()

    if std_count == 0:
        return {
            "threshold": 2.0,
            "mean_hourly_count": float(mean_count),
            "std_hourly_count": 0.0,
            "total_shocks": 0,
            "shock_events": [],
        }

    z_scores = (hourly_counts - mean_count) / std_count

    # Identify shocks (z-score > 2)
    shocks = z_scores[z_scores > 2]

    shock_events = []
    for timestamp, z in shocks.items():
        # Get articles in this hour bucket
        window_articles = df[df["hour_bucket"] == timestamp]

        shock_events.append({
            "timestamp": str(timestamp),
            "z_score": float(z),
            "article_count": len(window_articles),
            "top_titles": window_articles["title"].head(5).tolist() if "title" in window_articles.columns else [],
        })

    # Sort by z_score descending
    shock_events = sorted(shock_events, key=lambda x: x["z_score"], reverse=True)

    return {
        "threshold": 2.0,
        "mean_hourly_count": float(mean_count),
        "std_hourly_count": float(std_count),
        "total_shocks": len(shock_events),
        "shock_events": shock_events[:20],  # Top 20 shocks
    }


def generate_summary_report(
    stats: dict,
    temporal: dict,
    sentiment: dict,
    topics: dict,
    shocks: dict,
    output_dir: Path,
) -> str:
    """Generate a comprehensive summary report."""

    report = []
    report.append("=" * 70)
    report.append("GDELT DATA ANALYSIS REPORT")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append("=" * 70)

    # Descriptive Statistics
    report.append("\n## 1. DESCRIPTIVE STATISTICS")
    report.append("-" * 40)
    report.append(f"Total Articles: {stats.get('total_articles', 'N/A'):,}")
    report.append(f"Unique Articles: {stats.get('unique_articles', 'N/A'):,}")
    report.append(f"Unique Domains: {stats.get('unique_domains', 'N/A'):,}")
    report.append(f"Unique Countries: {stats.get('unique_countries', 'N/A'):,}")
    report.append(f"Memory Usage: {stats.get('memory_usage_mb', 0):.2f} MB")

    if "date_range" in stats and stats["date_range"]:
        report.append(f"\nDate Range:")
        report.append(f"  Start: {stats['date_range'].get('start', 'N/A')}")
        report.append(f"  End: {stats['date_range'].get('end', 'N/A')}")
        report.append(f"  Days: {stats['date_range'].get('days', 'N/A')}")

    # Top Domains
    if "top_domains" in stats:
        report.append("\nTop 10 Domains:")
        for domain, count in list(stats["top_domains"].items())[:10]:
            report.append(f"  {domain}: {count:,}")

    # Temporal Analysis
    report.append("\n## 2. TEMPORAL ANALYSIS")
    report.append("-" * 40)
    if "daily_article_counts" in temporal:
        dac = temporal["daily_article_counts"]
        report.append(f"Daily Articles (mean): {dac.get('mean', 0):.1f}")
        report.append(f"Daily Articles (std): {dac.get('std', 0):.1f}")
        report.append(f"Daily Articles (range): {dac.get('min', 0)} - {dac.get('max', 0)}")

    if "peak_hours" in temporal:
        report.append(f"\nPeak Hours (UTC): {temporal['peak_hours']}")

    if "high_activity_days" in temporal and temporal["high_activity_days"]:
        report.append("\nHigh Activity Days (>2σ):")
        for date, count in list(temporal["high_activity_days"].items())[:10]:
            report.append(f"  {date}: {count:,} articles")

    # Sentiment Analysis
    report.append("\n## 3. SENTIMENT ANALYSIS")
    report.append("-" * 40)
    if "overall" in sentiment:
        s = sentiment["overall"]
        report.append(f"Mean Tone: {s.get('mean', 0):.3f}")
        report.append(f"Std Tone: {s.get('std', 0):.3f}")
        report.append(f"Range: {s.get('min', 0):.3f} to {s.get('max', 0):.3f}")

    if "distribution" in sentiment:
        d = sentiment["distribution"]
        report.append(f"\nSentiment Distribution:")
        report.append(f"  Positive: {d.get('positive', 0):,}")
        report.append(f"  Negative: {d.get('negative', 0):,}")
        report.append(f"  Neutral: {d.get('neutral', 0):,}")

    # Topic Distribution
    report.append("\n## 4. TOPIC DISTRIBUTION")
    report.append("-" * 40)
    if "query_counts" in topics:
        report.append("Articles by Query:")
        for query, count in topics["query_counts"].items():
            report.append(f"  {query}: {count:,}")

    # News Shocks
    report.append("\n## 5. NEWS SHOCK DETECTION")
    report.append("-" * 40)
    report.append(f"Detection Threshold: z-score > {shocks.get('threshold', 2.0)}")
    report.append(f"Mean Hourly Count: {shocks.get('mean_hourly_count', 0):.1f}")
    report.append(f"Total Shocks Detected: {shocks.get('total_shocks', 0)}")

    if "shock_events" in shocks and shocks["shock_events"]:
        report.append("\nTop News Shock Events:")
        for event in shocks["shock_events"][:10]:
            report.append(f"  {event['timestamp']}: z={event['z_score']:.2f}, {event['article_count']} articles")

    report.append("\n" + "=" * 70)
    report.append("END OF REPORT")
    report.append("=" * 70)

    report_text = "\n".join(report)

    # Save report
    report_file = output_dir / "analysis_report.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info(f"Report saved to: {report_file}")

    return report_text


def main():
    parser = argparse.ArgumentParser(
        description="GDELT Data Analysis Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--input", type=str, required=True,
        help="Input directory (bronze or gold data)"
    )
    parser.add_argument(
        "--output", type=str, default="analysis_output",
        help="Output directory for analysis results"
    )
    parser.add_argument(
        "--data-type", type=str, choices=["bronze", "gold"], default="bronze",
        help="Type of input data (bronze=raw, gold=features)"
    )

    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("GDELT DATA ANALYSIS")
    logger.info("=" * 70)
    logger.info(f"Input: {input_dir}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Data Type: {args.data_type}")

    # Load data
    try:
        if args.data_type == "bronze":
            df = load_bronze_data(input_dir)
        else:
            df = load_gold_data(input_dir)
        logger.info(f"Loaded {len(df):,} records")
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1

    # Run analyses
    stats = analyze_descriptive_stats(df)
    temporal = analyze_temporal_patterns(df)
    sentiment = analyze_sentiment(df)
    topics = analyze_topic_distribution(df)
    shocks = detect_news_shocks(df)

    # Save detailed results as JSON
    results = {
        "descriptive_stats": stats,
        "temporal_analysis": temporal,
        "sentiment_analysis": sentiment,
        "topic_distribution": topics,
        "news_shocks": shocks,
        "analysis_timestamp": datetime.now().isoformat(),
    }

    results_file = output_dir / "analysis_results.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Detailed results saved to: {results_file}")

    # Generate summary report
    report = generate_summary_report(
        stats, temporal, sentiment, topics, shocks, output_dir
    )
    print("\n" + report)

    logger.info("Analysis complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
