"""
GDELT DOC 2.0 API data source.

Downloads news articles related to Federal Reserve policy decisions
using the GDELT DOC 2.0 API.

**Rate Limiting:**
GDELT API has rate limits. To avoid 429 errors:
- Use shorter timespan (e.g., "1w" instead of "3months")
- Use fewer queries per run (default is 1 query)
- Wait at least 5 seconds between queries
- If you get rate limited, wait 10-30 minutes before retrying

**Usage:**
    from data_sources import gdelt
    
    # Default (1 query, 3 months)
    gdelt.export_data()
    
    # Custom parameters
    gdelt.export_data(
        queries={"fed": '("Federal Reserve" OR FOMC)'},
        timespan="1w",
        maxrecords=100
    )
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import time

import pandas as pd
import requests

from .utils import ensure_dir, safe_write_csv, write_json

OUTPUT_DIR = Path("./market_data/gdelt")

# GDELT DOC 2.0 API endpoint
API_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Default query parameters for Federal Reserve related news
# Note: To avoid rate limits, use only 1-2 queries per run or use shorter timespan (e.g., "1w")
DEFAULT_QUERIES = {
    "fed_decision": '("Federal Reserve" OR FOMC OR "rate cut" OR "rate hike" OR "interest rate")',
}

# Additional queries you can use (uncomment as needed):
ADDITIONAL_QUERIES = {
    "fed_policy": '("Fed policy" OR "monetary policy" OR "Powell" OR "Jerome Powell")',
    "fed_inflation": '("Federal Reserve" AND (inflation OR CPI OR PCE))',
}

# API parameters
DEFAULT_PARAMS = {
    "mode": "artlist",  # Article list mode
    "maxrecords": 200,  # Maximum records to fetch
    "timespan": "3months",  # Time range (3months, 1w, 1d, etc.)
    "sort": "datedesc",  # Sort by date descending
    "format": "json",  # JSON format
}


def fetch_articles(
    query: str,
    timespan: str = "3months",
    maxrecords: int = 200,
    max_retries: int = 3,
) -> List[Dict]:
    """
    Fetch articles from GDELT DOC 2.0 API.
    
    Args:
        query: Search query string
        timespan: Time range (e.g., "3months", "1w", "1d")
        maxrecords: Maximum number of records to fetch
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of article dictionaries
    """
    params = {
        "query": query,
        "mode": DEFAULT_PARAMS["mode"],
        "maxrecords": maxrecords,
        "timespan": timespan,
        "sort": DEFAULT_PARAMS["sort"],
        "format": DEFAULT_PARAMS["format"],
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            articles = data.get("articles", [])
            
            if not articles:
                print(f"  ⚠️ No articles found for query: {query}")
                return []
            
            return articles
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Rate limit - use longer backoff
                wait_time = 10 * (2 ** attempt)  # 10s, 20s, 40s
                print(f"  ⚠️ Rate limit (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ Rate limit exceeded after {max_retries} attempts")
                    print(f"  💡 Tip: Try again later or use shorter timespan (e.g., '1w' instead of '3months')")
                    return []
            else:
                print(f"  ⚠️ HTTP error {e.response.status_code}: {e}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(3 ** attempt)  # 1s, 3s, 9s
            else:
                print(f"  ❌ Failed to fetch data after {max_retries} attempts")
                return []
        except Exception as e:
            print(f"  ❌ Unexpected error: {e}")
            return []
    
    return []


def articles_to_dataframe(articles: List[Dict], query_label: str) -> pd.DataFrame:
    """
    Convert list of articles to a pandas DataFrame.
    
    Args:
        articles: List of article dictionaries from GDELT API
        query_label: Label for the query that generated these articles
        
    Returns:
        DataFrame with article data
    """
    if not articles:
        return pd.DataFrame()
    
    # Extract relevant fields
    records = []
    for article in articles:
        record = {
            "query_label": query_label,
            "url": article.get("url", ""),
            "url_mobile": article.get("url_mobile", ""),
            "title": article.get("title", ""),
            "seendate": article.get("seendate", ""),
            "domain": article.get("domain", ""),
            "language": article.get("language", ""),
            "sourcecountry": article.get("sourcecountry", ""),
            "socialimage": article.get("socialimage", ""),
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Parse seendate to datetime
    if "seendate" in df.columns and not df.empty:
        try:
            df["seendate_parsed"] = pd.to_datetime(
                df["seendate"], 
                format="%Y%m%dT%H%M%SZ",
                errors="coerce"
            )
        except Exception:
            pass
    
    return df


def export_data(
    queries: Optional[Dict[str, str]] = None,
    timespan: str = "3months",
    maxrecords: int = 200,
) -> None:
    """
    Fetch and export GDELT news data.
    
    Args:
        queries: Dictionary of query_label -> query_string
        timespan: Time range for articles
        maxrecords: Maximum records per query
    """
    ensure_dir(OUTPUT_DIR)
    
    print("\n=== Fetching GDELT DOC 2.0 data ===")
    
    if queries is None:
        queries = DEFAULT_QUERIES
    
    # Metadata
    metadata = {
        "fetch_timestamp": datetime.now().isoformat(),
        "timespan": timespan,
        "maxrecords": maxrecords,
        "queries": queries,
        "api_base_url": API_BASE_URL,
    }
    
    metadata_path = OUTPUT_DIR / "gdelt_metadata.json"
    write_json(metadata_path, metadata)
    print(f"✅ Saved metadata to {metadata_path}")
    
    # Fetch data for each query
    all_articles = []
    total_queries = len(queries)
    
    for idx, (query_label, query_string) in enumerate(queries.items(), 1):
        print(f"\n📰 Fetching {idx}/{total_queries}: {query_label}")
        print(f"   Query: {query_string}")
        
        articles = fetch_articles(
            query=query_string,
            timespan=timespan,
            maxrecords=maxrecords,
        )
        
        if articles:
            df = articles_to_dataframe(articles, query_label)
            
            if not df.empty:
                # Save individual query results
                output_file = OUTPUT_DIR / f"gdelt_{query_label}.csv"
                safe_write_csv(df, output_file)
                
                # Add to combined list
                all_articles.append(df)
        
        # Be nice to the API - wait between queries
        if idx < total_queries:  # Don't wait after the last query
            wait_time = 5
            print(f"   ⏳ Waiting {wait_time}s before next query...")
            time.sleep(wait_time)
    
    # Save combined results
    if all_articles:
        combined_df = pd.concat(all_articles, ignore_index=True)
        
        # Sort by date if available
        if "seendate_parsed" in combined_df.columns:
            combined_df = combined_df.sort_values("seendate_parsed", ascending=False)
        
        combined_output = OUTPUT_DIR / "gdelt_combined.csv"
        safe_write_csv(combined_df, combined_output)
        
        # Statistics
        print("\n📊 Summary Statistics:")
        print(f"   Total articles: {len(combined_df):,}")
        print(f"   Unique domains: {combined_df['domain'].nunique():,}")
        print(f"   Languages: {combined_df['language'].nunique()}")
        print(f"   Countries: {combined_df['sourcecountry'].nunique()}")
        
        if "seendate_parsed" in combined_df.columns:
            date_range = combined_df["seendate_parsed"].dropna()
            if not date_range.empty:
                print(f"   Date range: {date_range.min()} to {date_range.max()}")
    else:
        print("\n⚠️ No articles fetched from any query")

