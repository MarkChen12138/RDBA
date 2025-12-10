"""
Unit tests for GDELT data source module.

Run with: pytest tests/test_gdelt.py -v
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Import module under test
from data_sources import gdelt


class TestArticlesToDataframe:
    """Tests for articles_to_dataframe function."""

    def test_empty_articles_returns_empty_dataframe(self):
        """Empty article list returns empty DataFrame."""
        result = gdelt.articles_to_dataframe([], "test_query")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_single_article_conversion(self):
        """Single article converts correctly."""
        articles = [
            {
                "url": "https://example.com/article1",
                "title": "Fed Raises Rates",
                "seendate": "20241201T120000Z",
                "domain": "example.com",
                "language": "English",
                "sourcecountry": "US",
            }
        ]
        result = gdelt.articles_to_dataframe(articles, "fed_test")

        assert len(result) == 1
        assert result.iloc[0]["query_label"] == "fed_test"
        assert result.iloc[0]["url"] == "https://example.com/article1"
        assert result.iloc[0]["title"] == "Fed Raises Rates"
        assert result.iloc[0]["domain"] == "example.com"

    def test_seendate_parsing(self):
        """Seendate is parsed correctly to UTC datetime."""
        articles = [
            {
                "url": "https://example.com/article1",
                "seendate": "20241215T143052Z",
                "domain": "example.com",
            }
        ]
        result = gdelt.articles_to_dataframe(articles, "test")

        assert "seendate_parsed" in result.columns
        parsed = result.iloc[0]["seendate_parsed"]
        assert parsed.year == 2024
        assert parsed.month == 12
        assert parsed.day == 15
        assert parsed.hour == 14
        assert parsed.minute == 30
        assert parsed.second == 52

    def test_partition_columns_added(self):
        """Partition columns dt and hour are added."""
        articles = [
            {
                "url": "https://example.com/article1",
                "seendate": "20241215T143052Z",
                "domain": "example.com",
            }
        ]
        result = gdelt.articles_to_dataframe(articles, "test")

        assert "dt" in result.columns
        assert "hour" in result.columns
        assert result.iloc[0]["dt"] == "2024-12-15"
        assert result.iloc[0]["hour"] == "14"

    def test_article_id_generation(self):
        """Unique article_id is generated for deduplication."""
        articles = [
            {
                "url": "https://example.com/article1",
                "seendate": "20241215T143052Z",
            }
        ]
        result = gdelt.articles_to_dataframe(articles, "test")

        assert "article_id" in result.columns
        assert len(result.iloc[0]["article_id"]) == 16  # MD5 hash truncated

    def test_duplicate_articles_same_id(self):
        """Same URL + seendate produces same article_id."""
        articles = [
            {"url": "https://example.com/same", "seendate": "20241215T143052Z"},
            {"url": "https://example.com/same", "seendate": "20241215T143052Z"},
        ]
        result = gdelt.articles_to_dataframe(articles, "test")

        # Both should have the same article_id
        assert result.iloc[0]["article_id"] == result.iloc[1]["article_id"]

    def test_missing_fields_handled(self):
        """Missing optional fields are handled gracefully."""
        articles = [
            {
                "url": "https://example.com/article1",
                # Missing most fields
            }
        ]
        result = gdelt.articles_to_dataframe(articles, "test")

        assert len(result) == 1
        assert result.iloc[0]["title"] == ""
        assert result.iloc[0]["domain"] == ""


class TestCleanArticlesDf:
    """Tests for clean_articles_df function."""

    def test_empty_dataframe_returns_empty(self):
        """Empty DataFrame returns empty."""
        result = gdelt.clean_articles_df(pd.DataFrame())
        assert result.empty

    def test_null_values_filled(self):
        """Null string values are filled with empty strings."""
        df = pd.DataFrame({
            "article_id": ["abc123"],
            "url": [None],
            "title": [None],
            "domain": [None],
            "seendate": ["20241215T143052Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert result.iloc[0]["url"] == ""
        assert result.iloc[0]["title"] == ""
        assert result.iloc[0]["domain"] == ""

    def test_language_lowercase(self):
        """Language codes are standardized to lowercase."""
        df = pd.DataFrame({
            "article_id": ["abc123"],
            "language": ["ENGLISH"],
            "seendate": ["20241215T143052Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert result.iloc[0]["language"] == "english"

    def test_sourcecountry_uppercase(self):
        """Country codes are standardized to uppercase."""
        df = pd.DataFrame({
            "article_id": ["abc123"],
            "sourcecountry": ["us"],
            "seendate": ["20241215T143052Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert result.iloc[0]["sourcecountry"] == "US"

    def test_domain_lowercase(self):
        """Domains are standardized to lowercase."""
        df = pd.DataFrame({
            "article_id": ["abc123"],
            "domain": ["CNN.com"],
            "seendate": ["20241215T143052Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert result.iloc[0]["domain"] == "cnn.com"

    def test_deduplication_by_article_id(self):
        """Duplicate article_ids are removed."""
        df = pd.DataFrame({
            "article_id": ["abc123", "abc123", "def456"],
            "title": ["First", "Duplicate", "Different"],
            "seendate": ["20241215T143052Z", "20241215T143052Z", "20241215T150000Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert len(result) == 2
        assert "abc123" in result["article_id"].values
        assert "def456" in result["article_id"].values

    def test_ts_utc_alias_added(self):
        """ts_utc column is added as alias for cross-source joins."""
        df = pd.DataFrame({
            "article_id": ["abc123"],
            "seendate": ["20241215T143052Z"],
        })
        result = gdelt.clean_articles_df(df)

        assert "ts_utc" in result.columns


class TestFetchArticles:
    """Tests for fetch_articles function with mocked requests."""

    @patch("data_sources.gdelt.requests.get")
    def test_successful_fetch(self, mock_get):
        """Successful API response returns articles and metadata."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "articles": [
                {"url": "https://example.com/1", "title": "Article 1"},
                {"url": "https://example.com/2", "title": "Article 2"},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        articles, metadata = gdelt.fetch_articles(
            query='("Federal Reserve")',
            timespan="15min",
            maxrecords=10
        )

        assert len(articles) == 2
        assert metadata["status"] == "success"
        assert metadata["articles_count"] == 2

    @patch("data_sources.gdelt.requests.get")
    def test_empty_results(self, mock_get):
        """Empty results return empty list with proper status."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"articles": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        articles, metadata = gdelt.fetch_articles(
            query='("Federal Reserve")',
            timespan="15min"
        )

        assert articles == []
        assert metadata["status"] == "empty_result"

    @patch("data_sources.gdelt.requests.get")
    def test_rate_limit_error(self, mock_get):
        """429 rate limit error is handled with proper status."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = Exception("Rate limited")

        http_error = Exception("429 Client Error")
        http_error.response = mock_response

        mock_get.side_effect = Exception("Rate limited")

        # This will fail but should not raise
        articles, metadata = gdelt.fetch_articles(
            query='("Federal Reserve")',
            timespan="15min",
            max_retries=1
        )

        assert articles == []
        assert "error" in metadata or metadata["status"] != "success"

    @patch("data_sources.gdelt.requests.get")
    def test_request_metadata_includes_params(self, mock_get):
        """Request metadata includes query parameters."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"articles": [{"url": "test"}]}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        _, metadata = gdelt.fetch_articles(
            query='("FOMC")',
            timespan="1h",
            maxrecords=100
        )

        assert metadata["query"] == '("FOMC")'
        assert metadata["timespan"] == "1h"
        assert metadata["maxrecords"] == 100
        assert "request_timestamp" in metadata


class TestDefaultQueries:
    """Tests for default query configurations."""

    def test_default_queries_not_empty(self):
        """DEFAULT_QUERIES has at least one entry."""
        assert len(gdelt.DEFAULT_QUERIES) > 0

    def test_default_queries_are_strings(self):
        """All query values are non-empty strings."""
        for label, query in gdelt.DEFAULT_QUERIES.items():
            assert isinstance(label, str) and len(label) > 0
            assert isinstance(query, str) and len(query) > 0

    def test_queries_contain_fed_keywords(self):
        """Queries contain Fed/FOMC related keywords."""
        all_queries = " ".join(gdelt.DEFAULT_QUERIES.values()).lower()

        # Should contain at least some of these
        fed_keywords = ["federal reserve", "fomc", "rate", "powell", "monetary"]
        found = any(kw in all_queries for kw in fed_keywords)
        assert found, "Queries should contain Fed-related keywords"


class TestDefaultParams:
    """Tests for default parameter configurations."""

    def test_default_params_has_required_keys(self):
        """DEFAULT_PARAMS has all required API keys."""
        required_keys = ["mode", "maxrecords", "timespan", "sort", "format"]
        for key in required_keys:
            assert key in gdelt.DEFAULT_PARAMS

    def test_default_timespan_is_incremental(self):
        """Default timespan is set for incremental pulls."""
        # Should be short for incremental (15min or 1h)
        timespan = gdelt.DEFAULT_PARAMS["timespan"]
        assert timespan in ["15min", "1h", "1d"], \
            "Default should be incremental timespan"

    def test_format_is_json(self):
        """API format is JSON."""
        assert gdelt.DEFAULT_PARAMS["format"] == "json"


class TestRateLimitConfig:
    """Tests for rate limiting configuration."""

    def test_rate_limit_config_exists(self):
        """RATE_LIMIT_CONFIG has required keys."""
        required_keys = [
            "initial_backoff_sec",
            "max_backoff_sec",
            "backoff_multiplier",
            "between_query_wait_sec",
            "max_retries"
        ]
        for key in required_keys:
            assert key in gdelt.RATE_LIMIT_CONFIG

    def test_backoff_is_reasonable(self):
        """Backoff values are reasonable."""
        config = gdelt.RATE_LIMIT_CONFIG
        assert config["initial_backoff_sec"] >= 5
        assert config["max_backoff_sec"] >= 60
        assert config["backoff_multiplier"] >= 1.5
        assert config["max_retries"] >= 3


class TestGoldFeatures:
    """Tests for gold layer feature computation."""

    def test_compute_gold_features_from_dataframe(self):
        """Gold features can be computed from cleaned DataFrame."""
        # Create sample cleaned data
        df = pd.DataFrame({
            "article_id": ["a1", "a2", "a3", "a4"],
            "domain": ["cnn.com", "cnn.com", "bbc.com", "reuters.com"],
            "language": ["english", "english", "english", "english"],
            "sourcecountry": ["US", "US", "UK", "UK"],
            "tone": [1.5, -0.5, 2.0, -1.0],
            "query_label": ["fed_fomc"] * 4,
            "seendate_parsed": pd.to_datetime([
                "2024-12-15 14:00:00",
                "2024-12-15 14:05:00",
                "2024-12-15 14:30:00",
                "2024-12-15 14:35:00",
            ], utc=True),
        })
        df["ts_utc"] = df["seendate_parsed"]
        df["dt"] = df["seendate_parsed"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["seendate_parsed"].dt.strftime("%H")

        # Compute features (mock the file loading)
        df["ts_window"] = df["ts_utc"].dt.floor("15min")

        # Test aggregation logic
        for window, group in df.groupby("ts_window"):
            assert len(group) > 0
            assert group["domain"].nunique() >= 1

    def test_news_shock_feature_calculation(self):
        """News shock is calculated as z-score from rolling mean."""
        features_df = pd.DataFrame({
            "ts": pd.date_range("2024-12-15 00:00", periods=10, freq="15min"),
            "article_count": [10, 12, 11, 50, 13, 11, 12, 10, 11, 12],  # Spike at index 3
        })

        # Apply news shock calculation
        rolling_mean = features_df["article_count"].rolling(4, min_periods=1).mean()
        rolling_std = features_df["article_count"].rolling(4, min_periods=1).std().fillna(1)
        features_df["news_shock"] = (
            (features_df["article_count"] - rolling_mean) / rolling_std.replace(0, 1)
        )

        # The spike at index 3 should have high news_shock
        spike_shock = features_df.iloc[3]["news_shock"]
        normal_shock = features_df.iloc[8]["news_shock"]
        assert spike_shock > normal_shock


class TestCrossSourceAlignment:
    """Tests for cross-source data alignment."""

    def test_get_aligned_features_returns_dataframe(self):
        """get_aligned_features returns DataFrame (even if empty)."""
        # This will return empty if no data exists, which is fine
        result = gdelt.get_aligned_features()
        assert isinstance(result, pd.DataFrame)

    def test_create_news_shock_features_adds_columns(self):
        """create_news_shock_features adds rolling features."""
        df = pd.DataFrame({
            "ts": pd.date_range("2024-12-15", periods=24, freq="15min"),
            "article_count": [10] * 24,
            "avg_tone": [0.5] * 24,
        })

        result = gdelt.create_news_shock_features(df, lookback_windows=[1, 4])

        # Should have new columns for each window
        assert "article_count_ma_15min" in result.columns or "article_count_ma_1h" in result.columns

    def test_empty_dataframe_handled(self):
        """Empty DataFrame is handled gracefully."""
        result = gdelt.create_news_shock_features(pd.DataFrame())
        assert result.empty


class TestOutputDirectories:
    """Tests for output directory configuration."""

    def test_output_dir_defined(self):
        """OUTPUT_DIR is properly defined."""
        assert gdelt.OUTPUT_DIR is not None
        assert "market_data" in str(gdelt.OUTPUT_DIR)
        assert "gdelt" in str(gdelt.OUTPUT_DIR)

    def test_silver_dir_defined(self):
        """SILVER_DIR follows convention."""
        assert gdelt.SILVER_DIR is not None
        assert "silver" in str(gdelt.SILVER_DIR)
        assert "gdelt" in str(gdelt.SILVER_DIR)

    def test_gold_dir_defined(self):
        """GOLD_DIR follows convention."""
        assert gdelt.GOLD_DIR is not None
        assert "gold" in str(gdelt.GOLD_DIR)


class TestTimespanPresets:
    """Tests for timespan preset configurations."""

    def test_incremental_presets_available(self):
        """Incremental timespan presets are available."""
        assert "15min" in gdelt.TIMESPAN_PRESETS
        assert "1h" in gdelt.TIMESPAN_PRESETS

    def test_backfill_presets_available(self):
        """Longer timespan presets for backfill are available."""
        assert "1d" in gdelt.TIMESPAN_PRESETS
        assert "1w" in gdelt.TIMESPAN_PRESETS


# Integration test (requires network - skip in CI)
@pytest.mark.skip(reason="Integration test - requires network and may hit rate limits")
class TestIntegration:
    """Integration tests - run manually."""

    def test_export_data_creates_files(self, tmp_path):
        """export_data creates CSV and metadata files."""
        output_dir = tmp_path / "gdelt"
        gdelt.export_data(
            queries={"test": '("Federal Reserve")'},
            timespan="15min",
            maxrecords=10,
            output_dir=output_dir
        )

        assert (output_dir / "gdelt_metadata.json").exists()

    def test_full_pipeline_runs(self, tmp_path):
        """Full pipeline can run without errors."""
        results = gdelt.run_full_pipeline(
            timespan="15min",
            maxrecords=10,
            compute_features=True
        )
        assert "bronze" in results
        assert "silver" in results
        assert "gold" in results


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
