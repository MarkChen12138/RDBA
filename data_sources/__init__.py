"""
Modules that know how to download data from individual sources.

Each module should implement an `export_data()` function.

Usage:
    from data_sources import kalshi, polymarket, yfinance
    
    # Export from a single source
    kalshi.export_data()
    
    # Or use the convenience function
    from data_sources import export_all
    export_all(["kalshi", "polymarket", "yfinance"])
"""

from __future__ import annotations

from . import base, kalshi, polymarket, yfinance, gdelt

__all__ = ["base", "kalshi", "polymarket", "yfinance", "gdelt", "export_all", "list_sources"]


def export_all(sources: list[str] = None) -> None:
    """
    Export data from multiple sources.
    
    Args:
        sources: List of source names. If None, exports from all available sources.
    """
    if sources is None:
        sources = base.list_available_sources()
    
    for source_name in sources:
        try:
            module = base.get_data_source_module(source_name)
            if hasattr(module, "export_data"):
                module.export_data()
            else:
                print(f"Warning: {source_name} does not have export_data() function")
        except Exception as e:
            print(f"Error exporting {source_name}: {e}")


def list_sources() -> list[str]:
    """Return a list of available data source names."""
    return base.list_available_sources()

