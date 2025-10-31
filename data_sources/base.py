"""
Base class and interfaces for data source modules.

All data source modules should:
1. Implement an `export_data()` function
2. Use the utilities from `utils.py`
3. Follow a consistent structure
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class DataSource(ABC):
    """Base class for data sources (optional, for future type checking)."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    @abstractmethod
    def export_data(self) -> None:
        """Fetch and export data to the output directory."""
        pass


def get_data_source_module(name: str):
    """
    Dynamically import a data source module by name.
    
    Usage:
        kalshi = get_data_source_module("kalshi")
        kalshi.export_data()
    """
    from importlib import import_module
    
    try:
        return import_module(f"data_sources.{name}")
    except ImportError as e:
        raise ImportError(f"Could not import data source '{name}': {e}")


def list_available_sources() -> list[str]:
    """Return a list of available data source module names."""
    import os
    
    data_sources_dir = Path(__file__).parent
    sources = []
    
    for file in data_sources_dir.glob("*.py"):
        if file.name.startswith("_") or file.name == "base.py":
            continue
        module_name = file.stem
        # Check if module has export_data function
        try:
            mod = get_data_source_module(module_name)
            if hasattr(mod, "export_data"):
                sources.append(module_name)
        except ImportError:
            continue
    
    return sorted(sources)

