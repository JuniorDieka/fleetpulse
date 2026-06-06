"""Pytest configuration and fixtures."""

import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_db_path():
    """Create a temporary DuckDB database path."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)
