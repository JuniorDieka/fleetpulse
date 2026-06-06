"""Utility functions for analytics modules."""

import logging
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


def get_duckdb_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Get a DuckDB connection.

    Args:
        db_path: Path to DuckDB database file

    Returns:
        DuckDB connection
    """
    try:
        conn = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise


def execute_query(conn: duckdb.DuckDBPyConnection, query: str) -> Any:
    """
    Execute a SQL query and return results.

    Args:
        conn: DuckDB connection
        query: SQL query string

    Returns:
        Query results
    """
    try:
        result = conn.execute(query).fetchdf()
        return result
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise
