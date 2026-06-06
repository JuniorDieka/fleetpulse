"""
Reliability Metrics Calculation

Implements MTBF (Mean Time Between Failures) and MTTR (Mean Time to Repair)
calculations for fleet maintenance analytics.
"""

import logging
from typing import Dict, List, Tuple

import pandas as pd
import duckdb

from fleetpulse.analytics.utils import get_duckdb_connection

logger = logging.getLogger(__name__)


def calculate_mtbf(
    truck_id: str, total_operating_hours: float, failure_count: int
) -> float:
    """
    Calculate Mean Time Between Failures (MTBF).

    Formula: MTBF = Total Operating Hours / Number of Failures

    Operational Interpretation:
    - Higher MTBF indicates better reliability
    - MTBF represents average operational time between breakdowns
    - Used for preventive maintenance scheduling

    Args:
        truck_id: Truck identifier
        total_operating_hours: Cumulative operating hours
        failure_count: Number of failure events

    Returns:
        MTBF in hours (returns infinity if no failures)
    """
    if failure_count == 0:
        logger.warning(f"{truck_id}: No failures recorded, MTBF = infinity")
        return float("inf")

    mtbf = total_operating_hours / failure_count
    logger.info(f"{truck_id}: MTBF = {mtbf:.2f} hours ({failure_count} failures)")
    return mtbf


def calculate_mttr(truck_id: str, total_repair_hours: float, repair_count: int) -> float:
    """
    Calculate Mean Time to Repair (MTTR).

    Formula: MTTR = Total Repair Hours / Number of Repair Events

    Operational Interpretation:
    - Lower MTTR indicates faster repair capability
    - MTTR represents average downtime per breakdown
    - Used for maintenance crew efficiency and spare parts planning

    Args:
        truck_id: Truck identifier
        total_repair_hours: Sum of all repair durations
        repair_count: Number of repair events

    Returns:
        MTTR in hours (returns 0 if no repairs)
    """
    if repair_count == 0:
        logger.warning(f"{truck_id}: No repairs recorded, MTTR = 0")
        return 0.0

    mttr = total_repair_hours / repair_count
    logger.info(f"{truck_id}: MTTR = {mttr:.2f} hours ({repair_count} repairs)")
    return mttr


def calculate_fleet_reliability_metrics(
    db_path: str,
) -> pd.DataFrame:
    """
    Calculate MTBF and MTTR for all trucks in the fleet.

    Reads from:
    - fct_maintenance_events: failure history
    - stg_telemetry: current odometer hours

    Writes to:
    - fct_reliability_metrics table

    Args:
        db_path: Path to DuckDB database

    Returns:
        DataFrame with reliability metrics per truck
    """
    conn = get_duckdb_connection(db_path)

    query = """
    WITH truck_operating_hours AS (
        SELECT
            truck_id,
            MAX(odometer_hours) as total_operating_hours
        FROM stg_telemetry
        GROUP BY truck_id
    ),
    truck_failures AS (
        SELECT
            truck_id,
            COUNT(*) as failure_count,
            SUM(repair_hours) as total_repair_hours
        FROM fct_maintenance_events
        GROUP BY truck_id
    )
    SELECT
        t.truck_id,
        COALESCE(t.total_operating_hours, 0) as total_operating_hours,
        COALESCE(f.failure_count, 0) as failure_count,
        COALESCE(f.total_repair_hours, 0) as total_repair_hours
    FROM truck_operating_hours t
    LEFT JOIN truck_failures f ON t.truck_id = f.truck_id
    ORDER BY t.truck_id
    """

    df = conn.execute(query).fetchdf()

    df["mtbf_hours"] = df.apply(
        lambda row: calculate_mtbf(
            row["truck_id"], row["total_operating_hours"], row["failure_count"]
        ),
        axis=1,
    )

    df["mttr_hours"] = df.apply(
        lambda row: calculate_mttr(
            row["truck_id"], row["total_repair_hours"], row["failure_count"]
        ),
        axis=1,
    )

    df["mtbf_hours"] = df["mtbf_hours"].replace(float("inf"), None)

    logger.info(f"Calculated reliability metrics for {len(df)} trucks")

    conn.close()
    return df


def write_reliability_metrics_to_db(
    df: pd.DataFrame, db_path: str
) -> None:
    """
    Write reliability metrics to DuckDB.

    Args:
        df: DataFrame with reliability metrics
        db_path: Path to DuckDB database
    """
    conn = get_duckdb_connection(db_path)

    conn.execute("DROP TABLE IF EXISTS fct_reliability_metrics_temp")

    conn.execute("""
        CREATE TABLE fct_reliability_metrics_temp AS
        SELECT * FROM df
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fct_reliability_metrics (
            truck_id VARCHAR PRIMARY KEY,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_operating_hours DOUBLE,
            failure_count INTEGER,
            total_repair_hours DOUBLE,
            mtbf_hours DOUBLE,
            mttr_hours DOUBLE,
            weibull_shape_beta DOUBLE,
            weibull_scale_eta DOUBLE,
            failure_prob_50hr DOUBLE
        )
    """)

    conn.execute("""
        INSERT OR REPLACE INTO fct_reliability_metrics
        (truck_id, total_operating_hours, failure_count, total_repair_hours,
         mtbf_hours, mttr_hours)
        SELECT
            truck_id,
            total_operating_hours,
            failure_count,
            total_repair_hours,
            mtbf_hours,
            mttr_hours
        FROM fct_reliability_metrics_temp
    """)

    conn.execute("DROP TABLE fct_reliability_metrics_temp")

    logger.info(f"Wrote {len(df)} reliability metrics to database")
    conn.close()
