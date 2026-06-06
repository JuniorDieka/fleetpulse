"""
Moving Z-Score Anomaly Detection

Detects sensor anomalies using rolling Z-score analysis against
fleet-wide historical baselines.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd

from fleetpulse.analytics.utils import get_duckdb_connection

logger = logging.getLogger(__name__)


def calculate_z_score(value: float, mean: float, std: float) -> float:
    """
    Calculate Z-score for a value.

    Formula: Z = (X - μ) / σ

    Args:
        value: Observed value
        mean: Population mean
        std: Population standard deviation

    Returns:
        Z-score (number of standard deviations from mean)
    """
    if std == 0:
        return 0.0
    return (value - mean) / std


def detect_anomalies(
    db_path: str,
    window_hours: int = 24,
    warning_threshold: float = 2.0,
    critical_threshold: float = 3.0,
    min_baseline_samples: int = 100,
) -> pd.DataFrame:
    """
    Detect sensor anomalies using moving Z-score analysis.

    Detection Logic:
    1. Calculate fleet-wide baseline (μ, σ) for each sensor
    2. For each truck, compute rolling Z-score over window_hours
    3. Flag anomalies:
       - Warning: |Z| > warning_threshold (default: 2σ)
       - Critical: |Z| > critical_threshold (default: 3σ)

    Operational Interpretation:
    - Critical anomalies (3σ): Immediate inspection required
    - Warning anomalies (2σ): Monitor closely, plan inspection
    - Normal (<2σ): Continue routine operations

    Args:
        db_path: Path to DuckDB database
        window_hours: Rolling window size in hours
        warning_threshold: Z-score threshold for warnings (σ)
        critical_threshold: Z-score threshold for critical alerts (σ)
        min_baseline_samples: Minimum samples required for baseline

    Returns:
        DataFrame with detected anomalies
    """
    conn = get_duckdb_connection(db_path)

    sensor_columns = [
        "engine_temp_c",
        "hydraulic_pressure_psi",
        "payload_weight_tons",
        "vibration_level_mm_s",
        "fuel_consumption_l_hr",
    ]

    baseline_query = f"""
    SELECT
        {', '.join([f'AVG({col}) as {col}_mean, STDDEV({col}) as {col}_std' for col in sensor_columns])}
    FROM stg_telemetry
    WHERE is_outlier = FALSE
    HAVING COUNT(*) >= {min_baseline_samples}
    """

    baseline = conn.execute(baseline_query).fetchdf()

    if baseline.empty:
        logger.warning("Insufficient data for baseline calculation")
        conn.close()
        return pd.DataFrame()

    baseline_stats = baseline.iloc[0].to_dict()

    cutoff_time = datetime.now() - timedelta(hours=window_hours)

    telemetry_query = f"""
    SELECT
        truck_id,
        timestamp,
        {', '.join(sensor_columns)}
    FROM stg_telemetry
    WHERE timestamp >= '{cutoff_time.isoformat()}'
        AND is_outlier = FALSE
    ORDER BY truck_id, timestamp
    """

    df = conn.execute(telemetry_query).fetchdf()

    if df.empty:
        logger.warning("No telemetry data in the specified window")
        conn.close()
        return pd.DataFrame()

    anomalies = []

    for truck_id in df["truck_id"].unique():
        truck_df = df[df["truck_id"] == truck_id]

        for sensor in sensor_columns:
            mean = baseline_stats[f"{sensor}_mean"]
            std = baseline_stats[f"{sensor}_std"]

            if pd.isna(std) or std == 0:
                continue

            truck_df[f"{sensor}_zscore"] = truck_df[sensor].apply(
                lambda x: calculate_z_score(x, mean, std)
            )

            critical_mask = truck_df[f"{sensor}_zscore"].abs() > critical_threshold
            warning_mask = (
                (truck_df[f"{sensor}_zscore"].abs() > warning_threshold)
                & (truck_df[f"{sensor}_zscore"].abs() <= critical_threshold)
            )

            for idx in truck_df[critical_mask].index:
                row = truck_df.loc[idx]
                anomalies.append(
                    {
                        "truck_id": truck_id,
                        "detected_at": row["timestamp"],
                        "sensor_name": sensor,
                        "sensor_value": row[sensor],
                        "z_score": row[f"{sensor}_zscore"],
                        "severity": "critical",
                        "acknowledged": False,
                    }
                )

            for idx in truck_df[warning_mask].index:
                row = truck_df.loc[idx]
                anomalies.append(
                    {
                        "truck_id": truck_id,
                        "detected_at": row["timestamp"],
                        "sensor_name": sensor,
                        "sensor_value": row[sensor],
                        "z_score": row[f"{sensor}_zscore"],
                        "severity": "warning",
                        "acknowledged": False,
                    }
                )

    anomaly_df = pd.DataFrame(anomalies)

    if not anomaly_df.empty:
        logger.info(
            f"Detected {len(anomaly_df)} anomalies "
            f"({len(anomaly_df[anomaly_df['severity']=='critical'])} critical, "
            f"{len(anomaly_df[anomaly_df['severity']=='warning'])} warnings)"
        )
    else:
        logger.info("No anomalies detected")

    conn.close()
    return anomaly_df


def write_anomalies_to_db(df: pd.DataFrame, db_path: str) -> None:
    """
    Write detected anomalies to the database.

    Args:
        df: DataFrame with anomalies
        db_path: Path to DuckDB database
    """
    if df.empty:
        logger.info("No anomalies to write")
        return

    conn = get_duckdb_connection(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fct_anomaly_flags (
            anomaly_id VARCHAR PRIMARY KEY,
            truck_id VARCHAR,
            detected_at TIMESTAMP,
            sensor_name VARCHAR,
            sensor_value DOUBLE,
            z_score DOUBLE,
            severity VARCHAR,
            acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_at TIMESTAMP
        )
    """)

    df["anomaly_id"] = df.apply(
        lambda row: f"{row['truck_id']}_{row['sensor_name']}_{row['detected_at'].strftime('%Y%m%d%H%M%S')}",
        axis=1,
    )

    conn.execute("CREATE TEMP TABLE anomalies_temp AS SELECT * FROM df")

    conn.execute("""
        INSERT OR IGNORE INTO fct_anomaly_flags
        SELECT * FROM anomalies_temp
    """)

    inserted = conn.execute("SELECT changes()").fetchone()[0]

    logger.info(f"Wrote {inserted} new anomalies to database")
    conn.close()
