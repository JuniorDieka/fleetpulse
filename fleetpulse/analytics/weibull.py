"""
Weibull Distribution Analysis for Failure Prediction

Fits 2-parameter Weibull distributions to inter-failure intervals and
predicts failure probability within a specified time horizon.
"""

import logging
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from fleetpulse.analytics.utils import get_duckdb_connection

logger = logging.getLogger(__name__)


def fit_weibull_distribution(
    inter_failure_times: np.ndarray, truck_id: str
) -> Tuple[Optional[float], Optional[float]]:
    """
    Fit a 2-parameter Weibull distribution to inter-failure intervals.

    Weibull Parameters:
    - β (shape): Failure mode indicator
      * β < 1: Infant mortality (decreasing failure rate, early-life failures)
      * β ≈ 1: Random failures (constant failure rate, exponential distribution)
      * β > 1: Wear-out failures (increasing failure rate, aging equipment)

    - η (scale): Characteristic life - time at which 63.2% of units will have failed

    Args:
        inter_failure_times: Array of time intervals between failures (hours)
        truck_id: Truck identifier for logging

    Returns:
        Tuple of (shape β, scale η) or (None, None) if fitting fails
    """
    if len(inter_failure_times) < 3:
        logger.warning(
            f"{truck_id}: Insufficient data for Weibull fit "
            f"({len(inter_failure_times)} intervals, need ≥3)"
        )
        return None, None

    try:
        shape, loc, scale = stats.weibull_min.fit(inter_failure_times, floc=0)

        logger.info(
            f"{truck_id}: Weibull fit - β={shape:.3f}, η={scale:.2f} hours "
            f"(failure mode: {_interpret_shape_parameter(shape)})"
        )

        return shape, scale

    except Exception as e:
        logger.error(f"{truck_id}: Weibull fitting failed - {e}")
        return None, None


def _interpret_shape_parameter(beta: float) -> str:
    """
    Interpret the Weibull shape parameter for operational context.

    Args:
        beta: Shape parameter

    Returns:
        Human-readable interpretation
    """
    if beta < 0.9:
        return "infant mortality"
    elif beta < 1.1:
        return "random failures"
    else:
        return "wear-out"


def predict_failure_probability(
    shape: float,
    scale: float,
    current_hours: float,
    horizon_hours: float,
    truck_id: str,
) -> float:
    """
    Predict probability of failure within the next N hours.

    Formula: P(failure in [t, t+h]) = F(t+h) - F(t)
    where F(t) = 1 - exp(-(t/η)^β) is the Weibull CDF

    Operational Interpretation:
    - P > 0.7: High risk - schedule immediate inspection
    - 0.3 < P < 0.7: Moderate risk - plan preventive maintenance
    - P < 0.3: Low risk - continue normal operations

    Args:
        shape: Weibull shape parameter (β)
        scale: Weibull scale parameter (η)
        current_hours: Current cumulative operating hours
        horizon_hours: Prediction horizon (e.g., 50 hours)
        truck_id: Truck identifier for logging

    Returns:
        Probability of failure (0.0 to 1.0)
    """
    try:
        cdf_current = stats.weibull_min.cdf(current_hours, shape, scale=scale)
        cdf_future = stats.weibull_min.cdf(current_hours + horizon_hours, shape, scale=scale)

        probability = cdf_future - cdf_current

        probability = max(0.0, min(1.0, probability))

        logger.info(
            f"{truck_id}: Failure probability in next {horizon_hours}h = {probability:.2%} "
            f"(current: {current_hours:.0f}h)"
        )

        return probability

    except Exception as e:
        logger.error(f"{truck_id}: Failure probability calculation failed - {e}")
        return 0.0


def calculate_fleet_weibull_metrics(
    db_path: str, prediction_horizon_hours: float = 50.0, min_failures: int = 3
) -> pd.DataFrame:
    """
    Calculate Weibull parameters and failure probabilities for all trucks.

    Args:
        db_path: Path to DuckDB database
        prediction_horizon_hours: Time horizon for failure prediction
        min_failures: Minimum number of failures required for fitting

    Returns:
        DataFrame with Weibull metrics per truck
    """
    conn = get_duckdb_connection(db_path)

    query = """
    WITH ranked_failures AS (
        SELECT
            truck_id,
            failure_timestamp,
            odometer_at_failure,
            LAG(odometer_at_failure) OVER (
                PARTITION BY truck_id ORDER BY failure_timestamp
            ) as prev_odometer
        FROM fct_maintenance_events
        ORDER BY truck_id, failure_timestamp
    ),
    inter_failure_intervals AS (
        SELECT
            truck_id,
            odometer_at_failure - prev_odometer as interval_hours
        FROM ranked_failures
        WHERE prev_odometer IS NOT NULL
    ),
    current_odometer AS (
        SELECT
            truck_id,
            MAX(odometer_hours) as current_hours
        FROM stg_telemetry
        GROUP BY truck_id
    )
    SELECT
        i.truck_id,
        ARRAY_AGG(i.interval_hours) as intervals,
        c.current_hours
    FROM inter_failure_intervals i
    JOIN current_odometer c ON i.truck_id = c.truck_id
    GROUP BY i.truck_id, c.current_hours
    HAVING COUNT(*) >= {min_failures}
    """.format(
        min_failures=min_failures
    )

    df = conn.execute(query).fetchdf()

    results = []

    for _, row in df.iterrows():
        truck_id = row["truck_id"]
        intervals = np.array(row["intervals"])
        current_hours = row["current_hours"]

        shape, scale = fit_weibull_distribution(intervals, truck_id)

        if shape is not None and scale is not None:
            failure_prob = predict_failure_probability(
                shape, scale, current_hours, prediction_horizon_hours, truck_id
            )

            results.append(
                {
                    "truck_id": truck_id,
                    "weibull_shape_beta": shape,
                    "weibull_scale_eta": scale,
                    "failure_prob_50hr": failure_prob,
                    "current_hours": current_hours,
                }
            )

    result_df = pd.DataFrame(results)
    logger.info(f"Calculated Weibull metrics for {len(result_df)} trucks")

    conn.close()
    return result_df


def update_weibull_metrics_in_db(df: pd.DataFrame, db_path: str) -> None:
    """
    Update Weibull metrics in the reliability metrics table.

    Args:
        df: DataFrame with Weibull metrics
        db_path: Path to DuckDB database
    """
    conn = get_duckdb_connection(db_path)

    for _, row in df.iterrows():
        conn.execute(
            """
            UPDATE fct_reliability_metrics
            SET
                weibull_shape_beta = ?,
                weibull_scale_eta = ?,
                failure_prob_50hr = ?,
                calculated_at = CURRENT_TIMESTAMP
            WHERE truck_id = ?
            """,
            [
                row["weibull_shape_beta"],
                row["weibull_scale_eta"],
                row["failure_prob_50hr"],
                row["truck_id"],
            ],
        )

    logger.info(f"Updated Weibull metrics for {len(df)} trucks in database")
    conn.close()
