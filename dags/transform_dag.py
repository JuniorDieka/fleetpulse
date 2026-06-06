"""
Transform DAG - dbt Transformations & Statistical Analysis

Triggered by ingest_dag asset updates.
Runs dbt models and statistical analytics pipeline.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import logging

sys.path.append("/opt/airflow")

from fleetpulse.analytics.reliability import (
    calculate_fleet_reliability_metrics,
    write_reliability_metrics_to_db,
)
from fleetpulse.analytics.weibull import (
    calculate_fleet_weibull_metrics,
    update_weibull_metrics_in_db,
)
from fleetpulse.analytics.anomaly import detect_anomalies, write_anomalies_to_db
from fleetpulse.alerter.alert_service import AlertService

logger = logging.getLogger(__name__)

default_args = {
    "owner": "fleetpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DB_PATH = "/opt/airflow/data/warehouse/fleetpulse.duckdb"

with DAG(
    "transform_dag",
    default_args=default_args,
    description="dbt transformations and statistical analytics",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleetpulse", "transformation", "analytics"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed --profiles-dir . --target dev",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select staging --profiles-dir . --target dev",
    )

    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select dimensions --profiles-dir . --target dev",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select marts --profiles-dir . --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir . --target dev",
    )

    @task
    def calculate_mtbf_mttr() -> dict:
        """
        Calculate MTBF and MTTR for all trucks.

        Returns:
            Statistics dictionary
        """
        logger.info("Calculating MTBF and MTTR metrics...")

        df = calculate_fleet_reliability_metrics(DB_PATH)

        if not df.empty:
            write_reliability_metrics_to_db(df, DB_PATH)
            logger.info(f"Calculated metrics for {len(df)} trucks")
            return {"trucks_processed": len(df)}
        else:
            logger.warning("No data available for reliability calculations")
            return {"trucks_processed": 0}

    @task
    def fit_weibull_distributions() -> dict:
        """
        Fit Weibull distributions and predict failure probabilities.

        Returns:
            Statistics dictionary
        """
        logger.info("Fitting Weibull distributions...")

        df = calculate_fleet_weibull_metrics(
            DB_PATH, prediction_horizon_hours=50.0, min_failures=3
        )

        if not df.empty:
            update_weibull_metrics_in_db(df, DB_PATH)
            logger.info(f"Fitted Weibull for {len(df)} trucks")
            return {"trucks_with_weibull": len(df)}
        else:
            logger.warning("Insufficient failure data for Weibull fitting")
            return {"trucks_with_weibull": 0}

    @task
    def detect_sensor_anomalies() -> dict:
        """
        Detect sensor anomalies using Z-score analysis.

        Returns:
            Statistics dictionary
        """
        logger.info("Detecting sensor anomalies...")

        df = detect_anomalies(
            DB_PATH,
            window_hours=24,
            warning_threshold=2.0,
            critical_threshold=3.0,
            min_baseline_samples=100,
        )

        if not df.empty:
            write_anomalies_to_db(df, DB_PATH)
            critical_count = len(df[df["severity"] == "critical"])
            warning_count = len(df[df["severity"] == "warning"])
            logger.info(f"Detected {len(df)} anomalies ({critical_count} critical)")
            return {
                "total_anomalies": len(df),
                "critical": critical_count,
                "warning": warning_count,
            }
        else:
            logger.info("No anomalies detected")
            return {"total_anomalies": 0, "critical": 0, "warning": 0}

    @task
    def trigger_alerts(anomaly_stats: dict) -> dict:
        """
        Process critical anomalies and send alerts.

        Args:
            anomaly_stats: Anomaly statistics from previous task

        Returns:
            Alert statistics
        """
        if anomaly_stats["critical"] == 0:
            logger.info("No critical anomalies to alert")
            return {"alerts_sent": 0}

        logger.info("Processing critical anomaly alerts...")

        service = AlertService("/opt/airflow/config.yaml")
        alerts_sent = service.process_alerts(DB_PATH)

        logger.info(f"Sent {alerts_sent} alerts")

        return {"alerts_sent": alerts_sent}

    reliability_task = calculate_mtbf_mttr()
    weibull_task = fit_weibull_distributions()
    anomaly_task = detect_sensor_anomalies()
    alert_task = trigger_alerts(anomaly_task)

    (
        dbt_seed
        >> dbt_run_staging
        >> dbt_run_dimensions
        >> dbt_run_marts
        >> dbt_test
        >> [reliability_task, weibull_task, anomaly_task]
    )

    anomaly_task >> alert_task
