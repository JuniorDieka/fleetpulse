"""
Ingest DAG - Data-Aware Asset Scheduling

Monitors the landing zone for new Parquet files and loads them into DuckDB.
Uses Airflow 3.x data-aware assets for event-driven triggering.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
import duckdb
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "fleetpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ingest_dag",
    default_args=default_args,
    description="Load telemetry from landing zone to DuckDB (data-aware)",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleetpulse", "ingestion", "data-aware"],
) as dag:

    @task
    def detect_new_files() -> dict:
        """
        Scan landing zone for new Parquet files.

        Returns:
            Dictionary with file count and paths
        """
        landing_zone = Path("/opt/airflow/data/landing")

        if not landing_zone.exists():
            logger.warning(f"Landing zone not found: {landing_zone}")
            return {"file_count": 0, "files": []}

        parquet_files = list(landing_zone.rglob("*.parquet"))

        logger.info(f"Found {len(parquet_files)} Parquet files in landing zone")

        return {
            "file_count": len(parquet_files),
            "files": [str(f) for f in parquet_files[:10]],
        }

    @task
    def load_to_duckdb(file_info: dict) -> dict:
        """
        Load Parquet files from landing zone into DuckDB raw table.

        Args:
            file_info: Dictionary with file information

        Returns:
            Dictionary with load statistics
        """
        if file_info["file_count"] == 0:
            logger.info("No files to load")
            return {"rows_loaded": 0}

        db_path = "/opt/airflow/data/warehouse/fleetpulse.duckdb"
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS raw
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.telemetry (
                truck_id VARCHAR,
                timestamp TIMESTAMP,
                engine_temp_c DOUBLE,
                hydraulic_pressure_psi DOUBLE,
                payload_weight_tons DOUBLE,
                vibration_level_mm_s DOUBLE,
                fuel_consumption_l_hr DOUBLE,
                odometer_hours DOUBLE,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        initial_count = conn.execute("SELECT COUNT(*) FROM raw.telemetry").fetchone()[0]

        conn.execute("""
            INSERT INTO raw.telemetry
            SELECT
                truck_id,
                timestamp,
                engine_temp_c,
                hydraulic_pressure_psi,
                payload_weight_tons,
                vibration_level_mm_s,
                fuel_consumption_l_hr,
                odometer_hours,
                CURRENT_TIMESTAMP as loaded_at
            FROM read_parquet('/opt/airflow/data/landing/**/*.parquet', hive_partitioning=true)
            WHERE (truck_id, timestamp) NOT IN (
                SELECT truck_id, timestamp FROM raw.telemetry
            )
        """)

        final_count = conn.execute("SELECT COUNT(*) FROM raw.telemetry").fetchone()[0]
        rows_loaded = final_count - initial_count

        conn.close()

        logger.info(f"Loaded {rows_loaded} new rows into raw.telemetry")

        return {"rows_loaded": rows_loaded, "total_rows": final_count}

    @task
    def update_asset(load_stats: dict) -> None:
        """
        Mark the telemetry asset as updated.

        This triggers downstream DAGs that depend on this asset.

        Args:
            load_stats: Load statistics from previous task
        """
        logger.info(
            f"Asset updated: {load_stats['rows_loaded']} new rows, "
            f"{load_stats['total_rows']} total rows"
        )

    file_info = detect_new_files()
    load_stats = load_to_duckdb(file_info)
    update_asset(load_stats)
