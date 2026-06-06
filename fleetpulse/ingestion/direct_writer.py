"""
Direct Parquet Writer for Lite Mode

Bypasses Kafka and writes telemetry directly to Parquet landing zone.
Useful for Windows machines without Docker Kafka or slower systems.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from fleetpulse.simulator.config import load_config
from fleetpulse.simulator.producer import TelemetryProducer

logger = logging.getLogger(__name__)


class DirectParquetWriter:
    """Writes telemetry directly to Parquet files without Kafka."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        """
        Initialize the direct writer.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.storage_config = self.config["storage"]

        self.landing_zone = Path(self.storage_config["landing_zone"])
        self.landing_zone.mkdir(parents=True, exist_ok=True)

        self.producer = TelemetryProducer(config_path)
        logger.info(f"DirectParquetWriter initialized (landing zone: {self.landing_zone})")

    def _get_partition_path(self, truck_id: str, timestamp: str) -> Path:
        """
        Get the partition path for a telemetry record.

        Args:
            truck_id: Truck identifier
            timestamp: ISO timestamp

        Returns:
            Path to partition directory
        """
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        date_str = dt.strftime("%Y-%m-%d")

        partition_path = self.landing_zone / f"date={date_str}" / f"truck_id={truck_id}"
        partition_path.mkdir(parents=True, exist_ok=True)

        return partition_path

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of telemetry records to partitioned Parquet files.

        Args:
            batch: List of telemetry records
        """
        if not batch:
            return

        df = pd.DataFrame(batch)

        for truck_id in df["truck_id"].unique():
            truck_df = df[df["truck_id"] == truck_id]

            for timestamp in truck_df["timestamp"].unique():
                timestamp_df = truck_df[truck_df["timestamp"] == timestamp]

                partition_path = self._get_partition_path(truck_id, timestamp)

                filename = f"telemetry_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
                file_path = partition_path / filename

                timestamp_df.to_parquet(file_path, engine="pyarrow", index=False)

        logger.info(f"Wrote batch of {len(batch)} records to Parquet (lite mode)")

    def run(self, batch_size: int = 100, num_batches: int = 10) -> None:
        """
        Generate and write telemetry batches.

        Args:
            batch_size: Number of readings per batch
            num_batches: Number of batches to generate
        """
        logger.info(f"Starting direct Parquet writer (lite mode)...")

        for batch_num in range(num_batches):
            batch = self.producer.generate_batch(batch_size)
            self.write_batch(batch)
            logger.info(f"Completed batch {batch_num + 1}/{num_batches}")

        logger.info("Direct Parquet writer completed")


def main() -> None:
    """Main entry point for the direct writer."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    writer = DirectParquetWriter()
    writer.run(batch_size=100, num_batches=10)


if __name__ == "__main__":
    main()
