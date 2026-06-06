"""
Kafka Consumer that writes micro-batches to partitioned Parquet files.

Creates a Data Lake landing zone partitioned by date and truck ID.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from fleetpulse.simulator.config import load_config

logger = logging.getLogger(__name__)


class KafkaParquetConsumer:
    """Consumes telemetry from Kafka and writes to partitioned Parquet files."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        """
        Initialize the Kafka consumer.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.kafka_config = self.config["kafka"]
        self.storage_config = self.config["storage"]

        self.landing_zone = Path(self.storage_config["landing_zone"])
        self.landing_zone.mkdir(parents=True, exist_ok=True)

        self.batch_size = self.kafka_config["batch_size"]
        self.batch_timeout = self.kafka_config["batch_timeout_seconds"]

        self.consumer = self._initialize_consumer()
        logger.info(
            f"KafkaParquetConsumer initialized (landing zone: {self.landing_zone})"
        )

    def _initialize_consumer(self) -> KafkaConsumer:
        """Initialize Kafka consumer connection."""
        try:
            consumer = KafkaConsumer(
                self.kafka_config["topic"],
                bootstrap_servers=self.kafka_config["bootstrap_servers"],
                group_id=self.kafka_config["consumer_group"],
                auto_offset_reset=self.kafka_config["auto_offset_reset"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=True,
            )
            logger.info(
                f"Kafka consumer connected to topic '{self.kafka_config['topic']}'"
            )
            return consumer
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

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

    def _write_batch_to_parquet(self, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of records to partitioned Parquet files.

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

        logger.info(f"Wrote batch of {len(batch)} records to Parquet")

    def consume(self, duration_seconds: int = None) -> None:
        """
        Consume messages from Kafka and write to Parquet.

        Args:
            duration_seconds: How long to consume (None = run indefinitely)
        """
        logger.info("Starting Kafka consumer...")
        batch: List[Dict[str, Any]] = []
        message_count = 0

        try:
            for message in self.consumer:
                batch.append(message.value)
                message_count += 1

                if len(batch) >= self.batch_size:
                    self._write_batch_to_parquet(batch)
                    batch = []

                if message_count % 500 == 0:
                    logger.info(f"Consumed {message_count} messages from Kafka")

        except KeyboardInterrupt:
            logger.info("Kafka consumer stopped by user")
        finally:
            if batch:
                self._write_batch_to_parquet(batch)

            self.consumer.close()
            logger.info("Kafka consumer closed")


def main() -> None:
    """Main entry point for the Kafka consumer."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    consumer = KafkaParquetConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
