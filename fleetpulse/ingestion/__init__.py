"""Data ingestion modules for Kafka and direct Parquet writing."""

from fleetpulse.ingestion.kafka_consumer import KafkaParquetConsumer
from fleetpulse.ingestion.direct_writer import DirectParquetWriter

__all__ = ["KafkaParquetConsumer", "DirectParquetWriter"]
