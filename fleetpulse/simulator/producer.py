"""
IoT Telemetry Producer for CAT 777D Haul Trucks

Generates realistic sensor telemetry with:
- Normal operational variance
- Gradual degradation trends for specified trucks
- Occasional out-of-range spikes
"""

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None
    KafkaError = Exception

from fleetpulse.simulator.config import load_config

logger = logging.getLogger(__name__)


class TelemetryProducer:
    """Produces realistic telemetry data for a fleet of haul trucks."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        """
        Initialize the telemetry producer.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.fleet_size = self.config["fleet"]["size"]
        self.truck_id_prefix = self.config["fleet"]["truck_id_prefix"]
        self.degradation_trucks = set(self.config["fleet"]["degradation_truck_ids"])
        self.sensor_config = self.config["sensors"]
        self.kafka_config = self.config["kafka"]
        self.simulator_config = self.config["simulator"]

        self.truck_states: Dict[str, Dict[str, float]] = {}
        self._initialize_truck_states()

        self.kafka_producer: Optional[KafkaProducer] = None
        if self.simulator_config["kafka_enabled"] and not self.simulator_config["lite_mode"]:
            self._initialize_kafka_producer()

        logger.info(
            f"TelemetryProducer initialized for {self.fleet_size} trucks "
            f"(Kafka: {self.kafka_producer is not None})"
        )

    def _initialize_truck_states(self) -> None:
        """Initialize odometer hours for each truck."""
        random.seed(42)
        odometer_config = self.sensor_config["odometer_hours"]

        for i in range(1, self.fleet_size + 1):
            truck_id = f"{self.truck_id_prefix}-{i:03d}"
            initial_hours = random.uniform(
                odometer_config["initial_min"], odometer_config["initial_max"]
            )
            self.truck_states[truck_id] = {"odometer_hours": initial_hours}

    def _initialize_kafka_producer(self) -> None:
        """Initialize Kafka producer connection."""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_config["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(
                f"Kafka producer connected to {self.kafka_config['bootstrap_servers']}"
            )
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            logger.warning("Continuing without Kafka - data will only be written to Parquet")
            self.kafka_producer = None

    def _generate_sensor_value(
        self, sensor_name: str, truck_id: str, reading_count: int
    ) -> float:
        """
        Generate a single sensor reading with realistic variance and degradation.

        Args:
            sensor_name: Name of the sensor
            truck_id: Truck identifier
            reading_count: Number of readings generated so far (for degradation trend)

        Returns:
            Sensor value
        """
        sensor = self.sensor_config[sensor_name]
        base_value = random.gauss(sensor["mean"], sensor["stddev"])

        if truck_id in self.degradation_trucks:
            degradation_factor = 1.0 + (reading_count * 0.00005)
            if sensor_name == "engine_temp":
                base_value *= degradation_factor
            elif sensor_name == "vibration_level":
                base_value *= degradation_factor
            elif sensor_name == "fuel_consumption":
                base_value *= degradation_factor

        if random.random() < 0.02:
            spike_multiplier = random.uniform(1.3, 1.8)
            base_value *= spike_multiplier

        if "physical_min" in sensor:
            base_value = max(base_value, sensor["physical_min"])
        if "physical_max" in sensor:
            base_value = min(base_value, sensor["physical_max"])

        return round(base_value, 2)

    def generate_telemetry_payload(self, truck_id: str, reading_count: int) -> Dict[str, Any]:
        """
        Generate a complete telemetry payload for a truck.

        Args:
            truck_id: Truck identifier
            reading_count: Number of readings generated so far

        Returns:
            Telemetry payload dictionary
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        odometer_increment = self.sensor_config["odometer_hours"]["increment_per_reading"]
        self.truck_states[truck_id]["odometer_hours"] += odometer_increment

        payload = {
            "truck_id": truck_id,
            "timestamp": timestamp,
            "engine_temp_c": self._generate_sensor_value("engine_temp", truck_id, reading_count),
            "hydraulic_pressure_psi": self._generate_sensor_value(
                "hydraulic_pressure", truck_id, reading_count
            ),
            "payload_weight_tons": self._generate_sensor_value(
                "payload_weight", truck_id, reading_count
            ),
            "vibration_level_mm_s": self._generate_sensor_value(
                "vibration_level", truck_id, reading_count
            ),
            "fuel_consumption_l_hr": self._generate_sensor_value(
                "fuel_consumption", truck_id, reading_count
            ),
            "odometer_hours": round(self.truck_states[truck_id]["odometer_hours"], 2),
        }

        return payload

    def send_to_kafka(self, payload: Dict[str, Any]) -> bool:
        """
        Send telemetry payload to Kafka topic.

        Args:
            payload: Telemetry data

        Returns:
            True if successful, False otherwise
        """
        if not self.kafka_producer:
            return False

        try:
            future = self.kafka_producer.send(self.kafka_config["topic"], value=payload)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            return False

    def run(self, duration_seconds: Optional[int] = None) -> None:
        """
        Run the telemetry producer.

        Args:
            duration_seconds: How long to run (None = run indefinitely)
        """
        logger.info("Starting telemetry producer...")
        start_time = time.time()
        reading_count = 0

        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break

                for i in range(1, self.fleet_size + 1):
                    truck_id = f"{self.truck_id_prefix}-{i:03d}"
                    payload = self.generate_telemetry_payload(truck_id, reading_count)

                    if self.kafka_producer:
                        success = self.send_to_kafka(payload)
                        if success and reading_count % 100 == 0:
                            logger.info(f"Sent telemetry for {truck_id} to Kafka")

                reading_count += 1

                if reading_count % 50 == 0:
                    logger.info(f"Generated {reading_count * self.fleet_size} total readings")

                time.sleep(self.simulator_config["interval_seconds"])

        except KeyboardInterrupt:
            logger.info("Telemetry producer stopped by user")
        finally:
            if self.kafka_producer:
                self.kafka_producer.flush()
                self.kafka_producer.close()
                logger.info("Kafka producer closed")

    def generate_batch(self, num_readings: int = 100) -> List[Dict[str, Any]]:
        """
        Generate a batch of telemetry readings for all trucks.

        Args:
            num_readings: Number of readings per truck

        Returns:
            List of telemetry payloads
        """
        batch = []
        for reading_idx in range(num_readings):
            for i in range(1, self.fleet_size + 1):
                truck_id = f"{self.truck_id_prefix}-{i:03d}"
                payload = self.generate_telemetry_payload(truck_id, reading_idx)
                batch.append(payload)
        return batch


def main() -> None:
    """Main entry point for the telemetry producer."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    producer = TelemetryProducer()
    producer.run()


if __name__ == "__main__":
    main()
