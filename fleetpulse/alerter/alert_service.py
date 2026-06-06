"""
Alert Microservice

Scans for unacknowledged critical anomalies and sends alerts via:
1. JSON file output (always enabled)
2. Webhook (optional, configured via environment variable)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from fleetpulse.analytics.utils import get_duckdb_connection
from fleetpulse.simulator.config import load_config

logger = logging.getLogger(__name__)


class AlertService:
    """Monitors anomalies and sends alerts."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        """
        Initialize the alert service.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.alert_config = self.config["alerts"]
        self.storage_config = self.config["storage"]

        self.output_dir = Path(self.alert_config["output_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.webhook_url = self.alert_config.get("webhook_url", "")
        self.enable_file_output = self.alert_config.get("enable_file_output", True)
        self.enable_webhook = self.alert_config.get("enable_webhook", False)

        logger.info(
            f"AlertService initialized (file: {self.enable_file_output}, "
            f"webhook: {self.enable_webhook})"
        )

    def get_unacknowledged_critical_anomalies(self, db_path: str) -> List[Dict[str, Any]]:
        """
        Retrieve unacknowledged critical anomalies from the database.

        Args:
            db_path: Path to DuckDB database

        Returns:
            List of anomaly records
        """
        conn = get_duckdb_connection(db_path)

        query = """
        SELECT
            anomaly_id,
            truck_id,
            detected_at,
            sensor_name,
            sensor_value,
            z_score,
            severity
        FROM fct_anomaly_flags
        WHERE severity = 'critical'
            AND acknowledged = FALSE
        ORDER BY detected_at DESC
        """

        df = conn.execute(query).fetchdf()
        conn.close()

        anomalies = df.to_dict("records")
        logger.info(f"Found {len(anomalies)} unacknowledged critical anomalies")

        return anomalies

    def create_alert_payload(self, anomaly: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a structured alert payload.

        Alert Schema:
        {
            "alert_id": str,
            "timestamp": str (ISO 8601),
            "truck_id": str,
            "severity": "critical",
            "sensor": str,
            "sensor_value": float,
            "z_score": float,
            "detected_at": str (ISO 8601),
            "message": str,
            "recommended_action": str
        }

        Args:
            anomaly: Anomaly record from database

        Returns:
            Alert payload dictionary
        """
        sensor_display = anomaly["sensor_name"].replace("_", " ").title()

        message = (
            f"CRITICAL ALERT: {anomaly['truck_id']} - {sensor_display} anomaly detected. "
            f"Value: {anomaly['sensor_value']:.2f}, Z-score: {anomaly['z_score']:.2f}σ"
        )

        recommended_action = (
            f"Immediate inspection required for {anomaly['truck_id']}. "
            f"Check {sensor_display} system. Potential failure risk."
        )

        payload = {
            "alert_id": anomaly["anomaly_id"],
            "timestamp": datetime.now().isoformat(),
            "truck_id": anomaly["truck_id"],
            "severity": "critical",
            "sensor": anomaly["sensor_name"],
            "sensor_value": float(anomaly["sensor_value"]),
            "z_score": float(anomaly["z_score"]),
            "detected_at": str(anomaly["detected_at"]),
            "message": message,
            "recommended_action": recommended_action,
        }

        return payload

    def write_alert_to_file(self, payload: Dict[str, Any]) -> None:
        """
        Write alert to JSON file.

        Files are organized by date: ./alerts/YYYY-MM-DD/TRUCK-XXX_alert.json

        Args:
            payload: Alert payload
        """
        if not self.enable_file_output:
            return

        date_str = datetime.now().strftime("%Y-%m-%d")
        date_dir = self.output_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{payload['truck_id']}_alert_{datetime.now().strftime('%H%M%S')}.json"
        file_path = date_dir / filename

        with open(file_path, "w") as f:
            json.dump(payload, f, indent=2)

        logger.info(f"Alert written to {file_path}")

    def send_webhook(self, payload: Dict[str, Any]) -> bool:
        """
        Send alert via webhook.

        Args:
            payload: Alert payload

        Returns:
            True if successful, False otherwise
        """
        if not self.enable_webhook or not self.webhook_url:
            logger.debug("Webhook disabled or URL not configured")
            return False

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Alert sent via webhook for {payload['truck_id']}")
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to send webhook: {e}")
            return False

    def acknowledge_anomaly(self, anomaly_id: str, db_path: str) -> None:
        """
        Mark an anomaly as acknowledged in the database.

        Args:
            anomaly_id: Anomaly identifier
            db_path: Path to DuckDB database
        """
        conn = get_duckdb_connection(db_path)

        conn.execute(
            """
            UPDATE fct_anomaly_flags
            SET acknowledged = TRUE,
                acknowledged_at = CURRENT_TIMESTAMP
            WHERE anomaly_id = ?
            """,
            [anomaly_id],
        )

        logger.info(f"Acknowledged anomaly {anomaly_id}")
        conn.close()

    def process_alerts(self, db_path: str) -> int:
        """
        Process all unacknowledged critical anomalies.

        Args:
            db_path: Path to DuckDB database

        Returns:
            Number of alerts processed
        """
        anomalies = self.get_unacknowledged_critical_anomalies(db_path)

        if not anomalies:
            logger.info("No critical anomalies to process")
            return 0

        alerts_sent = 0

        for anomaly in anomalies:
            payload = self.create_alert_payload(anomaly)

            self.write_alert_to_file(payload)

            webhook_sent = self.send_webhook(payload)

            if self.enable_file_output or webhook_sent:
                self.acknowledge_anomaly(anomaly["anomaly_id"], db_path)
                alerts_sent += 1

        logger.info(f"Processed {alerts_sent} alerts")
        return alerts_sent

    def run_continuous(self, db_path: str, check_interval_minutes: int = 5) -> None:
        """
        Run the alert service continuously.

        Args:
            db_path: Path to DuckDB database
            check_interval_minutes: How often to check for anomalies
        """
        import time

        logger.info(f"Starting continuous alert service (interval: {check_interval_minutes}m)")

        try:
            while True:
                self.process_alerts(db_path)
                time.sleep(check_interval_minutes * 60)

        except KeyboardInterrupt:
            logger.info("Alert service stopped by user")


def main() -> None:
    """Main entry point for the alert service."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    import sys

    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)

    service = AlertService(config_path)
    db_path = config["storage"]["warehouse_path"]

    service.run_continuous(db_path, config["alerts"]["check_interval_minutes"])


if __name__ == "__main__":
    main()
