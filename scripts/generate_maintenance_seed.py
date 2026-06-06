"""
Generate realistic synthetic maintenance events for seed data.

Creates 3 years of failure history for all 50 trucks with:
- Realistic failure types and frequencies
- Varying failure rates (degradation trucks have more failures)
- Random but reproducible (seeded) event generation
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

SEED = 42
random.seed(SEED)

TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 51)]

DEGRADATION_TRUCKS = ["TRUCK-003", "TRUCK-012", "TRUCK-027", "TRUCK-038", "TRUCK-045"]

FAILURE_TYPES = [
    ("Engine Overheating", 0.25, 8, 16, 4, 12),
    ("Hydraulic System Failure", 0.20, 12, 24, 6, 18),
    ("Transmission Issue", 0.15, 16, 32, 10, 24),
    ("Electrical Fault", 0.15, 4, 12, 2, 8),
    ("Brake System Failure", 0.10, 6, 16, 3, 10),
    ("Tire Damage", 0.08, 2, 6, 1, 4),
    ("Cooling System Leak", 0.07, 8, 20, 4, 14),
]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days


def generate_failure_events(truck_id: str, is_degradation: bool) -> List[Dict[str, Any]]:
    """Generate failure events for a single truck."""
    events = []

    base_failure_rate = 0.008 if is_degradation else 0.004

    current_date = START_DATE
    odometer_hours = random.uniform(5000, 15000)

    event_counter = 0

    while current_date < END_DATE:
        days_to_next_failure = random.expovariate(base_failure_rate)
        current_date += timedelta(days=days_to_next_failure)

        if current_date >= END_DATE:
            break

        failure_type, prob, down_min, down_max, rep_min, rep_max = random.choices(
            FAILURE_TYPES, weights=[f[1] for f in FAILURE_TYPES]
        )[0]

        downtime_hours = random.uniform(down_min, down_max)
        repair_hours = random.uniform(rep_min, min(rep_max, downtime_hours))

        odometer_hours += random.uniform(100, 500)

        event_id = f"{truck_id}_E{event_counter:03d}"

        events.append(
            {
                "event_id": event_id,
                "truck_id": truck_id,
                "failure_timestamp": current_date.isoformat(),
                "failure_type": failure_type,
                "downtime_hours": round(downtime_hours, 2),
                "repair_hours": round(repair_hours, 2),
                "odometer_at_failure": round(odometer_hours, 2),
            }
        )

        event_counter += 1

    return events


def main() -> None:
    """Generate maintenance events seed file."""
    all_events = []

    for truck_id in TRUCK_IDS:
        is_degradation = truck_id in DEGRADATION_TRUCKS
        events = generate_failure_events(truck_id, is_degradation)
        all_events.extend(events)
        print(f"{truck_id}: {len(events)} events ({'degradation' if is_degradation else 'normal'})")

    all_events.sort(key=lambda x: x["failure_timestamp"])

    output_path = Path(__file__).parent.parent / "dbt_project" / "seeds" / "maintenance_events_seed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id",
                "truck_id",
                "failure_timestamp",
                "failure_type",
                "downtime_hours",
                "repair_hours",
                "odometer_at_failure",
            ],
        )
        writer.writeheader()
        writer.writerows(all_events)

    print(f"\nGenerated {len(all_events)} total maintenance events")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
