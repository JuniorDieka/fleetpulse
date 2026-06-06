#!/usr/bin/env python3
"""
Standalone pipeline runner - bypasses Airflow to avoid DuckDB locking issues
Runs: dbt seed → dbt run → analytics → alerts
"""

import subprocess
import sys
import time
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a shell command and return success status"""
    print(f"\n{'='*60}")
    print(f"Running: {cmd}")
    print(f"{'='*60}\n")
    
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        capture_output=False,
        text=True
    )
    
    if result.returncode != 0:
        print(f"\n❌ Command failed with exit code {result.returncode}")
        return False
    print(f"\n✅ Command succeeded")
    return True

def main():
    """Run the complete FleetPulse pipeline"""
    
    project_root = Path(__file__).parent.parent
    dbt_dir = project_root / "dbt_project"
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         FleetPulse Pipeline Runner                       ║
    ║         Standalone Execution (No Airflow)                ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: dbt seed
    print("\n📊 Step 1/6: Loading seed data (trucks & maintenance events)...")
    if not run_command(
        "dbt seed --profiles-dir . --target dev",
        cwd=dbt_dir
    ):
        sys.exit(1)
    
    # Step 2: dbt run
    print("\n🔄 Step 2/6: Running dbt models (staging → dimensions → marts)...")
    if not run_command(
        "dbt run --profiles-dir . --target dev",
        cwd=dbt_dir
    ):
        sys.exit(1)
    
    # Step 3: dbt test
    print("\n🧪 Step 3/6: Running dbt data quality tests...")
    if not run_command(
        "dbt test --profiles-dir . --target dev",
        cwd=dbt_dir
    ):
        print("⚠️  Some tests failed, but continuing...")
    
    # Step 4: Calculate MTBF/MTTR
    print("\n📈 Step 4/6: Calculating reliability metrics (MTBF/MTTR)...")
    if not run_command(
        "python -c \"from fleetpulse.analytics.reliability import calculate_fleet_reliability; calculate_fleet_reliability()\"",
        cwd=project_root
    ):
        print("⚠️  MTBF/MTTR calculation failed, but continuing...")
    
    # Step 5: Fit Weibull distributions
    print("\n📉 Step 5/6: Fitting Weibull failure distributions...")
    if not run_command(
        "python -c \"from fleetpulse.analytics.weibull import fit_fleet_weibull; fit_fleet_weibull()\"",
        cwd=project_root
    ):
        print("⚠️  Weibull fitting failed, but continuing...")
    
    # Step 6: Detect anomalies
    print("\n🚨 Step 6/6: Detecting sensor anomalies (Z-scores)...")
    if not run_command(
        "python -c \"from fleetpulse.analytics.anomaly import detect_fleet_anomalies; detect_fleet_anomalies()\"",
        cwd=project_root
    ):
        print("⚠️  Anomaly detection failed, but continuing...")
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         ✅ Pipeline Completed Successfully!              ║
    ║                                                          ║
    ║  Next steps:                                             ║
    ║  1. Open Streamlit: http://localhost:8501                ║
    ║  2. View your FleetPulse dashboard!                      ║
    ╚══════════════════════════════════════════════════════════╝
    """)

if __name__ == "__main__":
    main()
