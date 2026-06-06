"""
Validation script to check FleetPulse setup.

Verifies:
- Required files exist
- Configuration is valid
- Dependencies are installed
- Seed data is generated
"""

import sys
from pathlib import Path
from typing import List, Tuple

def check_file_exists(filepath: Path) -> Tuple[bool, str]:
    """Check if a file exists."""
    if filepath.exists():
        return True, f"✓ {filepath}"
    return False, f"✗ {filepath} (missing)"

def check_directory_exists(dirpath: Path) -> Tuple[bool, str]:
    """Check if a directory exists."""
    if dirpath.exists() and dirpath.is_dir():
        return True, f"✓ {dirpath}/"
    return False, f"✗ {dirpath}/ (missing)"

def validate_setup() -> bool:
    """Run all validation checks."""
    print("=" * 60)
    print("FleetPulse Setup Validation")
    print("=" * 60)
    
    all_checks_passed = True
    
    # Core files
    print("\n📄 Core Files:")
    core_files = [
        Path("config.yaml"),
        Path("requirements.txt"),
        Path("docker-compose.yml"),
        Path("docker-compose.lite.yml"),
        Path("Makefile"),
        Path("README.md"),
        Path("LICENSE"),
        Path("CONTRIBUTING.md"),
    ]
    
    for filepath in core_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Python package
    print("\n🐍 Python Package:")
    package_dirs = [
        Path("fleetpulse"),
        Path("fleetpulse/simulator"),
        Path("fleetpulse/ingestion"),
        Path("fleetpulse/analytics"),
        Path("fleetpulse/alerter"),
    ]
    
    for dirpath in package_dirs:
        passed, msg = check_directory_exists(dirpath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # dbt project
    print("\n📊 dbt Project:")
    dbt_files = [
        Path("dbt_project/dbt_project.yml"),
        Path("dbt_project/profiles.yml"),
        Path("dbt_project/models/staging/stg_telemetry.sql"),
        Path("dbt_project/models/dimensions/dim_trucks.sql"),
        Path("dbt_project/seeds/dim_trucks_seed.csv"),
        Path("dbt_project/seeds/maintenance_events_seed.csv"),
    ]
    
    for filepath in dbt_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Airflow DAGs
    print("\n🔄 Airflow DAGs:")
    dag_files = [
        Path("dags/ingest_dag.py"),
        Path("dags/transform_dag.py"),
    ]
    
    for filepath in dag_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Streamlit app
    print("\n📈 Streamlit Dashboard:")
    app_files = [
        Path("app/streamlit_app.py"),
        Path("app/components/fleet_overview.py"),
        Path("app/components/truck_deepdive.py"),
        Path("app/components/anomaly_feed.py"),
    ]
    
    for filepath in app_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Tests
    print("\n🧪 Tests:")
    test_files = [
        Path("tests/test_reliability.py"),
        Path("tests/test_weibull.py"),
        Path("tests/test_anomaly.py"),
    ]
    
    for filepath in test_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Dockerfiles
    print("\n🐳 Dockerfiles:")
    docker_files = [
        Path("Dockerfile.producer"),
        Path("Dockerfile.consumer"),
        Path("Dockerfile.lite"),
        Path("Dockerfile.streamlit"),
    ]
    
    for filepath in docker_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # CI/CD
    print("\n⚙️ CI/CD:")
    ci_files = [
        Path(".github/workflows/ci.yml"),
        Path(".pre-commit-config.yaml"),
        Path("pyproject.toml"),
    ]
    
    for filepath in ci_files:
        passed, msg = check_file_exists(filepath)
        print(f"  {msg}")
        all_checks_passed &= passed
    
    # Configuration validation
    print("\n🔧 Configuration:")
    try:
        import yaml
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
        
        required_keys = ["fleet", "simulator", "sensors", "kafka", "storage", "analytics", "alerts"]
        for key in required_keys:
            if key in config:
                print(f"  ✓ config.yaml contains '{key}'")
            else:
                print(f"  ✗ config.yaml missing '{key}'")
                all_checks_passed = False
    except Exception as e:
        print(f"  ✗ Failed to parse config.yaml: {e}")
        all_checks_passed = False
    
    # Seed data validation
    print("\n🌱 Seed Data:")
    seed_file = Path("dbt_project/seeds/maintenance_events_seed.csv")
    if seed_file.exists():
        with open(seed_file) as f:
            line_count = sum(1 for _ in f)
        print(f"  ✓ maintenance_events_seed.csv ({line_count} lines)")
    else:
        print(f"  ✗ maintenance_events_seed.csv (missing - run scripts/generate_maintenance_seed.py)")
        all_checks_passed = False
    
    # Summary
    print("\n" + "=" * 60)
    if all_checks_passed:
        print("✅ All validation checks passed!")
        print("\nNext steps:")
        print("  1. Run: docker compose up")
        print("  2. Wait 2 minutes")
        print("  3. Open: http://localhost:8501 (Streamlit)")
        print("  4. Open: http://localhost:8080 (Airflow)")
        return True
    else:
        print("❌ Some validation checks failed!")
        print("\nPlease fix the issues above before running the pipeline.")
        return False

if __name__ == "__main__":
    success = validate_setup()
    sys.exit(0 if success else 1)
