"""
FleetPulse Streamlit Dashboard

Three-tab interface:
1. Fleet Overview - KPIs, health table, status map
2. Truck Deep-Dive - Weibull curves, Z-score timeseries, maintenance history
3. Anomaly Feed - Real-time critical alerts
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from fleetpulse.simulator.config import load_config
from app.components.fleet_overview import render_fleet_overview
from app.components.truck_deepdive import render_truck_deepdive
from app.components.anomaly_feed import render_anomaly_feed

st.set_page_config(
    page_title="FleetPulse - Mining Fleet Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🚛 FleetPulse - Mining Fleet Maintenance Dashboard")
st.markdown("**Gold-Mining Haul-Truck Fleet Telemetry & Reliability Analytics**")

config = load_config("config.yaml")
db_path = config["storage"]["warehouse_path"]

if not Path(db_path).exists():
    st.error(
        f"⚠️ Database not found at {db_path}. "
        "Please run the pipeline first: `docker compose up` or `make demo`"
    )
    st.stop()

tab1, tab2, tab3 = st.tabs(["📊 Fleet Overview", "🔍 Truck Deep-Dive", "🚨 Anomaly Feed"])

with tab1:
    render_fleet_overview(db_path)

with tab2:
    render_truck_deepdive(db_path)

with tab3:
    render_anomaly_feed(db_path)

st.sidebar.markdown("---")
st.sidebar.markdown("### About FleetPulse")
st.sidebar.info(
    """
    **Data Pipeline Stages:**
    1. **COLLECTION** - IoT Simulator → Kafka → Parquet
    2. **COMPILATION** - Airflow → DuckDB + dbt
    3. **ANALYSIS** - MTBF, MTTR, Weibull, Z-score
    4. **DISSEMINATION** - Dashboard + Alerts

    **Refresh Rate:** 30 seconds
    """
)
