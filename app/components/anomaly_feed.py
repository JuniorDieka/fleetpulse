"""Anomaly Feed Tab - Real-time critical alerts."""

import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime, timedelta


@st.cache_data(ttl=10)
def load_recent_anomalies(db_path: str, hours: int = 24) -> pd.DataFrame:
    """Load recent anomalies."""
    conn = duckdb.connect(db_path, read_only=True)

    cutoff = datetime.now() - timedelta(hours=hours)

    query = f"""
    SELECT
        ROW_NUMBER() OVER (ORDER BY detected_at DESC) as anomaly_id,
        truck_id,
        detected_at,
        sensor as sensor_name,
        value as sensor_value,
        z_score,
        severity,
        FALSE as acknowledged
    FROM analytics.anomalies
    WHERE detected_at >= '{cutoff.isoformat()}'
    ORDER BY detected_at DESC, severity DESC
    """

    df = conn.execute(query).fetchdf()
    conn.close()

    return df


def render_anomaly_feed(db_path: str) -> None:
    """Render the anomaly feed tab."""
    st.header("🚨 Anomaly Feed")

    st.markdown("**Real-time sensor anomaly detection feed** (auto-refreshes every 10 seconds)")

    time_filter = st.selectbox("Time Range", ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"])

    hours_map = {
        "Last 1 Hour": 1,
        "Last 6 Hours": 6,
        "Last 24 Hours": 24,
        "Last 7 Days": 168,
    }

    hours = hours_map[time_filter]

    df = load_recent_anomalies(db_path, hours)

    if df.empty:
        st.success("✅ No anomalies detected in the selected time range!")
        return

    col1, col2, col3 = st.columns(3)

    with col1:
        total_anomalies = len(df)
        st.metric("Total Anomalies", total_anomalies)

    with col2:
        critical_count = len(df[df["severity"] == "critical"])
        st.metric("🚨 Critical", critical_count)

    with col3:
        warning_count = len(df[df["severity"] == "warning"])
        st.metric("⚠️ Warning", warning_count)

    st.markdown("---")

    severity_filter = st.multiselect(
        "Filter by Severity",
        ["critical", "warning"],
        default=["critical", "warning"],
    )

    filtered_df = df[df["severity"].isin(severity_filter)]

    if filtered_df.empty:
        st.info("No anomalies match the selected filters.")
        return

    st.subheader(f"Anomaly Details ({len(filtered_df)} records)")

    for _, row in filtered_df.iterrows():
        severity_emoji = "🚨" if row["severity"] == "critical" else "⚠️"
        ack_status = "✅ Acknowledged" if row["acknowledged"] else "🔔 Unacknowledged"

        with st.expander(
            f"{severity_emoji} {row['truck_id']} - {row['sensor_name']} - "
            f"{row['detected_at'].strftime('%Y-%m-%d %H:%M:%S')}"
        ):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**Truck ID:** {row['truck_id']}")
                st.markdown(f"**Sensor:** {row['sensor_name'].replace('_', ' ').title()}")
                st.markdown(f"**Severity:** {row['severity'].upper()}")

            with col2:
                st.markdown(f"**Value:** {row['sensor_value']:.2f}")
                st.markdown(f"**Z-Score:** {row['z_score']:.2f}σ")
                st.markdown(f"**Status:** {ack_status}")

            if row["severity"] == "critical":
                st.error(
                    "⚠️ **IMMEDIATE ACTION REQUIRED** - This truck requires inspection. "
                    "Potential failure risk detected."
                )

    st.markdown("---")

    st.subheader("Anomaly Distribution by Truck")

    truck_counts = filtered_df["truck_id"].value_counts().reset_index()
    truck_counts.columns = ["Truck ID", "Anomaly Count"]

    st.bar_chart(truck_counts.set_index("Truck ID"))

    st.markdown("---")

    st.subheader("Anomaly Distribution by Sensor")

    sensor_counts = filtered_df["sensor_name"].value_counts().reset_index()
    sensor_counts.columns = ["Sensor", "Anomaly Count"]
    sensor_counts["Sensor"] = sensor_counts["Sensor"].str.replace("_", " ").str.title()

    st.bar_chart(sensor_counts.set_index("Sensor"))
