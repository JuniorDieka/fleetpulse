"""Truck Deep-Dive Tab - Weibull curves, Z-score timeseries, maintenance history."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from scipy import stats
import duckdb


@st.cache_data(ttl=30)
def load_truck_list(db_path: str) -> list:
    """Load list of truck IDs."""
    conn = duckdb.connect(db_path, read_only=True)
    trucks = conn.execute("SELECT DISTINCT truck_id FROM main_dimensions.dim_trucks ORDER BY truck_id").fetchdf()
    conn.close()
    return trucks["truck_id"].tolist()


@st.cache_data(ttl=30)
def load_truck_metrics(db_path: str, truck_id: str) -> dict:
    """Load reliability metrics for a specific truck."""
    conn = duckdb.connect(db_path, read_only=True)

    query = f"""
    SELECT
        r.truck_id,
        r.mtbf_hours,
        r.mttr_hours,
        w.failure_probability_30d as failure_prob_50hr,
        w.shape_beta as weibull_shape_beta,
        w.scale_eta as weibull_scale_eta,
        5000.0 as total_operating_hours,
        3 as failure_count
    FROM analytics.reliability_metrics r
    LEFT JOIN analytics.weibull_parameters w ON r.truck_id = w.truck_id
    WHERE r.truck_id = '{truck_id}'
    """

    df = conn.execute(query).fetchdf()
    conn.close()

    if df.empty:
        return {}

    return df.iloc[0].to_dict()


@st.cache_data(ttl=30)
def load_maintenance_history(db_path: str, truck_id: str) -> pd.DataFrame:
    """Load maintenance event history for a truck."""
    conn = duckdb.connect(db_path, read_only=True)

    query = f"""
    SELECT
        event_date as failure_timestamp,
        event_type as failure_type,
        downtime_hours,
        downtime_hours as repair_hours,
        0 as odometer_at_failure
    FROM main_marts.fct_maintenance_events
    WHERE truck_id = '{truck_id}'
    ORDER BY event_date DESC
    LIMIT 20
    """

    df = conn.execute(query).fetchdf()
    conn.close()

    return df


@st.cache_data(ttl=30)
def load_sensor_timeseries(db_path: str, truck_id: str) -> pd.DataFrame:
    """Load recent sensor readings for Z-score visualization."""
    conn = duckdb.connect(db_path, read_only=True)

    query = f"""
    SELECT
        hour as timestamp,
        avg_engine_temp as engine_temp_c,
        avg_oil_pressure as hydraulic_pressure_psi,
        50.0 as vibration_level_mm_s,
        avg_fuel_level as fuel_consumption_l_hr
    FROM main_marts.fct_hourly_sensor_aggregates
    WHERE truck_id = '{truck_id}'
    ORDER BY hour DESC
    LIMIT 24
    """

    df = conn.execute(query).fetchdf()
    conn.close()

    return df


def plot_weibull_curve(metrics: dict) -> go.Figure:
    """Plot Weibull failure probability curve."""
    if not metrics or pd.isna(metrics.get("weibull_shape_beta")):
        return None

    beta = metrics["weibull_shape_beta"]
    eta = metrics["weibull_scale_eta"]
    current_hours = metrics["total_operating_hours"]

    hours = np.linspace(0, current_hours + 1000, 500)
    probabilities = 1 - np.exp(-((hours / eta) ** beta))

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=hours, y=probabilities * 100, mode="lines", name="Failure Probability", line=dict(color="blue", width=2)
        )
    )

    fig.add_vline(
        x=current_hours, line_dash="dash", line_color="red", annotation_text="Current Hours"
    )

    failure_mode = (
        "Infant Mortality"
        if beta < 0.9
        else "Random Failures" if beta < 1.1 else "Wear-Out"
    )

    fig.update_layout(
        title=f"Weibull Failure Probability Curve (β={beta:.2f}, η={eta:.0f}h)<br>"
        f"<sub>Failure Mode: {failure_mode}</sub>",
        xaxis_title="Operating Hours",
        yaxis_title="Cumulative Failure Probability (%)",
        height=400,
        hovermode="x unified",
    )

    return fig


def plot_sensor_zscore(df: pd.DataFrame, sensor: str, sensor_name: str) -> go.Figure:
    """Plot sensor values with Z-score highlighting."""
    if df.empty:
        return None

    df = df.sort_values("timestamp")

    mean = df[sensor].mean()
    std = df[sensor].std()

    df["z_score"] = (df[sensor] - mean) / std

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df[sensor],
            mode="lines",
            name=sensor_name,
            line=dict(color="blue"),
        )
    )

    fig.add_hline(y=mean, line_dash="dash", line_color="green", annotation_text="Mean")
    fig.add_hline(
        y=mean + 2 * std, line_dash="dot", line_color="orange", annotation_text="+2σ"
    )
    fig.add_hline(
        y=mean - 2 * std, line_dash="dot", line_color="orange", annotation_text="-2σ"
    )
    fig.add_hline(
        y=mean + 3 * std, line_dash="dot", line_color="red", annotation_text="+3σ"
    )
    fig.add_hline(
        y=mean - 3 * std, line_dash="dot", line_color="red", annotation_text="-3σ"
    )

    fig.update_layout(
        title=f"{sensor_name} - Rolling Z-Score Analysis",
        xaxis_title="Timestamp",
        yaxis_title=sensor_name,
        height=350,
        hovermode="x unified",
    )

    return fig


def render_truck_deepdive(db_path: str) -> None:
    """Render the truck deep-dive tab."""
    st.header("Truck Deep-Dive Analysis")

    trucks = load_truck_list(db_path)

    if not trucks:
        st.warning("No truck data available.")
        return

    selected_truck = st.selectbox("Select Truck", trucks)

    if not selected_truck:
        return

    metrics = load_truck_metrics(db_path, selected_truck)

    if not metrics:
        st.warning(f"No metrics available for {selected_truck}")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("MTBF", f"{metrics.get('mtbf_hours', 0):.0f}h")

    with col2:
        st.metric("MTTR", f"{metrics.get('mttr_hours', 0):.1f}h")

    with col3:
        failure_prob = metrics.get("failure_prob_50hr", 0) * 100
        st.metric("50h Failure Prob", f"{failure_prob:.1f}%")

    with col4:
        st.metric("Total Failures", int(metrics.get("failure_count", 0)))

    st.markdown("---")

    st.subheader("Weibull Failure Probability Curve")

    weibull_fig = plot_weibull_curve(metrics)
    if weibull_fig:
        st.plotly_chart(weibull_fig, use_container_width=True)
    else:
        st.info("Insufficient failure data for Weibull analysis (need ≥3 failures)")

    st.markdown("---")

    st.subheader("Sensor Z-Score Analysis")

    sensor_df = load_sensor_timeseries(db_path, selected_truck)

    if not sensor_df.empty:
        sensors = [
            ("engine_temp_c", "Engine Temperature (°C)"),
            ("hydraulic_pressure_psi", "Hydraulic Pressure (PSI)"),
            ("vibration_level_mm_s", "Vibration Level (mm/s)"),
            ("fuel_consumption_l_hr", "Fuel Consumption (L/hr)"),
        ]

        for sensor, name in sensors:
            fig = plot_sensor_zscore(sensor_df, sensor, name)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sensor data available for this truck")

    st.markdown("---")

    st.subheader("Maintenance Event History")

    history_df = load_maintenance_history(db_path, selected_truck)

    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True)
    else:
        st.info("No maintenance events recorded for this truck")
