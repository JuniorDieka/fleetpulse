"""Fleet Overview Tab - KPIs, health table, and status map."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb


@st.cache_data(ttl=30)
def load_fleet_metrics(db_path: str) -> pd.DataFrame:
    """Load fleet-wide reliability metrics."""
    conn = duckdb.connect(db_path, read_only=True)

    query = """
    SELECT
        r.truck_id,
        r.mtbf_hours,
        r.mttr_hours,
        w.failure_probability_30d as failure_prob_50hr,
        w.shape_beta as weibull_shape_beta,
        t.pit_zone,
        t.maintenance_tier,
        CASE
            WHEN w.failure_probability_30d > 0.25 THEN 'Critical'
            WHEN w.failure_probability_30d > 0.15 THEN 'Warning'
            ELSE 'Operational'
        END as status,
        (
            SELECT MAX(detected_at)
            FROM analytics.anomalies a
            WHERE a.truck_id = r.truck_id
                AND a.severity = 'critical'
        ) as last_critical_anomaly
    FROM analytics.reliability_metrics r
    JOIN main_dimensions.dim_trucks t ON r.truck_id = t.truck_id
    LEFT JOIN analytics.weibull_parameters w ON r.truck_id = w.truck_id
    ORDER BY r.truck_id
    """

    df = conn.execute(query).fetchdf()
    conn.close()

    return df


def render_fleet_overview(db_path: str) -> None:
    """Render the fleet overview tab."""
    st.header("Fleet Health Overview")

    df = load_fleet_metrics(db_path)

    if df.empty:
        st.warning("No fleet data available. Pipeline may still be processing.")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        operational = len(df[df["status"] == "Operational"])
        st.metric("✅ Operational", operational, delta=None)

    with col2:
        warning = len(df[df["status"] == "Warning"])
        st.metric("⚠️ Warning", warning, delta=None)

    with col3:
        critical = len(df[df["status"] == "Critical"])
        st.metric("🚨 Critical", critical, delta=None)

    with col4:
        avg_mtbf = df["mtbf_hours"].mean()
        st.metric("Avg MTBF", f"{avg_mtbf:.0f}h", delta=None)

    st.markdown("---")

    st.subheader("Fleet Health Table")

    display_df = df[
        [
            "truck_id",
            "status",
            "mtbf_hours",
            "mttr_hours",
            "failure_prob_50hr",
            "pit_zone",
            "maintenance_tier",
        ]
    ].copy()

    display_df["failure_prob_50hr"] = (display_df["failure_prob_50hr"] * 100).round(1)
    display_df["mtbf_hours"] = display_df["mtbf_hours"].round(0)
    display_df["mttr_hours"] = display_df["mttr_hours"].round(1)

    display_df.columns = [
        "Truck ID",
        "Status",
        "MTBF (hrs)",
        "MTTR (hrs)",
        "Failure Prob 50h (%)",
        "Pit Zone",
        "Tier",
    ]

    def color_status(val):
        if val == "Critical":
            return "background-color: #ff4444; color: white"
        elif val == "Warning":
            return "background-color: #ffaa00; color: black"
        else:
            return "background-color: #44ff44; color: black"

    styled_df = display_df.style.applymap(color_status, subset=["Status"])

    st.dataframe(styled_df, use_container_width=True, height=400)

    st.markdown("---")

    st.subheader("Fleet Status Map by Pit Zone")

    status_colors = {"Operational": "green", "Warning": "orange", "Critical": "red"}

    df["color"] = df["status"].map(status_colors)
    df["size"] = df["failure_prob_50hr"] * 100

    # Namoya mine's four open-pit deposits in Salamabila, DRC
    zone_coords = {
        "North Pit": (0, 1),      # Mt. Mwendamboko
        "South Pit": (0, -1),     # Muviringu
        "East Pit": (1, 0),       # Kakula
        "West Pit": (-1, 0)       # Namoya Summit
    }

    df["x"] = df["pit_zone"].map(lambda z: zone_coords.get(z, (0, 0))[0])
    df["y"] = df["pit_zone"].map(lambda z: zone_coords.get(z, (0, 0))[1])

    df["x"] = df["x"] + (pd.Series(range(len(df))) % 5 - 2) * 0.15
    df["y"] = df["y"] + (pd.Series(range(len(df))) // 5 - 2) * 0.15

    fig = px.scatter(
        df,
        x="x",
        y="y",
        color="status",
        size="size",
        hover_data=["truck_id", "failure_prob_50hr", "mtbf_hours"],
        color_discrete_map=status_colors,
        title="Truck Status by Pit Zone",
    )

    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        showlegend=True,
        height=500,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    )

    # Add pit zone labels with actual deposit names
    zone_labels = {
        "North Pit": "Mt. Mwendamboko",
        "South Pit": "Muviringu", 
        "East Pit": "Kakula",
        "West Pit": "Namoya Summit"
    }
    
    for zone, (x, y) in zone_coords.items():
        fig.add_annotation(
            x=x,
            y=y + 0.7,
            text=f"<b>{zone}</b><br>{zone_labels[zone]}",
            showarrow=False,
            font=dict(size=12),
        )

    st.plotly_chart(fig, use_container_width=True)
