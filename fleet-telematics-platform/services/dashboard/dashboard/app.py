"""Streamlit dashboard: live H3 vehicle-density heatmap + speed-anomaly feed.

Run:
    poetry run streamlit run dashboard/app.py

Reads exclusively through services/api (never queries ClickHouse directly)
so the dashboard's access pattern is exactly what a third-party consumer of
this platform would get.
"""

from __future__ import annotations

import os

import h3
import httpx
import pandas as pd
import pydeck as pdk
import streamlit as st

API_BASE_URL = os.environ.get("FLEET_API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="Fleet Telematics — Live Density & Alerts", layout="wide")
st.title("Fleet Telematics — Live H3 Density & Speed Anomalies")

with st.sidebar:
    st.header("Viewport")
    lat = st.number_input("Center latitude", value=40.7128, format="%.6f")
    lon = st.number_input("Center longitude", value=-74.0060, format="%.6f")
    rings = st.slider("H3 k-ring radius", min_value=1, max_value=20, value=8)
    minutes = st.slider("Look-back window (minutes)", min_value=1, max_value=60, value=5)
    st.caption("Defaults to lower Manhattan; point it at wherever your ingestion feed covers.")


@st.cache_data(ttl=15)
def fetch_density(lat: float, lon: float, rings: int, minutes: int) -> pd.DataFrame:
    resp = httpx.get(
        f"{API_BASE_URL}/density",
        params={"lat": lat, "lon": lon, "rings": rings, "minutes": minutes},
        timeout=10,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


@st.cache_data(ttl=15)
def fetch_anomalies(lat: float, lon: float, minutes: int) -> pd.DataFrame:
    resp = httpx.get(
        f"{API_BASE_URL}/anomalies",
        params={"lat": lat, "lon": lon, "minutes": minutes},
        timeout=10,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


col_map, col_alerts = st.columns([2, 1])

with col_map:
    st.subheader("Vehicle density (last %d min)" % minutes)
    try:
        density_df = fetch_density(lat, lon, rings, minutes)
    except httpx.HTTPError as exc:
        st.error(f"Could not reach the API at {API_BASE_URL}: {exc}")
        density_df = pd.DataFrame()

    if density_df.empty:
        st.info("No density data yet — is the streaming job running and publishing to ClickHouse?")
    else:
        latest = density_df.sort_values("window_start").groupby("h3_r9").tail(1)
        latest["boundary"] = latest["h3_r9"].apply(
            lambda cell: [[p[1], p[0]] for p in h3.cell_to_boundary(h3.int_to_str(int(cell)))]
        )

        layer = pdk.Layer(
            "PolygonLayer",
            data=latest,
            get_polygon="boundary",
            get_fill_color="[255, 140 - vehicle_count * 4, 60, 160]",
            get_line_color=[30, 30, 30],
            pickable=True,
            auto_highlight=True,
        )
        view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=12)
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={"text": "vehicles: {vehicle_count}\navg speed: {avg_speed_mps} m/s"},
            )
        )
        st.caption(f"{len(latest)} H3-r9 cells with recent traffic")

with col_alerts:
    st.subheader("Speed anomaly alerts")
    try:
        anomalies_df = fetch_anomalies(lat, lon, minutes=15)
    except httpx.HTTPError as exc:
        st.error(f"Could not reach the API: {exc}")
        anomalies_df = pd.DataFrame()

    if anomalies_df.empty:
        st.info("No anomalies in range — quiet traffic, or nothing flagged yet.")
    else:
        for _, row in anomalies_df.iterrows():
            severity_icon = "🔴" if row["severity"] == "CRITICAL" else "🟠"
            st.markdown(
                f"{severity_icon} **{row['vehicle_id']}** ({row['route_id']}) — "
                f"{row['speed_mps']:.1f} m/s vs. local mean {row['window_mean_speed_mps']:.1f} m/s "
                f"(z={row['z_score']:.1f}) · {row['alert_time']}"
            )
