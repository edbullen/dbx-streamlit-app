import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from databricks import sql
from databricks.sdk.core import Config

import streamlit as st
import pydeck as pdk

# import custom functions
from warehouse_queries import warehouse_fares_query
from warehouse_queries import warehouse_dests_query

# for local development - picks up variables from .env file
load_dotenv() 

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
warehouse_http_path = os.getenv("DATABRICKS_HTTP_PATH")


def _local_header_overrides() -> Dict[str, str]:
    """Allow local development to mock Databricks forwarded headers."""
    overrides = {}
    if os.getenv("LOCAL_DEV_USER"):
        overrides["x-forwarded-user"] = os.getenv("LOCAL_DEV_USER")
    if os.getenv("LOCAL_DEV_EMAIL"):
        overrides["x-forwarded-email"] = os.getenv("LOCAL_DEV_EMAIL")
    if os.getenv("LOCAL_USER_TOKEN"):
        overrides["x-forwarded-access-token"] = os.getenv("LOCAL_USER_TOKEN")
    return {k: v for k, v in overrides.items() if v}


def get_forwarded_headers() -> Dict[str, str]:
    """Collect headers Databricks injects when the app runs in the service."""
    headers: Dict[str, str] = {}
    context = getattr(st, "context", None)
    if context is not None:
        context_headers = getattr(context, "headers", None) or {}
        headers = {k.lower(): v for k, v in context_headers.items() if isinstance(k, str)}

    # During local runs, let developers set LOCAL_* vars to mimic headers.
    overrides = _local_header_overrides()
    for key, value in overrides.items():
        headers.setdefault(key, value)

    return headers


def resolve_user_identity(headers: Dict[str, str]) -> Dict[str, Optional[str]]:
    """Get an email ID and user-name from the Streamlit header"""
    username = headers.get("x-forwarded-preferred-username") or headers.get("x-forwarded-user")
    email = headers.get("x-forwarded-email")
    return {"username": username, "email": email}


@st.cache_data(show_spinner=False)
def load_zip_centroids() -> pd.DataFrame:
    """Load ZIP centroids derived from NYC ZIP GeoJSON."""
    path = Path("data/nyc_zip_centroids.json")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_json(path)
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    return df


def credential_provider():
    """Databricks SDK authentication"""
    config = Config(host=f"https://{server_hostname}")
    return config.authenticate

# ** MAIN **
if __name__ == "__main__":
    forwarded_headers = get_forwarded_headers()
    user_identity = resolve_user_identity(forwarded_headers)
    zip_centroids_df = load_zip_centroids()

    # --- Page config / header ---
    st.set_page_config(
        page_title="NYC Taxi Trips – Databricks + Streamlit Demo",
        layout="wide",
    )

    st.title("🚕 NYC Taxi Trips – Databricks App Demo")
    st.caption(
        "Example Databricks App using **Streamlit**, a **Databricks SQL Warehouse**, "
        "and **Databricks authentication**."
    )

    # --- Sidebar: controls & connection info ---
    st.sidebar.header("Controls")

    default_table = "samples.nyctaxi.trips"
    table_name = st.sidebar.text_input("Table name", value=default_table)

    show_raw = st.sidebar.checkbox("Show aggregated data tables", value=False)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Warehouse Connection details")
    st.sidebar.caption("Pulled from environment variables:")
    st.sidebar.code(
        f"Workspace: {server_hostname or 'N/A'}\n"
        f"SQL Warehouse path: {warehouse_http_path or 'N/A'}",
        language="bash",
    )
    viewer_label = user_identity.get("email") or user_identity.get("username") or "Unknown user"
    st.sidebar.caption("Auth mode: App authorization")
    st.sidebar.caption(f"Viewer: {viewer_label}")

    connection_kwargs = dict(
        server_hostname=server_hostname,
        http_path=warehouse_http_path,
        credentials_provider=credential_provider,
    )
    with st.spinner("Loading aggregated data from Databricks SQL Warehouse..."):
        with sql.connect(**connection_kwargs) as connection:
            pickup_fares_df = warehouse_fares_query(connection, table_name)
            pickup_dest_df = warehouse_dests_query(connection, table_name)

    if pickup_fares_df.empty:
        st.warning("No aggregated data returned from the query.")
        st.stop()

    st.success(f"Loaded {len(pickup_fares_df)} pickup ZIP aggregates from `{table_name}`")

    # --- Metrics + map mode selector ---
    st.subheader("Quick stats")

    stats_col, mode_col = st.columns([3, 1])
    map_mode = mode_col.radio("Map view", ["zip trip fares", "zip trip count"], index=0)

    col1, col2, col3 = stats_col.columns(3)

    total_trips = int(pickup_fares_df["count"].sum())
    col1.metric("Total trips (pickup ZIPs)", f"{total_trips:,}")

    if map_mode == "zip trip fares":
        weighted_avg_fare = (pickup_fares_df["avg_fare"] * pickup_fares_df["count"]).sum() / max(total_trips, 1)
        max_avg_fare = pickup_fares_df["avg_fare"].max()
        col2.metric("Weighted avg fare (USD)", f"${weighted_avg_fare:,.2f}")
        col3.metric("Max avg fare by ZIP (USD)", f"${max_avg_fare:,.2f}")
    else:
        avg_trips_per_zip = total_trips / max(len(pickup_fares_df), 1)
        max_trips_zip = pickup_fares_df["count"].max()
        col2.metric("Avg trips per ZIP", f"{avg_trips_per_zip:,.1f}")
        col3.metric("Max trips for a ZIP", f"{max_trips_zip:,}")

    # --- Visualisation: map using ZIP centroids with selectable metric ---
    st.subheader("Pickup ZIP map")

    fares_for_map = pickup_fares_df.rename(columns={"pickup_zip": "zip"}).copy()
    fares_for_map["zip"] = fares_for_map["zip"].astype(str).str.zfill(5)

    merged = (
        fares_for_map.merge(zip_centroids_df, on="zip", how="inner")
        if not zip_centroids_df.empty
        else pd.DataFrame()
    )

    if merged.empty:
        st.info("No ZIP centroids available; showing table instead.")
        metric_sort = "avg_fare" if map_mode == "zip trip fares" else "count"
        st.dataframe(fares_for_map.sort_values(metric_sort, ascending=False), use_container_width=True)
    else:
        value_col = "avg_fare" if map_mode == "zip trip fares" else "count"
        merged = merged.rename(columns={value_col: "metric_value"})
        min_val = merged["metric_value"].min()
        max_val = merged["metric_value"].max()
        span = max(max_val - min_val, 0.01)
        merged = merged.assign(
            _norm=(merged["metric_value"] - min_val) / span,
        )
        merged["_norm"] = merged["_norm"].clip(0, 1)
        merged["_fill_r"] = 60
        merged["_fill_g"] = 255 - (merged["_norm"] * 360).clip(0, 180)
        merged["_fill_b"] = 60
        merged["_radius"] = 100 + merged["_norm"] * 500
        merged["avg_fare_display"] = merged.get("avg_fare", merged["metric_value"]).map(lambda x: f"${x:,.2f}") if map_mode == "zip trip fares" else None
        merged["count_display"] = merged.get("count", merged["metric_value"]).map(lambda x: f"{int(x):,}")

        layer = pdk.Layer(
            "ScatterplotLayer",
            merged,
            get_position="[lon, lat]",
            get_fill_color="[ _fill_r, _fill_g, _fill_b ]",
            get_radius="_radius",
            pickable=True,
            auto_highlight=True,
            opacity=0.6,
        )
        view_state = pdk.ViewState(latitude=40.73, longitude=-73.94, zoom=11, pitch=2)
        if map_mode == "zip trip fares":
            tooltip = {"html": "<b>ZIP {zip}</b><br/>Avg fare: {avg_fare_display}<br/>Trips: {count}"}
        else:
            tooltip = {"html": "<b>ZIP {zip}</b><br/>Trips: {count_display}"}
        st.pydeck_chart(pdk.Deck(map_style=None, layers=[layer], initial_view_state=view_state, tooltip=tooltip))


    # --- Raw data table in an expander ---
    if show_raw:
        st.subheader("Sample data")
        with st.expander("Show data frame", expanded=True):
            if not pickup_fares_df.empty:
                st.markdown("**Average fare by pickup ZIP (warehouse query)**")
                st.dataframe(pickup_fares_df, use_container_width=True, height=300)
            if not pickup_dest_df.empty:
                st.markdown("**Top pickup/dropoff pairs (warehouse query)**")
                st.dataframe(pickup_dest_df, use_container_width=True, height=300)
