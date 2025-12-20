import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from databricks import sql
from databricks.sdk.core import Config

import streamlit as st
import pydeck as pdk

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


def _run_query(table_name: str, limit: int, connection_kwargs: Dict[str, str]) -> pd.DataFrame:
    """Execute a SQL query against the provided table-name and connection to Databricks SQL Warehouse"""
    query = f"SELECT * FROM {table_name} LIMIT {int(limit)}"
    with sql.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = [r.asDict() for r in cursor.fetchall()]
            return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def get_data(table_name: str, limit: int = 200) -> pd.DataFrame:
    """Get Streamlit data by calling _run_query() against SQL Warehouse"""
    connection_kwargs = dict(
        server_hostname=server_hostname,
        http_path=warehouse_http_path,
        credentials_provider=credential_provider,
    )
    return _run_query(table_name, limit, connection_kwargs)


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

    row_limit = st.sidebar.slider(
        "Number of rows to load",
        min_value=10,
        max_value=1000,
        step=10,
        value=200,
    )

    show_raw = st.sidebar.checkbox("Show raw data table", value=True)

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

    # --- Data load --- 
    # ToDo: if the warehouse is not available this spins for ever. Need to time-out with a message 
    with st.spinner("Loading data from Databricks SQL Warehouse..."):
        data = get_data(table_name=table_name, limit=row_limit)

    if data.empty:
        st.warning("No data returned from the query.")
        st.stop()

    st.success(f"Loaded {len(data)} rows from `{table_name}`")

    # --- Top-level metrics (defensive: only if columns exist) ---
    st.subheader("Quick stats")

    col1, col2, col3 = st.columns(3)

    # Trip distance
    if "trip_distance" in data.columns:
        avg_dist = data["trip_distance"].mean()
        col1.metric("Average trip distance (miles)", f"{avg_dist:,.2f}")

    # Total amount
    if "total_amount" in data.columns:
        avg_total = data["total_amount"].mean()
        max_total = data["total_amount"].max()
        col2.metric("Average total fare (USD)", f"${avg_total:,.2f}")
        col3.metric("Max total fare (USD)", f"${max_total:,.2f}")

    # --- Visualisation: map using ZIP centroids + average fare by pickup_zip (if columns exist) ---
    if {"pickup_zip", "fare_amount"}.issubset(data.columns):
        st.subheader("Average fare by pickup ZIP (map view)")

        grouped = (
            data.groupby("pickup_zip")["fare_amount"]
            .mean()
            .reset_index()
            .rename(columns={"fare_amount": "avg_fare", "pickup_zip": "zip"})
        )
        grouped["zip"] = grouped["zip"].astype(str).str.zfill(5)

        merged = (
            grouped.merge(zip_centroids_df, on="zip", how="inner")
            if not zip_centroids_df.empty
            else pd.DataFrame()
        )

        if merged.empty:
            st.info("No ZIP centroids available; showing table instead.")
            st.dataframe(grouped.sort_values("avg_fare", ascending=False), use_container_width=True)
        else:
            min_fare = merged["avg_fare"].min()
            max_fare = merged["avg_fare"].max()
            span = max(max_fare - min_fare, 0.01)
            merged = merged.assign(
                _norm=(merged["avg_fare"] - min_fare) / span,
            )
            merged["_norm"] = merged["_norm"].clip(0, 1)
            merged["_fill_r"] = 255
            merged["_fill_g"] = (merged["_norm"] * 180).clip(0, 180)
            merged["_fill_b"] = 60
            merged["_radius"] = 100 + merged["_norm"] * 400

            layer = pdk.Layer(
                "ScatterplotLayer",
                merged,
                get_position="[lon, lat]",
                get_fill_color="[ _fill_r, _fill_g, _fill_b ]",
                get_radius="_radius",
                pickable=True,
                auto_highlight=True,
            )
            view_state = pdk.ViewState(latitude=40.73, longitude=-73.94, zoom=10, pitch=0)
            tooltip = {"html": "<b>ZIP {zip}</b><br/>Avg fare: ${avg_fare:.2f}"}
            st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))

    # --- Optional: top expensive trips ---
    if {"total_amount", "trip_distance"}.issubset(data.columns):
        st.subheader("Top 5 most expensive trips (sample)")
        top_trips = (
            data.sort_values("total_amount", ascending=False)
            .head(5)
            .reset_index(drop=True)
        )
        st.dataframe(top_trips, use_container_width=True)

    # --- Raw data table in an expander ---
    if show_raw:
        st.subheader("Sample data")
        with st.expander("Show data frame", expanded=True):
            st.dataframe(data, use_container_width=True, height=400)
