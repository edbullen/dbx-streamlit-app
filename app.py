import os
from typing import Dict, Optional

import pandas as pd
from dotenv import load_dotenv

from databricks import sql
from databricks.sdk.core import Config

import streamlit as st

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
    username = headers.get("x-forwarded-preferred-username") or headers.get("x-forwarded-user")
    email = headers.get("x-forwarded-email")
    return {"username": username, "email": email}


def credential_provider():
    config = Config(host=f"https://{server_hostname}")
    return config.authenticate


def _run_query(table_name: str, limit: int, connection_kwargs: Dict[str, str]) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name} LIMIT {int(limit)}"
    with sql.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = [r.asDict() for r in cursor.fetchall()]
            return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def get_data(table_name: str, limit: int = 200) -> pd.DataFrame:
    connection_kwargs = dict(
        server_hostname=server_hostname,
        http_path=warehouse_http_path,
        credentials_provider=credential_provider,
    )
    return _run_query(table_name, limit, connection_kwargs)


if __name__ == "__main__":
    forwarded_headers = get_forwarded_headers()
    user_identity = resolve_user_identity(forwarded_headers)

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
    st.sidebar.subheader("Connection details")
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

    # --- Visualisation: fare by passenger count (if columns exist) ---
    # --- Visualisation: average fare by pickup ZIP (works with common nyctaxi.trips schema) ---
    if {"pickup_zip", "fare_amount"}.issubset(data.columns):
        st.subheader("Average fare by pickup ZIP (sample)")

        grouped = (
            data.groupby("pickup_zip")["fare_amount"]
                .mean()
                .reset_index()
                .sort_values("pickup_zip")
        )
        grouped = grouped.rename(columns={"fare_amount": "avg_fare"})

        # Streamlit is happiest if the index is the category
        chart_data = grouped.set_index("pickup_zip")["avg_fare"]

        st.bar_chart(chart_data)

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
