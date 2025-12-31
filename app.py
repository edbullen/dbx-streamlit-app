import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from databricks import sql
from databricks.sdk.core import Config

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# SQL Alchemy for lakebase  python operations
from lakebase_psql import get_config, get_version, set_config

# import custom functions
from warehouse_queries import warehouse_fares_query
from warehouse_queries import warehouse_dests_query

# for local development - picks up variables from .env file
load_dotenv()

def _resolve_connection_settings() -> Tuple[Optional[str], Optional[str]]:
    """Prefer DB-stored config overrides, fall back to env vars."""
    env_workspace = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    env_warehouse = os.getenv("DATABRICKS_HTTP_PATH")
    try:
        db_workspace = get_config("workspace")
        db_warehouse = get_config("warehouse")
    except Exception:
        # If the config table is unavailable we silently fall back to env.
        db_workspace = db_warehouse = None
    return db_workspace or env_workspace, db_warehouse or env_warehouse


server_hostname, warehouse_http_path = _resolve_connection_settings()


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
    #sankey_limit = st.sidebar.slider("Sankey links (top N pickup→dropoff pairs)", min_value=20, max_value=200, step=10, value=20)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Warehouse Connection details")
    st.sidebar.caption("Config stored in Lakebase take precedence over env vars.")
    initial_workspace = server_hostname or ""
    initial_warehouse = warehouse_http_path or ""
    workspace_input = st.sidebar.text_input("Workspace hostname", value=initial_workspace)
    warehouse_input = st.sidebar.text_input("SQL Warehouse path", value=initial_warehouse)
    dirty = (workspace_input != initial_workspace) or (warehouse_input != initial_warehouse)
    if dirty and st.sidebar.button("Save connection settings"):
        try:
            if workspace_input:
                set_config("workspace", workspace_input)
            if warehouse_input:
                set_config("warehouse", warehouse_input)
            st.sidebar.success("Saved connection settings to Lakebase config.")
            server_hostname = workspace_input or server_hostname
            warehouse_http_path = warehouse_input or warehouse_http_path
        except Exception as exc:
            st.sidebar.error(f"Failed to save settings: {exc}")

    # Use any in-flight edits during the current run.
    server_hostname = workspace_input or server_hostname
    warehouse_http_path = warehouse_input or warehouse_http_path

    st.sidebar.code(
        f"Workspace: {server_hostname or 'N/A'}\n"
        f"SQL Warehouse path: {warehouse_http_path or 'N/A'}",
        language="bash",
    )

    st.sidebar.subheader("Lakebase Connection details")
    postgres_connect_version = get_version()
    st.sidebar.code(
        postgres_connect_version,
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
    map_mode = mode_col.radio("Map view", ["zip trip fares", "zip trip count", "zip trip destinations"], index=0)

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

    # --- Visualisation: map or sankey based on selection ---
    if map_mode in {"zip trip fares", "zip trip count"}:
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
            merged["_radius"] = 50 + (merged["_norm"] * 1000)
            merged["avg_fare_display"] = (
                merged.get("avg_fare", merged["metric_value"]).map(lambda x: f"${x:,.2f}")
                if map_mode == "zip trip fares"
                else None
            )
            merged["count_display"] = merged.get("count", merged["metric_value"]).map(lambda x: f"{int(x):,}")

            color_col = "metric_value"
            fig = px.scatter_map(
                merged,
                lat="lat",
                lon="lon",
                size="_radius",
                color=color_col,
                hover_name="zip",
                hover_data=None,
                color_continuous_scale="Aggrnyl",
                zoom=10.5,
                opacity=0.6,
                height=600,
            )
            if map_mode == "zip trip fares":
                fig.update_traces(
                    customdata=merged[["zip", "avg_fare_display", "count_display"]].values,
                    hovertemplate="<b>ZIP %{customdata[0]}</b><br>Avg Fare: %{customdata[1]}<br>Trip Count: %{customdata[2]}<extra></extra>",
                )
            else:
                fig.update_traces(
                    customdata=merged[["zip", "count_display"]].values,
                    hovertemplate="<b>ZIP %{customdata[0]}</b><br>Trip Count: %{customdata[1]}<extra></extra>",
                )
            fig.update_layout(
                mapbox_style="carto-positron",
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})
    else:
        st.subheader("Top pickup → dropoff ZIPs (Sankey)")

        sankey_limit = st.slider("Sankey links (top N pickup→dropoff pairs)", min_value=20, max_value=200, step=10, value=20)

        if pickup_dest_df.empty:
            st.info("No destination data returned from the query.")
        else:
            trips = pickup_dest_df.sort_values("count", ascending=False).head(sankey_limit)
            trips = trips.rename(columns={"pickup_zip": "source", "dropoff_zip": "target"})
            sources = trips["source"].astype(str)
            targets = trips["target"].astype(str)
            nodes = sorted(set(sources) | set(targets))
            node_index = {zip_code: idx for idx, zip_code in enumerate(nodes)}
            links = dict(
                source=[node_index[z] for z in sources],
                target=[node_index[z] for z in targets],
                value=trips["count"].tolist(),
            )
            sankey_fig = go.Figure(
                data=[
                    go.Sankey(
                        node=dict(label=nodes, pad=15, thickness=15, line=dict(color="gray", width=0.5)),
                        link=links,
                    )
                ]
            )
            sankey_fig.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Arial, sans-serif", size=14),
            )
            st.plotly_chart(sankey_fig, use_container_width=True)


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
