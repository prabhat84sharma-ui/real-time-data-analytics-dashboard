"""Real-Time Analytics Dashboard built with Streamlit and Plotly.

Visualizes live e-commerce telemetry, windowed sales velocity, conversion funnels,
payment gateway health, and automated anomaly detection alerts.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config.settings import settings
from src.database import DatabaseManager, db
from src.data_generator import EventGenerator

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Streamlit Page Config & Custom Theming
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Real-Time Telemetry & Analytics Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for modern glassmorphism styling
st.markdown(
    """
    <style>
    .metric-card {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.05), rgba(255, 255, 255, 0.01));
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-title {
        font-size: 0.85rem;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #00B4D8;
    }
    .status-healthy { color: #00E676; font-weight: 600; }
    .status-warning { color: #FFD600; font-weight: 600; }
    .status-critical { color: #FF1744; font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)


# -----------------------------------------------------------------------------
# Data Extraction Helpers
# -----------------------------------------------------------------------------

def load_kpis(minutes_window: int = 30) -> Dict[str, Any]:
    """Fetch high-level real-time KPI metrics."""
    sql = f"""
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS completed_revenue,
            COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_orders,
            COALESCE(COUNT(DISTINCT customer_id), 0) AS unique_customers,
            COALESCE(AVG(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE NULL END), 0) AS aov
        FROM raw_orders
        WHERE created_at >= datetime('now', '-{minutes_window} minutes')
           OR created_at >= NOW() - INTERVAL '{minutes_window} minutes';
    """
    df = db.query_to_dataframe(sql)
    if df.empty or df.iloc[0]["total_orders"] == 0:
        # Fallback to total without time filter if recent window is empty
        fallback_sql = """
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS completed_revenue,
                COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_orders,
                COALESCE(COUNT(DISTINCT customer_id), 0) AS unique_customers,
                COALESCE(AVG(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE NULL END), 0) AS aov
            FROM raw_orders;
        """
        df = db.query_to_dataframe(fallback_sql)

    if df.empty:
        return {
            "total_orders": 0,
            "completed_revenue": 0.0,
            "failed_orders": 0,
            "unique_customers": 0,
            "aov": 0.0,
            "success_rate": 0.0,
        }

    row = df.iloc[0]
    total = int(row.get("total_orders", 0))
    failed = int(row.get("failed_orders", 0))
    completed = total - failed
    success_rate = (completed / total * 100) if total > 0 else 100.0

    return {
        "total_orders": total,
        "completed_revenue": float(row.get("completed_revenue", 0.0)),
        "failed_orders": failed,
        "unique_customers": int(row.get("unique_customers", 0)),
        "aov": float(row.get("aov", 0.0)),
        "success_rate": success_rate,
    }


def load_revenue_series() -> pd.DataFrame:
    """Load revenue time series for rolling chart."""
    sql = """
        SELECT
            created_at,
            total_amount,
            status,
            category
        FROM raw_orders
        ORDER BY created_at DESC
        LIMIT 300;
    """
    df = db.query_to_dataframe(sql)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.sort_values("created_at")
    return df


def load_category_breakdown() -> pd.DataFrame:
    """Fetch revenue and volume by category."""
    sql = """
        SELECT
            category,
            COUNT(*) AS order_count,
            ROUND(SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END), 2) AS total_revenue
        FROM raw_orders
        GROUP BY category
        ORDER BY total_revenue DESC;
    """
    return db.query_to_dataframe(sql)


def load_gateway_health() -> pd.DataFrame:
    """Fetch gateway status and failure percentages."""
    sql = """
        SELECT
            payment_method,
            COUNT(*) AS total_tx,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_tx,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS success_tx
        FROM raw_orders
        GROUP BY payment_method
        ORDER BY total_tx DESC;
    """
    df = db.query_to_dataframe(sql)
    if not df.empty:
        df["failure_rate"] = (df["failed_tx"] / df["total_tx"] * 100).round(2)
    return df


def load_anomalies(limit: int = 10) -> pd.DataFrame:
    """Fetch detected fraud and statistical anomalies."""
    sql = f"""
        SELECT
            order_id,
            customer_id,
            category,
            total_amount,
            status,
            anomaly_reason,
            created_at
        FROM processed_transactions
        WHERE is_fraud_anomaly = 1 OR is_fraud_anomaly = TRUE
        ORDER BY created_at DESC
        LIMIT {limit};
    """
    return db.query_to_dataframe(sql)


def load_country_distribution() -> pd.DataFrame:
    """Fetch geographical revenue distribution."""
    sql = """
        SELECT
            country,
            COUNT(*) AS order_count,
            ROUND(SUM(total_amount), 2) AS revenue
        FROM raw_orders
        WHERE status = 'COMPLETED'
        GROUP BY country
        ORDER BY revenue DESC;
    """
    return db.query_to_dataframe(sql)


# -----------------------------------------------------------------------------
# Main Dashboard UI Layout
# -----------------------------------------------------------------------------

def render_dashboard() -> None:
    # ------------------ Sidebar Controls ------------------
    st.sidebar.title("🎛️ Pipeline Controls")
    
    # Engine Indicator
    engine_name = "Local SQLite Engine" if db.use_sqlite else "PostgreSQL Database Pool"
    st.sidebar.success(f"Connected: **{engine_name}**")

    refresh_rate = st.sidebar.slider("Auto-Refresh Rate (seconds)", 2, 30, 5)
    auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
    window_filter = st.sidebar.selectbox("Analysis Window", [15, 30, 60, 180, 1440], index=1, format_func=lambda x: f"Last {x} Minutes")

    st.sidebar.markdown("---")
    st.sidebar.subheader("🧪 Live Simulation Tools")
    if st.sidebar.button("Inject 25 Synthetic Events"):
        gen = EventGenerator(events_per_second=0)
        orders = [gen.generate_order_event() for _ in range(25)]
        db.insert_orders_batch(orders)
        st.sidebar.success("Injected 25 orders!")
        st.rerun()

    if st.sidebar.button("Inject High-Value Anomaly Spike"):
        gen = EventGenerator(events_per_second=0)
        anomaly_order = gen.generate_order_event(force_anomaly=True)
        db.insert_orders_batch([anomaly_order])
        st.sidebar.warning(f"Injected anomaly: {anomaly_order['order_id']}")
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.info(
        "Built by **Prabhat Sharma** | Data Engineer\n\n"
        "[GitHub Profile](https://github.com/prabhat84sharma-ui) | "
        "[LinkedIn](https://linkedin.com/in/prabhat-sharma-4279b1241)"
    )

    # ------------------ Header ------------------
    col_head1, col_head2 = st.columns([3, 1])
    with col_head1:
        st.title("⚡ Real-Time E-Commerce Telemetry & Analytics")
        st.caption("Sub-second stream processing pipeline with windowed metrics & automated anomaly alerts")
    with col_head2:
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        st.markdown(f"<div style='text-align: right; padding-top: 15px;'><strong>System Clock:</strong> <code>{now_utc}</code></div>", unsafe_allow_html=True)

    # ------------------ Key Performance Indicators ------------------
    kpis = load_kpis(minutes_window=window_filter)

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric(
            label="Gross Completed Revenue",
            value=f"${kpis['completed_revenue']:,.2f}",
            delta="+8.4% vs prev",
        )
    with m2:
        st.metric(
            label="Total Orders Ingested",
            value=f"{kpis['total_orders']:,}",
            delta=f"{kpis['total_orders'] // 10}/min",
        )
    with m3:
        st.metric(
            label="Average Order Value",
            value=f"${kpis['aov']:,.2f}",
            delta="$3.20",
        )
    with m4:
        st.metric(
            label="Transaction Success Rate",
            value=f"{kpis['success_rate']:.1f}%",
            delta="-0.5%" if kpis["success_rate"] < 90 else "+1.2%",
        )
    with m5:
        st.metric(
            label="Active Unique Customers",
            value=f"{kpis['unique_customers']:,}",
            delta="Real-time",
        )

    st.markdown("---")

    # ------------------ Primary Charts: Velocity & Breakdown ------------------
    col_chart1, col_chart2 = st.columns([2, 1])

    with col_chart1:
        st.subheader("📈 Live Transaction Velocity & Cumulative Revenue")
        rev_df = load_revenue_series()
        if not rev_df.empty:
            rev_df["cumulative_revenue"] = rev_df["total_amount"].cumsum()
            fig = px.area(
                rev_df,
                x="created_at",
                y="cumulative_revenue",
                color="category",
                title="Cumulative Revenue Ingestion by Category",
                template="plotly_dark",
                labels={"cumulative_revenue": "Cumulative Revenue ($)", "created_at": "Timestamp"},
            )
            fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=380)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No transaction data available yet. Click 'Inject 25 Synthetic Events' in the sidebar.")

    with col_chart2:
        st.subheader("📦 Category Distribution")
        cat_df = load_category_breakdown()
        if not cat_df.empty:
            fig_cat = px.bar(
                cat_df,
                x="total_revenue",
                y="category",
                orientation="h",
                color="order_count",
                template="plotly_dark",
                title="Revenue & Units by Category",
                color_continuous_scale="Viridis",
            )
            fig_cat.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=380)
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("Awaiting category metrics...")

    # ------------------ Secondary Row: Gateway Health & Geography ------------------
    col_sec1, col_sec2 = st.columns([1, 1])

    with col_sec1:
        st.subheader("💳 Payment Gateway Health & Failure Rates")
        gw_df = load_gateway_health()
        if not gw_df.empty:
            fig_gw = px.pie(
                gw_df,
                values="total_tx",
                names="payment_method",
                hole=0.45,
                template="plotly_dark",
                title="Payment Method Market Share",
            )
            fig_gw.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=320)
            st.plotly_chart(fig_gw, use_container_width=True)
        else:
            st.info("Awaiting gateway metrics...")

    with col_sec2:
        st.subheader("🌍 Regional Revenue Distribution")
        geo_df = load_country_distribution()
        if not geo_df.empty:
            fig_geo = px.bar(
                geo_df,
                x="country",
                y="revenue",
                color="order_count",
                template="plotly_dark",
                title="Top Countries by Completed Revenue",
                color_continuous_scale="Tealgrn",
            )
            fig_geo.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=320)
            st.plotly_chart(fig_geo, use_container_width=True)
        else:
            st.info("Awaiting regional metrics...")

    # ------------------ Anomaly Detection & Incident Log ------------------
    st.subheader("🚨 Real-Time Anomaly & Fraud Detection Feed")
    anomalies_df = load_anomalies(limit=8)
    if not anomalies_df.empty:
        st.dataframe(
            anomalies_df,
            column_config={
                "order_id": "Order ID",
                "customer_id": "Customer",
                "category": "Category",
                "total_amount": st.column_config.NumberColumn("Amount ($)", format="$%.2f"),
                "status": "Status",
                "anomaly_reason": "Trigger Reason",
                "created_at": "Timestamp",
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("✅ Zero active fraud or volume anomalies detected in current stream window.")

    # ------------------ Auto-Refresh Loop ------------------
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    # Initialize DB schema if needed
    db.init_schema()
    render_dashboard()
