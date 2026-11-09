"""
app.py — Real-time Streamlit dashboard for the stock pipeline

Displays live prices, technical indicators, alerts, and
daily summary from PostgreSQL.

Usage:
    streamlit run src/dashboard/app.py
"""

import sys
import os

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, SYMBOLS


# ─────────────────────── DB helpers ───────────────────────────

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)


# ─────────────────────── Page config ──────────────────────────

st.set_page_config(page_title="Stock Pipeline Dashboard", layout="wide")
st.title("Real-Time Stock Market Pipeline")
st.caption("Live data from Kafka → PostgreSQL → Streamlit")


# ─────────────────────── Sidebar controls ─────────────────────

st.sidebar.header("Controls")
selected_symbol = st.sidebar.selectbox("Symbol", SYMBOLS)
tick_limit = st.sidebar.slider("Ticks to display", 50, 500, 200, 50)
auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)

if auto_refresh:
    st.sidebar.info("Dashboard refreshes every 5 seconds")
    # Streamlit's auto-rerun
    import time
    st.empty()


# ─────────────────────── KPI tiles ────────────────────────────

st.subheader(f"{selected_symbol} — Latest Snapshot")

latest = query_df(
    """
    SELECT price, sma_20, sma_50, ema_20, vwap, rolling_std_20,
           pct_change, sma_cross_signal, price_vs_vwap, timestamp
    FROM stock.enriched_ticks
    WHERE symbol = %s
    ORDER BY timestamp DESC LIMIT 1
    """,
    (selected_symbol,),
)

if not latest.empty:
    row = latest.iloc[0]
    cols = st.columns(6)
    cols[0].metric("Price", f"${row['price']:.2f}",
                   f"{row['pct_change']*100:+.3f}%" if row['pct_change'] else None)
    cols[1].metric("SMA 20", f"${row['sma_20']:.2f}" if row['sma_20'] else "—")
    cols[2].metric("SMA 50", f"${row['sma_50']:.2f}" if row['sma_50'] else "—")
    cols[3].metric("EMA 20", f"${row['ema_20']:.2f}" if row['ema_20'] else "—")
    cols[4].metric("VWAP", f"${row['vwap']:.2f}" if row['vwap'] else "—")
    cols[5].metric("Signal", row['sma_cross_signal'] or "—")
else:
    st.info("No data yet — waiting for ticks to arrive from Kafka...")
    st.stop()


# ─────────────────────── Price chart with indicators ──────────

st.subheader("Price & Indicators")

chart_data = query_df(
    """
    SELECT timestamp, price, sma_20, sma_50, ema_20, vwap, volume
    FROM stock.enriched_ticks
    WHERE symbol = %s
    ORDER BY timestamp DESC LIMIT %s
    """,
    (selected_symbol, tick_limit),
)
chart_data = chart_data.sort_values("timestamp")

if not chart_data.empty:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=("Price & Moving Averages", "Volume"),
    )

    # Price line
    fig.add_trace(go.Scatter(
        x=chart_data["timestamp"], y=chart_data["price"],
        name="Price", line=dict(color="#2196F3", width=2),
    ), row=1, col=1)

    # SMA 20
    fig.add_trace(go.Scatter(
        x=chart_data["timestamp"], y=chart_data["sma_20"],
        name="SMA 20", line=dict(color="#FF9800", width=1, dash="dash"),
    ), row=1, col=1)

    # SMA 50
    fig.add_trace(go.Scatter(
        x=chart_data["timestamp"], y=chart_data["sma_50"],
        name="SMA 50", line=dict(color="#E91E63", width=1, dash="dot"),
    ), row=1, col=1)

    # VWAP
    fig.add_trace(go.Scatter(
        x=chart_data["timestamp"], y=chart_data["vwap"],
        name="VWAP", line=dict(color="#4CAF50", width=1),
    ), row=1, col=1)

    # Volume bars
    fig.add_trace(go.Bar(
        x=chart_data["timestamp"], y=chart_data["volume"],
        name="Volume", marker_color="#90CAF9", opacity=0.6,
    ), row=2, col=1)

    fig.update_layout(height=550, showlegend=True, margin=dict(t=40, b=20))
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────── Recent alerts ────────────────────────

st.subheader("Recent Alerts")

alerts = query_df(
    """
    SELECT triggered_at, symbol, alert_type, message, price
    FROM stock.alerts
    ORDER BY triggered_at DESC LIMIT 20
    """,
)

if not alerts.empty:
    st.dataframe(
        alerts,
        column_config={
            "triggered_at": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
            "price": st.column_config.NumberColumn("Price", format="$%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("No alerts triggered yet.")


# ─────────────────────── Cross-symbol comparison ──────────────

st.subheader("All Symbols — Latest")

all_latest = query_df(
    """
    SELECT DISTINCT ON (symbol)
        symbol, price, pct_change, sma_cross_signal, price_vs_vwap, timestamp
    FROM stock.enriched_ticks
    ORDER BY symbol, timestamp DESC
    """,
)

if not all_latest.empty:
    all_latest["pct_change"] = all_latest["pct_change"].apply(
        lambda x: f"{x*100:+.3f}%" if x else "—"
    )
    st.dataframe(all_latest, hide_index=True, use_container_width=True)


# ─────────────────────── Auto-refresh ─────────────────────────

if auto_refresh:
    time.sleep(5)
    st.rerun()
