#!/usr/bin/env python3
"""
RealtimeStream — Real-Time Pipeline Dashboard

Monitors the Medallion pipeline (Bronze → Silver → Gold):
  - Pipeline health & throughput
  - Real-time event metrics (KPI, session, funnel aggregations)
  - Anomaly alerts & data quality
  - Delta Lake table statistics

Run: streamlit run dashboard/app.py
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# Ensure src/ is on path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import (
    EVENT_TYPES,
    EVENT_PROBABILITIES,
    KAFKA_TOPICS,
    MEDALLION_PATHS,
    QUALITY_CONFIG,
    ANOMALY_CONFIG,
    ENRICHMENT_CONFIG,
    GENERATOR_CONFIG,
)

# ═══════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="RealtimeStream — Pipeline Dashboard",
    page_icon="🌀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════
# COLOR SCHEME
# ═══════════════════════════════════════════════════════════════════════

COLORS = {
    "primary": "#636EFA",
    "secondary": "#EF553B",
    "accent": "#00CC96",
    "warning": "#FFA15A",
    "danger": "#EF553B",
    "success": "#00CC96",
    "info": "#AB63FA",
    "gray": "#95A5A6",
    "dark": "#2C3E50",
    "bronze": "#CD7F32",
    "silver": "#9EA0A1",
    "gold": "#FFD700",
}

EVENT_COLORS = {
    "page_view": "#636EFA",
    "click": "#00CC96",
    "add_to_cart": "#FFA15A",
    "purchase": "#EF553B",
    "login": "#AB63FA",
    "logout": "#FF6692",
    "error": "#FF2B2B",
    "search": "#FECB52",
}

# ═══════════════════════════════════════════════════════════════════════
# MOCK / DEMO DATA GENERATION
# ═══════════════════════════════════════════════════════════════════════

# In production, this would connect to Delta Lake / Kafka.
# For demo, we generate realistic sample data.


@st.cache_data(ttl=30)
def generate_sample_events(num_events: int = 5000) -> pd.DataFrame:
    """Generate sample events for demo dashboard displays."""
    import random

    # Use a time-based seed so data changes on each refresh window
    random.seed(hash(datetime.now().strftime("%Y-%m-%d %H")) % (2**31))

    # Session & user setup
    num_users = min(GENERATOR_CONFIG["num_users"], 200)
    num_products = min(GENERATOR_CONFIG["num_products"], 50)
    num_categories = min(GENERATOR_CONFIG["num_categories"], 10)
    countries = GENERATOR_CONFIG["countries"]
    cities_lookup = GENERATOR_CONFIG["cities"]

    event_types = list(EVENT_PROBABILITIES.keys())
    event_weights = list(EVENT_PROBABILITIES.values())
    devices = GENERATOR_CONFIG["devices"]
    device_weights = GENERATOR_CONFIG["device_weights"]
    browsers = GENERATOR_CONFIG["browsers"]
    browser_weights = GENERATOR_CONFIG["browser_weights"]

    sessions = []
    user_ids = [f"user-{i:05d}" for i in range(1, num_users + 1)]
    product_ids = [f"prod-{i:03d}" for i in range(1, num_products + 1)]
    categories = [f"cat-{i:02d}" for i in range(1, num_categories + 1)]

    base_ts = datetime(2026, 5, 29, 0, 0, 0, tzinfo=timezone.utc)

    for i in range(num_events):
        user_id = random.choice(user_ids)
        session_id = f"sess-{random.getrandbits(48):012x}"
        ts = base_ts + timedelta(
            days=random.randint(0, 6),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        event_type = random.choices(event_types, weights=event_weights, k=1)[0]
        country = random.choice(countries)
        city = random.choice(cities_lookup[country])
        device = random.choices(devices, weights=device_weights, k=1)[0]
        browser = random.choices(browsers, weights=browser_weights, k=1)[0]

        amount = None
        product_id = None
        category_name = None
        error_code = None
        response_time = random.randint(20, 500)

        if event_type == "purchase":
            amount = round(random.uniform(5.0, 299.99), 2)
            product_id = random.choice(product_ids)
            category_name = random.choice(categories)
            response_time = random.randint(100, 3000)
        elif event_type == "add_to_cart":
            product_id = random.choice(product_ids)
            category_name = random.choice(categories)
            response_time = random.randint(50, 1000)
        elif event_type == "error":
            error_code = random.choice([400, 404, 500, 502, 503])
            response_time = random.randint(50, 8000)
        elif event_type == "search":
            response_time = random.randint(10, 2000)
        elif event_type == "page_view":
            response_time = random.randint(10, 3000)

        sessions.append({
            "event_id": f"evt-{i:06d}",
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": ts,
            "event_date": ts.strftime("%Y-%m-%d"),
            "hour_of_day": ts.hour,
            "day_of_week": ts.strftime("%A"),
            "country": country,
            "city": city,
            "device_type": device,
            "browser": browser,
            "response_time_ms": response_time,
            "amount": amount,
            "product_id": product_id,
            "category": category_name,
            "error_code": error_code,
            "traffic_source": random.choice(["organic", "direct", "social", "email", "referral"]),
        })

    df = pd.DataFrame(sessions)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@st.cache_data(ttl=60)
def generate_sample_kpis(events_df: pd.DataFrame) -> pd.DataFrame:
    """Compute KPIs from sample events (same logic as Gold layer)."""
    if events_df.empty:
        return pd.DataFrame()

    events_df["window"] = events_df["timestamp"].dt.floor("1H")

    kpis = events_df.groupby("window").agg(
        total_events=("event_id", "count"),
        page_views=("event_type", lambda x: (x == "page_view").sum()),
        clicks=("event_type", lambda x: (x == "click").sum()),
        add_to_carts=("event_type", lambda x: (x == "add_to_cart").sum()),
        purchases=("event_type", lambda x: (x == "purchase").sum()),
        errors=("event_type", lambda x: (x == "error").sum()),
        logins=("event_type", lambda x: (x == "login").sum()),
        searches=("event_type", lambda x: (x == "search").sum()),
        unique_users=("user_id", "nunique"),
        unique_sessions=("session_id", "nunique"),
        avg_response_time_ms=("response_time_ms", "mean"),
        max_response_time_ms=("response_time_ms", "max"),
        response_time_p95=("response_time_ms", lambda x: x.quantile(0.95)),
        revenue_total=("amount", lambda x: x.fillna(0).sum()),
    ).reset_index()

    kpis["error_rate"] = (kpis["errors"] / kpis["total_events"]).fillna(0).round(4)
    kpis["purchase_rate"] = (kpis["purchases"] / kpis["total_events"]).fillna(0).round(4)
    kpis["conversion_rate"] = (kpis["purchases"] / kpis["page_views"].replace(0, 1)).fillna(0).round(4)
    kpis["revenue_avg"] = (kpis["revenue_total"] / kpis["purchases"].replace(0, 1)).fillna(0).round(2)

    return kpis


@st.cache_data(ttl=60)
def generate_sample_sessions(events_df: pd.DataFrame) -> pd.DataFrame:
    """Compute session-level metrics from sample events."""
    if events_df.empty:
        return pd.DataFrame()

    session_agg = events_df.groupby(["session_id", "user_id"]).agg(
        session_start=("timestamp", "min"),
        session_end=("timestamp", "max"),
        event_count=("event_id", "count"),
        page_view_count=("event_type", lambda x: (x == "page_view").sum()),
        click_count=("event_type", lambda x: (x == "click").sum()),
        add_to_cart_count=("event_type", lambda x: (x == "add_to_cart").sum()),
        purchase_count=("event_type", lambda x: (x == "purchase").sum()),
        error_count=("event_type", lambda x: (x == "error").sum()),
        total_revenue=("amount", lambda x: x.fillna(0).sum()),
        entry_traffic_source=("traffic_source", "first"),
        device_type=("device_type", "first"),
        browser=("browser", "first"),
        country=("country", "first"),
    ).reset_index()

    session_agg["session_duration_seconds"] = (
        pd.to_numeric(pd.to_datetime(session_agg["session_end"]) - pd.to_datetime(session_agg["session_start"]))
        / 1e9
    )
    session_agg["has_purchased"] = session_agg["purchase_count"] > 0
    session_agg["is_bounced"] = session_agg["event_count"] <= 1
    session_agg["bounce_rate"] = session_agg["is_bounced"].mean()

    return session_agg


@st.cache_data(ttl=60)
def generate_sample_funnels(events_df: pd.DataFrame) -> pd.DataFrame:
    """Compute conversion funnel from sample events."""
    if events_df.empty:
        return pd.DataFrame()

    events_df["window"] = events_df["timestamp"].dt.floor("1D")

    funnel_steps = ["page_view", "click", "add_to_cart", "purchase"]
    funnel_data = events_df[events_df["event_type"].isin(funnel_steps)].copy()

    funnel_agg = funnel_data.groupby(["window", "event_type"]).agg(
        unique_users=("user_id", "nunique"),
        event_count=("event_id", "count"),
    ).reset_index()

    # Map step order
    step_order = {s: i + 1 for i, s in enumerate(funnel_steps)}
    funnel_agg["step_order"] = funnel_agg["event_type"].map(step_order)
    funnel_agg = funnel_agg.sort_values(["window", "step_order"])

    # Compute conversion rates
    funnel_agg["next_step_users"] = funnel_agg.groupby("window")["unique_users"].shift(-1)
    funnel_agg["conversion_to_next"] = (
        funnel_agg["next_step_users"] / funnel_agg["unique_users"]
    ).fillna(0).round(4)
    funnel_agg["drop_off_count"] = (funnel_agg["unique_users"] - funnel_agg["next_step_users"]).fillna(0).astype(int)
    funnel_agg["drop_off_rate"] = (funnel_agg["drop_off_count"] / funnel_agg["unique_users"]).fillna(0).round(4)

    return funnel_agg


@st.cache_data(ttl=30)
def generate_anomaly_alerts() -> List[Dict]:
    """Generate sample anomaly alerts for monitoring."""
    import random

    anomaly_types = ["error_rate_spike", "slow_response", "traffic_drop", "purchase_drop"]
    severities = ["low", "medium", "high", "critical"]
    alert_templates = [
        "Error rate exceeded 5% threshold ({value:.1f}%)",
        "Response time p99 exceeded 5s threshold ({value:.0f}ms)",
        "Traffic dropped by {value:.0f}% compared to rolling average",
        "Purchase conversion dropped below 50% of expected ({value:.1f}%)",
    ]

    alerts = []
    base_ts = datetime.now(timezone.utc) - timedelta(hours=6)

    for i in range(random.randint(3, 8)):
        ts = base_ts + timedelta(minutes=random.randint(0, 360))
        atype = random.choice(anomaly_types)
        severity = random.choices(severities, weights=[0.4, 0.3, 0.2, 0.1], k=1)[0]

        if atype == "error_rate_spike":
            value = random.uniform(0.05, 0.25)
        elif atype == "slow_response":
            value = random.uniform(5000, 15000)
        elif atype == "traffic_drop":
            value = random.uniform(30, 80)
        else:
            value = random.uniform(0.3, 0.7)

        template_idx = anomaly_types.index(atype)
        if atype == "error_rate_spike":
            message = alert_templates[template_idx].format(value=value * 100)
        elif atype == "slow_response":
            message = alert_templates[template_idx].format(value=value)
        elif atype == "traffic_drop":
            message = alert_templates[template_idx].format(value=value)
        else:
            message = alert_templates[template_idx].format(value=value * 100)

        alerts.append({
            "alert_id": f"alert-{i+1:03d}",
            "timestamp": ts,
            "type": atype,
            "severity": severity,
            "message": message,
            "value": round(value, 2),
            "acknowledged": random.random() < 0.4,
        })

    return sorted(alerts, key=lambda a: a["timestamp"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30)
def check_pipeline_health() -> Dict:
    """Simulate pipeline health check. In production, this pings Kafka, Spark, Delta, etc."""
    import random

    # Simulate health probes
    services = {
        "Kafka Broker": {"status": random.choices(["healthy", "degraded"], weights=[0.95, 0.05])[0]},
        "Spark Session": {"status": random.choices(["healthy", "idle"], weights=[0.9, 0.1])[0]},
        "Delta Lake": {"status": "healthy"},
        "MinIO Storage": {"status": random.choices(["healthy", "degraded"], weights=[0.92, 0.08])[0]},
        "Airflow Scheduler": {"status": random.choices(["healthy", "degraded"], weights=[0.88, 0.12])[0]},
        "Data Generator": {"status": random.choices(["running", "idle"], weights=[0.7, 0.3])[0]},
    }
    return services


# ═══════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════

PAGES = [
    ("📊 Overview", "Overview"),
    ("📈 Real-Time KPIs", "KPI"),
    ("👤 Session Analytics", "Session"),
    ("🔄 Conversion Funnel", "Funnel"),
    ("🚨 Anomaly Alerts", "Anomaly"),
    ("🔍 Data Explorer", "Data Explorer"),
]


def render_sidebar() -> str:
    """Render sidebar with navigation and pipeline status."""
    st.sidebar.markdown(
        "<h1 style='font-size: 1.5rem; margin-bottom: 0.2rem;'>🌀 RealtimeStream</h1>"
        "<p style='font-size: 0.8rem; color: #95A5A6;'>Pipeline Monitoring Dashboard</p>",
        unsafe_allow_html=True,
    )
    st.sidebar.divider()

    # Pipeline health summary
    st.sidebar.markdown("### 🩺 Pipeline Health")
    health = check_pipeline_health()

    all_ok = all(s["status"] in ("healthy", "running") for s in health.values())
    status_icon = "🟢" if all_ok else ("🟡" if any(s["status"] == "degraded" for s in health.values()) else "🔴")

    st.sidebar.markdown(f"**Overall:** {status_icon} {'All Systems OK' if all_ok else 'Issues Detected'}")

    for service, info in health.items():
        icon_map = {"healthy": "✅", "running": "🔄", "degraded": "⚠️", "idle": "💤"}
        st.sidebar.markdown(
            f"{icon_map.get(info['status'], '❓')} **{service}:** {info['status'].title()}"
        )

    st.sidebar.divider()

    # Navigation
    page_labels = [label for label, _ in PAGES]
    page = st.sidebar.radio("Dashboard Views", page_labels, index=0)
    # Map display label back to page key
    page_map = {label: key for label, key in PAGES}
    page_key = page_map.get(page, "Overview")

    st.sidebar.divider()

    # Refresh rate
    refresh_rate = st.sidebar.select_slider(
        "Auto-refresh (seconds)",
        options=[10, 30, 60, 120, 300],
        value=60,
    )

    st.sidebar.caption(
        f"Last updated: {datetime.now().strftime('%H:%M:%S')}"
    )

    # Pipeline arch diagram
    st.sidebar.divider()
    st.sidebar.markdown("### 📐 Pipeline Architecture")
    st.sidebar.markdown(
        """
        ```
        🛢️ Kafka ──→ 🥉 Bronze ──→ 🥈 Silver ──→ 🥇 Gold
                      (Raw)      (Clean)     (Aggregated)
                        │            │             │
                        ▼            ▼             ▼
                    Dead-Letter  Quarantine    Dashboards
        ```
        """
    )

    return page_key


# ═══════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════

def render_overview(events: pd.DataFrame, kpis: pd.DataFrame, sessions: pd.DataFrame):
    st.markdown("# 📊 Pipeline Overview")
    st.caption(f"Data range: {events['timestamp'].min().strftime('%b %d, %H:%M')} → "
               f"{events['timestamp'].max().strftime('%b %d, %H:%M')}")

    # ── Top-level KPI Cards ──
    total_events = len(events)
    total_users = events["user_id"].nunique()
    total_sessions = events["session_id"].nunique()
    total_purchases = (events["event_type"] == "purchase").sum()
    total_revenue = events["amount"].fillna(0).sum()
    bounce_rate = (sessions["is_bounced"].mean() * 100) if len(sessions) > 0 else 0
    error_rate = ((events["event_type"] == "error").sum() / total_events * 100) if total_events > 0 else 0
    conversion_rate = (total_purchases / (events["event_type"] == "page_view").sum() * 100) if total_events > 0 else 0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.metric("Total Events", f"{total_events:,}")
    with k2:
        st.metric("Active Users", f"{total_users:,}")
    with k3:
        st.metric("Sessions", f"{total_sessions:,}")
    with k4:
        st.metric("Purchases", f"{total_purchases:,}")
    with k5:
        st.metric("Revenue", f"${total_revenue:,.0f}")
    with k6:
        st.metric("Conversion Rate", f"{conversion_rate:.1f}%")

    k1b, k2b, k3b, k4b, k5b, k6b = st.columns(6)
    with k1b:
        st.metric("Avg Events/Session", f"{total_events / max(total_sessions, 1):.1f}")
    with k2b:
        st.metric("Bounce Rate", f"{bounce_rate:.1f}%", delta_color="inverse")
    with k3b:
        st.metric("Error Rate", f"{error_rate:.1f}%", delta_color="inverse")
    with k4b:
        st.metric("Avg Response", f"{events['response_time_ms'].mean():.0f}ms")
    with k5b:
        p95_rt = events["response_time_ms"].quantile(0.95)
        st.metric("p95 Response", f"{p95_rt:.0f}ms")
    with k6b:
        st.metric("Gold Windows", f"{len(kpis):,}")

    st.divider()

    # ── Medallion Flow Diagram ──
    st.subheader("🏗️ Medallion Pipeline Status")

    bronze_count = total_events
    silver_quality = (events["event_type"] == "error").mean() * 100
    silver_passed = total_events - (events["event_type"] == "error").sum()
    gold_kpis = len(kpis)
    gold_sessions = len(sessions)

    flow_col1, flow_arrow1, flow_col2, flow_arrow2, flow_col3 = st.columns([2, 0.5, 2, 0.5, 2])

    with flow_col1:
        st.markdown(
            f"""
            <div style='background:#2C3E50; border-radius:10px; padding:15px; text-align:center;'>
                <h3 style='color:#CD7F32; margin:0;'>🥉 Bronze</h3>
                <p style='color:white; font-size:24px; margin:5px 0;'>{bronze_count:,}</p>
                <p style='color:#95A5A6; font-size:12px;'>Raw Events</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with flow_arrow1:
        st.markdown(
            "<p style='text-align:center; font-size:32px; color:#95A5A6; margin-top:30px;'>→</p>",
            unsafe_allow_html=True,
        )

    with flow_col2:
        st.markdown(
            f"""
            <div style='background:#2C3E50; border-radius:10px; padding:15px; text-align:center;'>
                <h3 style='color:#9EA0A1; margin:0;'>🥈 Silver</h3>
                <p style='color:white; font-size:24px; margin:5px 0;'>{silver_passed:,}</p>
                <p style='color:#95A5A6; font-size:12px;'>Quality Passed</p>
                <p style='color:#EF553B; font-size:12px;'>{int(total_events - silver_passed):,} Quarantined</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with flow_arrow2:
        st.markdown(
            "<p style='text-align:center; font-size:32px; color:#95A5A6; margin-top:30px;'>→</p>",
            unsafe_allow_html=True,
        )

    with flow_col3:
        st.markdown(
            f"""
            <div style='background:#2C3E50; border-radius:10px; padding:15px; text-align:center;'>
                <h3 style='color:#FFD700; margin:0;'>🥇 Gold</h3>
                <p style='color:white; font-size:24px; margin:5px 0;'>{gold_kpis:,}</p>
                <p style='color:#95A5A6; font-size:12px;'>KPI Windows</p>
                <p style='color:#00CC96; font-size:12px;'>{gold_sessions:,} Sessions</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Event Timeline + Revenue ──
    col1, col2 = st.columns([1.2, 1])

    with col1:
        st.subheader("📈 Event Timeline (Hourly)")

        hourly = events.set_index("timestamp").resample("1H").size().reset_index()
        hourly.columns = ["timestamp", "event_count"]

        fig_timeline = make_subplots(specs=[[{"secondary_y": True}]])
        fig_timeline.add_trace(
            go.Bar(
                x=hourly["timestamp"],
                y=hourly["event_count"],
                name="Events",
                marker=dict(color=COLORS["primary"], opacity=0.6),
                hovertemplate="%{x|%b %d %H:%M}<br>Events: %{y}<extra></extra>",
            ),
            secondary_y=False,
        )

        # Revenue overlay
        hourly_rev = events.set_index("timestamp").resample("1H")["amount"].sum().reset_index()
        fig_timeline.add_trace(
            go.Scatter(
                x=hourly_rev["timestamp"],
                y=hourly_rev["amount"],
                name="Revenue",
                mode="lines+markers",
                line=dict(color=COLORS["gold"], width=3),
                marker=dict(size=6),
                hovertemplate="%{x|%b %d %H:%M}<br>Revenue: $%{y:.0f}<extra></extra>",
            ),
            secondary_y=True,
        )

        fig_timeline.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            hovermode="x unified",
            legend=dict(orientation="h", y=1.1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        fig_timeline.update_yaxes(title_text="Events", secondary_y=False, gridcolor="#333")
        fig_timeline.update_yaxes(title_text="Revenue ($)", secondary_y=True, gridcolor="#333")
        st.plotly_chart(fig_timeline, use_container_width=True)

    with col2:
        st.subheader("🎯 Event Type Distribution")

        event_dist = events["event_type"].value_counts().reset_index()
        event_dist.columns = ["event_type", "count"]

        fig_dist = go.Figure()
        for _, row in event_dist.iterrows():
            fig_dist.add_trace(go.Bar(
                x=[row["event_type"]],
                y=[row["count"]],
                name=row["event_type"],
                marker=dict(color=EVENT_COLORS.get(row["event_type"], COLORS["gray"])),
                text=row["count"],
                textposition="outside",
                hovertemplate="%{x}: %{y}<extra></extra>",
                showlegend=False,
            ))

        fig_dist.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=40),
            xaxis=dict(title="", tickangle=-45),
            yaxis=dict(title="Count", gridcolor="#333"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_dist, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: REAL-TIME KPIS
# ═══════════════════════════════════════════════════════════════════════

def render_kpis(events: pd.DataFrame, kpis: pd.DataFrame):
    st.markdown("# 📈 Real-Time KPIs")
    st.caption("Gold-layer aggregations — sliding window metrics from the pipeline")

    # ── KPI Time Series ──
    st.subheader("Hourly KPI Trends")

    if kpis.empty:
        st.warning("No KPI data available.")
        return

    metric_options = {
        "total_events": "Total Events",
        "unique_users": "Unique Users",
        "unique_sessions": "Unique Sessions",
        "avg_response_time_ms": "Avg Response (ms)",
        "response_time_p95": "p95 Response (ms)",
        "error_rate": "Error Rate",
        "conversion_rate": "Conversion Rate",
        "purchase_rate": "Purchase Rate",
        "revenue_total": "Revenue ($)",
    }

    selected_metrics = st.multiselect(
        "Select KPIs to display",
        options=list(metric_options.keys()),
        default=["total_events", "unique_users", "error_rate", "revenue_total"],
        format_func=lambda x: metric_options[x],
    )

    if selected_metrics:
        fig_kpi = go.Figure()
        colors = [COLORS["primary"], COLORS["success"], COLORS["danger"], COLORS["gold"],
                  COLORS["info"], COLORS["warning"], COLORS["secondary"], COLORS["accent"],
                  COLORS["bronze"]]

        for i, metric in enumerate(selected_metrics):
            if metric in kpis.columns:
                fig_kpi.add_trace(go.Scatter(
                    x=kpis["window"],
                    y=kpis[metric],
                    mode="lines+markers",
                    name=metric_options.get(metric, metric),
                    line=dict(width=2.5, color=colors[i % len(colors)]),
                    marker=dict(size=5),
                    hovertemplate="%{x|%b %d %H:%M}<br>%{y:.2f}<extra></extra>",
                ))

        fig_kpi.update_layout(
            height=450,
            margin=dict(l=20, r=20, t=20, b=40),
            hovermode="x unified",
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(title="", gridcolor="#333"),
            yaxis=dict(title="Value", gridcolor="#333"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_kpi, use_container_width=True)

    st.divider()

    # ── Latest Window Breakdown ──
    st.subheader("Latest Window Breakdown")

    if len(kpis) > 0:
        latest = kpis.iloc[-1]

        cols = st.columns(4)
        cols[0].metric("Total Events", f"{latest['total_events']:,}")
        cols[1].metric("Unique Users", f"{latest['unique_users']:,}")
        cols[2].metric("Avg Response", f"{latest['avg_response_time_ms']:.0f}ms")
        cols[3].metric("p95 Response", f"{latest['response_time_p95']:.0f}ms")

        cols2 = st.columns(4)
        cols2[0].metric("Error Rate", f"{latest['error_rate']*100:.2f}%",
                        delta=f"{'🔴' if latest['error_rate'] > 0.05 else '🟢'}")
        cols2[1].metric("Conversion Rate", f"{latest['conversion_rate']*100:.1f}%")
        cols2[2].metric("Revenue", f"${latest['revenue_total']:,.2f}")
        cols2[3].metric("Sessions", f"{latest['unique_sessions']:,}")

    st.divider()

    # ── Response Time Distribution ──
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("⏱️ Response Time Distribution")
        rt = events["response_time_ms"]
        fig_rt = px.histogram(
            rt,
            nbins=50,
            title="",
            color_discrete_sequence=[COLORS["primary"]],
            marginal="box",
            labels={"value": "Response Time (ms)", "count": "Events"},
        )
        fig_rt.add_vline(
            x=rt.median(),
            line_dash="dash",
            line_color=COLORS["success"],
            annotation_text=f"Median: {rt.median():.0f}ms",
        )
        fig_rt.add_vline(
            x=rt.quantile(0.95),
            line_dash="dot",
            line_color=COLORS["danger"],
            annotation_text=f"p95: {rt.quantile(0.95):.0f}ms",
        )
        fig_rt.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            xaxis=dict(gridcolor="#333"),
            yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig_rt, use_container_width=True)

    with col2:
        st.subheader("🌍 Traffic Sources")
        source_dist = events["traffic_source"].value_counts().reset_index()
        source_dist.columns = ["source", "count"]

        fig_source = go.Figure(
            go.Pie(
                labels=source_dist["source"],
                values=source_dist["count"],
                hole=0.4,
                textinfo="label+percent",
                marker=dict(
                    colors=[COLORS["primary"], COLORS["success"], COLORS["warning"],
                            COLORS["secondary"], COLORS["info"]]
                ),
                hovertemplate="%{label}: %{value} (%{percent})<extra></extra>",
            )
        )
        fig_source.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", y=-0.1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_source, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: SESSION ANALYTICS
# ═══════════════════════════════════════════════════════════════════════

def render_sessions(events: pd.DataFrame, sessions: pd.DataFrame):
    st.markdown("# 👤 Session Analytics")
    st.caption("Session-level metrics from Silver enrichment + Gold sessionization")

    if sessions.empty:
        st.warning("No session data available.")
        return

    # ── Session KPIs ──
    total_sessions = len(sessions)
    bounced = sessions["is_bounced"].sum()
    purchased = sessions["has_purchased"].sum()
    avg_duration = sessions["session_duration_seconds"].mean()
    avg_events = sessions["event_count"].mean()
    bounce_rate = bounced / total_sessions * 100

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Sessions", f"{total_sessions:,}")
    k2.metric("Bounced", f"{bounced:,}", delta=f"{bounce_rate:.1f}%")
    k3.metric("With Purchase", f"{purchased:,}")
    k4.metric("Avg Duration", f"{avg_duration:.0f}s")
    k5.metric("Avg Events/Session", f"{avg_events:.1f}")
    k6.metric("Bounce Rate", f"{bounce_rate:.1f}%", delta_color="inverse")

    st.divider()

    # ── Session Duration Distribution ──
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("⏱️ Session Duration Distribution")
        durations = sessions["session_duration_seconds"]
        # Cap at 99th percentile for display
        cap = durations.quantile(0.99)
        capped = durations.clip(upper=cap)

        fig_dur = px.histogram(
            capped,
            nbins=40,
            title="",
            color_discrete_sequence=[COLORS["info"]],
            labels={"value": "Duration (seconds)", "count": "Sessions"},
        )
        fig_dur.add_vline(
            x=durations.median(),
            line_dash="dash",
            line_color=COLORS["success"],
            annotation_text=f"Median: {durations.median():.0f}s",
        )
        fig_dur.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            xaxis=dict(gridcolor="#333"),
            yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig_dur, use_container_width=True)

    with col2:
        st.subheader("📱 Device Breakdown")
        device_dist = sessions["device_type"].value_counts().reset_index()
        device_dist.columns = ["device", "count"]

        fig_dev = go.Figure(
            go.Pie(
                labels=device_dist["device"],
                values=device_dist["count"],
                hole=0.4,
                textinfo="label+percent",
                marker=dict(
                    colors=[COLORS["primary"], COLORS["success"], COLORS["warning"]]
                ),
                hovertemplate="%{label}: %{value} sessions (%{percent})<extra></extra>",
            )
        )
        fig_dev.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", y=-0.1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_dev, use_container_width=True)

    st.divider()

    # ── Hourly Activity Heatmap ──
    col1, col2 = st.columns([1.2, 1])

    with col1:
        st.subheader("🗺️ Hourly Activity Heatmap")
        events["day_name"] = pd.Categorical(
            events["day_of_week"],
            categories=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            ordered=True,
        )
        heatmap_data = events.groupby(["day_name", "hour_of_day"], observed=True).size().unstack(fill_value=0)

        fig_heat = go.Figure(
            go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="YlOrRd",
                hovertemplate="Day: %{y}<br>Hour: %{x}<br>Events: %{z}<extra></extra>",
            )
        )
        fig_heat.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="Hour of Day", dtick=2, gridcolor="#333"),
            yaxis=dict(title="", gridcolor="#333"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    with col2:
        st.subheader("🌍 Geographic Distribution")
        geo_dist = sessions.groupby("country").size().reset_index(name="sessions")
        geo_dist = geo_dist.sort_values("sessions", ascending=True)

        fig_geo = go.Figure(
            go.Bar(
                y=geo_dist["country"],
                x=geo_dist["sessions"],
                orientation="h",
                marker=dict(
                    color=px.colors.sequential.Viridis_r[:len(geo_dist)],
                    line=dict(width=0.5, color="white"),
                ),
                text=geo_dist["sessions"],
                textposition="outside",
                hovertemplate="%{y}: %{x} sessions<extra></extra>",
            )
        )
        fig_geo.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="Sessions", gridcolor="#333"),
            yaxis=dict(title=""),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_geo, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: CONVERSION FUNNEL
# ═══════════════════════════════════════════════════════════════════════

def render_funnels(events: pd.DataFrame, funnels: pd.DataFrame):
    st.markdown("# 🔄 Conversion Funnel")
    st.caption("Gold-layer funnel analysis — page_view → click → add_to_cart → purchase")

    if funnels.empty:
        st.warning("No funnel data available.")
        return

    st.subheader("Overall Funnel")

    # Aggregate across all windows for overall view
    overall = funnels.groupby(["event_type", "step_order"]).agg(
        unique_users=("unique_users", "sum"),
        event_count=("event_count", "sum"),
    ).reset_index().sort_values("step_order")

    funnel_steps = overall["event_type"].tolist()
    funnel_users = overall["unique_users"].tolist()

    fig_funnel = go.Figure(
        go.Funnel(
            y=funnel_steps,
            x=funnel_users,
            textinfo="value+percent initial",
            marker=dict(
                color=[COLORS["primary"], COLORS["success"], COLORS["warning"], COLORS["secondary"]],
                line=dict(width=1, color="white"),
            ),
            hovertemplate="Step: %{y}<br>Users: %{x}<br>%{percent}</br><extra></extra>",
        )
    )
    fig_funnel.update_layout(
        height=450,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
        hovermode="y",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

    # ── Conversion Details ──
    st.subheader("Step-by-Step Breakdown")

    overall["conversion_to_next_pct"] = (
        overall["unique_users"].shift(-1) / overall["unique_users"] * 100
    ).fillna(0).round(1)
    overall["drop_off"] = (overall["unique_users"] - overall["unique_users"].shift(-1)).fillna(0).astype(int)

    for i, row in overall.iterrows():
        cvr = row["conversion_to_next_pct"]
        drop = row["drop_off"]
        if pd.notna(cvr) and i < len(overall) - 1:
            col1, col2, col3 = st.columns([2, 2, 2])
            col1.markdown(f"**{row['event_type'].replace('_', ' ').title()}** →")
            col2.markdown(f"**{cvr:.1f}%** conversion to next step")
            col3.markdown(f"{drop:,} users dropped off")
            st.progress(cvr / 100)
            st.caption("")

    st.divider()

    # ── Funnel Over Time ──
    st.subheader("Funnel Trends Over Time")
    pivot_funnel = funnels.pivot_table(
        index="window", columns="event_type", values="unique_users", aggfunc="sum"
    ).fillna(0)

    if not pivot_funnel.empty:
        fig_trend = go.Figure()
        colors_trend = [COLORS["primary"], COLORS["success"], COLORS["warning"], COLORS["secondary"]]
        for i, step in enumerate(funnel_steps):
            if step in pivot_funnel.columns:
                fig_trend.add_trace(go.Scatter(
                    x=pivot_funnel.index,
                    y=pivot_funnel[step],
                    mode="lines+markers",
                    name=step.replace("_", " ").title(),
                    line=dict(width=2.5, color=colors_trend[i % len(colors_trend)]),
                    marker=dict(size=6),
                    hovertemplate="%{x|%b %d}<br>%{y}<extra></extra>",
                ))

        fig_trend.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=40),
            hovermode="x unified",
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(title="", gridcolor="#333"),
            yaxis=dict(title="Unique Users", gridcolor="#333"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_trend, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# PAGE: ANOMALY ALERTS
# ═══════════════════════════════════════════════════════════════════════

def render_anomalies():
    st.markdown("# 🚨 Anomaly Alerts")
    st.caption("Real-time anomaly detection from the Silver layer")

    alerts = generate_anomaly_alerts()

    if not alerts:
        st.success("No anomalies detected. All systems normal.")
        return

    # ── Alert Summary ──
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Alerts", sum(1 for a in alerts if not a["acknowledged"]))
    col2.metric("Total Today", len(alerts))
    col3.metric(
        "Critical",
        sum(1 for a in alerts if a["severity"] == "critical"),
        delta="🔴" if any(a["severity"] == "critical" for a in alerts) else "🟢",
    )
    col4.metric("Acknowledged", sum(1 for a in alerts if a["acknowledged"]))

    st.divider()

    # ── Anomaly Type Distribution ──
    col1, col2 = st.columns([1, 1.5])

    with col1:
        type_dist = {}
        for a in alerts:
            t = a["type"].replace("_", " ").title()
            type_dist[t] = type_dist.get(t, 0) + 1

        fig_type = go.Figure(
            go.Pie(
                labels=list(type_dist.keys()),
                values=list(type_dist.values()),
                hole=0.4,
                textinfo="label+percent",
                marker=dict(
                    colors=[COLORS["danger"], COLORS["warning"], COLORS["primary"], COLORS["info"]]
                ),
                hovertemplate="%{label}: %{value} alerts<extra></extra>",
            )
        )
        fig_type.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", y=-0.1),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_type, use_container_width=True)

    with col2:
        severity_dist = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for a in alerts:
            severity_dist[a["severity"]] += 1

        sev_df = pd.DataFrame([
            {"severity": s.title(), "count": c, "color": col}
            for (s, c), col in zip(
                severity_dist.items(),
                [COLORS["success"], COLORS["warning"], COLORS["secondary"], COLORS["danger"]]
            )
        ])

        fig_sev = go.Figure(
            go.Bar(
                x=sev_df["severity"],
                y=sev_df["count"],
                marker=dict(
                    color=sev_df["color"],
                    line=dict(width=1, color="white"),
                ),
                text=sev_df["count"],
                textposition="outside",
                hovertemplate="%{x}: %{y} alerts<extra></extra>",
            )
        )
        fig_sev.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="Severity"),
            yaxis=dict(title="Count", gridcolor="#333"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
        )
        st.plotly_chart(fig_sev, use_container_width=True)

    st.divider()

    # ── Alert Timeline ──
    st.subheader("📅 Alert Timeline")
    alert_df = pd.DataFrame(alerts)
    alert_df["timestamp"] = pd.to_datetime(alert_df["timestamp"])

    fig_timeline = go.Figure()
    severity_colors = {"low": COLORS["success"], "medium": COLORS["warning"],
                       "high": COLORS["secondary"], "critical": COLORS["danger"]}

    for sev in ["low", "medium", "high", "critical"]:
        subset = alert_df[alert_df["severity"] == sev]
        if len(subset) > 0:
            fig_timeline.add_trace(go.Scatter(
                x=subset["timestamp"],
                y=[sev.title()] * len(subset),
                mode="markers",
                name=sev.title(),
                marker=dict(
                    color=severity_colors[sev],
                    size=12,
                    line=dict(width=1, color="white"),
                    symbol="diamond" if sev == "critical" else "circle",
                ),
                text=subset["message"],
                hovertemplate="%{x|%b %d %H:%M}<br>%{text}<extra></extra>",
            ))

    fig_timeline.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        hovermode="closest",
        showlegend=True,
        legend=dict(orientation="h", y=1.1),
        xaxis=dict(title="", gridcolor="#333"),
        yaxis=dict(title="", gridcolor="#333"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    # ── Alert List ──
    st.subheader("Alert Details")
    for alert in alerts:
        sev_icon = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}
        ack_icon = "✅" if alert["acknowledged"] else "⏳"
        ts = pd.to_datetime(alert["timestamp"]).strftime("%b %d, %H:%M:%S")

        with st.expander(
            f"{sev_icon.get(alert['severity'], '❓')} "
            f"[{alert['severity'].upper()}] {alert['type'].replace('_', ' ').title()} — {ts}",
            expanded=alert["severity"] in ("high", "critical"),
        ):
            col1, col2, col3 = st.columns([3, 1, 1])
            col1.markdown(f"**Message:** {alert['message']}")
            col2.markdown(f"**Value:** {alert['value']}")
            col3.markdown(f"**Status:** {ack_icon} {'Acknowledged' if alert['acknowledged'] else 'Pending'}")


# ═══════════════════════════════════════════════════════════════════════
# PAGE: DATA EXPLORER
# ═══════════════════════════════════════════════════════════════════════

def render_explorer(events: pd.DataFrame):
    st.markdown("# 🔍 Data Explorer")
    st.caption("Explore raw and enriched event data from the pipeline")

    tab1, tab2, tab3 = st.tabs(["📋 Event Browser", "📊 Column Profiles", "⚙️ Pipeline Config"])

    with tab1:
        st.subheader("Event Browser")

        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            event_filter = st.multiselect(
                "Event Type",
                options=sorted(events["event_type"].unique()),
                default=[],
            )
        with col2:
            source_filter = st.multiselect(
                "Traffic Source",
                options=sorted(events["traffic_source"].unique()),
                default=[],
            )
        with col3:
            country_filter = st.multiselect(
                "Country",
                options=sorted(events["country"].unique()),
                default=[],
            )

        filtered = events.copy()
        if event_filter:
            filtered = filtered[filtered["event_type"].isin(event_filter)]
        if source_filter:
            filtered = filtered[filtered["traffic_source"].isin(source_filter)]
        if country_filter:
            filtered = filtered[filtered["country"].isin(country_filter)]

        n_rows = st.slider("Rows to display", 10, 200, 50)
        display_cols = ["event_id", "event_type", "user_id", "session_id", "timestamp",
                        "country", "device_type", "traffic_source", "response_time_ms",
                        "amount", "error_code"]

        st.dataframe(
            filtered[display_cols].sort_values("timestamp", ascending=False).head(n_rows),
            use_container_width=True,
            height=400,
        )
        st.caption(f"Showing {min(n_rows, len(filtered))} of {len(filtered):,} events")

    with tab2:
        st.subheader("Column Profile")

        col = st.selectbox("Select column", events.columns)

        c1, c2 = st.columns(2)
        with c1:
            dtype = events[col].dtype
            nulls = events[col].isnull().sum()
            null_pct = nulls / len(events) * 100
            uniques = events[col].nunique()

            st.markdown("**Stats**")
            st.markdown(f"- **Type:** `{dtype}`")
            st.markdown(f"- **Missing:** {nulls:,} / {null_pct:.1f}%")
            st.markdown(f"- **Unique:** {uniques:,}")

            if pd.api.types.is_numeric_dtype(events[col]):
                st.markdown(f"- **Mean:** {events[col].mean():.2f}")
                st.markdown(f"- **Std:** {events[col].std():.2f}")
                st.markdown(f"- **Min:** {events[col].min():.2f}")
                st.markdown(f"- **Max:** {events[col].max():.2f}")
                st.markdown(f"- **Median:** {events[col].median():.2f}")

        with c2:
            if pd.api.types.is_numeric_dtype(events[col]):
                valid = events[col].dropna()
                if len(valid) > 0:
                    fig = px.histogram(
                        valid, x=col,
                        nbins=min(50, int(valid.nunique())),
                        title=f"Distribution of {col}",
                        color_discrete_sequence=[COLORS["primary"]],
                    )
                    fig.update_layout(
                        height=300,
                        margin=dict(l=20, r=20, t=40, b=20),
                        showlegend=False,
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="white"),
                        xaxis=dict(gridcolor="#333"),
                        yaxis=dict(gridcolor="#333"),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                value_counts = events[col].value_counts().head(20)
                fig = go.Figure(data=[
                    go.Bar(
                        x=value_counts.values,
                        y=value_counts.index,
                        orientation="h",
                        marker=dict(color=COLORS["primary"]),
                        text=value_counts.values,
                        textposition="outside",
                    )
                ])
                fig.update_layout(
                    title=f"Top 20 values in {col}",
                    height=max(300, len(value_counts) * 20),
                    margin=dict(l=20, r=20, t=40, b=20),
                    xaxis=dict(title="Count", gridcolor="#333"),
                    yaxis=dict(title=""),
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="white"),
                )
                st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("⚙️ Pipeline Configuration")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Kafka Topics**")
            for topic, info in KAFKA_TOPICS.items():
                st.markdown(f"- `{topic}`: {info['partitions']} partitions")

            st.markdown("**Medallion Paths**")
            for layer, path in MEDALLION_PATHS.items():
                st.markdown(f"- **{layer}:** `{path}`")

        with col2:
            st.markdown("**Event Types & Probabilities**")
            evt_df = pd.DataFrame([
                {"Event": evt, "Weight": f"{weight * 100:.0f}%"}
                for evt, weight in EVENT_PROBABILITIES.items()
            ])
            st.dataframe(evt_df, use_container_width=True, hide_index=True)

            st.markdown("**Quality Config**")
            st.markdown(f"- Min quality score: {QUALITY_CONFIG['min_quality_score']}")
            st.markdown(f"- Max response time: {QUALITY_CONFIG['max_response_time_ms']}ms")
            st.markdown(f"- Valid devices: {', '.join(QUALITY_CONFIG['valid_devices'])}")

            st.markdown("**Anomaly Config**")
            st.markdown(f"- Error rate threshold: {ANOMALY_CONFIG['error_rate_threshold']*100:.0f}%")
            st.markdown(f"- Response time threshold: {ANOMALY_CONFIG['response_time_p99_threshold_ms']}ms")
            st.markdown(f"- Z-score threshold: {ANOMALY_CONFIG['response_time_spike_threshold']}σ")


# ═══════════════════════════════════════════════════════════════════════
# DARK THEME FOR PLOTLY
# ═══════════════════════════════════════════════════════════════════════

def apply_dark_theme():
    """Apply dark theme to the Streamlit page."""
    st.markdown("""
    <style>
    .stApp {
        background-color: #1E1E1E;
        color: #FFFFFF;
    }
    .stMetric {
        background-color: #2C3E50;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    .stMetric label {
        color: #95A5A6 !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
    }
    .stMetric [data-testid="stMetricDelta"] {
        color: #00CC96 !important;
    }
    .st-emotion-cache-1y4p8pa {
        padding-top: 2rem;
    }
    div.stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    div.stTabs [data-baseweb="tab"] {
        background-color: #2C3E50;
        border-radius: 4px 4px 0 0;
        padding: 8px 16px;
    }
    div.stTabs [aria-selected="true"] {
        background-color: #3498DB;
    }
    .stDataFrame {
        background-color: #2C3E50;
    }
    .stExpander {
        background-color: #2C3E50;
        border-radius: 8px;
        margin-bottom: 8px;
    }
    </style>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    apply_dark_theme()

    # Load data
    with st.spinner("Loading pipeline data..."):
        events = generate_sample_events(8000)
        kpis = generate_sample_kpis(events)
        sessions = generate_sample_sessions(events)
        funnels = generate_sample_funnels(events)

    # Render sidebar and get selected page
    page = render_sidebar()

    st.divider()

    # Render selected page
    if "Overview" in page:
        render_overview(events, kpis, sessions)
    elif "KPI" in page:
        render_kpis(events, kpis)
    elif "Session" in page:
        render_sessions(events, sessions)
    elif "Funnel" in page:
        render_funnels(events, funnels)
    elif "Anomaly" in page:
        render_anomalies()
    elif "Data Explorer" in page:
        render_explorer(events)

    # Footer
    st.divider()
    st.markdown(
        "<p style='text-align: center; color: #95A5A6; font-size: 0.8rem;'>"
        "RealtimeStream v0.1.0 | Medallion Pipeline Dashboard | "
        f"Built with Streamlit & Plotly | {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
