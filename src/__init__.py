"""
RealtimeStream — Real-Time Streaming Data Pipeline.

A production-grade real-time streaming pipeline built on the Medallion
architecture (Bronze → Silver → Gold) using Apache Spark Structured
Streaming, Kafka, and Delta Lake.

Pipeline stages:
  - Bronze:   Raw event ingestion from Kafka → Delta Lake
  - Silver:   Deduplication, enrichment, quality scoring, anomaly detection
  - Gold:     Sliding-window KPIs, sessionization, funnels, anomaly aggs
  - Alerting: Multi-channel notification (console, webhook, email)
  - Generator: Clickstream event simulator for development/testing
"""

from __future__ import annotations

# Non-Spark imports (always available)
from src.alerting import Alert, AlertManager, AlertChannel, ConsoleChannel
from src.alerting import WebhookChannel, EmailChannel, send_anomaly_alert
from src.data_generator import ClickstreamGenerator, EventCounter, produce_events

# BasePipeline imports pyspark at module level — wrap so core-tests (no pyspark) can still use src
try:
    from src.base import BasePipeline, retry, validate_date, validate_positive_int  # noqa: F401
except ImportError:
    pass  # Spark not available — BasePipeline not importable until pyspark is installed

# Spark-dependent imports (may fail in CI where only requirements-core.txt is installed)
try:
    from src.bronze_streaming import BronzePipeline, parse_schema  # noqa: F401
    from src.gold_aggregations import (  # noqa: F401
        GoldPipeline,
        compute_kpis,
        compute_sessions,
        compute_funnels,
        compute_anomaly_aggregations,
    )
    from src.silver_streaming import (  # noqa: F401
        SilverPipeline,
        AnomalyDetector,
    )
except ImportError:
    pass  # Spark not available — these names won't be importable until pyspark is installed

__all__ = [
    # Pipeline classes
    "BasePipeline",
    "BronzePipeline",
    "SilverPipeline",
    "GoldPipeline",
    # Alerting
    "Alert",
    "AlertManager",
    "AlertChannel",
    "ConsoleChannel",
    "WebhookChannel",
    "EmailChannel",
    "send_anomaly_alert",
    # Data generator
    "ClickstreamGenerator",
    "EventCounter",
    "produce_events",
    # Gold aggregation functions
    "compute_kpis",
    "compute_sessions",
    "compute_funnels",
    "compute_anomaly_aggregations",
    # Silver utilities
    "AnomalyDetector",
    # Bronze utilities
    "parse_schema",
    # Base utilities
    "retry",
    "validate_date",
    "validate_positive_int",
]

__version__ = "0.1.0"
