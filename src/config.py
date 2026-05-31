"""
RealtimeStream — Shared Configuration
Centralized settings for all pipeline components.
"""

import os
from typing import Dict


# ─── Kafka Configuration ──────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPICS = {
    "raw_events": {"partitions": 3, "replication": 1},
    "silver_events": {"partitions": 3, "replication": 1},
    "anomaly_alerts": {"partitions": 1, "replication": 1},
}

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "true",
    "group.id": "streaming-pipeline",
}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}


# ─── MinIO / S3 Configuration ─────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-lake")

S3_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

DELTA_LAKE_PATH = f"s3a://{MINIO_BUCKET}/delta"


# ─── Medallion Paths ──────────────────────────────────────────────────

MEDALLION_PATHS = {
    "bronze": f"{DELTA_LAKE_PATH}/bronze/events",
    "silver": f"{DELTA_LAKE_PATH}/silver/events_clean",
    "gold_kpis": f"{DELTA_LAKE_PATH}/gold/kpis",
    "gold_sessions": f"{DELTA_LAKE_PATH}/gold/sessions",
    "gold_funnels": f"{DELTA_LAKE_PATH}/gold/funnels",
    "gold_anomalies": f"{DELTA_LAKE_PATH}/gold/anomalies",
}


# ─── Event Schema ─────────────────────────────────────────────────────

EVENT_SCHEMA = {
    "event_id": "string",
    "event_type": "string",        # page_view, click, purchase, add_to_cart, login, error, logout
    "user_id": "string",
    "session_id": "string",
    "timestamp": "timestamp",
    "page_url": "string",
    "referrer_url": "string",
    "user_agent": "string",
    "device_type": "string",       # desktop, mobile, tablet
    "browser": "string",
    "os": "string",
    "country": "string",
    "city": "string",
    "ip_address": "string",
    "amount": "double",            # for purchase events
    "currency": "string",
    "product_id": "string",
    "category": "string",
    "error_code": "integer",       # for error events
    "response_time_ms": "integer",
    "status_code": "integer",
}

EVENT_TYPES = [
    "page_view", "click", "add_to_cart", "purchase",
    "login", "logout", "error", "search",
]

# Probabilities for data generator (must sum to 1.0)
EVENT_PROBABILITIES = {
    "page_view": 0.35,
    "click": 0.25,
    "add_to_cart": 0.12,
    "purchase": 0.03,
    "login": 0.10,
    "logout": 0.08,
    "error": 0.02,
    "search": 0.05,
}


# ─── Quality Checks (Silver Layer) ───────────────────────────────────

QUALITY_CONFIG = {
    "min_quality_score": 0.5,           # Records below this go to quarantine
    "critical_fields": ["user_id", "session_id", "timestamp", "event_type", "event_id"],
    "valid_event_types": ["page_view", "click", "add_to_cart", "purchase", "login", "logout", "error", "search"],
    "valid_devices": ["desktop", "mobile", "tablet"],
    "max_response_time_ms": 30000,      # Cap for outlier detection
}


# ─── Anomaly Detection ────────────────────────────────────────────────

ANOMALY_CONFIG = {
    "error_rate_threshold": 0.05,            # Alert if error rate > 5%
    "response_time_p99_threshold_ms": 5000,  # Alert if p99 response > 5s
    "purchase_drop_threshold": 0.5,          # Alert if purchases drop > 50% compared to previous window
    "traffic_spike_threshold": 3.0,          # Alert if traffic > 3x rolling average
    "window_minutes": 5,                     # Sliding window size for anomaly detection
    "min_samples": 10,                       # Minimum samples before alerting
    "response_time_spike_threshold": 3.0,    # Z-score threshold for slow events
}


# ─── Enrichment Configuration ─────────────────────────────────────────

ENRICHMENT_CONFIG = {
    "referrer_sources": {
        "google": "organic",
        "facebook": "social",
        "twitter": "social",
        "linkedin": "social",
        "instagram": "social",
        "email": "email",
        "newsletter": "email",
        "bing": "organic",
        "yahoo": "organic",
    },
    "silver_version": "1.0.0",
}


# ─── Postgres / Serving Layer ─────────────────────────────────────────

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}


# ─── Spark Session Configuration ──────────────────────────────────────

def get_spark_config(app_name: str = "RealtimeStream") -> Dict[str, str]:
    """Return a dict of Spark configuration for a streaming or batch job."""
    config = {
        "spark.app.name": app_name,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.master": os.getenv("SPARK_MASTER", "local[*]"),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.sql.streaming.schemaInference": "true",
    }
    # Merge S3/MinIO config
    config.update(S3_CONFIG)
    return config


# ─── Data Generator ───────────────────────────────────────────────────

GENERATOR_CONFIG = {
    "events_per_second": int(os.getenv("EVENTS_PER_SECOND", "10")),
    "num_users": 5000,
    "num_products": 200,
    "num_categories": 20,
    "countries": ["US", "IN", "GB", "DE", "FR", "CA", "AU", "BR", "JP", "SG"],
    "cities": {
        "US": ["New York", "San Francisco", "Chicago", "Austin", "Seattle"],
        "IN": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"],
        "GB": ["London", "Manchester", "Birmingham", "Edinburgh"],
        "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
        "FR": ["Paris", "Lyon", "Marseille"],
        "CA": ["Toronto", "Vancouver", "Montreal"],
        "AU": ["Sydney", "Melbourne", "Brisbane"],
        "BR": ["Sao Paulo", "Rio de Janeiro"],
        "JP": ["Tokyo", "Osaka"],
        "SG": ["Singapore"],
    },
    "devices": ["desktop", "mobile", "tablet"],
    "device_weights": [0.55, 0.35, 0.10],
    "browsers": ["Chrome", "Firefox", "Safari", "Edge", "Brave"],
    "browser_weights": [0.55, 0.15, 0.18, 0.08, 0.04],
    "currencies": ["USD", "INR", "EUR", "GBP"],
    "currency_weights": [0.50, 0.20, 0.20, 0.10],
}
