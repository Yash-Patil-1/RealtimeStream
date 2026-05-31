# Architecture

## System Overview

RealtimeStream is a **real-time streaming data pipeline** built on the **Medallion architecture** (Bronze → Silver → Gold). It simulates clickstream events, processes them through multiple transformation stages, detects anomalies, and serves live dashboards — all running locally via Docker Compose.

All pipeline stages share a common **`BasePipeline`** class (`src/base.py`) that standardises Spark session creation, Delta table lifecycle management, and cleanup — eliminating duplicated boilerplate and ensuring consistent error handling across the Medallion layers.

```
┌──────────────────────────────────────────────────────────────────┐
│                    DATA GENERATOR                                │
│                (src/data_generator.py)                           │
│         Simulates clickstream → Kafka Producer                   │
└──────────────────────────┬───────────────────────────────────────┘
                           │ raw_events topic
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  SHARED BASE LAYER  ──────────────────────────────────────────┐  │
│  src/base.py                                                   │  │
│                                                               │  │
│  • BasePipeline class ─ inherited by all Spark pipeline stages │  │
│  • _create_spark_session() — standardised Delta + Spark setup  │  │
│  • _init_delta_table() — create Delta tables if not exist      │  │
│  • _ensure_tables() / _init_tables() — lazy table init hook    │  │
│  • stop() — safe Spark session teardown                       │  │
│  • retry() decorator — exponential backoff for I/O operations  │  │
│  • validate_date() / validate_positive_int() — CLI validation  │  │
│  ───────────────────────────────────────────────────────────── │  │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (src/bronze_streaming.py → BasePipeline)         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Spark Structured Streaming (foreachBatch)                │   │
│  │                                                          │   │
│  │ 1. Consume JSON from Kafka                               │   │
│  │ 2. Parse with RAW_EVENT_SCHEMA (all StringType)          │   │
│  │ 3. Cast numeric fields (amount→Double, error_code→Int)   │   │
│  │ 4. Cast timestamp string → TimestampType                  │   │
│  │ 5. Enrich with event_date partition column               │   │
│  │ 6. Separate clean vs malformed (dead-letter)             │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Output: Delta Lake (partitioned by event_date/event_type)      │
│          + Dead-letter table for malformed records              │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (src/silver_streaming.py → BasePipeline)         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Batch Processing (read Bronze → write Silver)            │   │
│  │                                                          │   │
│  │ 1. Read from Bronze Delta table                          │   │
│  │ 2. Deduplicate by event_id (keep earliest timestamp)     │   │
│  │ 3. Enrich: hour_of_day, day_of_week, traffic_source,    │   │
│  │    is_purchase/error, event_number_in_session             │   │
│  │ 4. Quality scoring (0.0-1.0):                           │   │
│  │    - Completeness (critical fields)                     │   │
│  │    - Consistency (valid event_type, device_type)        │   │
│  │    - Timeliness (no future/stale timestamps)            │   │
│  │    - Reasonableness (response time bounds)              │   │
│  │ 5. Split: passed (score ≥ 0.5) vs quarantined          │   │
│  │ 6. Anomaly detection: error rate spike + slow response  │   │
│  │    (z-score > 3σ)                                       │   │
│  │ 7. Write to Silver Delta + quarantine Delta             │   │
│  │ 8. Send anomaly alerts to Kafka topic                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Output: Silver Delta table (event_date partitioned)            │
│          + Quarantine Delta table (low-quality records)         │
│          + Kafka anomaly_alerts topic                           │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (src/gold_aggregations.py → BasePipeline)          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Spark Batch (Airflow-triggered or CLI)                   │   │
│  │                                                          │   │
│  │ 1. KPIs: sliding-window aggregations (5m/15m/1h/6h/24h) │   │
│  │    - Event counts by type                               │   │
│  │    - Unique users & sessions (approx_count_distinct)    │   │
│  │    - Response time stats (avg, p95, max)                │   │
│  │    - Error rate, purchase rate, conversion rate          │   │
│  │    - Revenue totals & averages                          │   │
│  │    - Bounce rate                                        │   │
│  │ 2. Sessions: user journey metrics                       │   │
│  │    - Duration, event counts, entry/exit pages           │   │
│  │    - Device, browser, OS, country, traffic source       │   │
│  │    - Purchase flag, revenue, bounce indicator           │   │
│  │ 3. Funnels: page_view→click→add_to_cart→purchase       │   │
│  │    - Conversion rates, drop-off counts and rates        │   │
│  │ 4. Anomaly Aggregations: grouped by window & type       │   │
│  │    - Counts, severity classification, metric tracking   │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Output: 4 Gold Delta tables (kpis, sessions, funnels,         │
│          anomaly_aggs)                                          │
└──────────────────────────┬───────────────────────────────────────┘
                           │
         ┌─────────────────┴─────────────────┐
         ▼                                   ▼
┌─────────────────────┐         ┌───────────────────────────┐
│  STREAMLIT           │         │  GRAFANA                  │
│  DASHBOARD           │         │  (Pipeline Health)        │
│  (dashboard/app.py)  │         │  (docker/grafana-        │
│                      │         │   dashboard.json)         │
│  6 pages:            │         │                           │
│  • Overview          │         │  8 panels:                │
│  • Real-Time KPIs   │         │  • Throughput             │
│  • Session Analytics │         │  • Kafka Lag             │
│  • Conversion Funnel │         │  • Heap Usage            │
│  • Anomaly Alerts    │         │  • Response Latency      │
│  • Data Explorer     │         │  • Error Rate            │
│                      │         │  • Dashboard Status      │
└─────────────────────┘         └───────────────────────────┘
```

## Data Flow

### 1. Ingestion (Generator → Kafka)
The data generator (`src/data_generator.py`) simulates clickstream events with realistic distributions:
- **8 event types**: page_view (35%), click (25%), add_to_cart (12%), purchase (3%), login (10%), logout (8%), error (2%), search (5%)
- **Session simulation**: consecutive events stay in the same session
- **Geographic distribution**: 10 countries with realistic city mapping
- **Device distribution**: desktop (55%), mobile (35%), tablet (10%)
- Configurable event rate (default 10 events/sec), batch or continuous mode

### 2. Bronze (Raw → Typed + Enriched)
The Bronze layer (`src/bronze_streaming.py`) performs:
- **Schema enforcement**: JSON parsing with `RAW_EVENT_SCHEMA` (all StringType for resilience)
- **Type coercion**: `amount` → Double, `error_code` → Integer, `response_time_ms` → Integer, `status_code` → Integer, `timestamp` → TimestampType
- **Partition enrichment**: `event_date` derived from timestamp (yyyy-MM-dd format)
- **Dead-letter quarantine**: records with null/empty `event_id`, null `event_type`, or null `timestamp` are quarantined
- **Output**: Bronze Delta table + Dead-letter Delta table

### 3. Silver (Clean → Enriched → Scored → Anomaly Detection)
The Silver layer (`src/silver_streaming.py`) performs:
- **Deduplication**: `row_number()` over `event_id` partition ordered by timestamp, keep `_rn = 1`
- **Enrichment**: hour_of_day, day_of_week, traffic_source (organic/social/email/direct/referral via referrer URL parsing), is_purchase_event, is_error_event, event_number_in_session (window function)
- **Quality scoring**: multi-dimensional 0.0-1.0 score:
  - **Completeness**: -0.15 per missing critical field (user_id, session_id, timestamp, event_type, event_id)
  - **Consistency**: -0.2 for invalid event_type, -0.1 for invalid device_type
  - **Timeliness**: -0.1 for future timestamps, -0.1 for stale (>7 days)
  - **Reasonableness**: -0.05 for response_time_ms > 30s
- **Quality split**: passed (score ≥ 0.5) → Silver Delta, quarantined (score < 0.5) → Quarantine Delta
- **Anomaly detection**: error rate spike (>5% threshold) + slow response (z-score > 3σ)
- **Alerting**: Kafka `anomaly_alerts` topic + AlertManager dispatch

### 4. Gold (Aggregation → Business Metrics)
The Gold layer (`src/gold_aggregations.py`) computes:
- **KPIs**: sliding-window aggregations with 5m/15m/30m/1h/6h/24h windows - event breakdowns, unique users (approx_count_distinct), response time percentiles, conversion/bounce rates, revenue
- **Sessions**: user journey metrics with deterministic entry/exit page detection via row_number(), duration, device/browser/geo dimensions
- **Funnels**: conversion rates through page_view → click → add_to_cart → purchase pipeline with drop-off analysis
- **Anomaly Aggregations**: grouped by window and type with severity classification (low/medium/high/critical)

### 5. Serving (Dashboard)
The Streamlit dashboard (`dashboard/app.py`) provides 6 views:
- **Overview**: pipeline health status, medallion throughput diagram, recent alerts
- **Real-Time KPIs**: hourly event distribution, response time metrics (avg/p95, box plot), error/conversion/bounce rates
- **Session Analytics**: duration distribution, events per session, device/geographic breakdowns, hourly heatmap
- **Conversion Funnel**: funnel visualization with step breakdown, conversion rates, trend analysis
- **Anomaly Alerts**: severity timeline, score distribution, alert detail list
- **Data Explorer**: raw event browser, column profiling, pipeline configuration viewer

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Streaming** | Apache Kafka | 7.5 (via Confluent) | Message broker for event ingestion |
| **Processing** | Apache Spark | 3.5.0 | Structured Streaming + batch transforms |
| **Storage** | MinIO | Latest | S3-compatible object store |
| **Table Format** | Delta Lake | 3.1.0 | ACID transactions, time travel, schema enforcement |
| **Orchestration** | Apache Airflow | 2.8 | Gold layer batch DAG scheduling |
| **Metadata DB** | PostgreSQL | 15 | Airflow backend + serving layer |
| **Monitoring** | Prometheus + Grafana | Latest | Pipeline health metrics & dashboards |
| **Dashboard** | Streamlit | 1.28+ | Real-time analytics UI |
| **Alerting** | Custom (src/alerting.py) | — | Webhook, email, console dispatch |
| **Containerization** | Docker Compose | — | Local development environment |

## Data Model

### Medallion Tables (Delta Lake on MinIO)

```yaml
streaming-lake:
  delta:
    bronze:
      events:           # Raw typed events, partitioned by event_date, event_type
    silver:
      events_clean:     # Deduplicated, enriched, quality-scored events
      quarantine:       # Low-quality records (score < 0.5)
    gold:
      kpis:             # Sliding-window KPI aggregations
      sessions:         # Session-level user journey metrics
      funnels:          # Conversion funnel analysis
      anomalies:        # Anomaly aggregation summaries
```

### Schema Evolution

Each layer enforces a specific schema defined in the respective source files:
- **Bronze**: 25 fields (22 raw + event_date + error_msg + raw_json)
- **Silver**: 35 fields (22 bronze + 6 enrichment + 2 quality + 3 anomaly + 2 metadata)
- **Gold KPIs**: 25 fields (window, counts, rates, revenue, timestamps)
- **Gold Sessions**: 23 fields (session metrics, device/geo dimensions, page info)
- **Gold Funnels**: 12 fields (step, users, conversion, drop-off)
- **Gold Anomalies**: 13 fields (type, count, severity, metrics)

## Directory Structure

```
RealtimeStream/
├── src/
│   ├── __init__.py              # Public API exports
│   ├── base.py                  # BasePipeline, retry(), validation utilities
│   ├── config.py                # Shared configuration
│   ├── data_generator.py        # Clickstream simulator
│   ├── bronze_streaming.py      # Bronze ingestion (inherits BasePipeline)
│   ├── silver_streaming.py      # Silver transformation (inherits BasePipeline)
│   ├── gold_aggregations.py     # Gold aggregation (inherits BasePipeline)
│   └── alerting.py              # Alert notification system
├── tests/
│   ├── test_data_generator.py   # 21 tests
│   ├── test_bronze_streaming.py # 20 tests
│   ├── test_silver_streaming.py # 39 tests
│   ├── test_gold_aggregations.py# 43 tests
│   └── test_alerting.py         # 40+ tests
├── dashboard/
│   ├── app.py                   # Streamlit dashboard (6 pages)
│   └── .streamlit/config.toml   # Streamlit configuration
├── dags/
│   └── gold_processing_dag.py   # Airflow DAG
├── docker/
│   ├── docker-compose.yml       # All services
│   ├── Dockerfile.spark         # Custom Spark image
│   ├── Dockerfile.generator     # Generator image
│   ├── prometheus.yml           # Metrics scraping
│   └── grafana-dashboard.json   # Grafana panels
├── docs/
│   ├── architecture.md          # This file
│   ├── getting_started.md       # Setup guide
│   ├── usage.md                 # Usage guide
│   └── development.md           # Development guide
├── .github/workflows/
│   └── ci.yml                   # CI pipeline
├── pyproject.toml               # Project metadata
├── requirements.txt             # Dependencies
└── README.md                    # Project overview
```

## Security & Configuration

All configurable settings are centralized in `src/config.py`:
- Kafka bootstrap servers, topics, consumer/producer config
- MinIO/S3 endpoint, credentials, bucket name
- Medallion Delta table paths
- Quality check thresholds (min quality score, valid devices, max response time)
- Anomaly detection thresholds (error rate, response time, traffic spike)
- Enrichment rules (referrer source mapping)
- Postgres connection details
- Spark session configuration
- Alerting channel configuration (webhook URL, SMTP settings)
- Data generator parameters (event rate, user base, geographic distribution)

Environment variables override defaults (e.g., `KAFKA_BOOTSTRAP_SERVERS`, `MINIO_ENDPOINT`, `SPARK_MASTER`).

## Observability

- **Streamlit Dashboard**: Business metrics, KPIs, anomaly timeline
- **Grafana**: Pipeline health (throughput, Kafka lag, heap, latency, error rate)
- **Prometheus**: Metrics scraping from Spark, Kafka, application
- **Console Alerts**: Default alert channel (always active)
- **Webhook Alerts**: Slack/HTTP POST integration
- **Email Alerts**: SMTP-based notification
