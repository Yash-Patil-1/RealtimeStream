# RealtimeStream — Real-Time Streaming Data Pipeline

[![CI](https://github.com/Yash-Patil-1/RealtimeStream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Yash-Patil-1/RealtimeStream/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-orange)
![Apache Kafka](https://img.shields.io/badge/Kafka-7.5.0-black)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1.0-green)

## Overview

**RealtimeStream** is a production-grade real-time streaming data pipeline built entirely for local development. It simulates clickstream event data, processes it through a Medallion architecture (Bronze → Silver → Gold), detects anomalies in real-time, and serves live dashboards — all running on your machine via Docker Compose with zero cloud costs.

All pipeline stages inherit from a shared **`BasePipeline`** class (`src/base.py`) that standardises Spark session creation, Delta table lifecycle management, and cleanup — eliminating duplicated boilerplate and ensuring consistent error handling.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      DATA GENERATOR (clickstream simulator)              │
│                      Python → Kafka Producer                            │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ raw_events topic
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SHARED BASE LAYER (src/base.py)                                       │
│  BasePipeline class — Spark session, Delta table lifecycle, retry()    │
│  ▲         ▲                        ▲                                  │
│  │         │                        │                                  │
└──┼─────────┼────────────────────────┼──────────────────────────────────┘
   │         │                        │
   ▼         ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Spark Structured Streaming)                              │
│  ├── Consumes from Kafka                                               │
│  ├── Schema enforcement + bad record quarantine                         │
│  └── Writes raw Delta tables to MinIO (S3-compatible)                  │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Spark Batch)                                             │
│  ├── Deduplication, type coercion, quality checks                       │
│  ├── Enrichment, anomaly detection                                     │
│  └── Writes clean Delta tables to MinIO                                │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Airflow → Spark Batch)                                    │
│  ├── Sliding window KPIs (5min, 1hr, 24hr)                            │
│  ├── Sessionization, conversion funnels                                │
│  └── Writes to MinIO for serving                                       │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                ┌───────────────────┴───────────────────┐
                ▼                                       ▼
┌─────────────────────────────┐         ┌─────────────────────────────┐
│  STREAMLIT DASHBOARD        │         │  GRAFANA (Pipeline Health)  │
│  └ Real-time KPIs, charts,  │         │  └ Consumer lag, throughput,│
│    alerts, anomaly timeline  │         │    error rates, latency     │
└─────────────────────────────┘         └─────────────────────────────┘
```

### Medallion Data Layout (MinIO)

```
streaming-lake/
├── bronze/
│   ├── events/          # Raw ingested events (Delta table)
│   │   ├── event_date=2026-05-01/
│   │   └── event_type=purchase/
│   └── dead_letter/     # Malformed records (Delta table)
├── silver/
│   ├── events_clean/    # Deduplicated, validated, enriched (Delta table)
│   │   ├── event_date=2026-05-01/
│   │   └── event_type=purchase/
│   └── quarantine/      # Low-quality records (score < 0.5)
├── gold/
│   ├── kpis/            # Windowed aggregations (Delta table)
│   ├── sessions/        # Sessionized user journeys (Delta table)
│   ├── funnels/         # Conversion funnel metrics (Delta table)
│   └── anomaly_aggs/    # Anomaly aggregation summaries (Delta table)
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Streaming** | Apache Kafka 7.5 | Message broker |
| **Processing** | Apache Spark 3.5 (Structured Streaming) | Real-time + batch transforms |
| **Storage** | MinIO + Delta Lake 3.1 | S3-compatible data lake with ACID |
| **Orchestration** | Apache Airflow 2.8 | Gold layer batch DAGs |
| **Metadata** | PostgreSQL 15 | Serving layer + Airflow backend |
| **Monitoring** | Prometheus + Grafana | Pipeline health metrics |
| **Dashboard** | Streamlit | Real-time analytics UI |
| **Infrastructure** | Docker Compose | Everything runs locally |

## Quick Start

### Prerequisites

- Docker Desktop 24+ (with Docker Compose)
- Python 3.10+
- 8GB+ RAM allocated to Docker

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/Yash-Patil-1/RealtimeStream.git
cd RealtimeStream

# 2. Build custom Spark image (includes Delta Lake + S3 JARs)
docker compose -f docker/docker-compose.yml build

# 3. Start all services
docker compose -f docker/docker-compose.yml up -d

# 4. Verify services are healthy
docker compose -f docker/docker-compose.yml ps

# 5. Install Python dependencies
pip install -r requirements.txt

# 6. Start the data generator
python src/data_generator.py

# 7. Submit the Bronze ingestion job
spark-submit \
  --master spark://localhost:7077 \
  src/bronze_streaming.py

# 8. Open the Streamlit dashboard
streamlit run dashboard/app.py
```

### Service Ports

| Service | Port | URL |
|---------|------|-----|
| Kafka | 9092 | `localhost:9092` |
| MinIO (API) | 9000 | `http://localhost:9000` |
| MinIO (Console) | 9001 | `http://localhost:9001` |
| Spark Master | 8080 | `http://localhost:8080` |
| Spark Worker | 8081 | `http://localhost:8081` |
| Airflow | 8082 | `http://localhost:8082` |
| PostgreSQL | 5432 | `localhost:5432` |
| Prometheus | 9090 | `http://localhost:9090` |
| Grafana | 3000 | `http://localhost:3000` |

## Pipeline Components

All Spark pipeline stages inherit from **`BasePipeline`** (`src/base.py`), which standardises:
- Spark session creation with Delta Lake + S3 configuration
- Delta table initialisation (create-if-not-exists with schemas)
- Safe resource cleanup via `stop()`
- `retry()` decorator for resilient I/O (Kafka reads, Delta writes)

### Shared Base (`src/base.py`)
Provides `BasePipeline` class with `_create_spark_session()`, `_init_delta_table()`, `_ensure_tables()`, and `stop()` methods plus utility decorators (`retry`) and CLI validation functions (`validate_date`, `validate_positive_int`).

### Data Generator (`src/data_generator.py`)
Simulates realistic clickstream data: page views, clicks, purchases, errors, and search events. Configurable event rate, user base, and geographic distribution. Supports `--events`, `--continuous`, `--stdout`, `--output`, `--topic`, and `--rate` CLI flags.

### Bronze Ingestion (`src/bronze_streaming.py` → `BasePipeline`)
Spark Structured Streaming job that consumes raw events from Kafka and writes them as Delta tables to MinIO. Enforces schema, quarantines malformed records to a dead-letter table.

### Silver Transform (`src/silver_streaming.py` → `BasePipeline`)
Spark batch job that reads from Bronze Delta tables, deduplicates, validates, enriches, performs quality scoring (0.0–1.0), detects anomalies (z-score > 3σ), and splits output into clean + quarantine tables.

### Gold Aggregations (`src/gold_aggregations.py` → `BasePipeline`)
Spark batch jobs (triggered by Airflow or CLI) that compute sliding-window KPIs, user sessionization, conversion funnels, and anomaly aggregations from Silver data.

### Alerting (`src/alerting.py`)
Multi-channel alert notification system supporting console, webhook (Slack/HTTP), and email (SMTP) dispatch. Integrates with the Silver layer's anomaly detection.

### Streamlit Dashboard (`dashboard/app.py`)
Real-time analytics dashboard with 6 pages (Overview, Real-Time KPIs, Session Analytics, Conversion Funnel, Anomaly Alerts, Data Explorer) powered by Gold Delta tables.

## Testing

```bash
pytest tests/ -v
```

## Documentation

- [Architecture](docs/architecture.md)
- [Getting Started](docs/getting_started.md)
- [Usage Guide](docs/usage.md)
- [Development](docs/development.md)

## License

MIT
