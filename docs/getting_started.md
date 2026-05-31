# Getting Started

## Prerequisites

### Hardware Requirements
- **RAM**: 8GB+ allocated to Docker
- **Disk**: 10GB+ free space
- **OS**: macOS, Linux, or Windows (WSL2 recommended)

### Software Requirements
- **Docker Desktop 24+** with Docker Compose
- **Python 3.10+**
- **Git**
- **Java 11** (for Spark — Java 17+ may cause compatibility issues)

## Quick Start (5 minutes)

### 1. Clone the Repository

```bash
git clone https://github.com/Yash-Patil-1/RealtimeStream.git
cd RealtimeStream
```

### 2. Build Docker Images

```bash
docker compose -f docker/docker-compose.yml build
```

This builds the custom Spark image (with Delta Lake 3.1 + S3/MinIO JARs) and the data generator image.

### 3. Start All Services

```bash
docker compose -f docker/docker-compose.yml up -d
```

This starts:
- Kafka + Zookeeper (messaging)
- MinIO (S3-compatible storage)
- Spark Master + Worker (processing)
- Airflow (orchestration)
- PostgreSQL (metadata)
- Prometheus + Grafana (monitoring)
- Streamlit Dashboard (UI)

### 4. Verify Services

```bash
docker compose -f docker/docker-compose.yml ps
```

All services should show `Up` status. Initial startup may take 2-3 minutes.

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 6. Run the Pipeline

```bash
# Start the data generator (continuous mode, 10 events/sec to Kafka)
python src/data_generator.py

# Or generate 100 events to stdout (quick test without Docker)
python src/data_generator.py --events 100 --stdout

# Or generate events to a JSONL file
python src/data_generator.py --events 1000 --output sample.jsonl

# Or write to a custom Kafka topic with higher rate
python src/data_generator.py --topic my-topic --rate 50 --continuous

# In a new terminal, run Bronze ingestion (streaming from Kafka)
python src/bronze_streaming.py --mode streaming

# Run Silver transformation (latest Bronze partition)
python src/silver_streaming.py

# Run Gold aggregations (all modes)
python src/gold_aggregations.py --mode all

# Open the dashboard
streamlit run dashboard/app.py
```

The data generator CLI supports:
- `--events N` — Generate exactly N events then exit
- `--continuous` — Run indefinitely until Ctrl+C
- `--stdout` — Print events to stdout instead of Kafka
- `--output FILE` — Write events as JSONL to a file
- `--topic NAME` — Kafka topic name (default: `raw_events`)
- `--rate N` — Events per second (default: 10)
- `--seed N` — Random seed for reproducible output

### 7. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8082 | airflow / airflow |
| Grafana | http://localhost:3000 | admin / admin |
| Kafka | localhost:9092 | — |

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_silver_streaming.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

## Stopping Everything

```bash
docker compose -f docker/docker-compose.yml down
```

To also remove volumes (data will be lost):
```bash
docker compose -f docker/docker-compose.yml down -v
```

## Next Steps

- Read the [Architecture](architecture.md) documentation for detailed system design
- Check the [Usage Guide](usage.md) for pipeline operation details
- See [Development](development.md) for contributing guidelines
- Explore the Streamlit dashboard at http://localhost:8501
