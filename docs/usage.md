# Usage Guide

## Pipeline Operation

All Spark-based pipeline stages (Bronze, Silver, Gold) inherit from **`BasePipeline`** (`src/base.py`), which provides shared Spark session creation, Delta table lifecycle management, `retry()` for resilient I/O, and safe resource cleanup via `stop()`.

### Data Generator

The data generator simulates realistic clickstream events. It runs as a standalone Python process — does **not** use Spark or BasePipeline.

```bash
# Continuous mode (default: 10 events/sec, produces to Kafka)
python src/data_generator.py

# Generate N events then exit
python src/data_generator.py --events 1000

# Specify event rate
python src/data_generator.py --rate 50

# Generate to JSONL file instead of Kafka
python src/data_generator.py --events 1000 --output sample.jsonl

# Print events to stdout (useful for testing without Docker)
python src/data_generator.py --events 100 --stdout

# Produce to a custom Kafka topic with seed for reproducibility
python src/data_generator.py --topic raw_events --seed 42
```

**CLI Arguments:**
| Argument | Default | Description |
|----------|---------|-------------|
| `--rate N` | `10` | Events per second |
| `--events N` | Infinite | Total events to generate then exit |
| `--continuous` | — | Run indefinitely (default without `--events`) |
| `--stdout` | — | Print events to stdout instead of Kafka |
| `--output PATH` | — | Write events as JSONL to file |
| `--topic NAME` | `raw_events` | Kafka topic name |
| `--seed N` | Random | Random seed for reproducible output |

### Bronze Layer (`→ BasePipeline`)

```bash
# Streaming mode (consumes from Kafka, default)
python src/bronze_streaming.py
python src/bronze_streaming.py --mode streaming --kafka-broker localhost:9092

# Batch mode (reads from a JSONL file)
python src/bronze_streaming.py --mode batch --input sample_data/sample_clickstream.json

# Batch mode with custom output path
python src/bronze_streaming.py --mode batch \
  --input sample_data/sample_clickstream.json \
  --output s3a://streaming-lake/delta/bronze/events

# With custom trigger interval
python src/bronze_streaming.py --mode streaming \
  --trigger-interval "30 seconds"
```

**CLI Arguments:**
| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `streaming` | `batch` or `streaming` |
| `--input` | — | Input path for batch mode (JSONL file) |
| `--output` | Config default | Override Bronze Delta table output path |
| `--kafka-broker` | `localhost:9092` | Kafka bootstrap servers |
| `--trigger-interval` | `10 seconds` | Streaming trigger interval (e.g. `5 seconds`) |

### Silver Layer (`→ BasePipeline`)

```bash
# Process latest Bronze partition (detected automatically)
python src/silver_streaming.py

# Process a specific date
python src/silver_streaming.py --date 2026-05-29

# Backfill a date range
python src/silver_streaming.py --start-date 2026-05-01 --end-date 2026-05-29

# Override Delta paths
python src/silver_streaming.py \
  --bronze-path s3a://streaming-lake/delta/bronze/events \
  --silver-path s3a://streaming-lake/delta/silver/events_clean

# Skip anomaly detection and alerting
python src/silver_streaming.py --skip-anomalies
```

**CLI Arguments:**
| Argument | Default | Description |
|----------|---------|-------------|
| `--date` | Latest partition | Single event date to process (yyyy-MM-dd) |
| `--start-date` | — | Backfill start date (yyyy-MM-dd) |
| `--end-date` | — | Backfill end date (yyyy-MM-dd) |
| `--bronze-path` | Config default | Override Bronze Delta table path |
| `--silver-path` | Config default | Override Silver Delta output path |
| `--skip-anomalies` | False | Skip anomaly detection & Kafka alerting |

### Gold Layer (`→ BasePipeline`)

```bash
# Run all aggregations (default: KPIs, sessions, funnels, anomaly_aggs)
python src/gold_aggregations.py

# Run a specific aggregation mode
python src/gold_aggregations.py --mode kpis
python src/gold_aggregations.py --mode sessions
python src/gold_aggregations.py --mode funnels
python src/gold_aggregations.py --mode anomaly_aggs

# With custom window duration
python src/gold_aggregations.py --mode kpis --window 5m
python src/gold_aggregations.py --mode funnels --window 24h

# Process a specific date
python src/gold_aggregations.py --date 2026-05-29

# Backfill a date range
python src/gold_aggregations.py \
  --start-date 2026-05-01 \
  --end-date 2026-05-29

# Override Silver input path
python src/gold_aggregations.py --silver-path s3a://streaming-lake/delta/silver/events_clean
```

**Aggregation Modes:**
| Mode | Description | Default Window |
|------|-------------|----------------|
| `kpis` | Sliding-window KPI aggregations | 1h |
| `sessions` | Session-level metrics | Full date |
| `funnels` | Conversion funnel analysis | 24h |
| `anomaly_aggs` | Anomaly grouping | 1h |
| `all` | All four modes sequentially | — |

**Supported Windows:** `5m`, `15m`, `30m`, `1h`, `6h`, `24h` (or custom Ns/Nm/Nh)

### Alerting

The alerting system runs automatically as part of the Silver pipeline. To configure:

```bash
# Environment variables for channels
export ALERT_WEBHOOK_URL="https://hooks.slack.com/..."
export ALERT_SLACK_CHANNEL="#anomaly-alerts"
export ALERT_SMTP_HOST="smtp.gmail.com"
export ALERT_SMTP_USER="user@gmail.com"
export ALERT_SMTP_PASSWORD="app-password"
export ALERT_EMAIL_TO="team@example.com"
export ALERT_MIN_SCORE="0.3"
```

**Alert Channels:**
| Channel | Config Key | Default | Description |
|---------|-----------|---------|-------------|
| Console | — | Always active | Logs to stdout/stderr |
| Webhook | `webhook_url` | — | HTTP POST (Slack/generic) |
| Email | `smtp_host` + `email_to` | — | SMTP email notification |

**Alert Types:**
| Type | Condition | Score Range |
|------|-----------|-------------|
| `error_rate_spike` | Error rate > 5% threshold | 0.0-2.0 (capped) |
| `slow_response` | Response time z-score > 3σ | 0.0+ |

### Dashboard

```bash
# Start Streamlit dashboard
streamlit run dashboard/app.py

# With custom port
streamlit run dashboard/app.py --server.port 8501

# With custom config
streamlit run dashboard/app.py --theme.base dark
```

The dashboard auto-refreshes at configurable intervals (10-120 seconds).

## Backfill Strategies

### Full Backfill
Process all data from scratch through the pipeline:
```bash
# 1. Reprocess Bronze
python src/bronze_streaming.py --mode batch --input /data/archive --output /tmp/delta/bronze

# 2. Reprocess Silver (backfill range)
python src/silver_streaming.py --start-date 2026-01-01 --end-date 2026-05-29

# 3. Reprocess Gold (backfill range)
python src/gold_aggregations.py --start-date 2026-01-01 --end-date 2026-05-29
```

### Incremental Backfill
Process only missing dates:
```bash
# Check which dates exist in Silver
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('delta').load('s3a://streaming-lake/delta/silver/events_clean')
dates = df.select('event_date').distinct().orderBy('event_date').collect()
print([d.event_date for d in dates])
"
```

## Monitoring

### Streamlit Dashboard Metrics
- **Overview**: Pipeline health check, throughput, recent alerts
- **Real-Time KPIs**: Hourly event breakdown, response time distribution, rates
- **Session Analytics**: Duration distribution, device breakdown, heatmap
- **Conversion Funnel**: Step-by-step funnel, conversion tracking, trends
- **Anomaly Alerts**: Severity timeline, score distribution, alert details
- **Data Explorer**: Raw event inspection, column profiling, config viewer

### Grafana Metrics
Dashboard at http://localhost:3000 (admin/admin):
- **Event Throughput**: Events per second across topics
- **Kafka Consumer Lag**: Processing latency
- **Memory Usage**: JVM heap across Spark executors
- **Response Latency**: p50/p95/p99 response times
- **Error Rate**: Percentage of error events over time

## Troubleshooting

### Common Issues

**"SparkContext not available"**
- Ensure `PYSPARK_PYTHON` environment variable is set
- On Windows, ensure Hadoop winutils is installed
- Check Java version (JDK 11 recommended)

**"Delta table does not exist"**
- Run Bronze ingestion first to create the Delta table
- Or run with `--output` pointing to an existing Delta path

**"No events found"**
- Ensure the data generator is running
- Check Kafka topic exists: `docker compose exec kafka kafka-topics --list`
- Verify MinIO bucket exists: `docker compose exec minio mc ls streaming-lake`

**"Kafka connection refused"**
- Wait for Kafka to finish initializing (2-3 minutes)
- Check `docker compose ps` to verify Kafka is running
- Verify `KAFKA_BOOTSTRAP_SERVERS` environment variable

**Webhook alerts not sending**
- Verify `ALERT_WEBHOOK_URL` is set correctly
- Check network connectivity to the webhook endpoint
- Console channel is always active — check logs

### Logs

```bash
# View specific service logs
docker compose -f docker/docker-compose.yml logs kafka
docker compose -f docker/docker-compose.yml logs spark-master

# Follow all logs
docker compose -f docker/docker-compose.yml logs -f

# Pipeline logs (when running outside Docker)
tail -f src/*.log
```
