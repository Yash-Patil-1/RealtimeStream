# Development Guide

## Project Structure

```
RealtimeStream/
├── src/                      # Source code
│   ├── __init__.py           # Public API exports
│   ├── base.py               # BasePipeline, retry(), validation utilities
│   ├── config.py             # Shared configuration
│   ├── data_generator.py     # Clickstream simulator
│   ├── bronze_streaming.py   # Bronze ingestion (inherits BasePipeline)
│   ├── silver_streaming.py   # Silver transformation (inherits BasePipeline)
│   ├── gold_aggregations.py  # Gold aggregation (inherits BasePipeline)
│   └── alerting.py           # Alert notification system
├── tests/                    # Test suite
│   ├── test_data_generator.py
│   ├── test_bronze_streaming.py
│   ├── test_silver_streaming.py
│   ├── test_gold_aggregations.py
│   └── test_alerting.py
├── dashboard/                # Streamlit dashboard
│   ├── app.py
│   └── .streamlit/config.toml
├── dags/                     # Airflow DAGs
│   └── gold_processing_dag.py
├── docker/                   # Docker configuration
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   ├── Dockerfile.generator
│   ├── prometheus.yml
│   └── grafana-dashboard.json
├── docs/                     # Documentation
│   ├── architecture.md
│   ├── getting_started.md
│   ├── usage.md
│   └── development.md
└── .github/workflows/        # CI/CD
    └── ci.yml
```

## Development Workflow

### 1. Set Up Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# Install development dependencies
pip install -r requirements.txt
pip install pytest pytest-cov ruff
```

### 2. Run Tests Before Making Changes

```bash
# Run the full test suite
pytest tests/ -v

# Check current pass/fail status
pytest tests/ -q
```

### 3. Make Changes

Follow the existing patterns:
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes
- **Types**: All functions must have type annotations
- **Docstrings**: All modules, classes, and public functions must have docstrings
- **Logging**: Use `logging.getLogger(__name__)` instead of `print()`
- **Config**: Add new configuration to `src/config.py`, don't hardcode values
- **Retry**: Use the `@retry()` decorator from `src/base.py` for I/O operations that may transiently fail (Kafka reads, Delta writes, network calls)
- **Validation**: Use `validate_date()`, `validate_positive_int()`, and `validate_rate()` from `src/base.py` for CLI argument parsing instead of reinventing validation logic

### 4. Test Your Changes

```bash
# Run specific test file
pytest tests/test_silver_streaming.py -v --tb=long

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing

# Lint
ruff check src/ tests/ --ignore E501
```

### 5. Code Review Checklist

Before submitting changes:
- [ ] All existing tests pass
- [ ] New tests cover the changes
- [ ] Type annotations are complete
- [ ] Docstrings are present
- [ ] No hardcoded configuration values
- [ ] Logging is appropriate (not too verbose)
- [ ] No unused imports or variables
- [ ] Follows existing naming conventions

## Coding Standards

### Python Style
- **Line length**: 100 characters max
- **Indentation**: 4 spaces (no tabs)
- **Imports**: Standard library → Third-party → Local, alphabetically sorted
- **Type annotations**: Always use `Optional[X]` instead of `X | None`
- **Docstrings**: Google-style or reStructuredText

### Naming Conventions
| Element | Convention | Example |
|---------|-----------|---------|
| Modules | `snake_case` | `bronze_streaming.py` |
| Classes | `PascalCase` | `SilverPipeline`, `AlertManager` |
| Functions | `snake_case` | `_deduplicate_events()` |
| Variables | `snake_case` | `bronze_count` |
| Constants | `UPPER_CASE` | `KAFKA_BOOTSTRAP_SERVERS` |
| Private | Prefix `_` | `_init_tables()` |

### Testing Standards
- **File name**: `test_<module_name>.py`
- **Class name**: `Test<ClassName>` or `Test<Feature>`
- **Function name**: `test_<behavior>` (descriptive)
- **Fixtures**: Use `pytest.fixture` for shared setup
- **Coverage**: Aim for >80% on new code
- **Mocking**: Mock external dependencies (Kafka, SMTP, HTTP)

### Git Workflow

```bash
# Create a feature branch
git checkout -b feature/my-feature

# Make focused commits
git add src/my_new_feature.py
git commit -m "feat: add my new feature"

# Run tests before pushing
pytest tests/ -q

# Push and create PR
git push origin feature/my-feature
```

**Commit message format:**
```
<type>: <description>

Types: feat, fix, refactor, test, docs, chore
```

## BasePipeline Architecture

All Spark-based pipeline stages (Bronze, Silver, Gold) inherit from **`BasePipeline`** (`src/base.py`), which provides a consistent lifecycle for Spark session management, Delta table initialisation, and resource cleanup.

### BasePipeline Class Reference

```python
class BasePipeline:
    TABLE_REGISTRY: Dict[str, StructType] = {}

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
        app_name: str = "RealtimeStream",
    ):
        ...
```

#### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config` | `Optional[Dict[str, Any]]` | `None` | Merged configuration dict (stage defaults + user overrides) |
| `spark` | `Optional[SparkSession]` | `None` | Inject an existing SparkSession (for testing); if `None`, one is created automatically |
| `app_name` | `str` | `"RealtimeStream"` | Spark application name (passed to `get_spark_config()`) |

When `spark` is provided, the pipeline reuses it and **will not** stop it on `stop()`. When omitted, `BasePipeline` creates and owns the session.

#### Lifecycle Methods

| Method | Description | Override? |
|--------|-------------|-----------|
| `_create_spark_session(app_name)` | Creates a Spark session with Delta Lake, S3, and adaptive query execution config | No (inherited) |
| `_init_delta_table(path, schema, partition_by)` | Creates a Delta table at the given path if it doesn't exist. Safe no-op when SparkContext is unavailable (e.g. unit tests) | No (inherited) |
| `_ensure_tables()` | Lazily calls `_init_tables()` once, guarded by `_tables_initialized` flag | No (inherited) |
| `_init_tables()` | Iterates `TABLE_REGISTRY` and calls `_init_delta_table()` for each entry | **Yes — subclass must override** |
| `stop()` | Stops the Spark session only if this instance created it | No (inherited) |

#### TABLE_REGISTRY Pattern

Subclasses define a class-level `TABLE_REGISTRY` dict mapping config path keys to `StructType` schemas. The base class uses this in `_init_tables()` to automatically create all required Delta tables on startup:

```python
class MyPipeline(BasePipeline):
    TABLE_REGISTRY: Dict[str, StructType] = {
        "my_table_path": MY_TABLE_SCHEMA,
        "my_other_path": MY_OTHER_SCHEMA,
    }
```

The keys correspond to entries in the merged `config` dict. The base `_init_tables()` method handles lookup and creation automatically — override only when custom logic is required.

#### Utility Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `retry(max_attempts, delay, backoff, retryable_exceptions)` | Decorator | Retry a function with exponential backoff on failure |
| `validate_date(date_str)` | `str → str` | Validate `yyyy-MM-dd` format; raises `ValueError` |
| `validate_positive_int(value, name)` | `(str, str) → int` | Parse and validate a positive integer; raises `ValueError` |
| `validate_rate(value)` | `Optional[str] → Optional[int]` | Validate an events-per-second CLI argument |

##### `@retry()` Decorator Examples

```python
from src.base import retry

# Basic usage — retries on any exception, 3 attempts, 1s delay
@retry()
def fetch_kafka_messages():
    ...

# Custom retry — 5 attempts, 0.5s initial delay, double after each retry
@retry(max_attempts=5, delay=0.5, backoff=2.0)
def write_to_delta(df):
    ...

# Targeted — only retry on specific exceptions
from py4j.protocol import Py4JJavaError

@retry(max_attempts=3, delay=1.0, retryable_exceptions=(Py4JJavaError, TimeoutError))
def fragile_operation():
    ...
```

### Inheritance Diagram

```
BasePipeline  (src/base.py)
├── BronzePipeline  (src/bronze_streaming.py)
│   ├── TABLE_REGISTRY: {bronze_events_path → RAW_EVENT_SCHEMA, bronze_dead_letter_path → RAW_EVENT_SCHEMA}
│   ├── read_from_kafka() — @retry-decorated Kafka consumer
│   └── run_batch() / run_stream() — ingestion entry points
│
├── SilverPipeline  (src/silver_streaming.py)
│   ├── TABLE_REGISTRY: {silver_clean_path, silver_quarantine_path, ← schemas}
│   ├── enrich() / score_quality() / detect_anomalies()
│   └── run(date) — batch transform entry point
│
└── GoldPipeline  (src/gold_aggregations.py)
    ├── TABLE_REGISTRY: {kpis_path, sessions_path, funnels_path, anomaly_aggs_path}
    ├── compute_kpis() / compute_sessions() / compute_funnels() / aggregate_anomalies()
    └── run(date, mode) — aggregation entry point
```

## Adding a New Pipeline Component

To add a new Medallion stage (e.g. `Platinum`) or a standalone Spark job:

1. **Create the source module** in `src/` (e.g. `src/platinum_streaming.py`)
2. **Inherit from `BasePipeline`**:

```python
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.base import BasePipeline, validate_date

# ─── Schema Definitions ────────────────────────────────────────────────

PLATINUM_SCHEMA = StructType([
    StructField("metric", StringType(), nullable=False),
    StructField("value", DoubleType(), nullable=False),
])

# ─── Pipeline Class ────────────────────────────────────────────────────

DEFAULT_CONFIG: Dict[str, Any] = {
    "platinum_path": "/path/to/delta/table",
}

class PlatinumPipeline(BasePipeline):
    """Pipeline for the Platinum aggregation layer."""

    TABLE_REGISTRY: Dict[str, StructType] = {
        "platinum_path": PLATINUM_SCHEMA,
    }

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ):
        merged_config = {**DEFAULT_CONFIG, **(config or {})}
        super().__init__(config=merged_config, spark=spark, app_name="PlatinumStreaming")
        self._ensure_tables()

    def run(self, date: str) -> None:
        """Run the Platinum aggregation for the given date."""
        validate_date(date)
        df = self.spark.read.format("delta").load(self.config["platinum_path"])
        # ... transformation logic ...
        df.write.format("delta").mode("overwrite").save(self.config["platinum_path"])


def main(date: str) -> None:
    pipeline = PlatinumPipeline()
    try:
        pipeline.run(date)
    finally:
        pipeline.stop()
```

3. **Define schemas** — Use PySpark `StructType` for Delta table schemas
4. **Add configuration** to `src/config.py` with sensible defaults + env var overrides
5. **Register tables** — Set `TABLE_REGISTRY` so `_ensure_tables()` auto-creates them on init
6. **Create tests** in `tests/` — unit tests with injected Spark sessions (see `tests/test_silver_streaming.py` for patterns)
7. **Integrate** — Wire it into the pipeline (Airflow DAG, CLI entry point, or Streamlit dashboard)
8. **Update documentation** in `docs/` — architecture, usage, getting started
9. **Run all tests** and fix any failures

## Adding a New Alert Channel

1. **Create a new class** in `src/alerting.py` extending `AlertChannel`
2. **Implement `send()` and optionally `send_batch()`**
3. **Add configuration defaults** to `ALERTING_CONFIG_DEFAULTS`
4. **Register the channel** in `CHANNEL_REGISTRY`
5. **Add auto-detection** in `AlertManager._init_channels()`
6. **Create tests** in `tests/test_alerting.py`
7. **Update documentation** with the new channel

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `streaming-lake` | MinIO bucket name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `POSTGRES_HOST` | `localhost` | Postgres host |
| `EVENTS_PER_SECOND` | `10` | Generator event rate |
| `ALERT_WEBHOOK_URL` | — | Webhook URL for alerts |
| `ALERT_SMTP_HOST` | — | SMTP server for email alerts |
| `ALERT_EMAIL_TO` | — | Email alert recipient |
| `ALERT_MIN_SCORE` | `0.0` | Minimum anomaly score for alerts |

## Windows-Specific Notes

When developing on Windows:

1. **Spark requires winutils**: Download `winutils.exe` and `hadoop.dll` for your Hadoop version
2. **Java version**: Use JDK 11 (Java 17+ has known compatibility issues with PySpark)
3. **PYSPARK_PYTHON**: Must be set to avoid "Accept timed out" errors
4. **Delta Lake JARs**: May need to download manually and set `spark.jars` with `file:///` URIs
5. **Path separators**: Use forward slashes or raw strings for Delta paths
6. **Docker**: Use WSL2 backend for best performance

## Profiling & Performance

```bash
# Profile a specific pipeline stage
python -m cProfile -o profile.out src/silver_streaming.py --date 2026-05-29

# Analyze the profile
python -c "
import pstats
p = pstats.Stats('profile.out')
p.sort_stats('cumtime').print_stats(20)
"
```

## Troubleshooting Tests

**PySpark UDF tests fail with "No module named 'src'"**
- Set `spark.executorEnv.PYTHONPATH` in the SparkSession builder
- Or set `PYTHONPATH` environment variable before creating SparkSession
- See `tests/test_silver_streaming.py` for the correct pattern

**Tests hang on Windows**
- Ensure `PYSPARK_PYTHON` is set to the correct Python executable
- Reduce test batch sizes if memory is constrained
- Use `--timeout=120` with pytest for long-running tests

**Delta table write tests fail**
- Apache Hadoop NativeIO has limitations on Windows
- Mark Windows-specific failures with `@pytest.mark.xfail`
- Tests should pass on Linux (CI runs on Ubuntu)
