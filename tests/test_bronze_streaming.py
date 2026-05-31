"""
Tests for the Bronze streaming layer (bronze_streaming.py).

Tests cover:
  - Schema definitions match expected field types
  - Numeric casting helper
  - Partition column enrichment
  - Dead-letter / clean record separation
  - Batch mode run against sample JSONL data
  - Config loading and Spark session creation (with local[*])
"""

import json
import os
import sys
from datetime import datetime, timezone

import pytest

from src.bronze_streaming import (
    CLEAN_EVENT_SCHEMA,
    RAW_EVENT_SCHEMA,
    BronzePipeline,
    _cast_numeric_fields,
    _enrich_partition_columns,
    _separate_bad_records,
)
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ─── Helpers ──────────────────────────────────────────────────────────


def _setup_windows_env():
    """Set up HADOOP_HOME, JAVA_HOME, PYSPARK_PYTHON for Spark on Windows compatibility."""
    hadoop_home = os.path.join(os.environ.get("USERPROFILE", "C:\\temp"), "hadoop-bin")
    bin_dir = os.path.join(hadoop_home, "bin")
    if os.path.isdir(bin_dir):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

    jdk11_path = r"C:\tools\jdk-11.0.26+4"
    if os.path.isdir(jdk11_path):
        os.environ["JAVA_HOME"] = jdk11_path
        java_bin = os.path.join(jdk11_path, "bin")
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    # PySpark worker connectivity: pin Python to avoid "Accept timed out" on Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


DELTA_JARS = r"C:\tools\spark-jars\delta-spark_2.12-3.1.0.jar"
DELTA_STORAGE = r"C:\tools\spark-jars\delta-storage-3.1.0.jar"
DELTA_AVAILABLE = os.path.isfile(DELTA_JARS) and os.path.isfile(DELTA_STORAGE)


def _file_uri(path: str) -> str:
    """Convert a local absolute path to a file:/// URI for Spark on Windows.

    Example: C:\\Users\\name\\tmp\\bronze -> file:///C:/Users/name/tmp/bronze
    """
    return "file:///" + path.replace("\\", "/").lstrip("/")


# ─── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for tests.

    If the Delta Lake JAR is available, Delta support is enabled. Otherwise
    helper function tests still work, while Delta integration tests skip.
    """
    _setup_windows_env()

    builder = (
        SparkSession.builder.appName("test_bronze")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )

    if DELTA_AVAILABLE:
        # Build a comma-separated list of Delta JARs (file:/// URIs to avoid Hadoop scheme confusion)
        delta_jars = [DELTA_JARS, DELTA_STORAGE]
        jar_uris = ",".join(
            "file:///" + j.replace("\\", "/").lstrip("/") for j in delta_jars if os.path.isfile(j)
        )
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars", jar_uris)
            .config("spark.hadoop.io.native.lib", "false")
            .config("spark.sql.hadoop.fs.local.block-size", "33554432")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def spark_delta(spark):
    """Delta-enabled Spark session for pipeline integration tests.
    Returns the shared session (which already has Delta configured if
    the JAR is available). Skips tests that request this fixture if
    the Delta JAR is not available.
    """
    if not DELTA_AVAILABLE:
        pytest.skip(f"Delta JAR not found at {DELTA_JARS}")
    return spark


@pytest.fixture
def sample_raw_df(spark):
    """Create a DataFrame with raw JSON strings as they come from Kafka."""
    events = [
        {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc123",
            "timestamp": "2026-05-29T10:00:00+00:00",
            "page_url": "/products",
            "referrer_url": "https://google.com",
            "user_agent": "Mozilla/5.0 (Windows) Chrome/120.0",
            "device_type": "desktop",
            "browser": "Chrome",
            "os": "Windows",
            "country": "US",
            "city": "New York",
            "ip_address": "192.168.1.1",
            "amount": "49.99",
            "currency": "USD",
            "product_id": "prod-001",
            "category": "electronics",
            "error_code": None,
            "response_time_ms": "150",
            "status_code": "200",
        },
        {
            "event_id": "evt-002",
            "event_type": "purchase",
            "user_id": "user-00002",
            "session_id": "sess-def456",
            "timestamp": "2026-05-29T10:01:00+00:00",
            "page_url": "/checkout/confirm",
            "referrer_url": None,
            "user_agent": "Mozilla/5.0 (Mac) Safari/605.1",
            "device_type": "mobile",
            "browser": "Safari",
            "os": "iOS",
            "country": "IN",
            "city": "Mumbai",
            "ip_address": "10.0.0.1",
            "amount": "129.99",
            "currency": "INR",
            "product_id": "prod-042",
            "category": "fashion",
            "error_code": None,
            "response_time_ms": "320",
            "status_code": "200",
        },
        {
            "event_id": "evt-003",
            "event_type": "error",
            "user_id": "user-00003",
            "session_id": "sess-ghi789",
            "timestamp": "2026-05-29T10:02:00+00:00",
            "page_url": "/checkout/payment",
            "referrer_url": "/cart",
            "user_agent": "Mozilla/5.0 (Linux) Firefox/125.0",
            "device_type": "desktop",
            "browser": "Firefox",
            "os": "Linux",
            "country": "DE",
            "city": "Berlin",
            "ip_address": "172.16.0.1",
            "amount": None,
            "currency": None,
            "product_id": "prod-015",
            "category": "electronics",
            "error_code": "500",
            "response_time_ms": "4500",
            "status_code": "500",
        },
        {
            "event_id": None,  # malformed: null event_id
            "event_type": "click",
            "user_id": "user-00004",
            "session_id": "sess-jkl012",
            "timestamp": "2026-05-29T10:03:00+00:00",
            "page_url": "/products/electronics/prod-010",
            "referrer_url": None,
            "user_agent": "Mozilla/5.0 (Windows) Edge/120.0",
            "device_type": "tablet",
            "browser": "Edge",
            "os": "Android",
            "country": "GB",
            "city": "London",
            "ip_address": "192.168.2.1",
            "amount": "0",
            "currency": None,
            "product_id": "prod-010",
            "category": "electronics",
            "error_code": None,
            "response_time_ms": "89",
            "status_code": "200",
        },
    ]
    rows = [
        Row(raw_value=json.dumps(e, default=str), kafka_timestamp=datetime.now(timezone.utc))
        for e in events
    ]
    return spark.createDataFrame(rows)


# ─── Schema Tests ─────────────────────────────────────────────────────


class TestSchemas:
    def test_raw_schema_has_all_string_fields(self):
        """All fields in RAW_EVENT_SCHEMA should be StringType (raw JSON)."""
        for field in RAW_EVENT_SCHEMA.fields:
            assert field.dataType == StringType(), (
                f"Field {field.name} should be StringType, got {field.dataType}"
            )

    def test_clean_schema_has_typed_fields(self):
        """CLEAN_EVENT_SCHEMA should have proper typed fields."""
        field_map = {f.name: f.dataType for f in CLEAN_EVENT_SCHEMA.fields}

        assert field_map["event_id"] == StringType()
        assert field_map["event_date"] == StringType()
        assert field_map["timestamp"] == TimestampType()
        assert field_map["amount"] == DoubleType()
        assert field_map["error_code"] == IntegerType()
        assert field_map["response_time_ms"] == IntegerType()
        assert field_map["status_code"] == IntegerType()

    def test_clean_schema_includes_event_date(self):
        """CLEAN_EVENT_SCHEMA must have event_date partition column."""
        field_names = [f.name for f in CLEAN_EVENT_SCHEMA.fields]
        assert "event_date" in field_names

    def test_clean_schema_event_date_is_string(self):
        """event_date should be StringType for Delta partition pruning."""
        field_map = {f.name: f.dataType for f in CLEAN_EVENT_SCHEMA.fields}
        assert field_map["event_date"] == StringType()


# ─── Numeric Casting Tests ────────────────────────────────────────────


class TestNumericCasting:
    def test_cast_valid_numeric_fields(self, spark):
        """Valid numeric strings should be cast to their proper types."""
        data = [
            Row(
                amount="49.99",
                error_code="500",
                response_time_ms="150",
                status_code="200",
            ),
            Row(amount="0", error_code="400", response_time_ms="0", status_code="400"),
        ]
        df = spark.createDataFrame(data)
        result = _cast_numeric_fields(df)

        row = result.collect()[0]
        assert isinstance(row["amount"], float)
        assert row["amount"] == 49.99
        assert row["error_code"] == 500
        assert row["response_time_ms"] == 150
        assert row["status_code"] == 200

    def test_cast_null_numeric_fields(self, spark):
        """None/null values should remain None after casting."""
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([
            StructField("amount", StringType(), True),
            StructField("error_code", StringType(), True),
            StructField("response_time_ms", StringType(), True),
            StructField("status_code", StringType(), True),
        ])
        data = [Row(amount=None, error_code=None, response_time_ms=None, status_code=None)]
        df = spark.createDataFrame(data, schema)
        result = _cast_numeric_fields(df)

        row = result.collect()[0]
        assert row["amount"] is None
        assert row["error_code"] is None
        assert row["response_time_ms"] is None
        assert row["status_code"] is None

    def test_cast_none_string_numeric_fields(self, spark):
        """'None' string values should become NULL after casting."""
        data = [
            Row(
                amount="None",
                error_code="None",
                response_time_ms="None",
                status_code="None",
            )
        ]
        df = spark.createDataFrame(data)
        result = _cast_numeric_fields(df)

        row = result.collect()[0]
        assert row["amount"] is None
        assert row["error_code"] is None
        assert row["response_time_ms"] is None
        assert row["status_code"] is None

    def test_cast_invalid_numeric_strings(self, spark):
        """Non-numeric strings should become NULL."""
        data = [
            Row(
                amount="abc",
                error_code="xyz",
                response_time_ms="not-a-number",
                status_code="",
            )
        ]
        df = spark.createDataFrame(data)
        result = _cast_numeric_fields(df)

        row = result.collect()[0]
        assert row["amount"] is None
        assert row["error_code"] is None
        assert row["response_time_ms"] is None
        assert row["status_code"] is None


# ─── Partition Column Tests ───────────────────────────────────────────


class TestPartitionColumns:
    def test_enrich_partition_columns_adds_event_date(self, spark):
        """event_date should be derived from the timestamp column."""
        data = [
            Row(timestamp=datetime(2026, 5, 29, 10, 0, 0)),
            Row(timestamp=datetime(2026, 6, 1, 0, 0, 0)),
        ]
        df = spark.createDataFrame(data)
        result = _enrich_partition_columns(df)

        rows = result.collect()
        assert rows[0]["event_date"] == "2026-05-29"
        assert rows[1]["event_date"] == "2026-06-01"

    def test_enrich_partition_columns_preserves_other_fields(self, spark):
        """Other columns should remain unchanged."""
        data = [Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 0, 0))]
        df = spark.createDataFrame(data)
        result = _enrich_partition_columns(df)

        row = result.collect()[0]
        assert row["event_id"] == "evt-001"
        assert row["event_date"] == "2026-05-29"


# ─── Dead-Letter Separation Tests ─────────────────────────────────────


class TestSeparation:
    _SEP_SCHEMA = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    def test_separate_clean_records(self, spark):
        """Valid records should go to the clean DataFrame."""
        data = [
            Row(
                event_id="evt-001",
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(
                event_id="evt-002",
                event_type="click",
                timestamp=datetime(2026, 5, 29, 10, 1, 0),
            ),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 2
        assert dead.count() == 0

    def test_separate_malformed_null_event_id(self, spark):
        """Records with NULL event_id should go to dead-letter."""
        data = [
            Row(
                event_id="evt-001",
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(
                event_id=None,
                event_type="click",
                timestamp=datetime(2026, 5, 29, 10, 1, 0),
            ),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 1
        assert dead.count() == 1

        clean_ids = [r.event_id for r in clean.collect()]
        assert "evt-001" in clean_ids
        assert None not in clean_ids

    def test_separate_malformed_empty_event_id(self, spark):
        """Records with empty event_id should go to dead-letter."""
        data = [
            Row(
                event_id="evt-001",
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(
                event_id="",
                event_type="click",
                timestamp=datetime(2026, 5, 29, 10, 1, 0),
            ),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 1
        assert dead.count() == 1

    def test_separate_malformed_null_event_type(self, spark):
        """Records with NULL event_type should go to dead-letter."""
        data = [
            Row(
                event_id="evt-001",
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(
                event_id="evt-002",
                event_type=None,
                timestamp=datetime(2026, 5, 29, 10, 1, 0),
            ),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 1
        assert dead.count() == 1

    def test_separate_malformed_null_timestamp(self, spark):
        """Records with NULL timestamp should go to dead-letter."""
        data = [
            Row(
                event_id="evt-001",
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(event_id="evt-002", event_type="click", timestamp=None),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 1
        assert dead.count() == 1

    def test_separate_all_malformed(self, spark):
        """All malformed records should go to dead-letter."""
        data = [
            Row(
                event_id=None,
                event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
            ),
            Row(event_id="", event_type=None, timestamp=datetime(2026, 5, 29, 10, 1, 0)),
        ]
        df = spark.createDataFrame(data, self._SEP_SCHEMA)
        clean, dead = _separate_bad_records(df)

        assert clean.count() == 0
        assert dead.count() == 2


# ─── End-to-End Parse + Separate Tests ────────────────────────────────


class TestBronzePipeline:
    def test_pipeline_parses_and_separates(self, spark, sample_raw_df):
        """Full parse + separate pipeline should produce correct split."""
        pipeline = BronzePipeline(
            config={
                "kafka_bootstrap_servers": "localhost:9092",
                "bronze_path": "/tmp/test_bronze/test_pipeline_parse",
                "checkpoint_path": "/tmp/test_bronze/test_checkpoint",
            },
            spark=spark,
        )

        parsed = pipeline.parse_events(sample_raw_df)
        clean, dead = _separate_bad_records(parsed)

        # 3 clean (evt-001, evt-002, evt-003), 1 malformed (evt-004 has null event_id)
        assert clean.count() == 3
        assert dead.count() == 1

        # Verify clean records have proper types
        clean_rows = clean.collect()
        for r in clean_rows:
            assert r.event_id is not None
            assert r.event_type is not None
            assert r.timestamp is not None
            assert r.event_date is not None

        # Verify numeric types
        evt_001 = [r for r in clean_rows if r.event_id == "evt-001"][0]
        assert isinstance(evt_001.amount, float) or evt_001.amount is None
        assert isinstance(evt_001.status_code, int) or evt_001.status_code is None

        # error event should have error_code as int
        evt_003 = [r for r in clean_rows if r.event_id == "evt-003"][0]
        assert evt_003.error_code == 500
        assert evt_003.response_time_ms == 4500

        pipeline.stop()

    @pytest.mark.xfail(
        os.name == "nt",
        reason="Delta Lake Hadoop NativeIO unsupported on Windows. Passes on Linux.",
    )
    def test_batch_mode_writes_to_delta(self, spark_delta, tmp_path):
        """Batch mode should write clean records to Delta and dead-letter."""
        bronze_path = _file_uri(str(tmp_path / "bronze"))
        dead_path = _file_uri(str(tmp_path / "dead_letter"))
        checkpoint_path = _file_uri(str(tmp_path / "checkpoint"))
        input_file = tmp_path / "input.jsonl"

        events = [
            {
                "event_id": "evt-001",
                "event_type": "page_view",
                "user_id": "user-00001",
                "session_id": "sess-abc",
                "timestamp": "2026-05-29T10:00:00+00:00",
                "page_url": "/",
                "referrer_url": None,
                "user_agent": "Mozilla/5.0",
                "device_type": "desktop",
                "browser": "Chrome",
                "os": "Windows",
                "country": "US",
                "city": "NYC",
                "ip_address": "1.2.3.4",
                "amount": "19.99",
                "currency": "USD",
                "product_id": "prod-001",
                "category": "electronics",
                "error_code": None,
                "response_time_ms": "100",
                "status_code": "200",
            },
            {
                "event_id": None,  # malformed
                "event_type": "click",
                "user_id": "user-00002",
                "session_id": "sess-def",
                "timestamp": "2026-05-29T10:01:00+00:00",
                "page_url": "/products",
                "referrer_url": None,
                "user_agent": "Mozilla/5.0",
                "device_type": "mobile",
                "browser": "Safari",
                "os": "iOS",
                "country": "IN",
                "city": "Mumbai",
                "ip_address": "5.6.7.8",
                "amount": None,
                "currency": None,
                "product_id": None,
                "category": None,
                "error_code": None,
                "response_time_ms": "50",
                "status_code": "200",
            },
        ]

        input_file.write_text("\n".join(json.dumps(e) for e in events))

        pipeline = BronzePipeline(
            config={
                "kafka_bootstrap_servers": "localhost:9092",
                "bronze_path": bronze_path,
                "dead_letter_path": dead_path,
                "checkpoint_path": checkpoint_path,
            },
            spark=spark_delta,
        )

        result = pipeline.run_batch(str(input_file))

        assert result["clean"] == 1
        assert result["dead_letter"] == 1

        # Verify Delta tables have data
        bronze_df = spark_delta.read.format("delta").load(bronze_path)
        assert bronze_df.count() == 1
        assert bronze_df.select("event_id").collect()[0][0] == "evt-001"
        assert bronze_df.select("event_date").collect()[0][0] == "2026-05-29"

        dead_df = spark_delta.read.format("delta").load(dead_path)
        assert dead_df.count() == 1
        assert dead_df.select("event_id").collect()[0][0] is None

        pipeline.stop()

    def test_pipeline_config_defaults(self, spark):
        """Default config should have all required keys."""
        pipeline = BronzePipeline(
            config={
                "kafka_bootstrap_servers": "localhost:9092",
                "bronze_path": "/tmp/test_bronze",
                "checkpoint_path": "/tmp/test_checkpoint",
            },
            spark=spark,
        )
        assert "kafka_bootstrap_servers" in pipeline.config
        assert "kafka_topic" in pipeline.config
        assert "bronze_path" in pipeline.config
        assert "dead_letter_path" in pipeline.config
        assert "checkpoint_path" in pipeline.config
        assert "trigger_interval" in pipeline.config
        assert "query_name" in pipeline.config

        assert pipeline.config["kafka_topic"] == "raw_events"
        assert pipeline.config["query_name"] == "bronze_streaming"
        pipeline.stop()

    def test_parse_events_creates_event_date(self, spark):
        """Parsing should add event_date partition column."""
        pipeline = BronzePipeline(
            config={
                "kafka_bootstrap_servers": "localhost:9092",
                "bronze_path": "/tmp/test_bronze/test_event_date",
                "checkpoint_path": "/tmp/test_checkpoint",
            },
            spark=spark,
        )

        data = [
            Row(
                raw_value=json.dumps(
                    {
                        "event_id": "evt-001",
                        "event_type": "page_view",
                        "user_id": "user-00001",
                        "session_id": "sess-abc",
                        "timestamp": "2026-05-29T10:00:00+00:00",
                        "page_url": "/",
                        "referrer_url": None,
                        "user_agent": "Mozilla/5.0",
                        "device_type": "desktop",
                        "browser": "Chrome",
                        "os": "Windows",
                        "country": "US",
                        "city": "NYC",
                        "ip_address": "1.2.3.4",
                        "amount": None,
                        "currency": None,
                        "product_id": None,
                        "category": None,
                        "error_code": None,
                        "response_time_ms": "100",
                        "status_code": "200",
                    }
                ),
                kafka_timestamp=datetime.now(timezone.utc),
            )
        ]

        raw_df = spark.createDataFrame(data)
        parsed = pipeline.parse_events(raw_df)
        clean, _ = _separate_bad_records(parsed)

        row = clean.collect()[0]
        assert row.event_date == "2026-05-29"
        assert row.event_type == "page_view"
        pipeline.stop()
