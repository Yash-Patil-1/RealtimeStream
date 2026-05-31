"""
Tests for the Silver transformation layer (silver_streaming.py).

Tests cover:
  - Schema definitions match expected field types
  - Traffic source parsing from referrer URLs
  - Quality scoring and flag computation
  - Deduplication by event_id
  - Enrichment (hour_of_day, day_of_week, is_purchase, is_error, traffic_source, event_number)
  - Anomaly detection (error rate spikes, slow response times)
  - Alert generation from anomalous events
  - Batch mode run (non-Delta helper tests + Delta integration)
  - Config defaults and pipeline state management
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Ensure src/ is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.silver_streaming import (
    SILVER_EVENT_SCHEMA,
    SilverPipeline,
    AnomalyDetector,
    _parse_traffic_source,
    _compute_quality_score,
    _deduplicate_events,
    _enrich_events,
)
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
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
    """Convert a local absolute path to a file:/// URI for Spark on Windows."""
    return "file:///" + path.replace("\\", "/").lstrip("/")


def _make_event_row(**overrides) -> dict:
    """Create a sample event dict with defaults, overridable."""
    base = {
        "event_id": "evt-001",
        "event_type": "page_view",
        "user_id": "user-00001",
        "session_id": "sess-abc123",
        "timestamp": datetime(2026, 5, 29, 10, 0, 0),
        "page_url": "/products",
        "referrer_url": "https://google.com/search?q=shoes",
        "user_agent": "Mozilla/5.0 Chrome/120",
        "device_type": "desktop",
        "browser": "Chrome",
        "os": "Windows",
        "country": "US",
        "city": "New York",
        "ip_address": "192.168.1.1",
        "amount": 49.99,
        "currency": "USD",
        "product_id": "prod-001",
        "category": "electronics",
        "error_code": None,
        "response_time_ms": 150,
        "status_code": 200,
        "event_date": "2026-05-29",
    }
    base.update(overrides)
    return base


# ─── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for tests."""
    _setup_windows_env()

    builder = (
        SparkSession.builder.appName("test_silver")
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

    # Add src/ to PySpark worker Python path so UDFs can import src.config
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    builder = builder.config("spark.executorEnv.PYTHONPATH", project_dir)

    if DELTA_AVAILABLE:
        delta_jars = [DELTA_JARS, DELTA_STORAGE]
        jar_uris = ",".join(
            "file:///" + j.replace("\\", "/").lstrip("/")
            for j in delta_jars
            if os.path.isfile(j)
        )
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
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
    """Delta-enabled Spark session for integration tests."""
    if not DELTA_AVAILABLE:
        pytest.skip(f"Delta JAR not found at {DELTA_JARS}")
    return spark


@pytest.fixture
def sample_bronze_df(spark):
    """Create a DataFrame structured like Bronze clean events (before Silver enrichment)."""
    rows = [
        Row(**_make_event_row(event_id="evt-001", session_id="sess-abc", event_type="page_view",
                              timestamp=datetime(2026, 5, 29, 10, 0, 0), response_time_ms=100)),
        Row(**_make_event_row(event_id="evt-002", session_id="sess-abc", event_type="click",
                              timestamp=datetime(2026, 5, 29, 10, 0, 30), response_time_ms=50)),
        Row(**_make_event_row(event_id="evt-003", session_id="sess-abc", event_type="purchase",
                              timestamp=datetime(2026, 5, 29, 10, 1, 0), amount=129.99, response_time_ms=2000)),
        Row(**_make_event_row(event_id="evt-004", session_id="sess-def", event_type="error",
                              timestamp=datetime(2026, 5, 29, 10, 2, 0), error_code=500,
                              status_code=500, response_time_ms=5000)),
        Row(**_make_event_row(event_id="evt-005", session_id="sess-def", event_type="page_view",
                              timestamp=datetime(2026, 5, 29, 10, 3, 0), referrer_url="https://facebook.com/post",
                              device_type="mobile", browser="Safari", os="iOS", country="IN", city="Mumbai")),
    ]
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("page_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("error_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("event_date", StringType(), False),
    ])
    return spark.createDataFrame(rows, schema)


# ─── Schema Tests ─────────────────────────────────────────────────────


class TestSilverSchema:
    def test_silver_schema_has_enriched_fields(self):
        """SILVER_EVENT_SCHEMA should include all Silver enrichment fields."""
        field_map = {f.name: f.dataType for f in SILVER_EVENT_SCHEMA.fields}

        # Bronze standard fields
        assert field_map["event_id"] == StringType()
        assert field_map["timestamp"] == TimestampType()

        # Enrichment fields
        assert field_map["hour_of_day"] == IntegerType()
        assert field_map["day_of_week"] == IntegerType()
        assert field_map["is_purchase_event"] == BooleanType()
        assert field_map["is_error_event"] == BooleanType()
        assert field_map["traffic_source"] == StringType()
        assert field_map["event_number_in_session"] == IntegerType()

        # Quality fields
        assert field_map["quality_score"] == DoubleType()
        assert field_map["quality_flags"] == StringType()

        # Anomaly fields
        assert field_map["is_anomaly"] == BooleanType()
        assert field_map["anomaly_type"] == StringType()
        assert field_map["anomaly_score"] == DoubleType()

        # Metadata
        assert field_map["processed_at"] == TimestampType()
        assert field_map["silver_version"] == StringType()

    def test_silver_schema_field_count(self):
        """SILVER_EVENT_SCHEMA should have the expected number of fields."""
        # 22 bronze fields + 6 enrichment + 2 quality + 3 anomaly + 2 metadata = 35
        expected_count = 22 + 6 + 2 + 3 + 2
        assert len(SILVER_EVENT_SCHEMA.fields) == expected_count


# ─── Traffic Source Tests ─────────────────────────────────────────────


class TestTrafficSource:
    def test_google_referrer_is_organic(self):
        """Google referrer should be classified as 'organic'."""
        assert _parse_traffic_source("https://google.com/search?q=shoes") == "organic"
        assert _parse_traffic_source("http://www.google.co.uk/") == "organic"

    def test_facebook_referrer_is_social(self):
        """Facebook referrer should be classified as 'social'."""
        assert _parse_traffic_source("https://facebook.com/post/123") == "social"
        assert _parse_traffic_source("https://www.facebook.com/groups") == "social"

    def test_twitter_referrer_is_social(self):
        """Twitter referrer should be classified as 'social'."""
        assert _parse_traffic_source("https://twitter.com/user/status/456") == "social"

    def test_direct_traffic_when_no_referrer(self):
        """None or empty referrer should be classified as 'direct'."""
        assert _parse_traffic_source(None) == "direct"
        assert _parse_traffic_source("") == "direct"
        assert _parse_traffic_source("/") == "direct"
        assert _parse_traffic_source("None") == "direct"

    def test_unknown_referrer_is_referral(self):
        """Unknown referrer domains should be classified as 'referral'."""
        assert _parse_traffic_source("https://someblog.com/article") == "referral"
        assert _parse_traffic_source("https://partner-site.net/deals") == "referral"

    def test_email_referrer_is_email(self):
        """Email/newsletter referrers should be classified as 'email'."""
        assert _parse_traffic_source("https://email.example.com/newsletter") == "email"
        assert _parse_traffic_source("https://newsletter.company.com/issue/42") == "email"


# ─── Quality Score Tests ──────────────────────────────────────────────


class TestQualityScore:
    def test_perfect_record_scores_1_0(self):
        """A record with all fields valid should score 1.0 with no flags."""
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score == 1.0
        assert flags == []

    def test_missing_critical_field_reduces_score(self):
        """Missing critical fields should reduce the quality score."""
        row = {
            "event_id": None,
            "event_type": "page_view",
            "user_id": None,
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score < 1.0
        # 2 missing fields × 0.15 = 0.30 reduction
        assert score == pytest.approx(0.7)
        assert any("missing" in f for f in flags)

    def test_invalid_event_type_reduces_score(self):
        """An unknown event_type should reduce the quality score."""
        row = {
            "event_id": "evt-001",
            "event_type": "unknown_type_xyz",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score == pytest.approx(0.8)  # 1.0 - 0.2 for invalid event_type
        assert any("invalid_event_type" in f for f in flags)

    def test_invalid_device_reduces_score(self):
        """An unknown device_type should reduce the quality score."""
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "smart_tv",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score == pytest.approx(0.9)  # 1.0 - 0.1 for invalid device
        assert any("invalid_device" in f for f in flags)

    def test_future_timestamp_reduces_score(self):
        """A timestamp in the future should reduce quality score."""
        future_ts = datetime.now(timezone.utc) + timedelta(hours=2)
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": future_ts,
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score < 1.0
        assert any("future_timestamp" in f for f in flags)

    def test_stale_timestamp_reduces_score(self):
        """A timestamp older than 7 days should reduce quality score."""
        old_ts = datetime.now(timezone.utc) - timedelta(days=10)
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": old_ts,
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score < 1.0
        assert any("stale_timestamp" in f for f in flags)

    def test_high_response_time_reduces_score(self):
        """Response time > max (30s) should reduce quality score."""
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "user-00001",
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "desktop",
            "response_time_ms": 60000,
        }
        score, flags = _compute_quality_score(row)
        assert score == pytest.approx(0.95)  # 1.0 - 0.05
        assert any("high_response_time" in f for f in flags)

    def test_score_clamps_to_zero(self):
        """Quality score should not go below 0.0."""
        # Future timestamp that's also stale (> 7 days ahead = both future AND stale)
        future_ts = datetime.now(timezone.utc) + timedelta(days=20)
        row = {
            "event_id": None,
            "event_type": "invalid_type_xyz",
            "user_id": None,
            "session_id": None,
            "timestamp": future_ts,
            "device_type": "unknown_device",
            "response_time_ms": 60000,
        }
        score, flags = _compute_quality_score(row)
        assert score == 0.0

    def test_empty_string_triggers_missing_flag(self):
        """Empty string fields should be treated as missing."""
        row = {
            "event_id": "evt-001",
            "event_type": "page_view",
            "user_id": "",
            "session_id": "sess-abc",
            "timestamp": datetime(2026, 5, 29, 10, 0, 0),
            "device_type": "desktop",
            "response_time_ms": 150,
        }
        score, flags = _compute_quality_score(row)
        assert score < 1.0
        assert any("missing:user_id" in f for f in flags)


# ─── Deduplication Tests ──────────────────────────────────────────────


class TestDeduplication:
    def test_no_duplicates_preserves_all(self, spark):
        """If no duplicates, all records should be preserved."""
        data = [
            Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 0, 0)),
            Row(event_id="evt-002", timestamp=datetime(2026, 5, 29, 10, 1, 0)),
            Row(event_id="evt-003", timestamp=datetime(2026, 5, 29, 10, 2, 0)),
        ]
        df = spark.createDataFrame(data)
        result = _deduplicate_events(df)
        assert result.count() == 3

    def test_duplicate_event_ids_deduplicated(self, spark):
        """Duplicate event_ids should be reduced to one (earliest timestamp)."""
        data = [
            Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 0, 0)),
            Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 1, 0)),  # duplicate
            Row(event_id="evt-002", timestamp=datetime(2026, 5, 29, 10, 2, 0)),
        ]
        df = spark.createDataFrame(data)
        result = _deduplicate_events(df)
        assert result.count() == 2
        ids = [r.event_id for r in result.collect()]
        assert ids.count("evt-001") == 1

    def test_dedup_keeps_earliest_timestamp(self, spark):
        """When deduplicating, the record with the earliest timestamp should be kept."""
        data = [
            Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 5, 0)),
            Row(event_id="evt-001", timestamp=datetime(2026, 5, 29, 10, 0, 0)),  # earlier
        ]
        df = spark.createDataFrame(data)
        result = _deduplicate_events(df)
        row = result.collect()[0]
        assert row.timestamp == datetime(2026, 5, 29, 10, 0, 0)


# ─── Enrichment Tests ─────────────────────────────────────────────────


class TestEnrichment:
    def test_enrich_adds_hour_of_day(self, spark, sample_bronze_df):
        """hour_of_day should be derived from timestamp (0-23)."""
        enriched = _enrich_events(sample_bronze_df)
        rows = enriched.collect()
        for r in rows:
            assert 0 <= r.hour_of_day <= 23
        # Our timestamp is 10:00, so hour_of_day should be 10
        assert rows[0].hour_of_day == 10

    def test_enrich_adds_day_of_week(self, spark, sample_bronze_df):
        """day_of_week should be 1-7 (Sunday=1)."""
        enriched = _enrich_events(sample_bronze_df)
        rows = enriched.collect()
        for r in rows:
            assert 1 <= r.day_of_week <= 7

    def test_enrich_marks_purchase_events(self, spark, sample_bronze_df):
        """is_purchase_event should be True for purchase events, False otherwise."""
        enriched = _enrich_events(sample_bronze_df)
        rows = {r.event_id: r for r in enriched.collect()}
        assert rows["evt-003"].is_purchase_event is True  # purchase
        assert rows["evt-001"].is_purchase_event is False  # page_view
        assert rows["evt-004"].is_purchase_event is False  # error

    def test_enrich_marks_error_events(self, spark, sample_bronze_df):
        """is_error_event should be True for error events, False otherwise."""
        enriched = _enrich_events(sample_bronze_df)
        rows = {r.event_id: r for r in enriched.collect()}
        assert rows["evt-004"].is_error_event is True  # error
        assert rows["evt-001"].is_error_event is False  # page_view

    def test_enrich_parses_traffic_source(self, spark, sample_bronze_df):
        """traffic_source should be parsed from referrer_url."""
        enriched = _enrich_events(sample_bronze_df)
        rows = {r.event_id: r for r in enriched.collect()}
        # Google -> organic
        assert rows["evt-001"].traffic_source == "organic"
        # Facebook -> social
        assert rows["evt-005"].traffic_source == "social"

    def test_enrich_computes_event_number_in_session(self, spark, sample_bronze_df):
        """event_number_in_session should be sequential by session_id + timestamp."""
        enriched = _enrich_events(sample_bronze_df)
        rows = {r.event_id: r for r in enriched.collect()}
        # sess-abc: evt-001 (10:00), evt-002 (10:00:30), evt-003 (10:01)
        assert rows["evt-001"].event_number_in_session == 1
        assert rows["evt-002"].event_number_in_session == 2
        assert rows["evt-003"].event_number_in_session == 3
        # sess-def: evt-004 (10:02), evt-005 (10:03)
        assert rows["evt-004"].event_number_in_session == 1
        assert rows["evt-005"].event_number_in_session == 2


# ─── Anomaly Detection Tests ──────────────────────────────────────────


class TestAnomalyDetection:
    def test_detect_no_anomalies_with_few_samples(self, spark):
        """Fewer than min_samples should result in no anomalies."""
        rows = [
            Row(event_id=f"evt-{i:03d}", event_type="page_view",
                response_time_ms=100, error_code=None, timestamp=datetime(2026, 5, 29, 10, i, 0))
            for i in range(5)  # 5 < min_samples (10)
        ]
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector({"min_samples": 10})
        result = detector.detect_anomalies(df)

        # With < min_samples, none should be anomalous
        anomalies = result.filter(col("is_anomaly")).collect()
        assert len(anomalies) == 0

    def test_error_rate_spike_triggers_anomaly(self, spark):
        """Error rate > threshold (5%) should flag error events as anomalous."""
        rows = []
        for i in range(19):
            rows.append(Row(event_id=f"evt-page-{i:03d}", event_type="page_view",
                            response_time_ms=100, error_code=None,
                            timestamp=datetime(2026, 5, 29, 10, i, 0)))
        # 1 error = 5% of 20 — at the threshold, but need > threshold
        rows.append(Row(event_id="evt-error-001", event_type="error",
                        response_time_ms=100, error_code=500,
                        timestamp=datetime(2026, 5, 29, 10, 20, 0)))
        # Add 2 more errors to push rate above 5% (3/22 ≈ 13.6%)
        rows.append(Row(event_id="evt-error-002", event_type="error",
                        response_time_ms=200, error_code=500,
                        timestamp=datetime(2026, 5, 29, 10, 21, 0)))
        rows.append(Row(event_id="evt-error-003", event_type="error",
                        response_time_ms=150, error_code=500,
                        timestamp=datetime(2026, 5, 29, 10, 22, 0)))
        # total: 22 events, 3 errors → 13.6% > 5%

        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector({"min_samples": 5, "error_rate_threshold": 0.05})
        result = detector.detect_anomalies(df)

        anomalies = result.filter(col("is_anomaly")).collect()
        assert len(anomalies) == 3  # all 3 error events should be flagged
        for a in anomalies:
            assert a.anomaly_type == "error_rate_spike"
            assert a.anomaly_score is not None

    def test_slow_response_time_triggers_anomaly(self, spark):
        """Response time z-score > threshold should flag as anomalous."""
        rows = []
        # Normal response times around 100ms
        for i in range(19):
            rows.append(Row(event_id=f"evt-{i:03d}", event_type="page_view",
                            response_time_ms=100, error_code=None,
                            timestamp=datetime(2026, 5, 29, 10, i, 0)))
        # One very slow response
        rows.append(Row(event_id="evt-slow-001", event_type="page_view",
                        response_time_ms=10000, error_code=None,
                        timestamp=datetime(2026, 5, 29, 10, 20, 0)))

        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector({"min_samples": 5})
        result = detector.detect_anomalies(df)

        anomalies = result.filter(col("is_anomaly")).collect()
        anomaly_ids = [a.event_id for a in anomalies]
        assert "evt-slow-001" in anomaly_ids
        for a in anomalies:
            if a.event_id == "evt-slow-001":
                assert a.anomaly_type == "slow_response"

    def test_generate_alerts_returns_alerts(self, spark):
        """Anomalous events should generate alert messages."""
        rows = [
            Row(event_id="evt-err-001", event_type="error",
                response_time_ms=100, error_code=500,
                timestamp=datetime(2026, 5, 29, 10, 0, 0),
                is_anomaly=True, anomaly_type="error_rate_spike", anomaly_score=0.8,
                quality_score=0.9, quality_flags=None,
                hour_of_day=10, day_of_week=6, is_purchase_event=False,
                is_error_event=True, traffic_source="direct",
                event_number_in_session=1,
                processed_at=datetime(2026, 5, 29, 10, 1, 0),
                silver_version="1.0.0"),
        ]
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("anomaly_type", StringType(), True),
            StructField("anomaly_score", DoubleType(), True),
            StructField("quality_score", DoubleType(), True),
            StructField("quality_flags", StringType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("is_purchase_event", BooleanType(), True),
            StructField("is_error_event", BooleanType(), True),
            StructField("traffic_source", StringType(), True),
            StructField("event_number_in_session", IntegerType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("silver_version", StringType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector()
        alerts = detector.generate_alerts(df)

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert["event_id"] == "evt-err-001"
        assert alert["anomaly_type"] == "error_rate_spike"
        assert alert["anomaly_score"] == 0.8
        assert "alert_id" in alert
        assert alert["alert_id"].startswith("alert-")
        assert "detected_at" in alert


# ─── AnomalyDetector Edge Cases ───────────────────────────────────────


class TestAnomalyDetectorEdgeCases:
    def test_no_response_time_data(self, spark):
        """Events without response_time_ms should not trigger slow_response anomalies."""
        rows = [
            Row(event_id=f"evt-{i:03d}", event_type="page_view",
                response_time_ms=None, error_code=None,
                timestamp=datetime(2026, 5, 29, 10, i, 0))
            for i in range(15)
        ]
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector({"min_samples": 5})
        result = detector.detect_anomalies(df)

        # All null response times, no errors -> no anomalies
        anomalies = result.filter(col("is_anomaly")).collect()
        assert len(anomalies) == 0

    def test_all_identical_response_times(self, spark):
        """When all response times are identical, stddev=0 should not cause division errors."""
        rows = [
            Row(event_id=f"evt-{i:03d}", event_type="page_view",
                response_time_ms=100, error_code=None,
                timestamp=datetime(2026, 5, 29, 10, i, 0))
            for i in range(15)
        ]
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema)
        detector = AnomalyDetector({"min_samples": 5})
        result = detector.detect_anomalies(df)

        # Should not crash — all values identical, stddev=0, division handled by guard
        anomalies = result.filter(col("is_anomaly")).collect()
        # No errors and no response time outliers -> no anomalies
        assert len(anomalies) == 0


# ─── Pipeline Config Tests ────────────────────────────────────────────


class TestSilverPipelineConfig:
    def test_default_config_has_all_keys(self, spark):
        """Default config should have all required keys."""
        pipeline = SilverPipeline(
            config={"bronze_path": "/tmp/test_silver", "silver_path": "/tmp/test_silver_out"},
            spark=spark,
        )
        assert "bronze_path" in pipeline.config
        assert "silver_path" in pipeline.config
        assert "quarantine_path" in pipeline.config
        assert "anomaly_topic" in pipeline.config
        assert pipeline.config["anomaly_topic"] == "anomaly_alerts"
        assert not pipeline._owns_spark  # externally-provided session
        pipeline.stop()

    def test_pipeline_uses_provided_spark(self, spark):
        """Pipeline should use an externally-provided Spark session without creating its own."""
        pipeline = SilverPipeline(
            config={"bronze_path": "/tmp/test_silver", "silver_path": "/tmp/test_silver_out"},
            spark=spark,
        )
        assert pipeline.spark is spark
        assert pipeline._owns_spark is False
        pipeline.stop()

    def test_pipeline_has_anomaly_detector(self, spark):
        """Pipeline should initialize an AnomalyDetector by default."""
        pipeline = SilverPipeline(
            config={"bronze_path": "/tmp/test_silver", "silver_path": "/tmp/test_silver_out"},
            spark=spark,
        )
        assert pipeline.anomaly_detector is not None
        assert isinstance(pipeline.anomaly_detector, AnomalyDetector)
        pipeline.stop()


# ─── pipeline.stop() Ownership Tests ──────────────────────────────────


class TestPipelineStop:
    def test_stop_does_not_kill_external_session(self, spark):
        """stop() should not stop an externally-provided Spark session."""
        pipeline = SilverPipeline(
            config={"bronze_path": "/tmp/test_stop", "silver_path": "/tmp/test_stop_out"},
            spark=spark,
        )
        pipeline.stop()
        # Session should still be alive
        assert spark.version is not None


# ─── End-to-End (Non-Delta) Tests ─────────────────────────────────────


class TestSilverPipelineEndToEnd:
    @pytest.mark.xfail(
        os.name == "nt",
        reason="Delta Lake Hadoop NativeIO unsupported on Windows. Passes on Linux.",
    )
    def test_run_batch_with_empty_bronze_returns_zero_stats(self, spark_delta, tmp_path):
        """When Bronze has no data, run_batch should return zero stats."""
        bronze_path = _file_uri(str(tmp_path / "bronze"))
        silver_path = _file_uri(str(tmp_path / "silver"))

        # Create an empty Bronze Delta table
        from src.bronze_streaming import CLEAN_EVENT_SCHEMA
        empty_df = spark_delta.createDataFrame([], CLEAN_EVENT_SCHEMA)
        empty_df.write.format("delta").mode("overwrite").save(bronze_path)

        pipeline = SilverPipeline(
            config={"bronze_path": bronze_path, "silver_path": silver_path},
            spark=spark_delta,
        )
        stats = pipeline.run_batch()
        assert stats["bronze_read"] == 0
        assert stats["silver_written"] == 0
        pipeline.stop()

    def test_dedup_via_enrich_gives_correct_results(self, spark, sample_bronze_df):
        """Applying dedup + enrich should produce correct column values."""
        deduped = _deduplicate_events(sample_bronze_df)
        enriched = _enrich_events(deduped)
        rows = {r.event_id: r for r in enriched.collect()}

        # Check a purchase event has correct flags
        purchase = rows.get("evt-003")
        assert purchase is not None
        assert purchase.is_purchase_event is True
        assert purchase.is_error_event is False

        # Check an error event
        error = rows.get("evt-004")
        assert error is not None
        assert error.is_purchase_event is False
        assert error.is_error_event is True

    def test_quality_flow_on_dataframe(self, spark):
        """Quality checks should produce correct quality_score and quality_flags columns."""
        rows = [
            Row(**_make_event_row(event_id="evt-001", event_type="page_view",
                                  user_id="user-01", session_id="sess-abc",
                                  device_type="desktop", response_time_ms=100,
                                  timestamp=datetime(2026, 5, 29, 10, 0, 0))),
            Row(**_make_event_row(event_id="evt-002", event_type="unknown_event",
                                  user_id=None, session_id="sess-abc",
                                  device_type="unknown", response_time_ms=99999,
                                  timestamp=datetime(2026, 5, 29, 10, 1, 0))),
        ]
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("page_url", StringType(), True),
            StructField("referrer_url", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("os", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("error_code", IntegerType(), True),
            StructField("response_time_ms", IntegerType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("event_date", StringType(), False),
        ])
        df = spark.createDataFrame(rows, schema)

        from src.silver_streaming import _apply_quality_checks
        passed_df, quarantined_df = _apply_quality_checks(df)

        passed_rows = {r.event_id: r for r in passed_df.collect()}
        quarantined_rows = {r.event_id: r for r in quarantined_df.collect()}

        # evt-001 is perfect -> passed
        assert "evt-001" in passed_rows
        assert passed_rows["evt-001"].quality_score == 1.0

        # evt-002 has multiple issues -> should be quarantined (score < 0.5)
        assert "evt-002" in quarantined_rows or "evt-002" not in passed_rows
        if "evt-002" in quarantined_rows:
            assert quarantined_rows["evt-002"].quality_score < 0.5
        else:
            # It might still pass if score >= 0.5, but should have flags
            pass
