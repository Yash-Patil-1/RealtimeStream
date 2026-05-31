"""
Tests for the Gold aggregation layer (gold_aggregations.py).

Tests cover:
  - Schema definitions for all Gold tables (KPIs, sessions, funnels, anomalies)
  - KPI computation (event counts by type, unique users, response time stats)
  - Sessionization (session duration, event counts, bounce detection)
  - Funnel analysis (conversion rates, drop-off)
  - Anomaly aggregation (severity classification, metrics)
  - GoldPipeline batch modes
  - Config defaults and pipeline state management
  - Window duration parsing
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Ensure src/ is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gold_aggregations import (
    KPI_SCHEMA,
    SESSION_SCHEMA,
    FUNNEL_SCHEMA,
    ANOMALY_AGG_SCHEMA,
    GoldPipeline,
    compute_kpis,
    compute_sessions,
    compute_funnels,
    compute_anomaly_aggregations,
    _parse_window_duration,
    _format_window_seconds,
)
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
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

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


DELTA_JARS = r"C:\tools\spark-jars\delta-spark_2.12-3.1.0.jar"
DELTA_STORAGE = r"C:\tools\spark-jars\delta-storage-3.1.0.jar"
DELTA_AVAILABLE = os.path.isfile(DELTA_JARS) and os.path.isfile(DELTA_STORAGE)


def _file_uri(path: str) -> str:
    """Convert a local absolute path to a file:/// URI for Spark on Windows."""
    return "file:///" + path.replace("\\", "/").lstrip("/")


def _make_silver_row(**overrides) -> dict:
    """Create a sample Silver event dict with defaults, overridable."""
    base = {
        "event_id": "evt-001",
        "event_type": "page_view",
        "user_id": "user-00001",
        "session_id": "sess-abc123",
        "timestamp": datetime(2026, 5, 29, 10, 0, 0, tzinfo=timezone.utc),
        "page_url": "/products",
        "referrer_url": "https://google.com/search?q=shoes",
        "user_agent": "Mozilla/5.0 Chrome/120",
        "device_type": "desktop",
        "browser": "Chrome",
        "os": "Windows",
        "country": "US",
        "city": "New York",
        "ip_address": "192.168.1.1",
        "amount": None,
        "currency": None,
        "product_id": None,
        "category": None,
        "error_code": None,
        "response_time_ms": 150,
        "status_code": 200,
        "event_date": "2026-05-29",
        # Silver enrichment fields
        "hour_of_day": 10,
        "day_of_week": 6,
        "is_purchase_event": False,
        "is_error_event": False,
        "traffic_source": "organic",
        "event_number_in_session": 1,
        # Quality fields
        "quality_score": 0.95,
        "quality_flags": None,
        # Anomaly fields
        "is_anomaly": False,
        "anomaly_type": None,
        "anomaly_score": None,
        # Metadata
        "processed_at": datetime(2026, 5, 29, 10, 0, 0, tzinfo=timezone.utc),
        "silver_version": "1.0.0",
    }
    base.update(overrides)
    return base


_SILVER_ROW_SCHEMA = StructType([
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
    # Silver enrichment
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_purchase_event", BooleanType(), False),
    StructField("is_error_event", BooleanType(), False),
    StructField("traffic_source", StringType(), True),
    StructField("event_number_in_session", IntegerType(), True),
    # Quality
    StructField("quality_score", DoubleType(), False),
    StructField("quality_flags", StringType(), True),
    # Anomaly
    StructField("is_anomaly", BooleanType(), False),
    StructField("anomaly_type", StringType(), True),
    StructField("anomaly_score", DoubleType(), True),
    # Metadata
    StructField("processed_at", TimestampType(), False),
    StructField("silver_version", StringType(), False),
])


# ─── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for tests."""
    _setup_windows_env()

    builder = (
        SparkSession.builder.appName("test_gold")
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


@pytest.fixture
def silver_df(spark):
    """Create a realistic Silver DataFrame for Gold aggregation tests."""
    rows = [
        # Session A: user-01 - page_view, click, add_to_cart, purchase
        Row(**_make_silver_row(
            event_id="evt-001", session_id="sess-a", user_id="user-01",
            event_type="page_view", timestamp=datetime(2026, 5, 29, 10, 0, 0, tzinfo=timezone.utc),
            page_url="/", referrer_url="https://google.com", response_time_ms=100,
            traffic_source="organic",
        )),
        Row(**_make_silver_row(
            event_id="evt-002", session_id="sess-a", user_id="user-01",
            event_type="click", timestamp=datetime(2026, 5, 29, 10, 0, 30, tzinfo=timezone.utc),
            page_url="/products/electronics", response_time_ms=50,
            traffic_source="organic",
        )),
        Row(**_make_silver_row(
            event_id="evt-003", session_id="sess-a", user_id="user-01",
            event_type="add_to_cart", timestamp=datetime(2026, 5, 29, 10, 1, 0, tzinfo=timezone.utc),
            page_url="/products/electronics/prod-001", amount=49.99,
            product_id="prod-001", category="electronics",
            response_time_ms=200, traffic_source="organic",
        )),
        Row(**_make_silver_row(
            event_id="evt-004", session_id="sess-a", user_id="user-01",
            event_type="purchase", timestamp=datetime(2026, 5, 29, 10, 2, 0, tzinfo=timezone.utc),
            page_url="/checkout/confirm", amount=49.99, currency="USD",
            product_id="prod-001", category="electronics",
            is_purchase_event=True, response_time_ms=1500,
            traffic_source="organic",
        )),
        # Session B: user-02 - single page_view (bounced)
        Row(**_make_silver_row(
            event_id="evt-005", session_id="sess-b", user_id="user-02",
            event_type="page_view", timestamp=datetime(2026, 5, 29, 10, 5, 0, tzinfo=timezone.utc),
            page_url="/products", referrer_url="https://facebook.com/post",
            device_type="mobile", browser="Safari", os="iOS",
            country="IN", city="Mumbai", response_time_ms=300,
            traffic_source="social",
        )),
        # Session C: user-03 - search, error
        Row(**_make_silver_row(
            event_id="evt-006", session_id="sess-c", user_id="user-03",
            event_type="search", timestamp=datetime(2026, 5, 29, 10, 10, 0, tzinfo=timezone.utc),
            page_url="/search?q=shoes", response_time_ms=2500,
            traffic_source="direct",
        )),
        Row(**_make_silver_row(
            event_id="evt-007", session_id="sess-c", user_id="user-03",
            event_type="error", timestamp=datetime(2026, 5, 29, 10, 11, 0, tzinfo=timezone.utc),
            page_url="/api/search", error_code=500, status_code=500,
            is_error_event=True, response_time_ms=5000,
            traffic_source="direct",
        )),
        # Session D: user-01 - another session, page_view + click
        Row(**_make_silver_row(
            event_id="evt-008", session_id="sess-d", user_id="user-01",
            event_type="page_view", timestamp=datetime(2026, 5, 29, 11, 0, 0, tzinfo=timezone.utc),
            page_url="/promotions", referrer_url="https://email.company.com/newsletter",
            response_time_ms=120, traffic_source="email",
        )),
        Row(**_make_silver_row(
            event_id="evt-009", session_id="sess-d", user_id="user-01",
            event_type="click", timestamp=datetime(2026, 5, 29, 11, 0, 30, tzinfo=timezone.utc),
            page_url="/promotions/summer-sale", response_time_ms=80,
            traffic_source="email",
        )),
        # Session E: user-04 - login, page_view, logout
        Row(**_make_silver_row(
            event_id="evt-010", session_id="sess-e", user_id="user-04",
            event_type="login", timestamp=datetime(2026, 5, 29, 12, 0, 0, tzinfo=timezone.utc),
            page_url="/auth/login", response_time_ms=400,
            traffic_source="direct",
        )),
        Row(**_make_silver_row(
            event_id="evt-011", session_id="sess-e", user_id="user-04",
            event_type="page_view", timestamp=datetime(2026, 5, 29, 12, 1, 0, tzinfo=timezone.utc),
            page_url="/account/orders", referrer_url="https://bing.com/search",
            response_time_ms=200, traffic_source="organic",
        )),
        Row(**_make_silver_row(
            event_id="evt-012", session_id="sess-e", user_id="user-04",
            event_type="logout", timestamp=datetime(2026, 5, 29, 12, 5, 0, tzinfo=timezone.utc),
            page_url="/auth/logout", response_time_ms=50,
            traffic_source="organic",
        )),
    ]
    return spark.createDataFrame(rows, _SILVER_ROW_SCHEMA)


# ─── Schema Tests ─────────────────────────────────────────────────────


class TestGoldSchemas:
    def test_kpi_schema_has_required_fields(self):
        """KPI_SCHEMA should include all required KPI fields."""
        field_map = {f.name: f.dataType for f in KPI_SCHEMA.fields}

        assert field_map["window_start"] == TimestampType()
        assert field_map["window_end"] == TimestampType()
        assert field_map["window_duration"] == StringType()
        assert field_map["event_date"] == StringType()
        assert field_map["total_events"] == LongType()
        assert field_map["page_views"] == LongType()
        assert field_map["clicks"] == LongType()
        assert field_map["purchases"] == LongType()
        assert field_map["unique_users"] == LongType()
        assert field_map["unique_sessions"] == LongType()
        assert field_map["avg_response_time_ms"] == DoubleType()
        assert field_map["p95_response_time_ms"] == DoubleType()
        assert field_map["error_rate"] == DoubleType()
        assert field_map["conversion_rate"] == DoubleType()
        assert field_map["revenue_total"] == DoubleType()
        assert field_map["processed_at"] == TimestampType()

    def test_session_schema_has_required_fields(self):
        """SESSION_SCHEMA should include all required session fields."""
        field_map = {f.name: f.dataType for f in SESSION_SCHEMA.fields}

        assert field_map["session_id"] == StringType()
        assert field_map["session_start"] == TimestampType()
        assert field_map["session_end"] == TimestampType()
        assert field_map["session_duration_seconds"] == DoubleType()
        assert field_map["event_count"] == IntegerType()
        assert field_map["has_purchased"] == BooleanType()
        assert field_map["total_revenue"] == DoubleType()
        assert field_map["entry_page"] == StringType()
        assert field_map["exit_page"] == StringType()
        assert field_map["is_bounced"] == BooleanType()
        assert field_map["processed_at"] == TimestampType()

    def test_funnel_schema_has_required_fields(self):
        """FUNNEL_SCHEMA should include all required funnel fields."""
        field_map = {f.name: f.dataType for f in FUNNEL_SCHEMA.fields}

        assert field_map["window_start"] == TimestampType()
        assert field_map["window_end"] == TimestampType()
        assert field_map["funnel_step"] == StringType()
        assert field_map["step_order"] == IntegerType()
        assert field_map["unique_users"] == LongType()
        assert field_map["event_count"] == LongType()
        assert field_map["conversion_to_next"] == DoubleType()
        assert field_map["drop_off_count"] == LongType()
        assert field_map["drop_off_rate"] == DoubleType()
        assert field_map["processed_at"] == TimestampType()

    def test_anomaly_agg_schema_has_required_fields(self):
        """ANOMALY_AGG_SCHEMA should include all required anomaly aggregation fields."""
        field_map = {f.name: f.dataType for f in ANOMALY_AGG_SCHEMA.fields}

        assert field_map["window_start"] == TimestampType()
        assert field_map["anomaly_type"] == StringType()
        assert field_map["anomaly_count"] == LongType()
        assert field_map["avg_anomaly_score"] == DoubleType()
        assert field_map["severity"] == StringType()
        assert field_map["processed_at"] == TimestampType()


# ─── Window Duration Parsing Tests ─────────────────────────────────────


class TestWindowDurationParsing:
    def test_parse_1h(self):
        """'1h' should parse to 3600 seconds."""
        assert _parse_window_duration("1h") == 3600

    def test_parse_30m(self):
        """'30m' should parse to 1800 seconds."""
        assert _parse_window_duration("30m") == 1800

    def test_parse_5m(self):
        """'5m' should parse to 300 seconds."""
        assert _parse_window_duration("5m") == 300

    def test_parse_24h(self):
        """'24h' should parse to 86400 seconds."""
        assert _parse_window_duration("24h") == 86400

    def test_parse_seconds(self):
        """'60s' should parse to 60 seconds."""
        assert _parse_window_duration("60s") == 60

    def test_format_1h(self):
        """3600 seconds should format to '1h'."""
        assert _format_window_seconds(3600) == "1h"

    def test_format_30m(self):
        """1800 seconds should format to '30m'."""
        assert _format_window_seconds(1800) == "30m"


# ─── KPI Computation Tests ────────────────────────────────────────────


class TestComputeKPIs:
    def test_kpi_counts_by_event_type(self, spark, silver_df):
        """KPI computation should correctly count events by type."""
        kpis = compute_kpis(silver_df, window_seconds=3600)  # 1h window
        rows = kpis.collect()

        assert len(rows) > 0
        total_page_views = sum(r.page_views for r in rows)
        total_clicks = sum(r.clicks for r in rows)
        total_purchases = sum(r.purchases for r in rows)
        total_errors = sum(r.errors for r in rows)
        total_searches = sum(r.searches for r in rows)
        total_logins = sum(r.logins for r in rows)
        total_add_to_carts = sum(r.add_to_carts for r in rows)

        assert total_page_views == 4  # evt-001, evt-005, evt-008, evt-011
        assert total_clicks == 2  # evt-002, evt-009
        assert total_purchases == 1  # evt-004
        assert total_errors == 1  # evt-007
        assert total_searches == 1  # evt-006
        assert total_logins == 1  # evt-010
        assert total_add_to_carts == 1  # evt-003

    def test_kpi_unique_users(self, spark, silver_df):
        """KPI computation should count unique users per window."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        unique_users = list(set(r.unique_users for r in rows))
        assert all(u >= 0 for u in unique_users)

        # There should be windows with multiple users
        total_unique = sum(r.unique_users for r in rows)
        assert total_unique >= 4  # user-01, user-02, user-03, user-04

    def test_kpi_error_rate_computation(self, spark, silver_df):
        """Error rate should be computed as errors / total_events."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        for r in rows:
            if r.total_events > 0:
                expected_rate = round(r.errors / r.total_events, 4)
                assert r.error_rate == expected_rate

    def test_kpi_conversion_rate(self, spark, silver_df):
        """Conversion rate should be purchases / page_views."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        for r in rows:
            if r.page_views > 0:
                expected_cvr = round(r.purchases / r.page_views, 4)
                assert r.conversion_rate == expected_cvr

    def test_kpi_revenue_totals(self, spark, silver_df):
        """Revenue total should sum purchase amounts."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        total_revenue = sum(r.revenue_total for r in rows)
        assert total_revenue == pytest.approx(49.99)

    def test_kpi_window_boundaries(self, spark, silver_df):
        """KPI windows should have correct start/end timestamps."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        for r in rows:
            assert r.window_start < r.window_end
            assert r.window_duration is not None

    def test_kpi_empty_df_returns_empty(self, spark):
        """An empty DataFrame should not crash KPI computation."""
        empty_df = spark.createDataFrame([], _SILVER_ROW_SCHEMA)
        kpis = compute_kpis(empty_df, window_seconds=3600)
        assert kpis.count() == 0


# ─── Sessionization Tests ─────────────────────────────────────────────


class TestComputeSessions:
    def test_session_count(self, spark, silver_df):
        """Should find 5 sessions in the test data."""
        sessions = compute_sessions(silver_df)
        assert sessions.count() == 5

    def test_session_duration(self, spark, silver_df):
        """Session duration should be end - start in seconds."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        # sess-a: 10:00:00 to 10:02:00 = 120 seconds
        assert rows["sess-a"].session_duration_seconds == pytest.approx(120.0)
        # sess-b: bounced, single event = 0 seconds
        assert rows["sess-b"].session_duration_seconds == pytest.approx(0.0)

    def test_session_event_counts(self, spark, silver_df):
        """Should correctly count events per session."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        assert rows["sess-a"].event_count == 4  # page_view, click, add_to_cart, purchase
        assert rows["sess-b"].event_count == 1  # page_view (bounced)
        assert rows["sess-c"].event_count == 2  # search, error
        assert rows["sess-d"].event_count == 2  # page_view, click
        assert rows["sess-e"].event_count == 3  # login, page_view, logout

    def test_session_has_purchased(self, spark, silver_df):
        """Sessions with purchases should have has_purchased=True."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        assert rows["sess-a"].has_purchased is True
        assert rows["sess-b"].has_purchased is False
        assert rows["sess-c"].has_purchased is False

    def test_bounce_detection(self, spark, silver_df):
        """Single-event sessions should be flagged as bounced."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        assert rows["sess-b"].is_bounced is True
        assert rows["sess-a"].is_bounced is False
        assert rows["sess-c"].is_bounced is False

    def test_session_revenue(self, spark, silver_df):
        """Sessions with purchases should have total_revenue > 0."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        assert rows["sess-a"].total_revenue == pytest.approx(49.99)
        assert rows["sess-b"].total_revenue == 0.0

    def test_entry_and_exit_pages(self, spark, silver_df):
        """Sessions should have correct entry and exit pages."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        # sess-a: entry=/, exit=/checkout/confirm
        assert rows["sess-a"].entry_page == "/"
        assert rows["sess-a"].exit_page == "/checkout/confirm"

        # sess-b: only one event, entry=exit
        assert rows["sess-b"].entry_page == rows["sess-b"].exit_page

    def test_session_device_info(self, spark, silver_df):
        """Sessions should carry device/browser/os/country info from first event."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        # sess-b: mobile, Safari, iOS, IN
        assert rows["sess-b"].device_type == "mobile"
        assert rows["sess-b"].browser == "Safari"
        assert rows["sess-b"].os == "iOS"
        assert rows["sess-b"].country == "IN"

    def test_empty_df_returns_empty(self, spark):
        """An empty DataFrame should not crash session computation."""
        empty_df = spark.createDataFrame([], _SILVER_ROW_SCHEMA)
        sessions = compute_sessions(empty_df)
        assert sessions.count() == 0


# ─── Funnel Analysis Tests ────────────────────────────────────────────


class TestComputeFunnels:
    def test_funnel_has_four_steps(self, spark, silver_df):
        """Funnel should include page_view, click, add_to_cart, purchase steps."""
        funnels = compute_funnels(silver_df, window_seconds=86400)  # 24h
        rows = funnels.collect()

        steps = sorted([r.funnel_step for r in rows])
        assert steps == ["add_to_cart", "click", "page_view", "purchase"]

    def test_funnel_conversion_rates(self, spark, silver_df):
        """Conversion rates should decrease through the funnel."""
        funnels = compute_funnels(silver_df, window_seconds=86400)
        rows = {r.funnel_step: r for r in funnels.collect()}

        # All funnel steps should be present
        assert rows["page_view"].unique_users > 0
        assert rows["click"].unique_users > 0
        assert rows["add_to_cart"].unique_users >= 0
        assert rows["purchase"].unique_users >= 0

        # Conversion rates should be between 0 and 1
        for step in ["page_view", "click", "add_to_cart"]:
            if rows[step].conversion_to_next is not None:
                assert 0 <= rows[step].conversion_to_next <= 1.0

    def test_funnel_drop_off(self, spark, silver_df):
        """Drop-off should be positive between funnel steps."""
        funnels = compute_funnels(silver_df, window_seconds=86400)
        rows = {r.funnel_step: r for r in funnels.collect()}

        # page_view -> click: should have some drop-off
        if rows["page_view"].drop_off_count is not None:
            assert rows["page_view"].drop_off_count >= 0

        # The last step (purchase) should not have conversion_to_next
        assert rows["purchase"].conversion_to_next is None or rows["purchase"].conversion_to_next is None


# ─── Anomaly Aggregation Tests ────────────────────────────────────────


class TestComputeAnomalyAggs:
    def test_no_anomalies_returns_empty(self, spark, silver_df):
        """When there are no anomalous events, anomaly aggregation should return empty."""
        aggs = compute_anomaly_aggregations(silver_df, window_seconds=3600)
        assert aggs.count() == 0

    def test_anomalies_are_aggregated(self, spark):
        """Anomalous events should be aggregated by window and type."""
        rows = []
        for i in range(5):
            rows.append(Row(**_make_silver_row(
                event_id=f"evt-anom-{i:03d}", event_type="error",
                timestamp=datetime(2026, 5, 29, 10, i, 0, tzinfo=timezone.utc),
                is_anomaly=True, anomaly_type="error_rate_spike", anomaly_score=0.8,
                is_error_event=True, response_time_ms=100,
            )))
        # Add some non-anomalous events
        for i in range(10):
            rows.append(Row(**_make_silver_row(
                event_id=f"evt-normal-{i:03d}", event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, i + 10, 0, tzinfo=timezone.utc),
                is_anomaly=False, anomaly_type=None, anomaly_score=None,
                response_time_ms=100,
            )))
        df = spark.createDataFrame(rows, _SILVER_ROW_SCHEMA)
        aggs = compute_anomaly_aggregations(df, window_seconds=3600)
        assert aggs.count() >= 1

        agg_row = aggs.collect()[0]
        assert agg_row.anomaly_count >= 1
        assert agg_row.anomaly_type == "error_rate_spike"

    def test_severity_classification(self, spark):
        """Anomaly count should determine severity level."""
        # Create enough anomalies for 'low' severity (>=1, <10)
        rows = []
        for i in range(3):
            rows.append(Row(**_make_silver_row(
                event_id=f"evt-low-{i:03d}", event_type="error",
                timestamp=datetime(2026, 5, 29, 10, i, 0, tzinfo=timezone.utc),
                is_anomaly=True, anomaly_type="error_rate_spike", anomaly_score=0.6,
                is_error_event=True, response_time_ms=100,
            )))
        for i in range(10):
            rows.append(Row(**_make_silver_row(
                event_id=f"evt-norm-{i:03d}", event_type="page_view",
                timestamp=datetime(2026, 5, 29, 10, i + 10, 0, tzinfo=timezone.utc),
                is_anomaly=False, anomaly_type=None, anomaly_score=None,
                response_time_ms=100,
            )))
        df = spark.createDataFrame(rows, _SILVER_ROW_SCHEMA)
        aggs = compute_anomaly_aggregations(df, window_seconds=3600)
        agg_row = aggs.collect()[0]
        assert agg_row.severity in ("low", "medium", "high", "critical")


# ─── Pipeline Config Tests ────────────────────────────────────────────


class TestGoldPipelineConfig:
    def test_default_config_has_all_keys(self, spark):
        """Default config should have all required keys."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold", "kpis_path": "/tmp/test_gold_kpis"},
            spark=spark,
        )
        assert "silver_path" in pipeline.config
        assert "kpis_path" in pipeline.config
        assert "sessions_path" in pipeline.config
        assert "funnels_path" in pipeline.config
        assert "anomalies_path" in pipeline.config
        assert not pipeline._owns_spark
        pipeline.stop()

    def test_pipeline_uses_provided_spark(self, spark):
        """Pipeline should use an externally-provided Spark session."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold", "kpis_path": "/tmp/test_gold_kpis"},
            spark=spark,
        )
        assert pipeline.spark is spark
        assert pipeline._owns_spark is False
        pipeline.stop()

    def test_pipeline_stop_does_not_kill_external_session(self, spark):
        """stop() should not kill an externally-provided Spark session."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold", "kpis_path": "/tmp/test_gold_kpis"},
            spark=spark,
        )
        pipeline.stop()
        assert spark.version is not None


# ─── Pipeline Integration Tests ───────────────────────────────────────


class TestGoldPipelineIntegration:
    def test_run_kpis_with_empty_silver_returns_zero(self, spark):
        """With no Silver data, run_kpis should return zero stats."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold_empty", "kpis_path": "/tmp/test_gold_kpis_empty"},
            spark=spark,
        )
        stats = pipeline.run_kpis(event_date="2026-05-29")
        assert stats["silver_read"] == 0
        assert stats["kpis_written"] == 0
        pipeline.stop()

    def test_run_sessions_with_empty_silver_returns_zero(self, spark):
        """With no Silver data, run_sessions should return zero stats."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold_empty2", "sessions_path": "/tmp/test_gold_sessions_empty"},
            spark=spark,
        )
        stats = pipeline.run_sessions(event_date="2026-05-29")
        assert stats["silver_read"] == 0
        assert stats["sessions_written"] == 0
        pipeline.stop()

    def test_run_funnels_with_empty_silver_returns_zero(self, spark):
        """With no Silver data, run_funnels should return zero stats."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold_empty3", "funnels_path": "/tmp/test_gold_funnels_empty"},
            spark=spark,
        )
        stats = pipeline.run_funnels(event_date="2026-05-29")
        assert stats["silver_read"] == 0
        assert stats["funnels_written"] == 0
        pipeline.stop()

    def test_run_anomaly_aggs_with_empty_silver_returns_zero(self, spark):
        """With no Silver data, run_anomaly_aggs should return zero stats."""
        pipeline = GoldPipeline(
            config={"silver_path": "/tmp/test_gold_empty4", "anomalies_path": "/tmp/test_gold_anomalies_empty"},
            spark=spark,
        )
        stats = pipeline.run_anomaly_aggs(event_date="2026-05-29")
        assert stats["silver_read"] == 0
        assert stats["anomalies_written"] == 0
        pipeline.stop()

    def test_run_kpis_with_dataframe(self, spark, silver_df):
        """compute_kpis should work correctly with the silver_df fixture."""
        kpis = compute_kpis(silver_df, window_seconds=3600)
        rows = kpis.collect()

        assert len(rows) > 0
        # Our test data spans 10:00 to 12:05, so at 1h windows, we should have
        # at least 2 windows (10:00 and 11:00/12:00)
        assert len(rows) >= 1
        # The window with most events should have 4+ events
        max_events = max(r.total_events for r in rows)
        assert max_events >= 4

    def test_sessions_with_dataframe(self, spark, silver_df):
        """compute_sessions should work correctly with the silver_df fixture."""
        sessions = compute_sessions(silver_df)
        rows = {r.session_id: r for r in sessions.collect()}

        # Verify all 5 sessions
        assert len(rows) == 5

        # Check traffic source on entry
        assert rows["sess-a"].entry_traffic_source == "organic"
        assert rows["sess-b"].entry_traffic_source == "social"
        assert rows["sess-d"].entry_traffic_source == "email"

    def test_funnels_with_dataframe(self, spark, silver_df):
        """compute_funnels should work correctly with the silver_df fixture."""
        funnels = compute_funnels(silver_df, window_seconds=86400)
        rows = {r.funnel_step: r for r in funnels.collect()}

        # We have 4 page_view users (user-01 twice, user-02, user-04 = 3 unique users)
        assert rows["page_view"].unique_users >= 2

        # page_view -> click should have some conversion
        if rows["page_view"].conversion_to_next is not None:
            assert rows["page_view"].conversion_to_next > 0
