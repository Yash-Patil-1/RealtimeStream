"""
RealtimeStream — Gold Aggregation Layer

Computes sliding-window KPIs, session-level metrics, conversion funnels,
and anomaly aggregations from Silver Delta tables.

Medallion role: Gold → Aggregated business metrics for dashboards & alerts.

Run (single window):
    spark-submit src/gold_aggregations.py --mode kpis --window 1h

Run (backfill):
    spark-submit src/gold_aggregations.py --mode sessions --start-date 2026-05-01 --end-date 2026-05-29
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    hour,
    lit,
    max as spark_max,
    mean,
    min as spark_min,
    percentile_approx,
    round as spark_round,
    sum as spark_sum,
    when,
    to_date,
    window as spark_window,
    row_number,
)
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

# Ensure src/ is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.base import BasePipeline
from src.config import (
    MEDALLION_PATHS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_aggregations")


# ─── Gold Delta Table Config ──────────────────────────────────────────

GOLD_PATHS = {
    "kpis": MEDALLION_PATHS.get("gold_kpis", "s3a://streaming-lake/delta/gold/kpis"),
    "sessions": MEDALLION_PATHS.get("gold_sessions", "s3a://streaming-lake/delta/gold/sessions"),
    "funnels": MEDALLION_PATHS.get("gold_funnels", "s3a://streaming-lake/delta/gold/funnels"),
    "anomalies": MEDALLION_PATHS.get("gold_anomalies", "s3a://streaming-lake/delta/gold/anomalies"),
}

WINDOW_DURATIONS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
    "24h": 24 * 60 * 60,
}

# ─── Delta Table Schemas ─────────────────────────────────────────────

KPI_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("window_duration", StringType(), False),
    StructField("event_date", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("total_events", LongType(), False),
    StructField("page_views", LongType(), False),
    StructField("clicks", LongType(), False),
    StructField("add_to_carts", LongType(), False),
    StructField("purchases", LongType(), False),
    StructField("logins", LongType(), False),
    StructField("errors", LongType(), False),
    StructField("searches", LongType(), False),
    StructField("unique_users", LongType(), False),
    StructField("unique_sessions", LongType(), False),
    StructField("avg_response_time_ms", DoubleType(), True),
    StructField("p95_response_time_ms", DoubleType(), True),
    StructField("max_response_time_ms", DoubleType(), True),
    StructField("error_rate", DoubleType(), True),
    StructField("purchase_rate", DoubleType(), True),
    StructField("conversion_rate", DoubleType(), True),
    StructField("revenue_total", DoubleType(), True),
    StructField("revenue_avg", DoubleType(), True),
    StructField("bounce_rate", DoubleType(), True),
    StructField("processed_at", TimestampType(), False),
])

SESSION_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("session_start", TimestampType(), False),
    StructField("session_end", TimestampType(), False),
    StructField("session_duration_seconds", DoubleType(), False),
    StructField("event_count", IntegerType(), False),
    StructField("page_view_count", IntegerType(), False),
    StructField("click_count", IntegerType(), False),
    StructField("add_to_cart_count", IntegerType(), False),
    StructField("purchase_count", IntegerType(), False),
    StructField("error_count", IntegerType(), False),
    StructField("has_purchased", BooleanType(), False),
    StructField("total_revenue", DoubleType(), True),
    StructField("entry_page", StringType(), True),
    StructField("exit_page", StringType(), True),
    StructField("entry_traffic_source", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_bounced", BooleanType(), False),
    StructField("event_date", StringType(), True),
    StructField("processed_at", TimestampType(), False),
])

FUNNEL_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("window_duration", StringType(), False),
    StructField("event_date", StringType(), True),
    StructField("funnel_step", StringType(), False),
    StructField("step_order", IntegerType(), False),
    StructField("unique_users", LongType(), False),
    StructField("event_count", LongType(), False),
    StructField("conversion_to_next", DoubleType(), True),
    StructField("drop_off_count", LongType(), True),
    StructField("drop_off_rate", DoubleType(), True),
    StructField("processed_at", TimestampType(), False),
])

ANOMALY_AGG_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("window_duration", StringType(), False),
    StructField("event_date", StringType(), True),
    StructField("anomaly_type", StringType(), False),
    StructField("anomaly_count", LongType(), False),
    StructField("avg_anomaly_score", DoubleType(), True),
    StructField("max_anomaly_score", DoubleType(), True),
    StructField("severity", StringType(), True),
    StructField("triggering_metric", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("metric_threshold", DoubleType(), True),
    StructField("processed_at", TimestampType(), False),
])

# ─── Default config ───────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "silver_path": MEDALLION_PATHS.get("silver", "s3a://streaming-lake/delta/silver/events_clean"),
    "kpis_path": GOLD_PATHS["kpis"],
    "sessions_path": GOLD_PATHS["sessions"],
    "funnels_path": GOLD_PATHS["funnels"],
    "anomalies_path": GOLD_PATHS["anomalies"],
    "checkpoint_path": "s3a://streaming-lake/checkpoints/gold",
    "query_name": "gold_aggregations",
    "default_window": "1h",
}


# ─── Helper: window duration helpers ──────────────────────────────────

def _parse_window_duration(window_str: str) -> int:
    """Convert a window duration string (e.g., '1h', '30m') to seconds."""
    window_str = str(window_str).strip().lower()
    if window_str in WINDOW_DURATIONS:
        return WINDOW_DURATIONS[window_str]
    # Try to parse dynamically
    if window_str.endswith("h"):
        return int(window_str[:-1]) * 3600
    if window_str.endswith("m"):
        return int(window_str[:-1]) * 60
    if window_str.endswith("s"):
        return int(window_str[:-1])
    return int(window_str)


def _format_window_seconds(seconds: int) -> str:
    """Format seconds back to a human-readable window label."""
    for key, val in sorted(WINDOW_DURATIONS.items(), key=lambda x: abs(x[1] - seconds)):
        if val == seconds:
            return key
    if seconds >= 3600 and seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds >= 60 and seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


# ─── KPI Aggregation ──────────────────────────────────────────────────


def compute_kpis(
    df: DataFrame,
    window_seconds: int = 3600,
    event_date: Optional[str] = None,
) -> DataFrame:
    """
    Compute sliding-window KPIs from Silver events.

    For each time window, calculates:
      - Event counts by type (page_view, click, purchase, error, etc.)
      - Unique users and sessions
      - Response time statistics (avg, p95, max)
      - Error rate, purchase rate, conversion rate
      - Revenue totals and averages
      - Bounce rate (single-event sessions)

    Args:
        df: Silver DataFrame with enrichment and quality columns.
        window_seconds: Sliding window duration in seconds.
        event_date: Optional date filter for backfill.

    Returns:
        DataFrame with KPI aggregations per window.
    """
    window_dur_str = _format_window_seconds(window_seconds)
    window_col = spark_window("timestamp", f"{window_seconds} seconds")

    # Compute session-level info for bounce analysis
    session_stats = df.groupBy("session_id").agg(
        count("event_id").alias("session_event_count"),
    )

    df_with_bounce = df.join(session_stats, "session_id", "left") \
        .withColumn(
            "is_bounced",
            col("session_event_count") <= 1
        )

    # Aggregate by time window
    kpis = (
        df_with_bounce.groupBy(window_col)
        .agg(
            # Event counts by type
            count(when(col("event_type") == "page_view", 1)).alias("page_views"),
            count(when(col("event_type") == "click", 1)).alias("clicks"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            count(when(col("event_type") == "purchase", 1)).alias("purchases"),
            count(when(col("event_type") == "login", 1)).alias("logins"),
            count(when(col("event_type") == "error", 1)).alias("errors"),
            count(when(col("event_type") == "search", 1)).alias("searches"),
            # Unique metrics
            approx_count_distinct("user_id").alias("unique_users"),
            approx_count_distinct("session_id").alias("unique_sessions"),
            # Response time
            mean("response_time_ms").alias("avg_response_time_ms"),
            spark_max("response_time_ms").alias("max_response_time_ms"),
            percentile_approx("response_time_ms", 0.95).alias("p95_response_time_ms"),
            # Revenue
            spark_sum(when(col("event_type") == "purchase", col("amount")).otherwise(0)).alias("revenue_total"),
            mean(when(col("event_type") == "purchase", col("amount"))).alias("revenue_avg"),
            # Bounce count
            count(when(col("is_bounced"), 1)).alias("bounce_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # Compute derived metrics
    total_events = col("page_views") + col("clicks") + col("add_to_carts") + \
                   col("purchases") + col("logins") + col("errors") + col("searches")

    kpis = kpis.withColumn("total_events", total_events) \
        .withColumn(
            "error_rate",
            spark_round(when(total_events > 0, col("errors") / total_events).otherwise(0), 4)
        ) \
        .withColumn(
            "purchase_rate",
            spark_round(when(total_events > 0, col("purchases") / total_events).otherwise(0), 4)
        ) \
        .withColumn(
            "conversion_rate",
            spark_round(
                when(col("page_views") > 0, col("purchases") / col("page_views")).otherwise(0),
                4
            )
        ) \
        .withColumn(
            "bounce_rate",
            spark_round(
                when(col("unique_sessions") > 0, col("bounce_count") / col("unique_sessions")).otherwise(0),
                4
            )
        ) \
        .withColumn("window_duration", lit(window_dur_str)) \
        .withColumn("event_date", to_date(col("window_start")).cast(StringType())) \
        .withColumn("hour", hour(col("window_start"))) \
        .withColumn("processed_at", lit(datetime.now(timezone.utc)))

    return kpis.select(*[f.name for f in KPI_SCHEMA.fields])


# ─── Sessionization ───────────────────────────────────────────────────


def compute_sessions(df: DataFrame) -> DataFrame:
    """
    Build session-level metrics from Silver events.

    For each session_id, computes:
      - Session start, end, duration
      - Event counts by type
      - Revenue, entry/exit pages, traffic source
      - Device/browser/OS/country info
      - Bounce flag (single event)

    Args:
        df: Silver DataFrame with event-level data.

    Returns:
        DataFrame with one row per session.
    """
    # Get entry page + metadata using struct-based ordering for deterministic results
    window_asc = Window.partitionBy("session_id").orderBy("timestamp")
    window_desc = Window.partitionBy("session_id").orderBy(col("timestamp").desc())

    # First event per session for entry info
    first_events = df.withColumn(
        "_rn_asc", row_number().over(window_asc)
    ).filter(col("_rn_asc") == 1).select(
        col("session_id").alias("_sid_entry"),
        col("page_url").alias("entry_page"),
        col("traffic_source").alias("entry_traffic_source"),
        col("device_type"),
        col("browser"),
        col("os"),
        col("country"),
        col("user_id"),
        col("event_date"),
    )

    # Last event per session for exit page
    last_events = df.withColumn(
        "_rn_desc", row_number().over(window_desc)
    ).filter(col("_rn_desc") == 1).select(
        col("session_id").alias("_sid_exit"),
        col("page_url").alias("exit_page"),
    )

    # Aggregate session-level counts
    session_counts = df.groupBy("session_id").agg(
        spark_min("timestamp").alias("session_start"),
        spark_max("timestamp").alias("session_end"),
        count("event_id").alias("event_count"),
        count(when(col("event_type") == "page_view", 1)).alias("page_view_count"),
        count(when(col("event_type") == "click", 1)).alias("click_count"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
        count(when(col("event_type") == "error", 1)).alias("error_count"),
        spark_sum(when(col("event_type") == "purchase", col("amount")).otherwise(0)).alias("total_revenue"),
    )

    # Join: session counts + entry info + exit info
    sessions = session_counts \
        .join(first_events, session_counts.session_id == first_events._sid_entry, "left") \
        .drop("_sid_entry") \
        .join(last_events, session_counts.session_id == last_events._sid_exit, "left") \
        .drop("_sid_exit")

    # Add derived columns
    sessions = sessions.withColumn(
        "session_duration_seconds",
        (col("session_end").cast("double") - col("session_start").cast("double"))
    ).withColumn(
        "has_purchased",
        col("purchase_count") > 0
    ).withColumn(
        "is_bounced",
        col("event_count") <= 1
    ).withColumn(
        "processed_at",
        lit(datetime.now(timezone.utc))
    )

    return sessions.select(*[f.name for f in SESSION_SCHEMA.fields])


# ─── Funnel Analysis ──────────────────────────────────────────────────


def compute_funnels(
    df: DataFrame,
    window_seconds: int = 86400,
) -> DataFrame:
    """
    Compute conversion funnel steps from Silver events.

    Funnel steps (in order):
      1. page_view
      2. click
      3. add_to_cart
      4. purchase

    For each time window, calculates unique users and events per step,
    plus conversion rates to the next step.

    Args:
        df: Silver DataFrame with event-level data.
        window_seconds: Window duration in seconds.

    Returns:
        DataFrame with funnel step aggregations per window.
    """
    window_dur_str = _format_window_seconds(window_seconds)
    window_col = spark_window("timestamp", f"{window_seconds} seconds")

    funnel_steps = [
        ("page_view", 1),
        ("click", 2),
        ("add_to_cart", 3),
        ("purchase", 4),
    ]

    # Aggregate by window + event_type
    funnel_raw = (
        df.groupBy(window_col, "event_type")
        .agg(
            approx_count_distinct("user_id").alias("unique_users"),
            count("event_id").alias("event_count"),
        )
    )

    # Filter to only funnel steps and add step ordering
    funnel_cond = col("event_type").isin([s[0] for s in funnel_steps])
    funnel_df = funnel_raw.filter(funnel_cond)

    # Map event_type to step_order
    step_map = {s[0]: s[1] for s in funnel_steps}
    step_expr = None
    for event_type, order in step_map.items():
        condition = col("event_type") == event_type
        if step_expr is None:
            step_expr = when(condition, order)
        else:
            step_expr = step_expr.when(condition, order)

    funnel_df = funnel_df.withColumn("step_order", step_expr) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window", "event_type")

    # Compute conversion rates between steps
    # Use a self-join-like approach via window to get the next step's data
    funnel_ordered = funnel_df.withColumnRenamed("unique_users", "step_users") \
        .withColumnRenamed("event_count", "step_events")

    # For each window, get the next step's user count
    next_step = funnel_ordered.alias("current") \
        .join(
            funnel_ordered.alias("next"),
            (col("current.window_start") == col("next.window_start")) &
            (col("current.step_order") == col("next.step_order") - 1),
            "left"
        ) \
        .select(
            col("current.window_start"),
            col("current.window_end"),
            col("current.step_order"),
            col("current.step_users").alias("unique_users"),
            col("current.step_events").alias("event_count"),
            col("next.step_users").alias("next_step_users"),
            col("next.step_events").alias("next_step_events"),
        )

    funnel_result = next_step.withColumn(
        "conversion_to_next",
        spark_round(
            when(col("unique_users") > 0, col("next_step_users") / col("unique_users")).otherwise(None),
            4
        )
    ).withColumn(
        "drop_off_count",
        col("unique_users") - col("next_step_users")
    ).withColumn(
        "drop_off_rate",
        spark_round(
            when(col("unique_users") > 0, col("drop_off_count") / col("unique_users")).otherwise(None),
            4
        )
    ).withColumn(
        "window_duration", lit(window_dur_str)
    ).withColumn(
        "event_date", to_date(col("window_start")).cast(StringType())
    ).withColumn(
        "processed_at", lit(datetime.now(timezone.utc))
    )

    # Map step_order back to funnel_step name
    order_to_step = {v: k for k, v in step_map.items()}
    step_name_expr = None
    for order, name in sorted(order_to_step.items()):
        condition = col("step_order") == order
        if step_name_expr is None:
            step_name_expr = when(condition, name)
        else:
            step_name_expr = step_name_expr.when(condition, name)

    funnel_result = funnel_result.withColumn("funnel_step", step_name_expr) \
        .drop("next_step_users", "next_step_events")

    return funnel_result.select(*[f.name for f in FUNNEL_SCHEMA.fields])


# ─── Anomaly Aggregation ──────────────────────────────────────────────


def compute_anomaly_aggregations(
    df: DataFrame,
    window_seconds: int = 3600,
) -> DataFrame:
    """
    Aggregate anomaly events into per-window summaries.

    Groups anomaly records by window and type, computing counts,
    average/max scores, severity classification, and the triggering metric.

    Args:
        df: Silver DataFrame with anomaly columns (is_anomaly, anomaly_type, anomaly_score).
        window_seconds: Window duration in seconds.

    Returns:
        DataFrame with anomaly aggregation rows.
    """
    window_dur_str = _format_window_seconds(window_seconds)
    window_col = spark_window("timestamp", f"{window_seconds} seconds")

    anomaly_events = df.filter(col("is_anomaly"))

    if anomaly_events.count() == 0:
        # Return empty DataFrame with correct schema
        empty_rdd = df.sparkSession.sparkContext.emptyRDD()
        return df.sparkSession.createDataFrame(empty_rdd, ANOMALY_AGG_SCHEMA)

    aggs = (
        anomaly_events.groupBy(window_col, "anomaly_type")
        .agg(
            count("event_id").alias("anomaly_count"),
            mean("anomaly_score").alias("avg_anomaly_score"),
            spark_max("anomaly_score").alias("max_anomaly_score"),
            spark_max("response_time_ms").alias("max_rt"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # Classify severity based on anomaly_count and max_anomaly_score
    aggs = aggs.withColumn(
        "severity",
        when(col("anomaly_count") >= 100, "critical")
        .when(col("anomaly_count") >= 50, "high")
        .when(col("anomaly_count") >= 10, "medium")
        .otherwise("low")
    ).withColumn(
        "triggering_metric",
        when(col("anomaly_type") == "error_rate_spike", lit("error_rate"))
        .when(col("anomaly_type") == "slow_response", lit("response_time_ms"))
        .otherwise(col("anomaly_type"))
    ).withColumn(
        "metric_value",
        when(col("anomaly_type") == "slow_response", col("max_rt"))
        .otherwise(col("anomaly_count"))
    ).withColumn(
        "metric_threshold",
        when(col("anomaly_type") == "error_rate_spike", lit(0.05))
        .when(col("anomaly_type") == "slow_response", lit(3.0))
        .otherwise(lit(1))
    ).withColumn(
        "window_duration", lit(window_dur_str)
    ).withColumn(
        "event_date", to_date(col("window_start")).cast(StringType())
    ).withColumn(
        "processed_at", lit(datetime.now(timezone.utc))
    ).drop("max_rt")

    return aggs.select(*[f.name for f in ANOMALY_AGG_SCHEMA.fields])


# ─── GoldPipeline ─────────────────────────────────────────────────────


class GoldPipeline(BasePipeline):
    """
    Gold aggregation pipeline.

    Reads clean events from Silver Delta, computes sliding-window KPIs,
    session-level metrics, conversion funnels, and anomaly aggregations,
    then writes results to Gold Delta tables.
    """

    def __init__(
        self,
        config: Optional[Dict] = None,
        spark: Optional["SparkSession"] = None,
    ):
        merged_config = {**DEFAULT_CONFIG, **(config or {})}
        super().__init__(merged_config, spark, "GoldAggregations")

    def _init_tables(self):
        """Ensure all Gold Delta tables exist."""
        schemas = {
            "kpis_path": KPI_SCHEMA,
            "sessions_path": SESSION_SCHEMA,
            "funnels_path": FUNNEL_SCHEMA,
            "anomalies_path": ANOMALY_AGG_SCHEMA,
        }
        for path_key, schema in schemas.items():
            table_path = self.config.get(path_key)
            if table_path:
                self._init_delta_table(table_path, schema)

    def read_from_silver(
        self,
        event_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> "DataFrame":
        """
        Read events from Silver Delta table.

        Args:
            event_date: Single date filter (yyyy-MM-dd).
            start_date: Backfill start date.
            end_date: Backfill end date.

        Returns:
            DataFrame with Silver event data.
        """
        silver_path = self.config["silver_path"]
        try:
            df = self.spark.read.format("delta").load(silver_path)
        except Exception as e:
            logger.warning(f"Could not read Silver Delta table at {silver_path}: {e}")
            # Return empty DataFrame with KPI schema subset (just event_date for filtering)
            empty_rdd = self.spark.sparkContext.emptyRDD()
            from pyspark.sql.types import StringType, StructField, StructType
            empty_schema = StructType([StructField("event_date", StringType(), True)])
            return self.spark.createDataFrame(empty_rdd, empty_schema)

        if event_date:
            logger.info(f"Reading Silver events for date: {event_date}")
            df = df.filter(col("event_date") == event_date)
        elif start_date and end_date:
            logger.info(f"Reading Silver events: {start_date} to {end_date}")
            df = df.filter(
                (col("event_date") >= start_date) & (col("event_date") <= end_date)
            )
        else:
            # Read latest partition
            try:
                available = (
                    df.select("event_date")
                    .distinct()
                    .orderBy(col("event_date").desc())
                    .limit(1)
                    .collect()
                )
                if available:
                    latest = available[0]["event_date"]
                    logger.info(f"Reading latest Silver partition: {latest}")
                    df = df.filter(col("event_date") == latest)
            except Exception:
                pass

        return df

    def run_kpis(
        self,
        event_date: Optional[str] = None,
        window: str = "1h",
    ) -> Dict:
        """
        Compute sliding-window KPIs from Silver events.

        Args:
            event_date: Optional date filter.
            window: Window duration string (e.g., '1h', '30m').

        Returns:
            Dict with processing stats.
        """
        self._ensure_tables()
        window_seconds = _parse_window_duration(window)
        logger.info(f"Computing KPIs (window={window})")

        silver_df = self.read_from_silver(event_date)
        silver_count = silver_df.count()
        logger.info(f"Read {silver_count:,} events from Silver")

        if silver_count == 0:
            return {"silver_read": 0, "windows": 0, "kpis_written": 0}

        kpi_df = compute_kpis(silver_df, window_seconds, event_date)
        kpi_count = kpi_df.count()
        logger.info(f"Computed {kpi_count:,} KPI windows")

        if kpi_count > 0:
            try:
                kpi_df.write.format("delta").mode("append").save(
                    self.config["kpis_path"]
                )
                logger.info(f"Wrote {kpi_count:,} KPI rows to Gold: {self.config['kpis_path']}")
            except Exception as e:
                logger.error(f"Failed to write KPIs: {e}")
                raise

        return {"silver_read": silver_count, "windows": kpi_count, "kpis_written": kpi_count}

    def run_sessions(self, event_date: Optional[str] = None) -> Dict:
        """
        Compute session-level metrics from Silver events.

        Args:
            event_date: Optional date filter.

        Returns:
            Dict with processing stats.
        """
        self._ensure_tables()
        logger.info("Computing sessionization")

        silver_df = self.read_from_silver(event_date)
        silver_count = silver_df.count()
        logger.info(f"Read {silver_count:,} events from Silver")

        if silver_count == 0:
            return {"silver_read": 0, "sessions": 0, "sessions_written": 0}

        session_df = compute_sessions(silver_df)
        session_count = session_df.count()
        logger.info(f"Computed {session_count:,} sessions")

        if session_count > 0:
            try:
                session_df.write.format("delta").mode("append").save(
                    self.config["sessions_path"]
                )
                logger.info(f"Wrote {session_count:,} sessions to Gold: {self.config['sessions_path']}")
            except Exception as e:
                logger.error(f"Failed to write sessions: {e}")
                raise

        return {"silver_read": silver_count, "sessions": session_count, "sessions_written": session_count}

    def run_funnels(
        self,
        event_date: Optional[str] = None,
        window: str = "24h",
    ) -> Dict:
        """
        Compute conversion funnel analysis from Silver events.

        Args:
            event_date: Optional date filter.
            window: Window duration string (default '24h' for funnels).

        Returns:
            Dict with processing stats.
        """
        self._ensure_tables()
        window_seconds = _parse_window_duration(window)
        logger.info(f"Computing funnels (window={window})")

        silver_df = self.read_from_silver(event_date)
        silver_count = silver_df.count()
        logger.info(f"Read {silver_count:,} events from Silver")

        if silver_count == 0:
            return {"silver_read": 0, "funnel_rows": 0, "funnels_written": 0}

        funnel_df = compute_funnels(silver_df, window_seconds)
        funnel_count = funnel_df.count()
        logger.info(f"Computed {funnel_count:,} funnel rows")

        if funnel_count > 0:
            try:
                funnel_df.write.format("delta").mode("append").save(
                    self.config["funnels_path"]
                )
                logger.info(f"Wrote {funnel_count:,} funnel rows to Gold: {self.config['funnels_path']}")
            except Exception as e:
                logger.error(f"Failed to write funnels: {e}")
                raise

        return {"silver_read": silver_count, "funnel_rows": funnel_count, "funnels_written": funnel_count}

    def run_anomaly_aggs(
        self,
        event_date: Optional[str] = None,
        window: str = "1h",
    ) -> Dict:
        """
        Aggregate anomaly events from Silver data.

        Args:
            event_date: Optional date filter.
            window: Window duration string.

        Returns:
            Dict with processing stats.
        """
        self._ensure_tables()
        window_seconds = _parse_window_duration(window)
        logger.info(f"Computing anomaly aggregations (window={window})")

        silver_df = self.read_from_silver(event_date)
        silver_count = silver_df.count()
        logger.info(f"Read {silver_count:,} events from Silver")

        if silver_count == 0:
            return {"silver_read": 0, "anomaly_rows": 0, "anomalies_written": 0}

        anomaly_df = compute_anomaly_aggregations(silver_df, window_seconds)
        anomaly_count = anomaly_df.count()
        logger.info(f"Computed {anomaly_count:,} anomaly aggregation rows")

        if anomaly_count > 0:
            try:
                anomaly_df.write.format("delta").mode("append").save(
                    self.config["anomalies_path"]
                )
                logger.info(f"Wrote {anomaly_count:,} anomaly rows to Gold: {self.config['anomalies_path']}")
            except Exception as e:
                logger.error(f"Failed to write anomaly aggregations: {e}")
                raise

        return {
            "silver_read": silver_count,
            "anomaly_rows": anomaly_count,
            "anomalies_written": anomaly_count,
        }

    def run_all(
        self,
        event_date: Optional[str] = None,
        kpi_window: str = "1h",
        funnel_window: str = "24h",
    ) -> Dict:
        """
        Run all Gold aggregation modes in sequence.

        Args:
            event_date: Optional date filter.
            kpi_window: Window duration for KPIs and anomaly aggs.
            funnel_window: Window duration for funnel analysis.

        Returns:
            Dict combining all processing stats.
        """
        logger.info("=" * 60)
        logger.info("Running all Gold aggregations")
        logger.info("=" * 60)

        kpi_stats = self.run_kpis(event_date, kpi_window)
        session_stats = self.run_sessions(event_date)
        funnel_stats = self.run_funnels(event_date, funnel_window)
        anomaly_stats = self.run_anomaly_aggs(event_date, kpi_window)

        combined = {**kpi_stats, **session_stats, **funnel_stats, **anomaly_stats}
        combined["silver_read"] = kpi_stats.get("silver_read", 0)
        combined["modes_run"] = ["kpis", "sessions", "funnels", "anomaly_aggs"]

        logger.info(
            f"\n{'='*60}\n"
            f"Gold aggregation complete\n"
            f"  Silver read: {combined['silver_read']:,}\n"
            f"  KPI windows: {combined.get('kpis_written', 0):,}\n"
            f"  Sessions:    {combined.get('sessions_written', 0):,}\n"
            f"  Funnel rows: {combined.get('funnels_written', 0):,}\n"
            f"  Anomaly aggs:{combined.get('anomalies_written', 0):,}\n"
            f"  Modes:       {', '.join(combined['modes_run'])}\n"
            f"{'='*60}"
        )
        return combined




# ─── CLI Entry Point ──────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Gold Aggregation Layer — Silver → Gold Delta"
    )
    parser.add_argument(
        "--mode",
        choices=["kpis", "sessions", "funnels", "anomaly_aggs", "all"],
        default="all",
        help="Aggregation mode to run",
    )
    parser.add_argument(
        "--window",
        default=None,
        help="Window duration (e.g., '5m', '15m', '1h', '6h', '24h'). "
             "Default: '1h' for KPIs/anomalies, '24h' for funnels.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Process a single event date (yyyy-MM-dd). Default: latest partition.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Backfill start date (yyyy-MM-dd).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Backfill end date (yyyy-MM-dd).",
    )
    parser.add_argument(
        "--silver-path",
        default=None,
        help="Override Silver Delta path.",
    )

    args = parser.parse_args()

    config = {}
    if args.silver_path:
        config["silver_path"] = args.silver_path

    pipeline = GoldPipeline(config=config)

    try:
        # Determine date range
        event_date = args.date
        start_date = args.start_date
        end_date = args.end_date

        if start_date and end_date:
            # Backfill mode: iterate through dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            current = start
            total_stats = {}
            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                logger.info(f"\n{'='*60}\nProcessing date: {date_str}\n{'='*60}")

                if args.mode == "all":
                    stats = pipeline.run_all(event_date=date_str)
                elif args.mode == "kpis":
                    window = args.window or "1h"
                    stats = pipeline.run_kpis(event_date=date_str, window=window)
                elif args.mode == "sessions":
                    stats = pipeline.run_sessions(event_date=date_str)
                elif args.mode == "funnels":
                    window = args.window or "24h"
                    stats = pipeline.run_funnels(event_date=date_str, window=window)
                elif args.mode == "anomaly_aggs":
                    window = args.window or "1h"
                    stats = pipeline.run_anomaly_aggs(event_date=date_str, window=window)

                for k, v in stats.items():
                    if isinstance(v, (int, float)):
                        total_stats[k] = total_stats.get(k, 0) + v

                current += timedelta(days=1)

            logger.info(f"\nBackfill complete ({start_date} → {end_date}): {total_stats}")
        else:
            # Single run
            if args.mode == "all":
                stats = pipeline.run_all(event_date=event_date)
            elif args.mode == "kpis":
                window = args.window or "1h"
                stats = pipeline.run_kpis(event_date=event_date, window=window)
            elif args.mode == "sessions":
                stats = pipeline.run_sessions(event_date=event_date)
            elif args.mode == "funnels":
                window = args.window or "24h"
                stats = pipeline.run_funnels(event_date=event_date, window=window)
            elif args.mode == "anomaly_aggs":
                window = args.window or "1h"
                stats = pipeline.run_anomaly_aggs(event_date=event_date, window=window)

            logger.info(f"Batch complete: {stats}")

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
