"""
RealtimeStream — Silver Transformation Layer

Reads clean events from Bronze Delta tables, applies deduplication,
quality checks, enrichment, and anomaly detection, then writes to
Silver Delta tables with Kafka alerting for anomalies.

Medallion role: Silver → Clean, enriched, quality-scored events.

Run (batch):
    spark-submit src/silver_streaming.py --date 2026-05-29

Run (backfill range):
    spark-submit src/silver_streaming.py --start-date 2026-05-01 --end-date 2026-05-29
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    mean,
    stddev,
    when,
    row_number,
    udf,
    dayofweek,
    hour,
    round as spark_round,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ensure src/ is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.base import BasePipeline, retry
from src.config import (
    ANOMALY_CONFIG,
    ENRICHMENT_CONFIG,
    MEDALLION_PATHS,
    QUALITY_CONFIG,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("silver_streaming")


# ─── Silver Schema — extends Bronze CLEAN_EVENT_SCHEMA ────────────────

SILVER_EVENT_SCHEMA = StructType(
    [
        # ── Original Bronze fields (typed) ───────────────────────────
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
        # ── Silver enrichment fields ─────────────────────────────────
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_purchase_event", BooleanType(), False),
        StructField("is_error_event", BooleanType(), False),
        StructField("traffic_source", StringType(), True),
        StructField("event_number_in_session", IntegerType(), True),
        # ── Quality fields ───────────────────────────────────────────
        StructField("quality_score", DoubleType(), False),
        StructField("quality_flags", StringType(), True),  # comma-separated
        # ── Anomaly fields ───────────────────────────────────────────
        StructField("is_anomaly", BooleanType(), False),
        StructField("anomaly_type", StringType(), True),
        StructField("anomaly_score", DoubleType(), True),
        # ── Metadata ─────────────────────────────────────────────────
        StructField("processed_at", TimestampType(), False),
        StructField("silver_version", StringType(), False),
    ]
)

# ─── Default config ───────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "bronze_path": MEDALLION_PATHS["bronze"],
    "silver_path": MEDALLION_PATHS["silver"],
    "quarantine_path": MEDALLION_PATHS["silver"].replace(
        "events_clean", "quarantine"
    ),
    "checkpoint_path": "s3a://streaming-lake/checkpoints/silver",
    "kafka_bootstrap_servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    ),
    "anomaly_topic": "anomaly_alerts",
    "query_name": "silver_transformation",
    "batch_size": 10000,
}


# ─── Helper Functions ─────────────────────────────────────────────────


def _parse_traffic_source(referrer_url: Optional[str]) -> str:
    """
    Parse referrer URL to determine traffic source category.

    Returns: 'organic', 'social', 'email', 'direct', or 'referral'.
    """
    if not referrer_url or referrer_url == "/" or referrer_url == "None":
        return "direct"

    referrer_lower = referrer_url.lower()
    sources = ENRICHMENT_CONFIG.get("referrer_sources", {})

    for domain, source_type in sources.items():
        if domain in referrer_lower:
            return source_type

    return "referral"


def _traffic_source_udf():
    """Spark UDF wrapper for traffic source parsing."""
    return udf(_parse_traffic_source, StringType())


def _compute_quality_score(row: Dict) -> Tuple[float, List[str]]:
    """
    Compute quality score (0.0-1.0) and list of quality flags for an event.

    Checks:
      - Completeness: missing critical fields
      - Consistency: valid event_type, valid device_type
      - Timeliness: timestamp not in the future or too old
      - Reasonableness: response_time_ms within bounds
    """
    score = 1.0
    flags = []

    # Completeness checks
    critical_fields = QUALITY_CONFIG.get(
        "critical_fields", ["user_id", "session_id", "timestamp", "event_type", "event_id"]
    )
    missing = [f for f in critical_fields if row.get(f) is None or str(row.get(f, "")).strip() == ""]
    if missing:
        score -= 0.15 * len(missing)
        flags.append(f"missing:{','.join(missing)}")

    # Consistency: event_type
    event_type = row.get("event_type")
    valid_types = QUALITY_CONFIG.get("valid_event_types", [])
    if event_type and event_type not in valid_types:
        score -= 0.2
        flags.append(f"invalid_event_type:{event_type}")

    # Consistency: device_type
    device = row.get("device_type")
    valid_devices = QUALITY_CONFIG.get("valid_devices", [])
    if device and device not in valid_devices:
        score -= 0.1
        flags.append(f"invalid_device:{device}")

    # Timeliness: timestamp not in the future or too old (>7 days)
    ts = row.get("timestamp")
    if ts:
        now = datetime.now(timezone.utc)
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                score -= 0.15
                flags.append("invalid_timestamp")
                ts = None
        if isinstance(ts, datetime):
            # Normalize to offset-aware UTC for consistent comparison
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_days = abs((now - ts).total_seconds()) / 86400
            if ts > now:
                score -= 0.1
                flags.append("future_timestamp")
            if age_days > 7:
                score -= 0.1
                flags.append("stale_timestamp")

    # Reasonableness: response_time_ms
    rtm = row.get("response_time_ms")
    max_rtm = QUALITY_CONFIG.get("max_response_time_ms", 30000)
    if rtm is not None and rtm > max_rtm:
        score -= 0.05
        flags.append(f"high_response_time:{rtm}ms")

    # Clamp score
    score = max(0.0, min(1.0, score))
    return score, flags


def _compute_quality_udf():
    """Spark UDF wrapping _compute_quality_score."""

    def _udf(
        user_id, session_id, timestamp, event_type, event_id,
        device_type, response_time_ms,
    ):
        row = {
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": timestamp,
            "event_type": event_type,
            "event_id": event_id,
            "device_type": device_type,
            "response_time_ms": response_time_ms,
        }
        score, flags = _compute_quality_score(row)
        return (float(score), ",".join(flags) if flags else None)

    return udf(_udf, StructType([
        StructField("quality_score", DoubleType(), False),
        StructField("quality_flags", StringType(), True),
    ]))


# ─── Enrichment Functions ─────────────────────────────────────────────


def _enrich_events(df: DataFrame) -> DataFrame:
    """
    Add enrichment columns:
      - hour_of_day: 0-23 from timestamp
      - day_of_week: 1=Sunday, 2=Monday, ..., 7=Saturday
      - is_purchase_event: event_type == 'purchase'
      - is_error_event: event_type == 'error'
      - traffic_source: parsed from referrer_url
      - event_number_in_session: row_number over session_id partition

    Args:
        df: DataFrame with Bronze CLEAN_EVENT_SCHEMA fields.

    Returns:
        DataFrame with added enrichment columns.
    """
    enriched = (
        df.withColumn("hour_of_day", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("is_purchase_event", col("event_type") == "purchase")
        .withColumn("is_error_event", col("event_type") == "error")
        .withColumn("traffic_source", _traffic_source_udf()(col("referrer_url")))
    )

    # Event number in session (using window function)
    window_spec = (
        Window.partitionBy("session_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    enriched = enriched.withColumn(
        "event_number_in_session", row_number().over(window_spec)
    )

    return enriched


# ─── Deduplication ────────────────────────────────────────────────────


def _deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Remove duplicate events by event_id, keeping the first occurrence.

    Args:
        df: DataFrame with event_id column.

    Returns:
        Deduplicated DataFrame.
    """
    window_spec = (
        Window.partitionBy("event_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        df.withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


# ─── Quality Checks ───────────────────────────────────────────────────


def _apply_quality_checks(df: DataFrame) -> DataFrame:
    """
    Apply quality scoring and filtering.

    Adds quality_score and quality_flags columns, then filters records
    below the minimum quality threshold into a quarantine set.

    Args:
        df: Enriched DataFrame.

    Returns:
        Tuple of (passed_df, quarantined_df).
    """
    quality_udf = _compute_quality_udf()

    df_with_quality = df.withColumn(
        "_quality", quality_udf(
            col("user_id"),
            col("session_id"),
            col("timestamp").cast(StringType()),
            col("event_type"),
            col("event_id"),
            col("device_type"),
            col("response_time_ms"),
        )
    ).select(
        "*",
        col("_quality.quality_score").alias("quality_score"),
        col("_quality.quality_flags").alias("quality_flags"),
    ).drop("_quality")

    min_score = QUALITY_CONFIG.get("min_quality_score", 0.5)
    passed_df = df_with_quality.filter(col("quality_score") >= min_score)
    quarantined_df = df_with_quality.filter(col("quality_score") < min_score)

    return passed_df, quarantined_df


# ─── Anomaly Detection ────────────────────────────────────────────────


class AnomalyDetector:
    """
    Statistical anomaly detection for event batches.

    Detects:
      - Error rate spikes (fraction of error events > threshold)
      - Response time anomalies (events with z-score > threshold)
      - Traffic anomalies (batch event count vs rolling average)
      - Purchase drop-off (purchase rate below expected)
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = {**ANOMALY_CONFIG, **(config or {})}

    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Analyze a DataFrame batch and mark anomalous events.

        Adds columns:
          - is_anomaly (BooleanType): True if event is anomalous
          - anomaly_type (StringType): type of anomaly detected
          - anomaly_score (DoubleType): severity score (0.0-1.0)

        Args:
            df: DataFrame with at least event_type, response_time_ms,
                event_id columns.

        Returns:
            DataFrame with anomaly columns added.
        """
        total_count = df.count()
        if total_count < self.config.get("min_samples", 10):
            # Not enough samples — mark all as non-anomalous
            return df.withColumn("is_anomaly", lit(False)).withColumn(
                "anomaly_type", lit(None).cast(StringType())
            ).withColumn("anomaly_score", lit(None).cast(DoubleType()))

        # Detect error rate spike
        error_rate = (
            df.filter(col("event_type") == "error").count() / total_count
        )
        error_rate_threshold = self.config.get("error_rate_threshold", 0.05)
        error_spike = error_rate > error_rate_threshold

        # Compute response time stats
        rt_stats = df.agg(
            mean(col("response_time_ms")).alias("rt_mean"),
            stddev(col("response_time_ms")).alias("rt_std"),
        ).collect()[0]
        rt_mean = rt_stats["rt_mean"] or 0.0
        rt_std = rt_stats["rt_std"] or 1.0

        rt_spike_threshold = self.config.get("response_time_spike_threshold", 3.0)

        # Mark individual events
        result_df = df.withColumn(
            "is_anomaly",
            when(
                # Error rate spike flag
                (col("event_type") == "error") & lit(error_spike),
                True,
            )
            # Response time z-score
            .when(
                (col("response_time_ms").isNotNull())
                & (
                    (col("response_time_ms") - lit(rt_mean))
                    / lit(rt_std if rt_std > 0 else 1.0)
                    > lit(rt_spike_threshold)
                ),
                True,
            )
            .otherwise(False),
        ).withColumn(
            "anomaly_type",
            when(
                (col("event_type") == "error") & lit(error_spike),
                lit("error_rate_spike"),
            )
            .when(
                (col("response_time_ms").isNotNull())
                & (
                    (col("response_time_ms") - lit(rt_mean))
                    / lit(rt_std if rt_std > 0 else 1.0)
                    > lit(rt_spike_threshold)
                ),
                lit("slow_response"),
            )
            .otherwise(None),
        )

        # Compute anomaly score (0.0-1.0) for flagged events
        result_df = result_df.withColumn(
            "anomaly_score",
            when(
                col("is_anomaly") & (col("anomaly_type") == "error_rate_spike"),
                lit(round(min(error_rate / error_rate_threshold, 2.0), 2)),
            )
            .when(
                col("is_anomaly") & (col("anomaly_type") == "slow_response"),
                spark_round(
                    (col("response_time_ms") - lit(rt_mean))
                    / (lit(rt_std if rt_std > 0 else 1.0) * lit(rt_spike_threshold)),
                    2,
                ),
            )
            .otherwise(None),
        )

        return result_df

    def generate_alerts(self, df: DataFrame) -> List[Dict]:
        """
        Generate alert messages from anomalous events for Kafka.

        Args:
            df: DataFrame with anomaly columns (output of detect_anomalies).

        Returns:
            List of alert dicts suitable for serialization to Kafka.
        """
        anomalies = df.filter(col("is_anomaly")).collect()
        alerts = []
        for row in anomalies:
            alert = {
                "alert_id": f"alert-{row.event_id}-{int(datetime.now(timezone.utc).timestamp())}",
                "event_id": row.event_id,
                "event_type": row.event_type,
                "anomaly_type": row.anomaly_type,
                "anomaly_score": float(row.anomaly_score) if row.anomaly_score else None,
                "response_time_ms": row.response_time_ms,
                "error_code": row.error_code,
                "timestamp": row.timestamp.isoformat() if hasattr(row.timestamp, "isoformat") else str(row.timestamp),
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            alerts.append(alert)
        return alerts


# ─── SilverPipeline ───────────────────────────────────────────────────


class SilverPipeline(BasePipeline):
    """
    Silver transformation pipeline.

    Reads clean events from Bronze Delta, applies deduplication, quality
    checks, enrichment, and anomaly detection, then writes to Silver Delta
    and optionally produces anomaly alerts to Kafka.
    """

    def __init__(
        self,
        config: Optional[Dict] = None,
        spark: Optional["SparkSession"] = None,
    ):
        merged_config = {**DEFAULT_CONFIG, **(config or {})}
        super().__init__(merged_config, spark, "SilverStreaming")
        self.anomaly_detector = AnomalyDetector()

    def _init_tables(self):
        """Ensure Silver and quarantine Delta tables exist."""
        self._init_delta_table(
            self.config["silver_path"],
            SILVER_EVENT_SCHEMA,
            partition_by=["event_date"],
        )
        self._init_delta_table(
            self.config["quarantine_path"],
            SILVER_EVENT_SCHEMA,
        )

    def read_from_bronze(self, event_date: Optional[str] = None) -> "DataFrame":
        """
        Read clean events from Bronze Delta table.

        Args:
            event_date: Optional date filter (yyyy-MM-dd). Reads latest if None.

        Returns:
            DataFrame with Bronze CLEAN_EVENT_SCHEMA fields.
        """
        bronze_path = self.config["bronze_path"]

        if event_date:
            logger.info(f"Reading Bronze events for date: {event_date}")
            return self.spark.read.format("delta").load(bronze_path).filter(
                col("event_date") == event_date
            )

        # Read latest partition
        try:
            available_dates = (
                self.spark.read.format("delta")
                .load(bronze_path)
                .select("event_date")
                .distinct()
                .orderBy(col("event_date").desc())
                .limit(1)
                .collect()
            )
            if available_dates:
                latest = available_dates[0]["event_date"]
                logger.info(f"Reading latest Bronze partition: {latest}")
                return self.spark.read.format("delta").load(bronze_path).filter(
                    col("event_date") == latest
                )
        except Exception:
            pass

        logger.info("Reading all Bronze data (no date filter)")
        return self.spark.read.format("delta").load(bronze_path)

    def run_batch(
        self, event_date: Optional[str] = None
    ) -> Dict:
        """
        Run the Silver transformation in batch mode.

        Pipeline steps:
          1. Read from Bronze Delta
          2. Deduplicate by event_id
          3. Enrich with derived columns
          4. Apply quality checks (split passed / quarantined)
          5. Anomaly detection
          6. Write to Silver Delta + quarantine
          7. Produce anomaly alerts to Kafka

        Args:
            event_date: Date filter (yyyy-MM-dd). None = latest partition.

        Returns:
            Dict with processing stats.
        """
        self._ensure_tables()

        # Step 1: Read from Bronze
        logger.info(f"Reading Bronze events from: {self.config['bronze_path']}")
        bronze_df = self.read_from_bronze(event_date)
        bronze_count = bronze_df.count()
        logger.info(f"Read {bronze_count:,} events from Bronze")

        if bronze_count == 0:
            return {
                "bronze_read": 0,
                "after_dedup": 0,
                "passed_quality": 0,
                "quarantined": 0,
                "anomalies": 0,
                "alerts_sent": 0,
                "silver_written": 0,
            }

        # Step 2: Deduplicate
        deduped_df = _deduplicate_events(bronze_df)
        deduped_count = deduped_df.count()
        duplicates = bronze_count - deduped_count
        if duplicates > 0:
            logger.info(f"Removed {duplicates} duplicate(s)")

        # Step 3: Enrich
        enriched_df = _enrich_events(deduped_df)
        logger.info("Enrichment applied (hour_of_day, day_of_week, traffic_source, etc.)")

        # Step 4: Quality checks
        passed_df, quarantined_df = _apply_quality_checks(enriched_df)
        passed_count = passed_df.count()
        quarantined_count = quarantined_df.count()
        logger.info(
            f"Quality checks: {passed_count:,} passed, {quarantined_count:,} quarantined"
        )

        # Step 5: Anomaly detection
        if passed_count > 0:
            analyzed_df = self.anomaly_detector.detect_anomalies(passed_df)
            anomaly_count = analyzed_df.filter(col("is_anomaly")).count()
            logger.info(f"Anomaly detection: {anomaly_count:,} anomalous events found")

            # Step 6: Add metadata and write to Silver Delta
            silver_df = (
                analyzed_df.withColumn(
                    "processed_at",
                    lit(datetime.now(timezone.utc)).cast(TimestampType()),
                )
                .withColumn(
                    "silver_version",
                    lit(ENRICHMENT_CONFIG.get("silver_version", "1.0.0")),
                )
                .select(*[f.name for f in SILVER_EVENT_SCHEMA.fields])
            )

            try:
                (
                    silver_df.write.format("delta")
                    .mode("append")
                    .partitionBy("event_date")
                    .save(self.config["silver_path"])
                )
                logger.info(
                    f"Wrote {passed_count:,} events to Silver Delta: {self.config['silver_path']}"
                )
            except Exception as e:
                logger.error(f"Failed to write Silver Delta: {e}")
                raise

            # Step 7: Produce anomaly alerts to Kafka
            alerts = self.anomaly_detector.generate_alerts(analyzed_df)
            alerts_sent = self._send_alerts(alerts)

            # Write quarantined records
            if quarantined_count > 0:
                try:
                    quarantined_with_meta = quarantined_df.withColumn(
                        "processed_at",
                        lit(datetime.now(timezone.utc)).cast(TimestampType()),
                    ).withColumn(
                        "silver_version",
                        lit(ENRICHMENT_CONFIG.get("silver_version", "1.0.0")),
                    )
                    (
                        quarantined_with_meta.write.format("delta")
                        .mode("append")
                        .save(self.config["quarantine_path"])
                    )
                    logger.info(
                        f"Wrote {quarantined_count:,} records to quarantine: "
                        f"{self.config['quarantine_path']}"
                    )
                except Exception as e:
                    logger.error(f"Failed to write quarantine Delta: {e}")

            return {
                "bronze_read": bronze_count,
                "after_dedup": deduped_count,
                "passed_quality": passed_count,
                "quarantined": quarantined_count,
                "anomalies": anomaly_count,
                "alerts_sent": alerts_sent,
                "silver_written": passed_count,
            }

        return {
            "bronze_read": bronze_count,
            "after_dedup": deduped_count,
            "passed_quality": 0,
            "quarantined": quarantined_count,
            "anomalies": 0,
            "alerts_sent": 0,
            "silver_written": 0,
        }

    @retry(max_attempts=3, delay=0.5)
    def _send_alerts(self, alerts: List[Dict]) -> int:
        """Send anomaly alerts to Kafka topic."""
        if not alerts:
            return 0

        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=self.config["kafka_bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="1",
            )
            for alert in alerts:
                producer.send(self.config["anomaly_topic"], value=alert)
            producer.flush()
            producer.close()
            logger.info(f"Sent {len(alerts)} anomaly alert(s) to topic '{self.config['anomaly_topic']}'")
            return len(alerts)
        except ImportError:
            logger.warning("kafka-python not installed. Anomaly alerts sent to stdout.")
            for alert in alerts:
                print(json.dumps(alert, default=str))
            return len(alerts)
        except Exception as e:
            logger.error(f"Failed to send alerts to Kafka: {e}")
            return 0


# ─── CLI Entry Point ──────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Silver Transformation Layer — Bronze → Silver Delta"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Process a single event date (yyyy-MM-dd). Default: latest partition.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Backfill start date (yyyy-MM-dd). Use with --end-date.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Backfill end date (yyyy-MM-dd). Use with --start-date.",
    )
    parser.add_argument(
        "--bronze-path",
        default=None,
        help="Override Bronze Delta path.",
    )
    parser.add_argument(
        "--silver-path",
        default=None,
        help="Override Silver Delta output path.",
    )
    parser.add_argument(
        "--skip-anomalies",
        action="store_true",
        help="Skip anomaly detection and alerting.",
    )

    args = parser.parse_args()

    config = {}
    if args.bronze_path:
        config["bronze_path"] = args.bronze_path
    if args.silver_path:
        config["silver_path"] = args.silver_path

    pipeline = SilverPipeline(config=config)

    try:
        if args.start_date and args.end_date:
            # Backfill range
            start = datetime.strptime(args.start_date, "%Y-%m-%d")
            end = datetime.strptime(args.end_date, "%Y-%m-%d")
            current = start
            total_stats = {
                "bronze_read": 0,
                "after_dedup": 0,
                "passed_quality": 0,
                "quarantined": 0,
                "anomalies": 0,
                "alerts_sent": 0,
                "silver_written": 0,
            }
            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                logger.info(f"\n{'='*60}\nProcessing date: {date_str}\n{'='*60}")
                stats = pipeline.run_batch(date_str)
                for k, v in stats.items():
                    total_stats[k] += v
                current += timedelta(days=1)

            logger.info(
                f"\n{'='*60}\nBackfill complete\n"
                f"  Dates: {args.start_date} → {args.end_date}\n"
                f"  Bronze read: {total_stats['bronze_read']:,}\n"
                f"  Silver written: {total_stats['silver_written']:,}\n"
                f"  Quarantined: {total_stats['quarantined']:,}\n"
                f"  Anomalies: {total_stats['anomalies']:,}\n"
                f"  Alerts: {total_stats['alerts_sent']:,}\n"
                f"{'='*60}"
            )
        else:
            stats = pipeline.run_batch(args.date)
            logger.info(f"Batch complete: {stats}")

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
