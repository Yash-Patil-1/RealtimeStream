"""
RealtimeStream — Bronze Streaming Layer
Spark Structured Streaming job that consumes raw clickstream events from Kafka,
validates/parses them, enriches with partition columns, and writes to Delta Lake
on MinIO (S3-compatible storage).

Medallion role: Bronze → Raw ingestion with schema enforcement + bad record quarantine.

Run (streaming):
    spark-submit src/bronze_streaming.py --mode streaming

Run (batch / historical):
    spark-submit src/bronze_streaming.py --mode batch --input /path/to/events.jsonl
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
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
    MEDALLION_PATHS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bronze_streaming")

# ─── Spark Schema matching config.py EVENT_SCHEMA ─────────────────────
# All fields nullable because raw data may have missing/invalid values

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", StringType(), True),  # parsed to TimestampType later
        StructField("page_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("amount", StringType(), True),  # numeric string; cast later
        StructField("currency", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("error_code", StringType(), True),  # numeric string; cast later
        StructField("response_time_ms", StringType(), True),  # numeric string
        StructField("status_code", StringType(), True),  # numeric string
    ]
)

# ─── Clean typed schema used after validation ─────────────────────────

CLEAN_EVENT_SCHEMA = StructType(
    [
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
        StructField("event_date", StringType(), False),  # partition column
    ]
)

# ─── Default config ───────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "kafka_topic": "raw_events",
    "bronze_path": MEDALLION_PATHS["bronze"],
    "dead_letter_path": f"{'/'.join(MEDALLION_PATHS['bronze'].split('/')[:-1])}/bronze/dead_letter",
    "checkpoint_path": "s3a://streaming-lake/checkpoints/bronze",
    "trigger_interval": "10 seconds",
    "max_files_per_trigger": 10,
    "query_name": "bronze_streaming",
}


# ─── Helper Functions ─────────────────────────────────────────────────


def _cast_numeric_fields(df: DataFrame) -> DataFrame:
    """
    Cast numeric string fields (amount, error_code, response_time_ms, status_code)
    to their proper types. Invalid/missing values become NULL.
    """
    return (
        df.withColumn("amount", when(col("amount").isNotNull(), col("amount").cast(DoubleType())))
        .withColumn("error_code", when(col("error_code").isNotNull(), col("error_code").cast(IntegerType())))
        .withColumn("response_time_ms", when(col("response_time_ms").isNotNull(), col("response_time_ms").cast(IntegerType())))
        .withColumn("status_code", when(col("status_code").isNotNull(), col("status_code").cast(IntegerType())))
    )


def _enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add partition columns derived from the timestamp:
      - event_date (STRING): 'yyyy-MM-dd'
    """
    return df.withColumn("event_date", to_date(col("timestamp")).cast(StringType()))


def _separate_bad_records(df: DataFrame) -> (DataFrame, DataFrame):
    """
    Split a DataFrame into clean records and malformed records.

    A record is considered malformed if:
      - event_id is NULL or empty
      - event_type is NULL or empty
      - timestamp failed to parse

    Returns (clean_df, dead_letter_df).
    """
    clean_df = df.filter(
        col("event_id").isNotNull()
        & (col("event_id") != "")
        & col("event_type").isNotNull()
        & (col("event_type") != "")
        & col("timestamp").isNotNull()
    )

    dead_letter_df = df.filter(
        col("event_id").isNull()
        | (col("event_id") == "")
        | col("event_type").isNull()
        | (col("event_type") == "")
        | col("timestamp").isNull()
    )

    return clean_df, dead_letter_df


# ─── BronzePipeline ───────────────────────────────────────────────────


class BronzePipeline(BasePipeline):
    """
    Bronze ingestion pipeline.

    Reads raw JSON events from Kafka, validates, enriches with partition
    columns, and writes to Delta Lake. Malformed records are quarantined
    in a separate dead-letter Delta table.
    """

    def __init__(self, config: Optional[Dict] = None, spark: Optional[SparkSession] = None):
        merged_config = {**DEFAULT_CONFIG, **(config or {})}
        super().__init__(merged_config, spark, "BronzeStreaming")

    def _init_tables(self):
        """Ensure Bronze and dead-letter Delta tables exist.

        Bronze is partitioned by ``event_date, event_type``. Dead-letter
        uses the same schema but is unpartitioned (error_timestamp is added
        at write time). If Delta is unavailable (unit tests) this is a no-op.
        """
        self._init_delta_table(
            self.config["bronze_path"],
            CLEAN_EVENT_SCHEMA,
            partition_by=["event_date", "event_type"],
        )
        self._init_delta_table(
            self.config["dead_letter_path"],
            CLEAN_EVENT_SCHEMA,
        )

    @retry(max_attempts=3, delay=0.5)
    def read_from_kafka(self) -> DataFrame:
        """
        Create a streaming DataFrame that reads raw JSON from Kafka.

        Returns a DataFrame with columns:
          - value (binary): raw JSON event
          - topic, partition, offset, timestamp (kafka metadata)
        """
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config["kafka_bootstrap_servers"])
            .option("subscribe", self.config["kafka_topic"])
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "10000")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) as raw_value", "timestamp as kafka_timestamp")
        )

    def parse_events(self, raw_df: DataFrame) -> DataFrame:
        """
        Parse raw JSON strings into structured fields.

        Steps:
          1. Parse JSON using the raw schema (all string fields)
          2. Cast numeric fields (amount, error_code, etc.)
          3. Cast timestamp string to TimestampType
          4. Add event_date partition column

        Bad parse results (NULL struct) become null rows.
        """
        parsed = raw_df.select(
            from_json(col("raw_value"), RAW_EVENT_SCHEMA).alias("event")
        ).select("event.*")

        # Cast numeric fields
        parsed = _cast_numeric_fields(parsed)

        # Cast timestamp
        parsed = parsed.withColumn(
            "timestamp", to_timestamp(col("timestamp"))
        )

        # Add partition columns
        parsed = _enrich_partition_columns(parsed)

        return parsed

    def write_batch(self, df: DataFrame, epoch_id: int):
        """
        foreachBatch callback: validate records, split clean vs dead-letter,
        and write each to the corresponding Delta table.

        Args:
            df: Micro-batch DataFrame from the streaming query.
            epoch_id: Micro-batch ID (for logging).
        """
        if df.count() == 0:
            logger.debug(f"Epoch {epoch_id}: empty batch, skipping")
            return

        clean_df, dead_letter_df = _separate_bad_records(df)

        # Write clean records
        clean_count = clean_df.count()
        if clean_count > 0:
            logger.info(
                f"Epoch {epoch_id}: writing {clean_count} clean events to Bronze"
            )
            try:
                (
                    clean_df.write.format("delta")
                    .mode("append")
                    .option("delta.autoOptimize.optimizeWrite", "true")
                    .partitionBy("event_date", "event_type")
                    .save(self.config["bronze_path"])
                )
            except Exception as e:
                logger.error(f"Epoch {epoch_id}: failed to write clean events: {e}")
                raise

        # Write dead-letter records
        dead_count = dead_letter_df.count()
        if dead_count > 0:
            logger.warning(
                f"Epoch {epoch_id}: quarantining {dead_count} malformed records to dead-letter"
            )
            try:
                dead_letter_df = dead_letter_df.withColumn(
                    "error_timestamp",
                    lit(datetime.now(timezone.utc).isoformat()),
                )
                (
                    dead_letter_df.write.format("delta")
                    .mode("append")
                    .save(self.config["dead_letter_path"])
                )
            except Exception as e:
                logger.error(f"Epoch {epoch_id}: failed to write dead-letter records: {e}")
                raise

    def run_stream(self):
        """Run the streaming pipeline continuously."""
        self._ensure_tables()
        logger.info(
            f"Starting Bronze streaming pipeline:\n"
            f"  Kafka topic: {self.config['kafka_topic']}\n"
            f"  Target:      {self.config['bronze_path']}\n"
            f"  Checkpoint:  {self.config['checkpoint_path']}\n"
            f"  Dead-letter: {self.config['dead_letter_path']}\n"
            f"  Trigger:     {self.config['trigger_interval']}"
        )

        raw_df = self.read_from_kafka()
        events_df = self.parse_events(raw_df)

        query = (
            events_df.writeStream.foreachBatch(self.write_batch)
            .outputMode("update")
            .option("checkpointLocation", self.config["checkpoint_path"])
            .trigger(processingTime=self.config["trigger_interval"])
            .queryName(self.config["query_name"])
            .start()
        )

        logger.info(f"Streaming query '{self.config['query_name']}' started")
        query.awaitTermination()

    def run_batch(self, input_path: str, output_path: Optional[str] = None):
        """
        Run in batch mode for testing or historical backfill.

        Args:
            input_path: Path to a JSONL file (one JSON event per line).
            output_path: Optional override for the Bronze output path.
        """
        self._ensure_tables()
        target_path = output_path or self.config["bronze_path"]
        logger.info(
            f"Running Bronze batch job:\n"
            f"  Input:  {input_path}\n"
            f"  Output: {target_path}"
        )

        raw_df = self.spark.read.text(input_path).withColumnRenamed("value", "raw_value")
        events_df = self.parse_events(raw_df)
        clean_df, dead_letter_df = _separate_bad_records(events_df)

        clean_count = clean_df.count()
        dead_count = dead_letter_df.count()
        logger.info(f"Batch results — clean: {clean_count}, dead-letter: {dead_count}")

        if clean_count > 0:
            (
                clean_df.write.format("delta")
                .mode("append")
                .partitionBy("event_date", "event_type")
                .save(target_path)
            )

        if dead_count > 0:
            dead_letter_df = dead_letter_df.withColumn(
                "error_timestamp",
                lit(datetime.now(timezone.utc).isoformat()),
            )
            dead_letter_df.write.format("delta").mode("append").save(
                self.config["dead_letter_path"]
            )

        logger.info("Batch job complete")
        return {"clean": clean_count, "dead_letter": dead_count}

# ─── CLI Entry Point ──────────────────────────────────────────────────


def parse_schema(schema_str: str) -> StructType:
    """Parse a JSON schema string into a StructType. For testing convenience."""
    return StructType.fromJson(json.loads(schema_str))


def main():
    parser = argparse.ArgumentParser(description="Bronze Streaming Layer — Kafka → Delta Lake")
    parser.add_argument(
        "--mode",
        choices=["streaming", "batch"],
        default="streaming",
        help="Run mode: continuous streaming or one-time batch",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Input path for batch mode (JSONL file)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path override (optional)",
    )
    parser.add_argument(
        "--kafka-broker",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--trigger-interval",
        default="10 seconds",
        help="Streaming trigger interval (e.g. '5 seconds', '1 minute')",
    )

    args = parser.parse_args()

    pipeline = BronzePipeline(
        config={
            "kafka_bootstrap_servers": args.kafka_broker,
            "trigger_interval": args.trigger_interval,
        }
    )

    try:
        if args.mode == "streaming":
            pipeline.run_stream()
        elif args.mode == "batch":
            if not args.input:
                parser.error("--input is required for batch mode")
            pipeline.run_batch(args.input, args.output)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
