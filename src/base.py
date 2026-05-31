"""
RealtimeStream — Shared Pipeline Base

Provides:
  - BasePipeline: common Spark session creation, table initialization, cleanup
  - retry(): decorator for resilient operations (Kafka, Delta writes)
  - validate_date(): CLI argument validation utilities

All pipeline stages (Bronze, Silver, Gold) inherit from BasePipeline to
eliminate code duplication and ensure consistent error handling.
"""

from __future__ import annotations

import functools
import logging
import re
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from src.config import get_spark_config

logger = logging.getLogger(__name__)


# ─── Retry Utility ─────────────────────────────────────────────────────

_DEFAULT_RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    Exception,  # wide net; use with care
)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
) -> Callable:
    """
    Decorator that retries a function on failure with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts (including the first).
        delay: Initial delay in seconds between retries.
        backoff: Multiplier for delay after each retry.
        retryable_exceptions: Tuple of exception types to retry on.
            Defaults to (Exception,) — retries all exceptions.

    Usage::

        @retry(max_attempts=3, delay=0.5)
        def connect_to_kafka():
            return KafkaProducer(...)

    Returns:
        Decorated function with retry behaviour.
    """
    if retryable_exceptions is None:
        retryable_exceptions = _DEFAULT_RETRYABLE_EXCEPTIONS

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )

            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator


# ─── Date Validation ──────────────────────────────────────────────────


DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_date(date_str: str) -> str:
    """
    Validate and normalize a date string in ``yyyy-MM-dd`` format.

    Args:
        date_str: The date string to validate.

    Returns:
        The same date string if valid.

    Raises:
        ValueError: If the format or value is invalid.
    """
    if not DATE_PATTERN.match(date_str):
        raise ValueError(
            f"Invalid date format: '{date_str}'. Expected yyyy-MM-dd (e.g. 2026-05-29)."
        )
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            f"Invalid date value: '{date_str}'. {e}"
        ) from e
    return date_str


def validate_positive_int(value: str, name: str = "value") -> int:
    """
    Parse and validate a positive integer argument.

    Args:
        value: The string to parse.
        name: Human-readable name for error messages.

    Returns:
        The parsed integer.

    Raises:
        ValueError: If the value is not a positive integer.
    """
    try:
        parsed = int(value)
    except (ValueError, TypeError) as e:
        raise ValueError(f"{name} must be an integer, got '{value}'.") from e
    if parsed <= 0:
        raise ValueError(f"{name} must be positive, got {parsed}.")
    return parsed


def validate_rate(value: Optional[str]) -> Optional[int]:
    """Validate and parse an events-per-second CLI argument."""
    if value is None:
        return None
    return validate_positive_int(value, "event rate")


# ─── Base Pipeline ────────────────────────────────────────────────────


class BasePipeline:
    """
    Shared base for Bronze, Silver, and Gold pipeline classes.

    Provides:
      - ``_create_spark_session()`` — standardised Spark session setup
      - ``_init_delta_table()`` — create a Delta table if it doesn't exist
      - ``_ensure_tables()`` — lazy initialisation of all required tables
      - ``stop()`` — clean up Spark session if owned

    Subclasses call ``super().__init__(...)`` and define their own
    ``_init_tables()`` to register their table schemas.
    """

    TABLE_REGISTRY: Dict[str, StructType] = {}

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
        app_name: str = "RealtimeStream",
    ):
        self.config: Dict[str, Any] = config or {}
        if spark is not None:
            self.spark = spark
            self._owns_spark = False
        else:
            self.spark = self._create_spark_session(app_name)
            self._owns_spark = True
        self._tables_initialized = False

    # ── Spark Session ──────────────────────────────────────────────

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create and configure a Spark session with Delta Lake support."""
        spark_config = get_spark_config(app_name)
        builder = SparkSession.builder.appName(spark_config["spark.app.name"])

        for key, value in spark_config.items():
            if key != "spark.app.name":
                builder = builder.config(key, value)

        builder = builder.config("spark.sql.adaptive.enabled", "true")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created for %s", app_name)
        return spark

    # ── Delta Table Lifecycle ──────────────────────────────────────

    def _init_delta_table(
        self,
        table_path: str,
        schema: StructType,
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """
        Create a Delta table at ``table_path`` if it does not already exist.

        If SparkContext is unavailable (e.g. in unit tests) this is a no-op.
        """
        try:
            if self.spark._jsc is None:
                logger.warning(
                    "SparkContext not available, skipping Delta init: %s", table_path
                )
                return
        except Exception:
            return

        try:
            # Probe existing table
            self.spark.read.format("delta").load(table_path).limit(1)
            logger.info("Delta table already exists: %s", table_path)
        except Exception:
            logger.info("Creating Delta table at: %s", table_path)
            try:
                empty_df = self.spark.createDataFrame([], schema)
                writer = empty_df.write.format("delta").mode("overwrite").option(
                    "delta.autoOptimize.optimizeWrite", "true"
                )
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.save(table_path)
                logger.info("Created Delta table: %s", table_path)
            except Exception as e:
                logger.warning("Could not create Delta table at %s: %s", table_path, e)

    def _ensure_tables(self) -> None:
        """Lazily initialise all tables defined in the subclass registry."""
        if not self._tables_initialized:
            self._init_tables()
            self._tables_initialized = True

    def _init_tables(self) -> None:
        """
        Subclasses override this to call ``_init_delta_table()`` for each
        table they need.
        """
        for path_key, schema in self.TABLE_REGISTRY.items():
            table_path = self.config.get(path_key)
            if table_path:
                self._init_delta_table(table_path, schema)

    # ── Lifecycle ──────────────────────────────────────────────────

    def stop(self) -> None:
        """Stop the Spark session if this pipeline instance created it."""
        if self._owns_spark and self.spark:
            try:
                self.spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.warning("Error stopping Spark session: %s", e)
