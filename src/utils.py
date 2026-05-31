"""
RealtimeStream — Shared Utilities (no pyspark dependency)

Provides:
  - validate_date():       CLI date argument validation
  - validate_positive_int():  CLI positive integer validation
  - validate_rate():       CLI events-per-second validation

These are extracted from ``base.py`` so that non-Spark modules (e.g.
``data_generator``) can use them without pulling in pyspark.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional


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
