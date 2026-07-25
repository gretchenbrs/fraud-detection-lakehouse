"""Spark-native cleaning expressions shared by Silver transformations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import Column
else:
    Column = Any


def blank_to_null(column: Column) -> Column:
    """Trim a string column and convert blank values to null."""
    from pyspark.sql import functions as F

    trimmed = F.trim(column.cast("string"))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def currency_to_decimal(column: Column) -> Column:
    """Parse signed currency text, commas, symbols, and accounting parentheses."""
    from pyspark.sql import functions as F

    raw = blank_to_null(column)
    numeric_text = F.regexp_replace(raw, r"[^0-9.\-]", "")
    parenthesized = raw.rlike(r"^\s*\(.*\)\s*$")
    normalized = F.when(
        parenthesized & ~numeric_text.startswith("-"),
        F.concat(F.lit("-"), numeric_text),
    ).otherwise(numeric_text)
    return F.when(raw.isNull(), F.lit(None)).otherwise(normalized.cast("decimal(18,2)"))


def currency_parse_failed(column: Column) -> Column:
    """Flag non-blank currency text that cannot be parsed."""
    raw = blank_to_null(column)
    return raw.isNotNull() & currency_to_decimal(column).isNull()


def yes_no_to_boolean(column: Column) -> Column:
    """Map common yes/no strings to boolean while preserving unknowns as null."""
    from pyspark.sql import functions as F

    normalized = F.lower(blank_to_null(column))
    return (
        F.when(normalized.isin("yes", "y", "true", "1"), F.lit(True))
        .when(normalized.isin("no", "n", "false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )
