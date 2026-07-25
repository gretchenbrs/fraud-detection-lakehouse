"""Bronze ingestion for card records."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import read_csv_dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def read_cards(spark: SparkSession, source_path: str) -> DataFrame:
    """Load raw cards into Bronze with schema validation."""
    return read_csv_dataset(spark, source_path=source_path, dataset_name="cards")
