"""Bronze ingestion for fraud-label JSON."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import add_ingestion_metadata
from src.utils.schemas import build_json_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def read_fraud_labels(spark: SparkSession, source_path: str) -> DataFrame:
    """Explode the fraud label map into a tabular Bronze dataframe."""
    from pyspark.sql import functions as F

    raw_json = spark.sparkContext.wholeTextFiles(source_path).toDF(
        ["_raw_source_file", "raw_json"]
    )
    parsed = raw_json.select(
        F.col("_raw_source_file"),
        F.from_json(F.col("raw_json"), build_json_schema("fraud_labels")).alias("payload"),
    )

    malformed_count = parsed.filter(F.col("payload").isNull()).count()
    if malformed_count:
        raise ValueError(
            f"fraud_labels JSON could not be parsed from {source_path}. "
            f"Malformed top-level payload count: {malformed_count}"
        )

    exploded = parsed.select(
        F.col("_raw_source_file"),
        F.explode_outer(F.col("payload.target")).alias("transaction_id", "is_fraud"),
    )
    return add_ingestion_metadata(
        exploded.select("transaction_id", "is_fraud", "_raw_source_file"),
        source_file_column="_raw_source_file",
    )
