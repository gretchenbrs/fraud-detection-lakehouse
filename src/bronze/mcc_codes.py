"""Bronze ingestion for MCC code mappings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import add_ingestion_metadata
from src.utils.schemas import build_json_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def read_mcc_codes(spark: SparkSession, source_path: str) -> DataFrame:
    """Explode the MCC mapping JSON into a tabular Bronze dataframe."""
    from pyspark.sql import functions as F

    raw_json = spark.sparkContext.wholeTextFiles(source_path).toDF(
        ["_raw_source_file", "raw_json"]
    )
    parsed = raw_json.select(
        F.col("_raw_source_file"),
        F.from_json(F.col("raw_json"), build_json_schema("mcc_codes")).alias("payload"),
    )

    malformed_count = parsed.filter(F.col("payload").isNull()).count()
    if malformed_count:
        raise ValueError(
            f"mcc_codes JSON could not be parsed from {source_path}. "
            f"Malformed top-level payload count: {malformed_count}"
        )

    exploded = parsed.select(
        F.col("_raw_source_file"),
        F.explode_outer(F.col("payload")).alias("mcc", "mcc_category"),
    )
    return add_ingestion_metadata(
        exploded.select("mcc", "mcc_category", "_raw_source_file"),
        source_file_column="_raw_source_file",
    )
