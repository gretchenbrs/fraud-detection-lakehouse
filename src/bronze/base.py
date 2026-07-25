"""Shared helpers for Bronze ingestion, validation, and loading."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.utils.config import bronze_dataset_runtime
from src.utils.schemas import build_csv_struct_type, ordered_raw_columns, validate_required_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def read_csv_dataset(spark: SparkSession, source_path: str, dataset_name: str) -> DataFrame:
    """Validate a CSV header and load the dataset with an explicit all-string schema."""
    discovered_columns = (
        spark.read.option("header", True).option("inferSchema", False).csv(source_path).columns
    )
    validate_required_columns(discovered_columns, ordered_raw_columns(dataset_name), dataset_name)

    dataframe = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(build_csv_struct_type(dataset_name))
        .csv(source_path)
    )
    return add_ingestion_metadata(dataframe)


def add_ingestion_metadata(
    dataframe: DataFrame,
    source_file_column: str | None = None,
) -> DataFrame:
    """Append standard Bronze ingestion metadata columns."""
    from pyspark.sql import functions as F

    if source_file_column:
        source_expr = F.col(source_file_column)
    else:
        source_expr = F.expr("_metadata.file_path")
    result = (
        dataframe.withColumn("_source_file", source_expr)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date(F.col("_ingested_at")))
    )
    if source_file_column:
        result = result.drop(source_file_column)
    return result


def write_bronze_dataset(
    dataframe: DataFrame,
    runtime_config: dict[str, str],
) -> None:
    """Persist a Bronze dataset to a managed table or Delta path."""
    writer = dataframe.write.format(runtime_config["write_format"]).mode(runtime_config["write_mode"])

    if runtime_config["storage_mode"] == "unity_catalog":
        writer.saveAsTable(runtime_config["target_table"])
    else:
        writer.save(runtime_config["target_path"])


def load_bronze_dataset(
    spark: SparkSession,
    config: dict[str, Any],
    dataset_name: str,
) -> DataFrame:
    """Load a previously written Bronze dataset using the configured storage mode."""
    runtime_config = bronze_dataset_runtime(config, dataset_name)
    if runtime_config["storage_mode"] == "unity_catalog":
        return spark.table(runtime_config["target_table"])
    return spark.read.format(runtime_config["write_format"]).load(runtime_config["target_path"])


def log_ingestion_result(
    dataset_name: str,
    source_path: str,
    row_count: int,
    column_count: int,
    target_identifier: str,
    storage_mode: str,
) -> None:
    """Print a compact ingestion summary for notebook execution."""
    print(
        f"[bronze:{dataset_name}] source={source_path} rows={row_count} "
        f"columns={column_count} storage_mode={storage_mode} target={target_identifier}"
    )
