"""Reusable Bronze validation helpers for Databricks notebooks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import load_bronze_dataset
from src.utils.schemas import dataset_names, important_columns, primary_key_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def duplicate_count(dataframe: DataFrame, key_columns: tuple[str, ...]) -> int:
    """Count duplicate rows for the most likely primary key."""
    total_rows = dataframe.count()
    distinct_rows = dataframe.select(*key_columns).distinct().count()
    return total_rows - distinct_rows


def null_count_summary(dataframe: DataFrame, columns: tuple[str, ...]) -> list[dict[str, int]]:
    """Return null-count stats for a small set of important columns."""
    from pyspark.sql import functions as F

    metrics = dataframe.agg(
        *[
            F.sum(F.when(F.col(column).isNull() | (F.col(column) == ""), 1).otherwise(0)).alias(column)
            for column in columns
        ]
    ).collect()[0].asDict()
    return [{"column_name": column, "null_or_blank_count": int(metrics[column])} for column in columns]


def dataset_validation_summary(
    spark: SparkSession,
    config: dict[str, Any],
    dataset_name: str,
) -> dict[str, Any]:
    """Collect validation metrics for one Bronze dataset."""
    dataframe = load_bronze_dataset(spark, config, dataset_name)
    keys = primary_key_columns(dataset_name)
    important = important_columns(dataset_name)
    timestamp_bounds = dataframe.agg({"_ingested_at": "min"}).collect()[0][0], dataframe.agg(
        {"_ingested_at": "max"}
    ).collect()[0][0]

    return {
        "dataset_name": dataset_name,
        "row_count": dataframe.count(),
        "column_count": len(dataframe.columns),
        "duplicate_key_count": duplicate_count(dataframe, keys),
        "source_file_count": dataframe.select("_source_file").distinct().count(),
        "min_ingested_at": timestamp_bounds[0],
        "max_ingested_at": timestamp_bounds[1],
        "null_counts": null_count_summary(dataframe, important),
    }


def load_all_bronze_datasets(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Load all Bronze datasets using the configured storage mode."""
    return {dataset_name: load_bronze_dataset(spark, config, dataset_name) for dataset_name in dataset_names()}


def preliminary_match_rates(spark: SparkSession, config: dict[str, Any]):
    """Return preliminary match-rate dataframes for the expected join keys."""
    from pyspark.sql import functions as F

    datasets = load_all_bronze_datasets(spark, config)
    transactions = datasets["transactions"]
    cards = datasets["cards"]
    users = datasets["users"]
    fraud_labels = datasets["fraud_labels"]
    mcc_codes = datasets["mcc_codes"]

    checks = [
        (
            "transactions_to_cards",
            transactions.select("card_id").distinct(),
            cards.select(F.col("id").alias("card_id")).distinct(),
            "card_id",
        ),
        (
            "cards_to_users",
            cards.select("client_id").distinct(),
            users.select(F.col("id").alias("client_id")).distinct(),
            "client_id",
        ),
        (
            "transactions_to_users",
            transactions.select("client_id").distinct(),
            users.select(F.col("id").alias("client_id")).distinct(),
            "client_id",
        ),
        (
            "transactions_to_fraud_labels",
            transactions.select(F.col("id").alias("transaction_id")).distinct(),
            fraud_labels.select("transaction_id").distinct(),
            "transaction_id",
        ),
        (
            "transactions_to_mcc_codes",
            transactions.select("mcc").distinct(),
            mcc_codes.select("mcc").distinct(),
            "mcc",
        ),
    ]

    rows: list[dict[str, Any]] = []
    for check_name, left_df, right_df, key_column in checks:
        total_keys = left_df.count()
        matched_keys = left_df.join(right_df, on=key_column, how="left_semi").count()
        rows.append(
            {
                "check_name": check_name,
                "key_column": key_column,
                "total_left_distinct_keys": total_keys,
                "matched_distinct_keys": matched_keys,
                "match_rate": float(matched_keys / total_keys) if total_keys else None,
            }
        )

    return spark.createDataFrame(rows)
