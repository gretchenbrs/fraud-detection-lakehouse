"""Chronological train, validation, and test assignment."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


VALID_SPLITS = ("train", "validation", "test")


def validate_split_dates(train_end_date: str, validation_end_date: str) -> None:
    """Validate deterministic chronological split boundaries."""
    train_end = date.fromisoformat(train_end_date)
    validation_end = date.fromisoformat(validation_end_date)
    if train_end >= validation_end:
        raise ValueError("train_end_date must be before validation_end_date.")


def assign_time_split(
    dataframe: DataFrame,
    train_end_date: str,
    validation_end_date: str,
) -> DataFrame:
    """Keep labeled rows and assign them to non-overlapping chronological splits."""
    from pyspark.sql import functions as F

    validate_split_dates(train_end_date, validation_end_date)
    transaction_date = F.col("transaction_date")
    split = (
        F.when(transaction_date <= F.to_date(F.lit(train_end_date)), F.lit("train"))
        .when(
            transaction_date <= F.to_date(F.lit(validation_end_date)),
            F.lit("validation"),
        )
        .when(transaction_date.isNotNull(), F.lit("test"))
        .otherwise(F.lit(None).cast("string"))
    )
    return dataframe.filter(F.col("is_fraud").isNotNull()).withColumn("data_split", split)


def split_distribution(dataframe: DataFrame) -> DataFrame:
    """Return row counts and natural fraud prevalence for each split."""
    from pyspark.sql import functions as F

    return (
        dataframe.groupBy("data_split")
        .agg(
            F.count("*").alias("row_count"),
            F.sum(F.col("is_fraud").cast("long")).alias("fraud_count"),
            F.avg(F.col("is_fraud").cast("double")).alias("fraud_rate"),
            F.min("transaction_date").alias("min_transaction_date"),
            F.max("transaction_date").alias("max_transaction_date"),
        )
        .orderBy(
            F.when(F.col("data_split") == "train", 1)
            .when(F.col("data_split") == "validation", 2)
            .when(F.col("data_split") == "test", 3)
            .otherwise(4)
        )
    )
