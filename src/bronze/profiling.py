"""Raw-data profiling helpers for exploratory Bronze analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import load_bronze_dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def _parsed_amount_expression(column_name: str):
    """Return a Spark expression that parses the raw amount string without changing source data."""
    from pyspark.sql import functions as F

    cleaned = F.regexp_replace(F.col(column_name), r"[$,]", "")
    cleaned = F.when(
        F.col(column_name).rlike(r"^\(.*\)$"),
        F.concat(F.lit("-"), F.regexp_replace(cleaned, r"[()]", "")),
    ).otherwise(cleaned)
    return cleaned.cast("double")


def load_profile_inputs(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Load all Bronze tables used by the profiling notebook."""
    return {
        "transactions": load_bronze_dataset(spark, config, "transactions"),
        "users": load_bronze_dataset(spark, config, "users"),
        "cards": load_bronze_dataset(spark, config, "cards"),
        "fraud_labels": load_bronze_dataset(spark, config, "fraud_labels"),
        "mcc_codes": load_bronze_dataset(spark, config, "mcc_codes"),
    }


def transactions_overview(transactions: DataFrame):
    """Return a compact one-row transaction overview."""
    from pyspark.sql import functions as F

    amount_double = _parsed_amount_expression("amount")
    return transactions.agg(
        F.count(F.lit(1)).alias("total_row_count"),
        F.countDistinct("id").alias("distinct_transaction_ids"),
        F.min("date").alias("min_raw_date"),
        F.max("date").alias("max_raw_date"),
        F.sum(F.when(F.col("amount").isNotNull() & amount_double.isNull(), 1).otherwise(0)).alias(
            "amount_parse_failure_estimate"
        ),
        F.countDistinct("client_id").alias("distinct_client_ids"),
        F.countDistinct("card_id").alias("distinct_card_ids"),
        F.countDistinct("mcc").alias("distinct_mcc_codes"),
    )


def raw_missingness(dataframe: DataFrame):
    """Return null-or-blank counts for every column in a dataframe."""
    from pyspark.sql import functions as F

    aggregations = [
        F.sum(F.when(F.col(column).isNull() | (F.col(column) == ""), 1).otherwise(0)).alias(column)
        for column in dataframe.columns
    ]
    row = dataframe.agg(*aggregations).collect()[0].asDict()
    return [{"column_name": column, "null_or_blank_count": int(row[column])} for column in dataframe.columns]


def top_frequency(dataframe: DataFrame, column_name: str, limit: int = 20) -> DataFrame:
    """Return a top-frequency table for one raw column."""
    return dataframe.groupBy(column_name).count().orderBy("count", ascending=False).limit(limit)


def key_quality_overview(dataframe: DataFrame, key_column: str) -> dict[str, int]:
    """Return row, distinct-key, duplicate-key, and missing-key counts."""
    from pyspark.sql import functions as F

    total_rows = dataframe.count()
    distinct_keys = dataframe.select(key_column).distinct().count()
    missing_keys = dataframe.filter(F.col(key_column).isNull() | (F.col(key_column) == "")).count()
    return {
        "total_rows": total_rows,
        "distinct_keys": distinct_keys,
        "duplicate_keys": total_rows - distinct_keys,
        "missing_keys": missing_keys,
    }


def fraud_label_overview(transactions: DataFrame, fraud_labels: DataFrame) -> dict[str, Any]:
    """Return high-level fraud-label coverage and duplication metrics."""
    from pyspark.sql import functions as F

    total_labels = fraud_labels.count()
    fraud_count = fraud_labels.filter(F.col("is_fraud") == "Yes").count()
    nonfraud_count = fraud_labels.filter(F.col("is_fraud") == "No").count()
    duplicate_label_ids = total_labels - fraud_labels.select("transaction_id").distinct().count()
    unmatched_label_ids = (
        fraud_labels.select("transaction_id")
        .distinct()
        .join(transactions.select(F.col("id").alias("transaction_id")).distinct(), on="transaction_id", how="left_anti")
        .count()
    )
    matched_transactions = (
        transactions.select(F.col("id").alias("transaction_id")).distinct()
        .join(fraud_labels.select("transaction_id").distinct(), on="transaction_id", how="left_semi")
        .count()
    )
    total_transactions = transactions.select("id").distinct().count()

    return {
        "total_labels": total_labels,
        "fraud_count": fraud_count,
        "nonfraud_count": nonfraud_count,
        "natural_fraud_rate": float(fraud_count / total_labels) if total_labels else None,
        "duplicate_label_ids": duplicate_label_ids,
        "unmatched_label_ids": unmatched_label_ids,
        "transaction_label_coverage": float(matched_transactions / total_transactions)
        if total_transactions
        else None,
    }


def exploratory_fraud_relationships(
    transactions: DataFrame,
    fraud_labels: DataFrame,
    mcc_codes: DataFrame,
) -> dict[str, DataFrame]:
    """Return exploratory fraud-rate tables for notebook display only."""
    from pyspark.sql import functions as F

    labeled = (
        transactions.join(
            fraud_labels,
            transactions["id"] == fraud_labels["transaction_id"],
            how="inner",
        )
        .drop("transaction_id")
        .join(mcc_codes, on="mcc", how="left")
        .withColumn("fraud_flag", F.when(F.col("is_fraud") == "Yes", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("parsed_timestamp", F.to_timestamp("date"))
        .withColumn("hour", F.hour("parsed_timestamp"))
        .withColumn("weekday", F.date_format("parsed_timestamp", "E"))
        .withColumn("is_weekend", F.dayofweek("parsed_timestamp").isin([1, 7]))
        .withColumn("raw_amount_double", _parsed_amount_expression("amount"))
        .withColumn(
            "merchant_location_category",
            F.when(F.col("merchant_state").isNull() | (F.col("merchant_state") == ""), F.lit("unknown_or_blank"))
            .otherwise(F.lit("state_present")),
        )
        .withColumn(
            "amount_bucket",
            F.when(F.col("raw_amount_double").isNull(), F.lit("parse_failed"))
            .when(F.col("raw_amount_double") < 0, F.lit("negative"))
            .when(F.col("raw_amount_double") < 25, F.lit("0_to_24_99"))
            .when(F.col("raw_amount_double") < 100, F.lit("25_to_99_99"))
            .when(F.col("raw_amount_double") < 500, F.lit("100_to_499_99"))
            .otherwise(F.lit("500_plus")),
        )
    )

    def fraud_rate_by(column_name: str) -> DataFrame:
        return (
            labeled.groupBy(column_name)
            .agg(
                F.count(F.lit(1)).alias("transaction_count"),
                F.sum("fraud_flag").alias("fraud_count"),
                F.avg("fraud_flag").alias("fraud_rate"),
            )
            .orderBy("fraud_rate", ascending=False)
        )

    return {
        "by_hour": fraud_rate_by("hour"),
        "by_weekday": fraud_rate_by("weekday"),
        "by_weekend": fraud_rate_by("is_weekend"),
        "by_transaction_type": fraud_rate_by("use_chip"),
        "by_error_category": fraud_rate_by("errors"),
        "by_mcc": fraud_rate_by("mcc"),
        "by_mcc_category": fraud_rate_by("mcc_category"),
        "by_merchant_location_category": fraud_rate_by("merchant_location_category"),
        "by_amount_bucket": fraud_rate_by("amount_bucket"),
    }
