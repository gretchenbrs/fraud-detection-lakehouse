"""Silver-layer orchestration and verified-key enrichment."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import load_bronze_dataset
from src.silver.dimensions import (
    transform_cards,
    transform_fraud_labels,
    transform_mcc_codes,
    transform_users,
)
from src.silver.transactions import transform_transactions
from src.utils.config import SILVER_DATASET_NAMES, silver_dataset_runtime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def build_silver_datasets(
    spark: SparkSession,
    project_config: dict[str, Any],
) -> dict[str, DataFrame]:
    """Load Bronze tables, clean dimensions, and build the enriched transaction fact."""
    from pyspark.sql import functions as F

    users = transform_users(load_bronze_dataset(spark, project_config, "users"))
    cards = transform_cards(load_bronze_dataset(spark, project_config, "cards"))
    fraud_labels = transform_fraud_labels(
        load_bronze_dataset(spark, project_config, "fraud_labels")
    )
    mcc_codes = transform_mcc_codes(load_bronze_dataset(spark, project_config, "mcc_codes"))
    transactions = transform_transactions(
        load_bronze_dataset(spark, project_config, "transactions"),
        night_start_hour=project_config["silver"]["night_start_hour"],
        night_end_hour=project_config["silver"]["night_end_hour"],
    )

    enriched_transactions = (
        transactions.join(
            cards.withColumn("_card_record_matched", F.lit(True)),
            on="card_id",
            how="left",
        )
        .join(
            users.withColumn("_user_record_matched", F.lit(True)),
            on="user_id",
            how="left",
        )
        .join(
            mcc_codes.withColumn("_mcc_record_matched", F.lit(True)),
            on="mcc",
            how="left",
        )
        .join(
            fraud_labels.withColumn("_fraud_label_matched", F.lit(True)),
            on="transaction_id",
            how="left",
        )
        .withColumn(
            "card_user_matches_transaction_user",
            F.when(F.col("card_user_id").isNull(), F.lit(None).cast("boolean")).otherwise(
                F.col("card_user_id") == F.col("user_id")
            ),
        )
        .withColumn("card_record_matched", F.coalesce(F.col("_card_record_matched"), F.lit(False)))
        .withColumn("user_record_matched", F.coalesce(F.col("_user_record_matched"), F.lit(False)))
        .withColumn("mcc_record_matched", F.coalesce(F.col("_mcc_record_matched"), F.lit(False)))
        .withColumn("fraud_label_matched", F.coalesce(F.col("_fraud_label_matched"), F.lit(False)))
        .drop(
            "_card_record_matched",
            "_user_record_matched",
            "_mcc_record_matched",
            "_fraud_label_matched",
        )
    )

    return {
        "users": users,
        "cards": cards,
        "fraud_labels": fraud_labels,
        "mcc_codes": mcc_codes,
        "transactions": enriched_transactions,
    }


def write_silver_dataset(
    dataframe: DataFrame,
    runtime_config: dict[str, str],
) -> None:
    """Persist one Silver dataset to a managed table or Delta path."""
    writer = dataframe.write.format(runtime_config["write_format"]).mode(
        runtime_config["write_mode"]
    )
    if runtime_config["write_mode"] == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if runtime_config["storage_mode"] == "unity_catalog":
        writer.saveAsTable(runtime_config["target_table"])
    else:
        writer.save(runtime_config["target_path"])


def run_silver_pipeline(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
):
    """Build all Silver datasets and optionally persist them."""
    datasets = build_silver_datasets(spark, project_config)
    summary_rows: list[dict[str, Any]] = []

    for dataset_name in SILVER_DATASET_NAMES:
        dataframe = datasets[dataset_name]
        runtime = silver_dataset_runtime(project_config, dataset_name)
        target = runtime["target_table"] or runtime["target_path"]
        if write_output:
            write_silver_dataset(dataframe, runtime)
        print(
            f"[silver:{dataset_name}] columns={len(dataframe.columns)} "
            f"storage_mode={runtime['storage_mode']} target={target}"
        )
        summary_rows.append(
            {
                "dataset_name": dataset_name,
                "column_count": len(dataframe.columns),
                "storage_mode": runtime["storage_mode"],
                "target_table": runtime["target_table"],
                "target_path": runtime["target_path"],
                "write_mode": runtime["write_mode"],
            }
        )

    return spark.createDataFrame(summary_rows)


def silver_join_quality_summary(spark: SparkSession, project_config: dict[str, Any]):
    """Summarize enrichment coverage without changing or filtering the fact table."""
    from pyspark.sql import functions as F

    runtime = silver_dataset_runtime(project_config, "transactions")
    if runtime["storage_mode"] == "unity_catalog":
        transactions = spark.table(runtime["target_table"])
    else:
        transactions = spark.read.format(runtime["write_format"]).load(runtime["target_path"])

    return transactions.agg(
        F.count("*").alias("transaction_count"),
        F.sum((~F.col("card_record_matched")).cast("long")).alias("unmatched_card_count"),
        F.sum((~F.col("user_record_matched")).cast("long")).alias("unmatched_user_count"),
        F.sum((~F.col("mcc_record_matched")).cast("long")).alias("unmatched_mcc_count"),
        F.sum((~F.col("fraud_label_matched")).cast("long")).alias("unlabeled_transaction_count"),
        F.sum((F.col("card_user_matches_transaction_user") == F.lit(False)).cast("long")).alias(
            "card_user_mismatch_count"
        ),
        F.sum(F.col("amount_parse_failed").cast("long")).alias("amount_parse_failure_count"),
        F.sum(F.col("timestamp_parse_failed").cast("long")).alias(
            "timestamp_parse_failure_count"
        ),
    )
