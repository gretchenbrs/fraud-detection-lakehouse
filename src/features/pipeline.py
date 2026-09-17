"""Feature-layer orchestration with leakage-aware fitting."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.features.engineering import (
    add_behavioral_features,
    apply_mcc_fraud_rates,
    fit_training_mcc_fraud_rates,
)
from src.features.splits import assign_time_split
from src.utils.config import FEATURE_DATASET_NAMES, feature_dataset_runtime, silver_dataset_runtime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def load_silver_transactions(
    spark: SparkSession,
    project_config: dict[str, Any],
) -> DataFrame:
    """Load the enriched Silver transaction table."""
    runtime = silver_dataset_runtime(project_config, "transactions")
    if runtime["storage_mode"] == "unity_catalog":
        return spark.table(runtime["target_table"])
    return spark.read.format(runtime["write_format"]).load(runtime["target_path"])


def build_feature_datasets(
    spark: SparkSession,
    project_config: dict[str, Any],
) -> dict[str, DataFrame]:
    """Build split assignments, training MCC mapping, and model features."""
    from pyspark.sql import functions as F

    feature_cfg = project_config["features"]
    split_transactions = assign_time_split(
        load_silver_transactions(spark, project_config),
        train_end_date=feature_cfg["train_end_date"],
        validation_end_date=feature_cfg["validation_end_date"],
    )
    invalid_split_exists = (
        split_transactions.filter(F.col("data_split").isNull()).limit(1).count() > 0
    )
    if invalid_split_exists:
        raise ValueError("At least one labeled transaction could not be assigned a time split.")

    split_assignments = split_transactions.select(
        "transaction_id",
        "transaction_timestamp",
        "transaction_date",
        "data_split",
        "is_fraud",
    )
    base_features = add_behavioral_features(split_transactions)
    mcc_mapping, training_global_rate = fit_training_mcc_fraud_rates(
        base_features,
        alpha=float(feature_cfg["mcc_smoothing_alpha"]),
    )
    model_features = apply_mcc_fraud_rates(
        base_features,
        mcc_mapping=mcc_mapping,
        training_global_rate=training_global_rate,
    )

    return {
        "split_assignments": split_assignments,
        "mcc_fraud_rates": mcc_mapping,
        "model_features": model_features,
    }


def write_feature_dataset(
    dataframe: DataFrame,
    runtime_config: dict[str, str],
) -> None:
    """Persist one feature dataset to a managed table or Delta path."""
    writer = dataframe.write.format(runtime_config["write_format"]).mode(
        runtime_config["write_mode"]
    )
    if runtime_config["write_mode"] == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if runtime_config["storage_mode"] == "unity_catalog":
        writer.saveAsTable(runtime_config["target_table"])
    else:
        writer.save(runtime_config["target_path"])


def run_feature_pipeline(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
):
    """Build all feature datasets and optionally persist them."""
    datasets = build_feature_datasets(spark, project_config)
    summary_rows: list[dict[str, Any]] = []

    for dataset_name in FEATURE_DATASET_NAMES:
        dataframe = datasets[dataset_name]
        runtime = feature_dataset_runtime(project_config, dataset_name)
        target = runtime["target_table"] or runtime["target_path"]
        if write_output:
            write_feature_dataset(dataframe, runtime)
        print(
            f"[features:{dataset_name}] columns={len(dataframe.columns)} "
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


def load_feature_dataset(
    spark: SparkSession,
    project_config: dict[str, Any],
    dataset_name: str,
) -> DataFrame:
    """Load one persisted feature dataset."""
    runtime = feature_dataset_runtime(project_config, dataset_name)
    if runtime["storage_mode"] == "unity_catalog":
        return spark.table(runtime["target_table"])
    return spark.read.format(runtime["write_format"]).load(runtime["target_path"])
