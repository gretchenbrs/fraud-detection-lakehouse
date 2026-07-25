"""Bronze-layer orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.bronze.base import log_ingestion_result, write_bronze_dataset
from src.bronze.cards import read_cards
from src.bronze.fraud_labels import read_fraud_labels
from src.bronze.mcc_codes import read_mcc_codes
from src.bronze.transactions import read_transactions
from src.bronze.users import read_users
from src.utils.config import apply_runtime_overrides, bronze_dataset_runtime, load_project_config, raw_source_paths
from src.utils.schemas import dataset_names

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    SparkSession = Any


DATASET_READERS = {
    "transactions": read_transactions,
    "users": read_users,
    "cards": read_cards,
    "fraud_labels": read_fraud_labels,
    "mcc_codes": read_mcc_codes,
}


def run_bronze_ingestion(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
):
    """Run all Bronze ingestions and optionally persist them."""
    results: list[dict[str, Any]] = []
    source_paths = raw_source_paths(project_config)

    for dataset_name in dataset_names():
        reader = DATASET_READERS[dataset_name]
        runtime_config = bronze_dataset_runtime(project_config, dataset_name)
        dataframe = reader(spark, source_paths[dataset_name])
        row_count = dataframe.count()
        column_count = len(dataframe.columns)
        target_identifier = runtime_config["target_table"] or runtime_config["target_path"]

        log_ingestion_result(
            dataset_name=dataset_name,
            source_path=source_paths[dataset_name],
            row_count=row_count,
            column_count=column_count,
            target_identifier=target_identifier,
            storage_mode=runtime_config["storage_mode"],
        )

        if write_output:
            write_bronze_dataset(dataframe, runtime_config=runtime_config)

        results.append(
            {
                "dataset_name": dataset_name,
                "source_path": source_paths[dataset_name],
                "row_count": row_count,
                "column_count": column_count,
                "storage_mode": runtime_config["storage_mode"],
                "target_table": runtime_config["target_table"],
                "target_path": runtime_config["target_path"],
                "write_mode": runtime_config["write_mode"],
            }
        )

    return spark.createDataFrame(results)


def resolve_runtime_project_config(
    config_path: str,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load config from disk and apply notebook runtime overrides."""
    base_config = load_project_config(config_path)
    return apply_runtime_overrides(base_config, overrides=overrides)
