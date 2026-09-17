"""Gold-layer orchestration for risk reporting and investigation prioritization."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.features.pipeline import load_feature_dataset
from src.gold.analytics import (
    build_daily_risk_kpis,
    build_investigation_queue,
    build_model_scorecard,
    select_champion_model,
)
from src.models.pipeline import load_model_dataset
from src.utils.config import GOLD_DATASET_NAMES, gold_dataset_runtime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def write_gold_dataset(dataframe: DataFrame, runtime_config: dict[str, str]) -> None:
    """Persist one Gold dataset to a managed table or Delta path."""
    writer = dataframe.write.format(runtime_config["write_format"]).mode(
        runtime_config["write_mode"]
    )
    if runtime_config["write_mode"] == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if runtime_config["storage_mode"] == "unity_catalog":
        writer.saveAsTable(runtime_config["target_table"])
    else:
        writer.save(runtime_config["target_path"])


def load_gold_dataset(
    spark: SparkSession,
    project_config: dict[str, Any],
    dataset_name: str,
) -> DataFrame:
    """Load one persisted Gold dataset."""
    runtime = gold_dataset_runtime(project_config, dataset_name)
    if runtime["storage_mode"] == "unity_catalog":
        return spark.table(runtime["target_table"])
    return spark.read.format(runtime["write_format"]).load(runtime["target_path"])


def build_gold_datasets(
    spark: SparkSession,
    project_config: dict[str, Any],
) -> dict[str, DataFrame]:
    """Build leakage-safe business outputs from persisted model results."""
    from pyspark.sql import functions as F

    gold_config = project_config["gold"]
    overall_metrics = load_model_dataset(spark, project_config, "overall_metrics")
    threshold_metrics = load_model_dataset(spark, project_config, "threshold_metrics")
    scored_predictions = load_model_dataset(spark, project_config, "scored_predictions")
    model_features = load_feature_dataset(spark, project_config, "model_features")

    champion = select_champion_model(overall_metrics, gold_config["champion_metric"])
    champion_row = champion.first()
    if champion_row is None:
        raise ValueError("No validation metrics are available for champion selection.")
    champion_model = champion_row["model_name"]
    selected_threshold_row = (
        threshold_metrics.filter(
            (F.col("model_name") == champion_model)
            & (F.col("evaluation_split") == "validation")
            & F.col("is_selected_threshold")
        )
        .select("threshold")
        .first()
    )
    if selected_threshold_row is None:
        raise ValueError("The champion model has no validation-selected threshold.")
    selected_threshold = float(selected_threshold_row["threshold"])
    champion_scores = scored_predictions.filter(
        (F.col("model_name") == champion_model)
        & (F.col("data_split") == gold_config["evaluation_split"])
    )

    return {
        "model_scorecard": build_model_scorecard(
            overall_metrics,
            threshold_metrics,
            champion,
            gold_config["champion_metric"],
        ),
        "daily_risk_kpis": build_daily_risk_kpis(
            champion_scores,
            selected_threshold,
            champion_model,
        ),
        "investigation_queue": build_investigation_queue(
            champion_scores,
            model_features,
            selected_threshold,
            champion_model,
            float(gold_config["investigation_queue_fraction"]),
            [float(value) for value in gold_config["priority_fractions"]],
        ),
    }


def run_gold_pipeline(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
) -> DataFrame:
    """Build and optionally persist all Gold outputs."""
    datasets = build_gold_datasets(spark, project_config)
    summary_rows: list[dict[str, Any]] = []
    for dataset_name in GOLD_DATASET_NAMES:
        dataframe = datasets[dataset_name]
        runtime = gold_dataset_runtime(project_config, dataset_name)
        target = runtime["target_table"] or runtime["target_path"]
        if write_output:
            write_gold_dataset(dataframe, runtime)
        print(
            f"[gold:{dataset_name}] columns={len(dataframe.columns)} "
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
