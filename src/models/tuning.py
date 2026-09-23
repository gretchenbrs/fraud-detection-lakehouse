"""Leakage-safe candidate tuning for the fraud classification models."""

from __future__ import annotations

import json
from functools import reduce
from typing import TYPE_CHECKING, Any

from src.features.pipeline import load_feature_dataset
from src.gold.analytics import select_champion_model
from src.models.evaluation import (
    overall_auc_row,
    score_predictions,
    select_best_validation_threshold,
    threshold_metrics,
    top_k_capture_rows,
)
from src.models.features import fit_training_preprocessor, prepare_model_input
from src.models.pipeline import write_model_dataset
from src.models.training import (
    add_training_class_weights,
    fit_logistic_regression,
    fit_random_forest,
)
from src.utils.config import MODEL_DATASET_NAMES, TUNING_DATASET_NAME, model_dataset_runtime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def tuning_candidates(model_config: dict[str, Any]) -> list[dict[str, Any]]:
    """Return configured candidate definitions when tuning is explicitly enabled."""
    tuning_config = model_config.get("tuning", {})
    if not tuning_config.get("enabled"):
        raise ValueError("Model tuning is disabled in models.tuning.enabled.")
    candidates = tuning_config.get("candidates", [])
    if not candidates:
        raise ValueError("Model tuning requires at least one configured candidate.")
    return candidates


def _union(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def _fit_candidate(
    training: DataFrame,
    model_config: dict[str, Any],
    candidate: dict[str, Any],
) -> Any:
    """Fit one candidate from the selected family with explicit parameters."""
    if candidate["family"] == "logistic_regression":
        return fit_logistic_regression(
            training,
            model_config,
            hyperparameters=candidate["parameters"],
        )
    if candidate["family"] == "random_forest":
        return fit_random_forest(
            training,
            model_config,
            hyperparameters=candidate["parameters"],
        )
    raise ValueError(f"Unsupported tuning candidate family: {candidate['family']}")


def build_tuning_outputs(
    spark: SparkSession,
    project_config: dict[str, Any],
) -> dict[str, DataFrame]:
    """Tune on validation only and score the held-out test split once for the champion."""
    from pyspark.sql import functions as F

    model_config = project_config["models"]
    candidates = tuning_candidates(model_config)
    prepared = prepare_model_input(
        load_feature_dataset(spark, project_config, "model_features")
    )
    training = prepared.filter(F.col("data_split") == "train")
    validation = prepared.filter(F.col("data_split") == "validation")
    test = prepared.filter(F.col("data_split") == "test")

    weighted_training = add_training_class_weights(
        training,
        enabled=bool(model_config["use_class_weights"]),
    )
    preprocessor = fit_training_preprocessor(weighted_training)
    transformed_training = preprocessor.transform(weighted_training)
    transformed_validation = preprocessor.transform(validation)
    transformed_test = preprocessor.transform(test)

    fitted_models: dict[str, Any] = {}
    validation_scores: dict[str, DataFrame] = {}
    validation_metric_rows: list[dict[str, Any]] = []
    validation_threshold_frames: list[DataFrame] = []
    validation_top_k_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        model_name = candidate["model_name"]
        print(f"[tuning] fitting {model_name} ({candidate['family']}) on train only")
        model = _fit_candidate(transformed_training, model_config, candidate)
        fitted_models[model_name] = model
        scores = score_predictions(model, transformed_validation, model_name)
        validation_scores[model_name] = scores
        validation_metric_rows.append(overall_auc_row(scores, model_name, "validation"))
        validation_threshold_frames.append(
            threshold_metrics(
                spark,
                scores,
                model_name,
                "validation",
                [float(value) for value in model_config["thresholds"]],
            )
        )
        validation_top_k_rows.extend(
            top_k_capture_rows(
                spark,
                scores,
                model_name,
                "validation",
                [float(value) for value in model_config["top_k_fractions"]],
            )
        )

    validation_overall_metrics = spark.createDataFrame(validation_metric_rows)
    validation_threshold_metrics = _union(validation_threshold_frames)
    selected_validation_thresholds = select_best_validation_threshold(
        validation_threshold_metrics
    ).select("model_name", F.col("threshold").alias("selected_threshold"))
    marked_validation_thresholds = (
        validation_threshold_metrics.join(
            selected_validation_thresholds,
            on="model_name",
            how="left",
        )
        .withColumn(
            "is_selected_threshold",
            F.col("threshold") == F.col("selected_threshold"),
        )
        .withColumn("threshold_role", F.lit("validation_candidate"))
        .drop("selected_threshold")
    )

    champion = select_champion_model(
        validation_overall_metrics,
        model_config["tuning"]["selection_metric"],
    )
    champion_row = champion.first()
    champion_name = champion_row["model_name"]
    champion_threshold = float(
        selected_validation_thresholds.filter(
            F.col("model_name") == champion_name
        ).first()["selected_threshold"]
    )
    print(
        f"[tuning] validation champion={champion_name} "
        f"threshold={champion_threshold} "
        f"metric={model_config['tuning']['selection_metric']}"
    )

    # The held-out test split is scored only after validation has selected one champion.
    champion_test_scores = score_predictions(
        fitted_models[champion_name],
        transformed_test,
        champion_name,
    )
    champion_test_overall = spark.createDataFrame(
        [overall_auc_row(champion_test_scores, champion_name, "test")]
    )
    champion_test_thresholds = (
        threshold_metrics(
            spark,
            champion_test_scores,
            champion_name,
            "test",
            [champion_threshold],
        )
        .withColumn("is_selected_threshold", F.lit(True))
        .withColumn("threshold_role", F.lit("final_test"))
    )
    champion_test_top_k_rows = top_k_capture_rows(
        spark,
        champion_test_scores,
        champion_name,
        "test",
        [float(value) for value in model_config["top_k_fractions"]],
    )

    candidate_metadata = spark.createDataFrame(
        [
            (
                candidate["model_name"],
                candidate["family"],
                json.dumps(candidate["parameters"], sort_keys=True),
            )
            for candidate in candidates
        ],
        "model_name string, model_family string, candidate_parameters string",
    )
    tuning_trials = (
        validation_overall_metrics.join(
            selected_validation_thresholds,
            on="model_name",
            how="inner",
        )
        .join(candidate_metadata, on="model_name", how="inner")
        .withColumn(
            "selection_metric",
            F.lit(model_config["tuning"]["selection_metric"]),
        )
        .withColumn("is_validation_champion", F.col("model_name") == champion_name)
        .withColumn("test_split_used_for_selection", F.lit(False))
        .orderBy(F.desc(model_config["tuning"]["selection_metric"]), F.asc("model_name"))
    )

    return {
        "scored_predictions": _union(
            [*validation_scores.values(), champion_test_scores]
        ),
        "overall_metrics": validation_overall_metrics.unionByName(champion_test_overall),
        "threshold_metrics": marked_validation_thresholds.unionByName(
            champion_test_thresholds
        ),
        "top_k_metrics": spark.createDataFrame(
            [*validation_top_k_rows, *champion_test_top_k_rows]
        ),
        TUNING_DATASET_NAME: tuning_trials,
    }


def run_tuning_pipeline(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
) -> DataFrame:
    """Persist validation-only tuning artifacts and the final champion evaluation."""
    outputs = build_tuning_outputs(spark, project_config)
    summary_rows: list[dict[str, Any]] = []
    for dataset_name in (*MODEL_DATASET_NAMES, TUNING_DATASET_NAME):
        dataframe = outputs[dataset_name]
        runtime = model_dataset_runtime(project_config, dataset_name)
        target = runtime["target_table"] or runtime["target_path"]
        if write_output:
            write_model_dataset(dataframe, runtime)
        print(
            f"[tuning:{dataset_name}] columns={len(dataframe.columns)} "
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
