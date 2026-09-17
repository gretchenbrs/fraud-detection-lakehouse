"""End-to-end model training, validation selection, and final test evaluation."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any

from src.features.pipeline import load_feature_dataset
from src.models.evaluation import (
    overall_auc_row,
    score_predictions,
    select_best_validation_threshold,
    threshold_metrics,
    top_k_capture_rows,
)
from src.models.features import fit_training_preprocessor, prepare_model_input
from src.models.training import (
    add_training_class_weights,
    fit_logistic_regression,
    fit_random_forest,
)
from src.utils.config import MODEL_DATASET_NAMES, model_dataset_runtime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def write_model_dataset(dataframe: DataFrame, runtime_config: dict[str, str]) -> None:
    """Persist one model output to a managed table or Delta path."""
    writer = dataframe.write.format(runtime_config["write_format"]).mode(
        runtime_config["write_mode"]
    )
    if runtime_config["write_mode"] == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if runtime_config["storage_mode"] == "unity_catalog":
        writer.saveAsTable(runtime_config["target_table"])
    else:
        writer.save(runtime_config["target_path"])


def load_model_dataset(
    spark: SparkSession,
    project_config: dict[str, Any],
    dataset_name: str,
) -> DataFrame:
    """Load a persisted model output."""
    runtime = model_dataset_runtime(project_config, dataset_name)
    if runtime["storage_mode"] == "unity_catalog":
        return spark.table(runtime["target_table"])
    return spark.read.format(runtime["write_format"]).load(runtime["target_path"])


def _union(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def build_model_outputs(
    spark: SparkSession,
    project_config: dict[str, Any],
    materialize_scores: bool = False,
) -> dict[str, DataFrame]:
    """Train two models and build leakage-safe evaluation outputs."""
    from pyspark.sql import functions as F

    model_config = project_config["models"]
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

    models = {
        "logistic_regression": fit_logistic_regression(
            transformed_training,
            model_config,
        ),
        "random_forest": fit_random_forest(
            transformed_training,
            model_config,
        ),
    }

    score_frames: list[DataFrame] = []
    for model_name, model in models.items():
        score_frames.extend(
            [
                score_predictions(model, transformed_validation, model_name),
                score_predictions(model, transformed_test, model_name),
            ]
        )

    scored_predictions = _union(score_frames)
    if materialize_scores:
        score_runtime = model_dataset_runtime(project_config, "scored_predictions")
        write_model_dataset(scored_predictions, score_runtime)
        scored_predictions = load_model_dataset(
            spark,
            project_config,
            "scored_predictions",
        )

    scored_by_model: dict[str, dict[str, DataFrame]] = {}
    overall_rows: list[dict[str, Any]] = []
    validation_threshold_frames: list[DataFrame] = []
    top_k_rows: list[dict[str, Any]] = []
    for model_name in models:
        scored_by_model[model_name] = {
            split_name: scored_predictions.filter(
                (F.col("model_name") == model_name)
                & (F.col("data_split") == split_name)
            )
            for split_name in ("validation", "test")
        }
        for split_name, scores in scored_by_model[model_name].items():
            overall_rows.append(overall_auc_row(scores, model_name, split_name))
            top_k_rows.extend(
                top_k_capture_rows(
                    spark,
                    scores,
                    model_name,
                    split_name,
                    [float(value) for value in model_config["top_k_fractions"]],
                )
            )
        validation_threshold_frames.append(
            threshold_metrics(
                spark,
                scored_by_model[model_name]["validation"],
                model_name,
                "validation",
                [float(value) for value in model_config["thresholds"]],
            )
        )

    validation_metrics = _union(validation_threshold_frames)
    best_validation = select_best_validation_threshold(validation_metrics).select(
        "model_name",
        F.col("threshold").alias("selected_threshold"),
    )
    marked_validation_metrics = (
        validation_metrics.join(best_validation, on="model_name", how="left")
        .withColumn(
            "is_selected_threshold",
            F.col("threshold") == F.col("selected_threshold"),
        )
        .withColumn("threshold_role", F.lit("validation_candidate"))
        .drop("selected_threshold")
    )

    selected_thresholds = {
        row["model_name"]: float(row["selected_threshold"])
        for row in best_validation.collect()
    }
    test_threshold_frames: list[DataFrame] = []
    for model_name, threshold in selected_thresholds.items():
        test_threshold_frames.append(
            threshold_metrics(
                spark,
                scored_by_model[model_name]["test"],
                model_name,
                "test",
                [threshold],
            )
            .withColumn("is_selected_threshold", F.lit(True))
            .withColumn("threshold_role", F.lit("final_test"))
        )
    threshold_output = marked_validation_metrics.unionByName(_union(test_threshold_frames))

    overall_metrics = spark.createDataFrame(overall_rows)
    top_k_metrics = spark.createDataFrame(top_k_rows)

    return {
        "scored_predictions": scored_predictions,
        "overall_metrics": overall_metrics,
        "threshold_metrics": threshold_output,
        "top_k_metrics": top_k_metrics,
    }


def run_model_pipeline(
    spark: SparkSession,
    project_config: dict[str, Any],
    write_output: bool = True,
) -> DataFrame:
    """Train, evaluate, persist model outputs, and return a run summary."""
    outputs = build_model_outputs(
        spark,
        project_config,
        materialize_scores=write_output,
    )
    summary_rows: list[dict[str, Any]] = []
    for dataset_name in MODEL_DATASET_NAMES:
        dataframe = outputs[dataset_name]
        runtime = model_dataset_runtime(project_config, dataset_name)
        target = runtime["target_table"] or runtime["target_path"]
        scores_already_written = dataset_name == "scored_predictions" and write_output
        if write_output and not scores_already_written:
            write_model_dataset(dataframe, runtime)
        print(
            f"[models:{dataset_name}] columns={len(dataframe.columns)} "
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
