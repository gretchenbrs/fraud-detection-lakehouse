"""Model evaluation for rare-event fraud classification."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


def validate_thresholds(thresholds: list[float]) -> None:
    """Validate an ordered probability-threshold grid."""
    if not thresholds or any(not 0 < value < 1 for value in thresholds):
        raise ValueError("Thresholds must be a non-empty list with values between 0 and 1.")
    if thresholds != sorted(set(thresholds)):
        raise ValueError("Thresholds must be unique and sorted ascending.")


def validate_top_k_fractions(top_k_fractions: list[float]) -> None:
    """Validate ordered top-k population fractions."""
    if not top_k_fractions or any(not 0 < value <= 1 for value in top_k_fractions):
        raise ValueError("Top-k fractions must be a non-empty list with values in (0, 1].")
    if top_k_fractions != sorted(set(top_k_fractions)):
        raise ValueError("Top-k fractions must be unique and sorted ascending.")


def binary_metrics_from_counts(
    true_positive: int,
    false_positive: int,
    true_negative: int,
    false_negative: int,
) -> dict[str, float]:
    """Calculate threshold metrics from a confusion matrix."""
    if min(true_positive, false_positive, true_negative, false_negative) < 0:
        raise ValueError("Confusion-matrix counts cannot be negative.")

    def ratio(numerator: int, denominator: int) -> float:
        return float(numerator / denominator) if denominator else 0.0

    precision = ratio(true_positive, true_positive + false_positive)
    recall = ratio(true_positive, true_positive + false_negative)
    return {
        "precision": precision,
        "recall": recall,
        "f1": ratio(2.0 * precision * recall, precision + recall),
        "false_positive_rate": ratio(false_positive, false_positive + true_negative),
    }


def score_predictions(model: Any, dataframe: DataFrame, model_name: str) -> DataFrame:
    """Transform rows and retain a compact, auditable score dataset."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    return model.transform(dataframe).select(
        "transaction_id",
        "transaction_timestamp",
        "transaction_date",
        "data_split",
        F.col("label").cast("int").alias("is_fraud"),
        F.col("amount_abs").cast("double").alias("amount_abs"),
        F.lit(model_name).alias("model_name"),
        vector_to_array("probability")[1].cast("double").alias("score"),
    )


def overall_auc_row(
    predictions: DataFrame,
    model_name: str,
    evaluation_split: str,
) -> dict[str, Any]:
    """Calculate PR-AUC, ROC-AUC, row count, and natural prevalence."""
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.sql import functions as F

    evaluation_frame = predictions.select(
        F.col("is_fraud").cast("double").alias("label"),
        "score",
    )
    summary = evaluation_frame.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("label").cast("long")).alias("fraud_count"),
        F.avg("label").alias("fraud_rate"),
    ).first()
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="score",
    )
    pr_auc = evaluator.setMetricName("areaUnderPR").evaluate(evaluation_frame)
    roc_auc = evaluator.setMetricName("areaUnderROC").evaluate(evaluation_frame)
    return {
        "model_name": model_name,
        "evaluation_split": evaluation_split,
        "row_count": int(summary["row_count"]),
        "fraud_count": int(summary["fraud_count"]),
        "fraud_rate": float(summary["fraud_rate"]),
        "pr_auc": float(pr_auc),
        "roc_auc": float(roc_auc),
    }


def threshold_metrics(
    spark: SparkSession,
    predictions: DataFrame,
    model_name: str,
    evaluation_split: str,
    thresholds: list[float],
) -> DataFrame:
    """Evaluate a systematic threshold grid without changing class prevalence."""
    from pyspark.sql import functions as F

    validate_thresholds(thresholds)
    threshold_frame = spark.createDataFrame(
        [(float(value),) for value in thresholds],
        "threshold double",
    )
    expanded = predictions.select("is_fraud", "score").crossJoin(
        F.broadcast(threshold_frame)
    )
    predicted_positive = F.col("score") >= F.col("threshold")
    actual_positive = F.col("is_fraud") == 1
    metrics = expanded.groupBy("threshold").agg(
        F.sum((predicted_positive & actual_positive).cast("long")).alias("true_positive"),
        F.sum((predicted_positive & ~actual_positive).cast("long")).alias("false_positive"),
        F.sum((~predicted_positive & ~actual_positive).cast("long")).alias("true_negative"),
        F.sum((~predicted_positive & actual_positive).cast("long")).alias("false_negative"),
    )
    precision = F.when(
        F.col("true_positive") + F.col("false_positive") > 0,
        F.col("true_positive")
        / (F.col("true_positive") + F.col("false_positive")),
    ).otherwise(F.lit(0.0))
    recall = F.when(
        F.col("true_positive") + F.col("false_negative") > 0,
        F.col("true_positive")
        / (F.col("true_positive") + F.col("false_negative")),
    ).otherwise(F.lit(0.0))
    false_positive_rate = F.when(
        F.col("false_positive") + F.col("true_negative") > 0,
        F.col("false_positive")
        / (F.col("false_positive") + F.col("true_negative")),
    ).otherwise(F.lit(0.0))
    return (
        metrics.withColumn("precision", precision)
        .withColumn("recall", recall)
        .withColumn(
            "f1",
            F.when(
                F.col("precision") + F.col("recall") > 0,
                2.0
                * F.col("precision")
                * F.col("recall")
                / (F.col("precision") + F.col("recall")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("false_positive_rate", false_positive_rate)
        .withColumn("model_name", F.lit(model_name))
        .withColumn("evaluation_split", F.lit(evaluation_split))
        .select(
            "model_name",
            "evaluation_split",
            "threshold",
            "true_positive",
            "false_positive",
            "true_negative",
            "false_negative",
            "precision",
            "recall",
            "f1",
            "false_positive_rate",
        )
    )


def select_best_validation_threshold(validation_metrics: DataFrame) -> DataFrame:
    """Select one threshold per model by validation F1, with deterministic tie breaks."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ranking = Window.partitionBy("model_name").orderBy(
        F.desc("f1"),
        F.desc("recall"),
        F.desc("precision"),
        F.asc("threshold"),
    )
    return validation_metrics.withColumn("_rank", F.row_number().over(ranking)).filter(
        F.col("_rank") == 1
    ).drop("_rank")


def top_k_capture_rows(
    spark: SparkSession,
    predictions: DataFrame,
    model_name: str,
    evaluation_split: str,
    top_k_fractions: list[float],
) -> list[dict[str, Any]]:
    """Calculate exact top-k fraud and fraud-amount capture using ranked scores."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    validate_top_k_fractions(top_k_fractions)
    totals = predictions.agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("is_fraud").cast("long")).alias("fraud_count"),
        F.sum(
            F.when(F.col("is_fraud") == 1, F.coalesce(F.col("amount_abs"), F.lit(0.0))).otherwise(
                F.lit(0.0)
            )
        ).alias("fraud_amount"),
    ).first()
    row_count = int(totals["row_count"])
    fraud_count = int(totals["fraud_count"])
    fraud_amount = float(totals["fraud_amount"] or 0.0)
    maximum_top_count = max(1, math.ceil(row_count * max(top_k_fractions)))

    candidates = predictions.select(
        "transaction_id", "is_fraud", "amount_abs", "score"
    ).orderBy(F.desc("score"), F.asc("transaction_id")).limit(maximum_top_count)
    ranked = candidates.withColumn(
        "score_rank",
        F.row_number().over(Window.orderBy(F.desc("score"), F.asc("transaction_id"))),
    )
    limits = spark.createDataFrame(
        [
            (float(fraction), max(1, math.ceil(row_count * fraction)))
            for fraction in top_k_fractions
        ],
        "top_fraction double, top_count long",
    )
    captured = (
        ranked.crossJoin(F.broadcast(limits))
        .filter(F.col("score_rank") <= F.col("top_count"))
        .groupBy("top_fraction", "top_count")
        .agg(
            F.sum(F.col("is_fraud").cast("long")).alias("captured_fraud_count"),
            F.sum(
                F.when(
                    F.col("is_fraud") == 1,
                    F.coalesce(F.col("amount_abs"), F.lit(0.0)),
                ).otherwise(F.lit(0.0))
            ).alias("captured_fraud_amount"),
        )
        .orderBy("top_fraction")
        .collect()
    )

    return [
        {
            "model_name": model_name,
            "evaluation_split": evaluation_split,
            "top_fraction": float(row["top_fraction"]),
            "top_count": int(row["top_count"]),
            "captured_fraud_count": int(row["captured_fraud_count"]),
            "fraud_capture_rate": (
                float(row["captured_fraud_count"] / fraud_count) if fraud_count else 0.0
            ),
            "precision_at_k": float(row["captured_fraud_count"] / row["top_count"]),
            "captured_fraud_amount": float(row["captured_fraud_amount"] or 0.0),
            "fraud_amount_capture_rate": (
                float((row["captured_fraud_amount"] or 0.0) / fraud_amount)
                if fraud_amount
                else 0.0
            ),
        }
        for row in captured
    ]
