"""Spark-native Gold risk KPIs and investigation prioritization."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from src.gold.rules import validate_priority_fractions

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


def select_champion_model(overall_metrics: DataFrame, metric_name: str) -> DataFrame:
    """Select one model using validation ranking metrics only."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if metric_name not in {"pr_auc", "roc_auc"}:
        raise ValueError("Champion metric must be pr_auc or roc_auc.")
    ranking = Window.orderBy(
        F.desc(metric_name),
        F.desc("roc_auc" if metric_name == "pr_auc" else "pr_auc"),
        F.asc("model_name"),
    )
    return (
        overall_metrics.filter(F.col("evaluation_split") == "validation")
        .withColumn("_rank", F.row_number().over(ranking))
        .filter(F.col("_rank") == 1)
        .select("model_name", F.col(metric_name).alias("selection_metric_value"))
    )


def build_model_scorecard(
    overall_metrics: DataFrame,
    threshold_metrics: DataFrame,
    champion: DataFrame,
    metric_name: str,
) -> DataFrame:
    """Combine ranking and selected-threshold metrics into a business scorecard."""
    from pyspark.sql import functions as F

    selected_thresholds = threshold_metrics.filter(
        (F.col("evaluation_split") == "validation") & F.col("is_selected_threshold")
    ).select("model_name", F.col("threshold").alias("selected_threshold"))
    champion_name = champion.select(
        F.col("model_name").alias("champion_model_name")
    )
    return (
        overall_metrics.join(selected_thresholds, on="model_name", how="left")
        .crossJoin(champion_name)
        .withColumn("is_champion", F.col("model_name") == F.col("champion_model_name"))
        .withColumn("champion_selection_metric", F.lit(metric_name))
        .drop("champion_model_name")
    )


def build_daily_risk_kpis(
    champion_scores: DataFrame,
    selected_threshold: float,
    champion_model: str,
) -> DataFrame:
    """Build retrospective daily test KPIs at the validation-selected threshold."""
    from pyspark.sql import functions as F

    flagged = F.col("score") >= F.lit(float(selected_threshold))
    fraud = F.col("is_fraud") == 1
    grouped = champion_scores.groupBy("transaction_date").agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.col("is_fraud").cast("long")).alias("fraud_count"),
        F.sum(flagged.cast("long")).alias("flagged_count"),
        F.sum((flagged & fraud).cast("long")).alias("true_positive_count"),
        F.sum((flagged & ~fraud).cast("long")).alias("false_positive_count"),
        F.sum(F.col("amount_abs")).alias("transaction_amount"),
        F.sum(F.when(flagged, F.col("amount_abs")).otherwise(F.lit(0.0))).alias(
            "flagged_amount"
        ),
        F.sum(F.when(fraud, F.col("amount_abs")).otherwise(F.lit(0.0))).alias(
            "fraud_amount"
        ),
        F.sum(
            F.when(flagged & fraud, F.col("amount_abs")).otherwise(F.lit(0.0))
        ).alias("captured_fraud_amount"),
    )
    return (
        grouped.withColumn(
            "alert_rate",
            F.when(F.col("transaction_count") > 0, F.col("flagged_count") / F.col("transaction_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "precision",
            F.when(F.col("flagged_count") > 0, F.col("true_positive_count") / F.col("flagged_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "recall",
            F.when(F.col("fraud_count") > 0, F.col("true_positive_count") / F.col("fraud_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "fraud_amount_capture_rate",
            F.when(
                F.col("fraud_amount") > 0,
                F.col("captured_fraud_amount") / F.col("fraud_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("model_name", F.lit(champion_model))
        .withColumn("selected_threshold", F.lit(float(selected_threshold)))
        .withColumn("evaluation_split", F.lit("test"))
    )


def build_investigation_queue(
    champion_scores: DataFrame,
    model_features: DataFrame,
    selected_threshold: float,
    champion_model: str,
    queue_fraction: float,
    priority_fractions: list[float],
) -> DataFrame:
    """Rank test transactions without using outcome labels in prioritization."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    validate_priority_fractions(priority_fractions, queue_fraction)
    row_count = champion_scores.count()
    queue_count = max(1, math.ceil(row_count * queue_fraction))
    cutoffs = [max(1, math.ceil(row_count * value)) for value in priority_fractions]
    ranking = Window.orderBy(F.desc("score"), F.desc("amount_abs"), F.asc("transaction_id"))
    ranked = (
        champion_scores.withColumn("investigation_rank", F.row_number().over(ranking))
        .filter(F.col("investigation_rank") <= F.lit(queue_count))
        .withColumn(
            "priority_tier",
            F.when(F.col("investigation_rank") <= cutoffs[0], F.lit("critical"))
            .when(F.col("investigation_rank") <= cutoffs[1], F.lit("high"))
            .when(F.col("investigation_rank") <= cutoffs[2], F.lit("medium"))
            .otherwise(F.lit("standard")),
        )
    )
    context_columns = [
        "transaction_id",
        "transaction_type",
        "merchant_location_category",
        "mcc",
        "mcc_category",
        "is_online_transaction",
        "is_night",
        "has_transaction_error",
        "error_bad_cvv",
        "error_bad_pin",
        "error_insufficient_balance",
        "error_technical_glitch",
        "is_negative_amount",
        "is_card_expired",
        "mcc_unseen_in_training",
        "amount_to_credit_limit",
    ]
    joined = ranked.join(
        model_features.filter(F.col("data_split") == "test").select(*context_columns),
        on="transaction_id",
        how="left",
    )
    reason_codes = F.concat_ws(
        "|",
        F.when(F.col("merchant_location_category") == "international", "international"),
        F.when(F.col("is_online_transaction"), "online"),
        F.when(F.col("is_night"), "night"),
        F.when(F.col("has_transaction_error"), "transaction_error"),
        F.when(F.col("error_bad_cvv"), "bad_cvv"),
        F.when(F.col("error_bad_pin"), "bad_pin"),
        F.when(F.col("error_insufficient_balance"), "insufficient_balance"),
        F.when(F.col("error_technical_glitch"), "technical_glitch"),
        F.when(F.col("is_negative_amount"), "negative_amount"),
        F.when(F.col("is_card_expired"), "expired_card"),
        F.when(F.col("mcc_unseen_in_training"), "unseen_mcc"),
        F.when(F.col("amount_to_credit_limit") >= 0.5, "high_limit_utilization"),
    )
    return (
        joined.withColumn("risk_reason_codes", reason_codes)
        .withColumn("model_name", F.lit(champion_model))
        .withColumn("selected_threshold", F.lit(float(selected_threshold)))
        .withColumn("is_above_selected_threshold", F.col("score") >= selected_threshold)
        .withColumnRenamed("is_fraud", "actual_is_fraud")
        .withColumn("outcome_usage", F.lit("post_outcome_audit_only"))
    )
