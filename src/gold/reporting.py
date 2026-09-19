"""Spark-native reporting datasets built from persisted Gold outputs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


REPORTING_SEGMENT_DIMENSIONS = (
    "priority_tier",
    "mcc_category",
    "merchant_location_category",
    "transaction_type",
)


def validate_segment_dimension(dimension: str) -> None:
    """Reject arbitrary columns before they are used in a reporting group-by."""
    if dimension not in REPORTING_SEGMENT_DIMENSIONS:
        raise ValueError(
            f"Unsupported reporting dimension: {dimension}. "
            f"Expected one of {REPORTING_SEGMENT_DIMENSIONS}."
        )


def build_executive_summary(
    model_scorecard: DataFrame,
    daily_risk_kpis: DataFrame,
) -> DataFrame:
    """Build a one-row champion-model and operating-performance summary."""
    from pyspark.sql import functions as F

    champion_test = model_scorecard.filter(
        F.col("is_champion") & (F.col("evaluation_split") == "test")
    ).select(
        "model_name",
        "selected_threshold",
        "pr_auc",
        "roc_auc",
    )
    operating_totals = daily_risk_kpis.agg(
        F.sum("transaction_count").alias("transaction_count"),
        F.sum("fraud_count").alias("fraud_count"),
        F.sum("flagged_count").alias("flagged_count"),
        F.sum("true_positive_count").alias("true_positive_count"),
        F.sum("false_positive_count").alias("false_positive_count"),
        F.sum("transaction_amount").alias("transaction_amount"),
        F.sum("fraud_amount").alias("fraud_amount"),
        F.sum("captured_fraud_amount").alias("captured_fraud_amount"),
    )
    return (
        champion_test.crossJoin(operating_totals)
        .withColumn(
            "fraud_rate",
            F.col("fraud_count") / F.col("transaction_count"),
        )
        .withColumn(
            "alert_rate",
            F.col("flagged_count") / F.col("transaction_count"),
        )
        .withColumn(
            "precision",
            F.when(
                F.col("flagged_count") > 0,
                F.col("true_positive_count") / F.col("flagged_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "recall",
            F.when(
                F.col("fraud_count") > 0,
                F.col("true_positive_count") / F.col("fraud_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "fraud_amount_capture_rate",
            F.when(
                F.col("fraud_amount") > 0,
                F.col("captured_fraud_amount") / F.col("fraud_amount"),
            ).otherwise(F.lit(0.0)),
        )
    )


def build_monthly_risk_trend(daily_risk_kpis: DataFrame) -> DataFrame:
    """Aggregate daily retrospective KPIs into dashboard-friendly calendar months."""
    from pyspark.sql import functions as F

    return (
        daily_risk_kpis.withColumn(
            "transaction_month",
            F.trunc(F.col("transaction_date"), "month"),
        )
        .groupBy("transaction_month", "model_name", "selected_threshold")
        .agg(
            F.sum("transaction_count").alias("transaction_count"),
            F.sum("fraud_count").alias("fraud_count"),
            F.sum("flagged_count").alias("flagged_count"),
            F.sum("true_positive_count").alias("true_positive_count"),
            F.sum("false_positive_count").alias("false_positive_count"),
            F.sum("transaction_amount").alias("transaction_amount"),
            F.sum("fraud_amount").alias("fraud_amount"),
            F.sum("captured_fraud_amount").alias("captured_fraud_amount"),
        )
        .withColumn("fraud_rate", F.col("fraud_count") / F.col("transaction_count"))
        .withColumn("alert_rate", F.col("flagged_count") / F.col("transaction_count"))
        .withColumn(
            "precision",
            F.when(
                F.col("flagged_count") > 0,
                F.col("true_positive_count") / F.col("flagged_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "recall",
            F.when(
                F.col("fraud_count") > 0,
                F.col("true_positive_count") / F.col("fraud_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "fraud_amount_capture_rate",
            F.when(
                F.col("fraud_amount") > 0,
                F.col("captured_fraud_amount") / F.col("fraud_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .orderBy("transaction_month")
    )


def build_queue_segment_summary(
    investigation_queue: DataFrame,
    dimension: str,
) -> DataFrame:
    """Summarize queue volume and risk by an approved operational dimension."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    validate_segment_dimension(dimension)
    return (
        investigation_queue.withColumn(
            dimension,
            F.coalesce(F.col(dimension).cast("string"), F.lit("unknown")),
        )
        .groupBy(dimension)
        .agg(
            F.count("*").alias("queue_count"),
            F.avg("score").alias("average_score"),
            F.max("score").alias("maximum_score"),
            F.sum("amount_abs").alias("queued_amount"),
            F.sum(F.col("is_above_selected_threshold").cast("long")).alias(
                "above_threshold_count"
            ),
        )
        .withColumn(
            "queue_share",
            F.col("queue_count") / F.sum("queue_count").over(Window.partitionBy()),
        )
        .orderBy(F.desc("queue_count"), F.desc("average_score"))
    )


def build_queue_audit_summary(investigation_queue: DataFrame) -> DataFrame:
    """Summarize known test outcomes for retrospective audit only."""
    from pyspark.sql import functions as F

    return (
        investigation_queue.groupBy("priority_tier")
        .agg(
            F.count("*").alias("queue_count"),
            F.sum(F.col("actual_is_fraud").cast("long")).alias("captured_fraud_count"),
            F.sum("amount_abs").alias("queued_amount"),
            F.sum(
                F.when(F.col("actual_is_fraud") == 1, F.col("amount_abs")).otherwise(
                    F.lit(0.0)
                )
            ).alias("captured_fraud_amount"),
        )
        .withColumn(
            "audit_precision",
            F.col("captured_fraud_count") / F.col("queue_count"),
        )
        .withColumn("outcome_usage", F.lit("post_outcome_audit_only"))
        .orderBy(
            F.expr(
                "CASE priority_tier "
                "WHEN 'critical' THEN 1 WHEN 'high' THEN 2 "
                "WHEN 'medium' THEN 3 ELSE 4 END"
            )
        )
    )
