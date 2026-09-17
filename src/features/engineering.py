"""Label-independent features and training-only MCC target encoding."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


def smoothed_binary_rate(
    positive_count: int,
    observation_count: int,
    global_rate: float,
    alpha: float,
) -> float:
    """Return an additive-smoothed binary rate for testable formula validation."""
    if observation_count < 0 or positive_count < 0 or positive_count > observation_count:
        raise ValueError("Counts must satisfy 0 <= positive_count <= observation_count.")
    if alpha <= 0:
        raise ValueError("alpha must be positive.")
    return (positive_count + alpha * global_rate) / (observation_count + alpha)


def add_behavioral_features(dataframe: DataFrame) -> DataFrame:
    """Create Spark-native numeric and categorical model features."""
    from pyspark.sql import functions as F

    transaction_year = F.year("transaction_date")
    transaction_month = F.month("transaction_date")
    age_at_transaction = (
        transaction_year
        - F.col("birth_year")
        - F.when(transaction_month < F.col("birth_month"), 1).otherwise(0)
    )
    account_age_days = F.when(
        F.col("account_open_date") <= F.col("transaction_date"),
        F.datediff(F.col("transaction_date"), F.col("account_open_date")),
    )
    years_since_pin_change = F.when(
        F.col("year_pin_last_changed") <= transaction_year,
        transaction_year - F.col("year_pin_last_changed"),
    )
    amount_abs = F.col("amount_abs").cast("double")
    snapshot_credit_limit = F.col("credit_limit").cast("double")
    snapshot_yearly_income = F.col("yearly_income").cast("double")
    snapshot_total_debt = F.col("total_debt").cast("double")

    return dataframe.select(
        "transaction_id",
        "transaction_timestamp",
        "transaction_date",
        "data_split",
        "is_fraud",
        F.col("amount").cast("double").alias("amount"),
        amount_abs.alias("amount_abs"),
        "is_negative_amount",
        "transaction_hour",
        "transaction_day_of_week",
        "is_weekend",
        "is_night",
        "is_online_transaction",
        "has_transaction_error",
        "error_bad_cvv",
        "error_bad_card_number",
        "error_bad_expiration",
        "error_bad_pin",
        "error_bad_zipcode",
        "error_insufficient_balance",
        "error_technical_glitch",
        "transaction_type",
        "merchant_location_category",
        "mcc",
        "mcc_category",
        "card_brand",
        "card_type",
        "card_has_chip",
        snapshot_credit_limit.alias("snapshot_credit_limit"),
        snapshot_yearly_income.alias("snapshot_yearly_income"),
        snapshot_total_debt.alias("snapshot_total_debt"),
        F.col("credit_score").cast("double").alias("snapshot_credit_score"),
        F.col("num_credit_cards").cast("double").alias("snapshot_num_credit_cards"),
        "gender",
        age_at_transaction.cast("double").alias("age_at_transaction"),
        account_age_days.cast("double").alias("account_age_days"),
        years_since_pin_change.cast("double").alias("years_since_pin_change"),
        (
            F.col("card_expiry_date").isNotNull()
            & (F.col("card_expiry_date") < F.col("transaction_date"))
        ).alias("is_card_expired"),
        F.when(
            snapshot_yearly_income > 0,
            snapshot_total_debt / snapshot_yearly_income,
        ).alias("snapshot_debt_to_income"),
        F.when(
            snapshot_credit_limit > 0,
            amount_abs / snapshot_credit_limit,
        ).alias("amount_to_credit_limit"),
    )


def fit_training_mcc_fraud_rates(
    dataframe: DataFrame,
    alpha: float,
) -> tuple[DataFrame, float]:
    """Fit smoothed MCC fraud rates exclusively from training rows."""
    from pyspark.sql import functions as F

    if alpha <= 0:
        raise ValueError("alpha must be positive.")
    training = dataframe.filter(F.col("data_split") == "train")
    global_row = training.agg(F.avg(F.col("is_fraud").cast("double")).alias("rate")).first()
    if global_row is None or global_row["rate"] is None:
        raise ValueError("Training split is empty; MCC fraud-rate encoding cannot be fitted.")
    global_rate = float(global_row["rate"])

    mapping = (
        training.groupBy("mcc")
        .agg(
            F.count("*").alias("training_mcc_count"),
            F.sum(F.col("is_fraud").cast("long")).alias("training_mcc_fraud_count"),
        )
        .withColumn("training_global_fraud_rate", F.lit(global_rate))
        .withColumn("smoothing_alpha", F.lit(float(alpha)))
        .withColumn(
            "mcc_smoothed_fraud_rate",
            (
                F.col("training_mcc_fraud_count")
                + F.lit(float(alpha) * global_rate)
            )
            / (F.col("training_mcc_count") + F.lit(float(alpha))),
        )
    )
    return mapping, global_rate


def apply_mcc_fraud_rates(
    dataframe: DataFrame,
    mcc_mapping: DataFrame,
    training_global_rate: float,
) -> DataFrame:
    """Apply train-fitted MCC rates to all splits with a train-global fallback."""
    from pyspark.sql import functions as F

    joined = dataframe.join(
        mcc_mapping.select(
            "mcc",
            "training_mcc_count",
            "mcc_smoothed_fraud_rate",
        ),
        on="mcc",
        how="left",
    )
    return (
        joined.withColumn(
            "mcc_unseen_in_training",
            F.col("training_mcc_count").isNull(),
        )
        .withColumn(
            "mcc_smoothed_fraud_rate",
            F.coalesce(
                F.col("mcc_smoothed_fraud_rate"),
                F.lit(float(training_global_rate)),
            ),
        )
        .drop("training_mcc_count")
    )
