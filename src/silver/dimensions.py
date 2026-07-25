"""Silver transformations for user, card, label, and MCC dimensions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.silver.common import (
    blank_to_null,
    currency_parse_failed,
    currency_to_decimal,
    yes_no_to_boolean,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


def transform_users(bronze_users: DataFrame) -> DataFrame:
    """Type and standardize user attributes used in risk analysis."""
    from pyspark.sql import functions as F

    return bronze_users.select(
        blank_to_null(F.col("id")).alias("user_id"),
        F.col("current_age").cast("int").alias("current_age"),
        F.col("retirement_age").cast("int").alias("retirement_age"),
        F.col("birth_year").cast("int").alias("birth_year"),
        F.col("birth_month").cast("int").alias("birth_month"),
        blank_to_null(F.col("gender")).alias("gender"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        currency_to_decimal(F.col("per_capita_income")).alias("per_capita_income"),
        currency_to_decimal(F.col("yearly_income")).alias("yearly_income"),
        currency_to_decimal(F.col("total_debt")).alias("total_debt"),
        currency_parse_failed(F.col("per_capita_income")).alias(
            "per_capita_income_parse_failed"
        ),
        currency_parse_failed(F.col("yearly_income")).alias("yearly_income_parse_failed"),
        currency_parse_failed(F.col("total_debt")).alias("total_debt_parse_failed"),
        F.col("credit_score").cast("int").alias("credit_score"),
        F.col("num_credit_cards").cast("int").alias("num_credit_cards"),
    )


def transform_cards(bronze_cards: DataFrame) -> DataFrame:
    """Type card attributes while excluding raw card number and CVV from Silver."""
    from pyspark.sql import functions as F

    return bronze_cards.select(
        blank_to_null(F.col("id")).alias("card_id"),
        blank_to_null(F.col("client_id")).alias("card_user_id"),
        blank_to_null(F.col("card_brand")).alias("card_brand"),
        blank_to_null(F.col("card_type")).alias("card_type"),
        F.to_date(F.concat(F.lit("01/"), F.col("expires")), "dd/MM/yyyy").alias(
            "card_expiry_date"
        ),
        yes_no_to_boolean(F.col("has_chip")).alias("card_has_chip"),
        F.col("num_cards_issued").cast("int").alias("num_cards_issued"),
        currency_to_decimal(F.col("credit_limit")).alias("credit_limit"),
        currency_parse_failed(F.col("credit_limit")).alias("credit_limit_parse_failed"),
        F.to_date(F.concat(F.lit("01/"), F.col("acct_open_date")), "dd/MM/yyyy").alias(
            "account_open_date"
        ),
        F.col("year_pin_last_changed").cast("int").alias("year_pin_last_changed"),
        yes_no_to_boolean(F.col("card_on_dark_web")).alias("card_on_dark_web"),
    )


def transform_fraud_labels(bronze_fraud_labels: DataFrame) -> DataFrame:
    """Normalize Yes/No fraud labels without filling transactions that lack labels."""
    from pyspark.sql import functions as F

    normalized_label = F.lower(blank_to_null(F.col("is_fraud")))
    fraud_value = (
        F.when(normalized_label == "yes", F.lit(1))
        .when(normalized_label == "no", F.lit(0))
        .otherwise(F.lit(None).cast("int"))
    )
    return bronze_fraud_labels.select(
        blank_to_null(F.col("transaction_id")).alias("transaction_id"),
        blank_to_null(F.col("is_fraud")).alias("fraud_label_raw"),
        fraud_value.alias("is_fraud"),
        (normalized_label.isNotNull() & fraud_value.isNull()).alias(
            "fraud_label_parse_failed"
        ),
    )


def transform_mcc_codes(bronze_mcc_codes: DataFrame) -> DataFrame:
    """Standardize MCC lookup keys and category descriptions."""
    from pyspark.sql import functions as F

    return bronze_mcc_codes.select(
        blank_to_null(F.col("mcc")).alias("mcc"),
        blank_to_null(F.col("mcc_category")).alias("mcc_category"),
    )
