"""Spark-native transaction cleaning and interpretable feature creation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.silver.common import blank_to_null, currency_parse_failed, currency_to_decimal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


US_LOCATION_CODES = frozenset(
    {
        "AA",
        "AE",
        "AK",
        "AL",
        "AP",
        "AR",
        "AS",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "GU",
        "HI",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MP",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "PR",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VI",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    }
)

KNOWN_ERROR_TOKENS = (
    "Bad CVV",
    "Bad Card Number",
    "Bad Expiration",
    "Bad PIN",
    "Bad Zipcode",
    "Insufficient Balance",
    "Technical Glitch",
)


def transform_transactions(
    bronze_transactions: DataFrame,
    night_start_hour: int = 0,
    night_end_hour: int = 6,
) -> DataFrame:
    """Clean transactions and create label-independent risk signals."""
    from pyspark.sql import functions as F

    if not 0 <= night_start_hour < night_end_hour <= 24:
        raise ValueError("Night hours must satisfy 0 <= start < end <= 24.")

    raw_timestamp = blank_to_null(F.col("date"))
    timestamp = F.to_timestamp(raw_timestamp, "yyyy-MM-dd HH:mm:ss")
    amount = currency_to_decimal(F.col("amount"))
    merchant_state = blank_to_null(F.col("merchant_state"))
    raw_errors = blank_to_null(F.col("errors"))
    error_tokens = F.filter(
        F.transform(
            F.split(F.coalesce(raw_errors, F.lit("")), r"\s*,\s*"),
            lambda token: F.trim(token),
        ),
        lambda token: token != "",
    )
    transaction_hour = F.hour(timestamp)

    return bronze_transactions.select(
        blank_to_null(F.col("id")).alias("transaction_id"),
        blank_to_null(F.col("client_id")).alias("user_id"),
        blank_to_null(F.col("card_id")).alias("card_id"),
        raw_timestamp.alias("transaction_timestamp_raw"),
        timestamp.alias("transaction_timestamp"),
        (raw_timestamp.isNotNull() & timestamp.isNull()).alias("timestamp_parse_failed"),
        F.to_date(timestamp).alias("transaction_date"),
        transaction_hour.alias("transaction_hour"),
        F.date_format(timestamp, "EEEE").alias("transaction_weekday"),
        F.dayofweek(timestamp).alias("transaction_day_of_week"),
        F.dayofweek(timestamp).isin(1, 7).alias("is_weekend"),
        (
            (transaction_hour >= F.lit(night_start_hour))
            & (transaction_hour < F.lit(night_end_hour))
        ).alias("is_night"),
        blank_to_null(F.col("amount")).alias("amount_raw"),
        amount.alias("amount"),
        F.abs(amount).alias("amount_abs"),
        (amount < 0).alias("is_negative_amount"),
        currency_parse_failed(F.col("amount")).alias("amount_parse_failed"),
        blank_to_null(F.col("use_chip")).alias("transaction_type"),
        (F.lower(blank_to_null(F.col("use_chip"))) == "online transaction").alias(
            "is_online_transaction"
        ),
        blank_to_null(F.col("merchant_id")).alias("merchant_id"),
        blank_to_null(F.col("merchant_city")).alias("merchant_city"),
        merchant_state.alias("merchant_state"),
        F.regexp_replace(blank_to_null(F.col("zip")), r"\.0$", "").alias("merchant_zip"),
        (
            F.when(merchant_state.isNull(), F.lit("unknown"))
            .when(F.upper(merchant_state).isin(*sorted(US_LOCATION_CODES)), F.lit("domestic"))
            .otherwise(F.lit("international"))
        ).alias("merchant_location_category"),
        blank_to_null(F.col("mcc")).alias("mcc"),
        raw_errors.alias("errors_raw"),
        error_tokens.alias("error_tokens"),
        (F.size(error_tokens) > 0).alias("has_transaction_error"),
        F.array_contains(error_tokens, "Bad CVV").alias("error_bad_cvv"),
        F.array_contains(error_tokens, "Bad Card Number").alias("error_bad_card_number"),
        F.array_contains(error_tokens, "Bad Expiration").alias("error_bad_expiration"),
        F.array_contains(error_tokens, "Bad PIN").alias("error_bad_pin"),
        F.array_contains(error_tokens, "Bad Zipcode").alias("error_bad_zipcode"),
        F.array_contains(error_tokens, "Insufficient Balance").alias(
            "error_insufficient_balance"
        ),
        F.array_contains(error_tokens, "Technical Glitch").alias("error_technical_glitch"),
    )
