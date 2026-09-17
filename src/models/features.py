"""Spark ML feature preparation fitted exclusively on training rows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.ml import PipelineModel
    from pyspark.sql import DataFrame
else:
    DataFrame = Any
    PipelineModel = Any


CATEGORICAL_FEATURES = (
    "transaction_type",
    "merchant_location_category",
    "mcc_category",
    "card_brand",
    "card_type",
    "gender",
)

NUMERIC_FEATURES = (
    "amount",
    "amount_abs",
    "transaction_hour",
    "transaction_day_of_week",
    "snapshot_credit_limit",
    "snapshot_yearly_income",
    "snapshot_total_debt",
    "snapshot_credit_score",
    "snapshot_num_credit_cards",
    "age_at_transaction",
    "account_age_days",
    "years_since_pin_change",
    "snapshot_debt_to_income",
    "amount_to_credit_limit",
    "mcc_smoothed_fraud_rate",
    "is_negative_amount",
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
    "card_has_chip",
    "is_card_expired",
    "mcc_unseen_in_training",
)


def build_preprocessing_pipeline():
    """Build index, encoding, imputation, and vector assembly stages."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler

    indexed_columns = [f"{column_name}__index" for column_name in CATEGORICAL_FEATURES]
    encoded_columns = [f"{column_name}__encoded" for column_name in CATEGORICAL_FEATURES]
    imputed_columns = [f"{column_name}__imputed" for column_name in NUMERIC_FEATURES]

    indexers = [
        StringIndexer(
            inputCol=column_name,
            outputCol=indexed_column,
            handleInvalid="keep",
            stringOrderType="frequencyDesc",
        )
        for column_name, indexed_column in zip(CATEGORICAL_FEATURES, indexed_columns)
    ]
    encoder = OneHotEncoder(
        inputCols=indexed_columns,
        outputCols=encoded_columns,
        handleInvalid="keep",
        dropLast=True,
    )
    imputer = Imputer(
        inputCols=list(NUMERIC_FEATURES),
        outputCols=imputed_columns,
        strategy="median",
    )
    assembler = VectorAssembler(
        inputCols=[*imputed_columns, *encoded_columns],
        outputCol="raw_features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[*indexers, encoder, imputer, assembler])


def prepare_model_input(dataframe: DataFrame) -> DataFrame:
    """Cast labels and model inputs without using labels to alter feature values."""
    from pyspark.sql import functions as F

    prepared = dataframe.withColumn("label", F.col("is_fraud").cast("double"))
    for column_name in NUMERIC_FEATURES:
        prepared = prepared.withColumn(column_name, F.col(column_name).cast("double"))
    for column_name in CATEGORICAL_FEATURES:
        prepared = prepared.withColumn(
            column_name,
            F.coalesce(F.col(column_name).cast("string"), F.lit("unknown")),
        )
    return prepared


def fit_training_preprocessor(training: DataFrame) -> PipelineModel:
    """Fit preprocessing statistics and category vocabularies on training only."""
    return build_preprocessing_pipeline().fit(training)
