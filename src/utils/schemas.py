"""Schema registry and validation helpers for raw-source ingestion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetSchema:
    """Minimal dataset metadata used across Bronze ingestion and validation."""

    dataset_name: str
    source_format: str
    source_file_name: str
    raw_columns: tuple[str, ...]
    bronze_columns: tuple[str, ...]
    primary_key_columns: tuple[str, ...]
    important_columns: tuple[str, ...]
    json_root_key: str | None = None
    description: str = ""


DATASET_SCHEMAS: dict[str, DatasetSchema] = {
    "transactions": DatasetSchema(
        dataset_name="transactions",
        source_format="csv",
        source_file_name="transactions_data.csv",
        raw_columns=(
            "id",
            "date",
            "client_id",
            "card_id",
            "amount",
            "use_chip",
            "merchant_id",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",
        ),
        bronze_columns=(
            "id",
            "date",
            "client_id",
            "card_id",
            "amount",
            "use_chip",
            "merchant_id",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",
        ),
        primary_key_columns=("id",),
        important_columns=("id", "date", "client_id", "card_id", "amount", "mcc", "errors"),
        description="Raw financial transactions loaded as all-string Bronze columns.",
    ),
    "users": DatasetSchema(
        dataset_name="users",
        source_format="csv",
        source_file_name="users_data.csv",
        raw_columns=(
            "id",
            "current_age",
            "retirement_age",
            "birth_year",
            "birth_month",
            "gender",
            "address",
            "latitude",
            "longitude",
            "per_capita_income",
            "yearly_income",
            "total_debt",
            "credit_score",
            "num_credit_cards",
        ),
        bronze_columns=(
            "id",
            "current_age",
            "retirement_age",
            "birth_year",
            "birth_month",
            "gender",
            "address",
            "latitude",
            "longitude",
            "per_capita_income",
            "yearly_income",
            "total_debt",
            "credit_score",
            "num_credit_cards",
        ),
        primary_key_columns=("id",),
        important_columns=("id", "credit_score", "yearly_income", "total_debt"),
        description="Raw user dimension attributes preserved as strings in Bronze.",
    ),
    "cards": DatasetSchema(
        dataset_name="cards",
        source_format="csv",
        source_file_name="cards_data.csv",
        raw_columns=(
            "id",
            "client_id",
            "card_brand",
            "card_type",
            "card_number",
            "expires",
            "cvv",
            "has_chip",
            "num_cards_issued",
            "credit_limit",
            "acct_open_date",
            "year_pin_last_changed",
            "card_on_dark_web",
        ),
        bronze_columns=(
            "id",
            "client_id",
            "card_brand",
            "card_type",
            "card_number",
            "expires",
            "cvv",
            "has_chip",
            "num_cards_issued",
            "credit_limit",
            "acct_open_date",
            "year_pin_last_changed",
            "card_on_dark_web",
        ),
        primary_key_columns=("id",),
        important_columns=("id", "client_id", "card_brand", "card_type", "credit_limit"),
        description="Raw card/account attributes preserved as strings in Bronze.",
    ),
    "fraud_labels": DatasetSchema(
        dataset_name="fraud_labels",
        source_format="json",
        source_file_name="train_fraud_labels.json",
        raw_columns=(),
        bronze_columns=("transaction_id", "is_fraud"),
        primary_key_columns=("transaction_id",),
        important_columns=("transaction_id", "is_fraud"),
        json_root_key="target",
        description="Top-level target map exploded into transaction-level fraud labels.",
    ),
    "mcc_codes": DatasetSchema(
        dataset_name="mcc_codes",
        source_format="json",
        source_file_name="mcc_codes.json",
        raw_columns=(),
        bronze_columns=("mcc", "mcc_category"),
        primary_key_columns=("mcc",),
        important_columns=("mcc", "mcc_category"),
        json_root_key=None,
        description="Top-level MCC map exploded into MCC-category rows.",
    ),
}


def dataset_names() -> tuple[str, ...]:
    """Return the supported dataset names in stable order."""
    return tuple(DATASET_SCHEMAS.keys())


def dataset_schema(dataset_name: str) -> DatasetSchema:
    """Return the schema metadata for a dataset."""
    try:
        return DATASET_SCHEMAS[dataset_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset: {dataset_name}") from exc


def ordered_raw_columns(dataset_name: str) -> tuple[str, ...]:
    """Return the expected raw-file columns for a dataset."""
    return dataset_schema(dataset_name).raw_columns


def required_bronze_columns(dataset_name: str) -> tuple[str, ...]:
    """Return the expected Bronze columns for a dataset."""
    return dataset_schema(dataset_name).bronze_columns


def expected_source_file_name(dataset_name: str) -> str:
    """Return the expected raw file name for a dataset."""
    return dataset_schema(dataset_name).source_file_name


def primary_key_columns(dataset_name: str) -> tuple[str, ...]:
    """Return the most likely primary key columns for a dataset."""
    return dataset_schema(dataset_name).primary_key_columns


def important_columns(dataset_name: str) -> tuple[str, ...]:
    """Return high-value fields used in validation summaries."""
    return dataset_schema(dataset_name).important_columns


def validate_required_columns(
    actual_columns: list[str] | tuple[str, ...],
    required_columns: list[str] | tuple[str, ...],
    dataset_name: str,
) -> None:
    """Raise a helpful error when required columns are missing."""
    missing = [column for column in required_columns if column not in actual_columns]
    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {missing}. "
            f"Actual columns were: {list(actual_columns)}"
        )


def build_csv_struct_type(dataset_name: str):
    """Build an all-string Spark StructType for CSV ingestion."""
    try:
        from pyspark.sql.types import StringType, StructField, StructType
    except ImportError as exc:
        raise ImportError(
            "pyspark is required to build Spark schemas for Bronze ingestion."
        ) from exc

    schema = dataset_schema(dataset_name)
    if schema.source_format != "csv":
        raise ValueError(f"{dataset_name} is not a CSV dataset.")
    return StructType(
        [StructField(column_name, StringType(), True) for column_name in schema.raw_columns]
    )


def build_json_schema(dataset_name: str):
    """Build the Spark schema needed to parse a supported JSON dataset."""
    try:
        from pyspark.sql.types import MapType, StringType, StructField, StructType
    except ImportError as exc:
        raise ImportError(
            "pyspark is required to build Spark schemas for Bronze ingestion."
        ) from exc

    if dataset_name == "fraud_labels":
        return StructType(
            [StructField("target", MapType(StringType(), StringType()), True)]
        )
    if dataset_name == "mcc_codes":
        return MapType(StringType(), StringType())
    raise ValueError(f"Unsupported JSON dataset: {dataset_name}")
