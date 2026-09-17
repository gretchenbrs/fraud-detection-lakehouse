"""Configuration helpers for Databricks-ready Lakehouse execution."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

from src.utils.schemas import dataset_names, expected_source_file_name

VALID_STORAGE_MODES = {"unity_catalog", "path"}
VALID_WRITE_MODES = {"append", "overwrite", "errorifexists", "ignore"}
SILVER_DATASET_NAMES = ("users", "cards", "fraud_labels", "mcc_codes", "transactions")
FEATURE_DATASET_NAMES = ("split_assignments", "mcc_fraud_rates", "model_features")


def load_project_config(config_path: str) -> dict[str, Any]:
    """Load the YAML project configuration."""
    try:
        import yaml
    except ImportError as exc:
        raise ImportError("PyYAML is required to load config/project_config.yml.") from exc

    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    validate_project_config(config)
    return config


def is_placeholder(value: str | None) -> bool:
    """Return True when a config value still contains template placeholders."""
    return not value or "<" in value or ">" in value


def validate_identifier(name: str, label: str = "identifier", allow_placeholder: bool = False) -> None:
    """Validate a catalog, schema, volume, or table identifier."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{label} must be a non-empty string.")
    if not allow_placeholder and is_placeholder(name):
        raise ValueError(f"{label} still contains placeholder text: {name}")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if any(char not in allowed for char in name):
        raise ValueError(
            f"{label} contains unsupported characters: {name}. Use letters, numbers, and underscores only."
        )


def build_raw_data_path(
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    raw_data_subdirectory: str = "fraud_raw",
) -> str:
    """Build the expected Unity Catalog Volume folder path for raw uploads."""
    base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    subdirectory = raw_data_subdirectory.strip("/")
    return f"{base_path}/{subdirectory}" if subdirectory else base_path


def validate_project_config(config: dict[str, Any]) -> None:
    """Validate the minimum config contract needed by Bronze and Silver."""
    for section_name in (
        "project",
        "databricks",
        "bronze",
        "silver",
        "features",
        "raw_sources",
    ):
        if section_name not in config:
            raise ValueError(f"project_config.yml is missing the '{section_name}' section.")

    expected_files = config["raw_sources"].get("expected_files", {})
    table_names = config["bronze"].get("table_names", {})

    for dataset_name in dataset_names():
        if expected_files.get(dataset_name) != expected_source_file_name(dataset_name):
            raise ValueError(
                f"raw_sources.expected_files.{dataset_name} must be {expected_source_file_name(dataset_name)}"
            )
        table_name = table_names.get(dataset_name)
        if not table_name:
            raise ValueError(f"bronze.table_names.{dataset_name} is required.")
        validate_identifier(table_name, label=f"bronze.table_names.{dataset_name}", allow_placeholder=False)

    storage_mode = config["bronze"].get("storage_mode")
    if storage_mode not in VALID_STORAGE_MODES:
        raise ValueError(f"bronze.storage_mode must be one of {sorted(VALID_STORAGE_MODES)}")

    write_mode = config["bronze"].get("write_mode")
    if write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"bronze.write_mode must be one of {sorted(VALID_WRITE_MODES)}")

    if storage_mode == "path" and not config["bronze"].get("base_path"):
        raise ValueError("bronze.base_path is required when bronze.storage_mode=path")

    silver_cfg = config["silver"]
    silver_storage_mode = silver_cfg.get("storage_mode")
    if silver_storage_mode not in VALID_STORAGE_MODES:
        raise ValueError(f"silver.storage_mode must be one of {sorted(VALID_STORAGE_MODES)}")
    silver_write_mode = silver_cfg.get("write_mode")
    if silver_write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"silver.write_mode must be one of {sorted(VALID_WRITE_MODES)}")
    if silver_storage_mode == "path" and not silver_cfg.get("base_path"):
        raise ValueError("silver.base_path is required when silver.storage_mode=path")
    for dataset_name in SILVER_DATASET_NAMES:
        table_name = silver_cfg.get("table_names", {}).get(dataset_name)
        if not table_name:
            raise ValueError(f"silver.table_names.{dataset_name} is required.")
        validate_identifier(
            table_name,
            label=f"silver.table_names.{dataset_name}",
            allow_placeholder=False,
        )

    night_start = silver_cfg.get("night_start_hour")
    night_end = silver_cfg.get("night_end_hour")
    if not isinstance(night_start, int) or not 0 <= night_start <= 23:
        raise ValueError("silver.night_start_hour must be an integer from 0 through 23.")
    if not isinstance(night_end, int) or not 1 <= night_end <= 24:
        raise ValueError("silver.night_end_hour must be an integer from 1 through 24.")
    if night_start >= night_end:
        raise ValueError("Silver night hours must be a non-wrapping interval.")

    features_cfg = config["features"]
    feature_storage_mode = features_cfg.get("storage_mode")
    if feature_storage_mode not in VALID_STORAGE_MODES:
        raise ValueError(f"features.storage_mode must be one of {sorted(VALID_STORAGE_MODES)}")
    feature_write_mode = features_cfg.get("write_mode")
    if feature_write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"features.write_mode must be one of {sorted(VALID_WRITE_MODES)}")
    if feature_storage_mode == "path" and not features_cfg.get("base_path"):
        raise ValueError("features.base_path is required when features.storage_mode=path")
    for dataset_name in FEATURE_DATASET_NAMES:
        table_name = features_cfg.get("table_names", {}).get(dataset_name)
        if not table_name:
            raise ValueError(f"features.table_names.{dataset_name} is required.")
        validate_identifier(
            table_name,
            label=f"features.table_names.{dataset_name}",
            allow_placeholder=False,
        )

    try:
        train_end_date = date.fromisoformat(features_cfg["train_end_date"])
        validation_end_date = date.fromisoformat(features_cfg["validation_end_date"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Feature split dates must use YYYY-MM-DD format.") from exc
    if train_end_date >= validation_end_date:
        raise ValueError("features.train_end_date must be before validation_end_date.")
    alpha = features_cfg.get("mcc_smoothing_alpha")
    if not isinstance(alpha, (int, float)) or alpha <= 0:
        raise ValueError("features.mcc_smoothing_alpha must be positive.")


def apply_runtime_overrides(
    config: dict[str, Any],
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a copy of the config with notebook runtime overrides applied."""
    resolved = deepcopy(config)
    overrides = overrides or {}

    databricks_cfg = resolved["databricks"]
    bronze_cfg = resolved["bronze"]
    silver_cfg = resolved["silver"]
    features_cfg = resolved["features"]

    for key in (
        "catalog_name",
        "schema_name",
        "volume_name",
        "raw_data_subdirectory",
        "raw_data_path",
        "create_catalog_if_missing",
        "create_schema_if_missing",
    ):
        if key in overrides and overrides[key] not in (None, ""):
            databricks_cfg[key] = overrides[key]

    for key in ("storage_mode", "write_format", "write_mode", "base_path"):
        if key in overrides and overrides[key] not in (None, ""):
            bronze_cfg[key] = overrides[key]

    for key in (
        "silver_storage_mode",
        "silver_write_format",
        "silver_write_mode",
        "silver_base_path",
    ):
        if key in overrides and overrides[key] not in (None, ""):
            silver_cfg[key.removeprefix("silver_")] = overrides[key]

    for key in (
        "feature_storage_mode",
        "feature_write_format",
        "feature_write_mode",
        "feature_base_path",
    ):
        if key in overrides and overrides[key] not in (None, ""):
            features_cfg[key.removeprefix("feature_")] = overrides[key]

    if not overrides.get("raw_data_path"):
        if not any(
            is_placeholder(databricks_cfg.get(name))
            for name in ("catalog_name", "schema_name", "volume_name")
        ):
            databricks_cfg["raw_data_path"] = build_raw_data_path(
                databricks_cfg["catalog_name"],
                databricks_cfg["schema_name"],
                databricks_cfg["volume_name"],
                databricks_cfg["raw_data_subdirectory"],
            )

    validate_project_config(resolved)
    return resolved


def bronze_table_name(config: dict[str, Any], dataset_name: str) -> str:
    """Return the fully-qualified Unity Catalog Bronze table name."""
    catalog_name = config["databricks"]["catalog_name"]
    schema_name = config["databricks"]["schema_name"]
    table_name = config["bronze"]["table_names"][dataset_name]
    validate_identifier(catalog_name, label="catalog_name", allow_placeholder=False)
    validate_identifier(schema_name, label="schema_name", allow_placeholder=False)
    return f"{catalog_name}.{schema_name}.{table_name}"


def bronze_target_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the configured Delta path for a Bronze dataset when path mode is used."""
    overrides = config["bronze"].get("table_path_overrides", {})
    if dataset_name in overrides and overrides[dataset_name]:
        return overrides[dataset_name].rstrip("/")
    base_path = config["bronze"]["base_path"].rstrip("/")
    table_name = config["bronze"]["table_names"][dataset_name]
    return f"{base_path}/{table_name}"


def raw_source_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the full raw file path for one dataset."""
    raw_data_path = config["databricks"]["raw_data_path"].rstrip("/")
    file_name = config["raw_sources"]["expected_files"][dataset_name]
    return f"{raw_data_path}/{file_name}"


def raw_source_paths(config: dict[str, Any]) -> dict[str, str]:
    """Return full raw source paths for every supported dataset."""
    return {dataset_name: raw_source_path(config, dataset_name) for dataset_name in dataset_names()}


def bronze_dataset_runtime(config: dict[str, Any], dataset_name: str) -> dict[str, str]:
    """Return the resolved source and target settings for one Bronze dataset."""
    runtime = {
        "dataset_name": dataset_name,
        "source_path": raw_source_path(config, dataset_name),
        "storage_mode": config["bronze"]["storage_mode"],
        "write_format": config["bronze"]["write_format"],
        "write_mode": config["bronze"]["write_mode"],
    }
    if runtime["storage_mode"] == "unity_catalog":
        runtime["target_table"] = bronze_table_name(config, dataset_name)
        runtime["target_path"] = ""
    else:
        runtime["target_table"] = ""
        runtime["target_path"] = bronze_target_path(config, dataset_name)
    return runtime


def silver_table_name(config: dict[str, Any], dataset_name: str) -> str:
    """Return the fully-qualified Unity Catalog Silver table name."""
    if dataset_name not in SILVER_DATASET_NAMES:
        raise ValueError(f"Unsupported Silver dataset: {dataset_name}")
    catalog_name = config["databricks"]["catalog_name"]
    schema_name = config["databricks"]["schema_name"]
    table_name = config["silver"]["table_names"][dataset_name]
    validate_identifier(catalog_name, label="catalog_name", allow_placeholder=False)
    validate_identifier(schema_name, label="schema_name", allow_placeholder=False)
    return f"{catalog_name}.{schema_name}.{table_name}"


def silver_target_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the configured Delta path for a Silver dataset."""
    if dataset_name not in SILVER_DATASET_NAMES:
        raise ValueError(f"Unsupported Silver dataset: {dataset_name}")
    overrides = config["silver"].get("table_path_overrides", {})
    if dataset_name in overrides and overrides[dataset_name]:
        return overrides[dataset_name].rstrip("/")
    base_path = config["silver"]["base_path"].rstrip("/")
    table_name = config["silver"]["table_names"][dataset_name]
    return f"{base_path}/{table_name}"


def silver_dataset_runtime(config: dict[str, Any], dataset_name: str) -> dict[str, str]:
    """Return the resolved target settings for one Silver dataset."""
    silver_cfg = config["silver"]
    runtime = {
        "dataset_name": dataset_name,
        "storage_mode": silver_cfg["storage_mode"],
        "write_format": silver_cfg["write_format"],
        "write_mode": silver_cfg["write_mode"],
    }
    if runtime["storage_mode"] == "unity_catalog":
        runtime["target_table"] = silver_table_name(config, dataset_name)
        runtime["target_path"] = ""
    else:
        runtime["target_table"] = ""
        runtime["target_path"] = silver_target_path(config, dataset_name)
    return runtime


def feature_table_name(config: dict[str, Any], dataset_name: str) -> str:
    """Return the fully-qualified Unity Catalog feature table name."""
    if dataset_name not in FEATURE_DATASET_NAMES:
        raise ValueError(f"Unsupported feature dataset: {dataset_name}")
    catalog_name = config["databricks"]["catalog_name"]
    schema_name = config["databricks"]["schema_name"]
    table_name = config["features"]["table_names"][dataset_name]
    validate_identifier(catalog_name, label="catalog_name", allow_placeholder=False)
    validate_identifier(schema_name, label="schema_name", allow_placeholder=False)
    return f"{catalog_name}.{schema_name}.{table_name}"


def feature_target_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the configured Delta path for a feature dataset."""
    if dataset_name not in FEATURE_DATASET_NAMES:
        raise ValueError(f"Unsupported feature dataset: {dataset_name}")
    overrides = config["features"].get("table_path_overrides", {})
    if dataset_name in overrides and overrides[dataset_name]:
        return overrides[dataset_name].rstrip("/")
    base_path = config["features"]["base_path"].rstrip("/")
    table_name = config["features"]["table_names"][dataset_name]
    return f"{base_path}/{table_name}"


def feature_dataset_runtime(config: dict[str, Any], dataset_name: str) -> dict[str, str]:
    """Return the resolved target settings for one feature dataset."""
    feature_cfg = config["features"]
    runtime = {
        "dataset_name": dataset_name,
        "storage_mode": feature_cfg["storage_mode"],
        "write_format": feature_cfg["write_format"],
        "write_mode": feature_cfg["write_mode"],
    }
    if runtime["storage_mode"] == "unity_catalog":
        runtime["target_table"] = feature_table_name(config, dataset_name)
        runtime["target_path"] = ""
    else:
        runtime["target_table"] = ""
        runtime["target_path"] = feature_target_path(config, dataset_name)
    return runtime
