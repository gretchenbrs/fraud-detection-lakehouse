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
MODEL_DATASET_NAMES = (
    "scored_predictions",
    "overall_metrics",
    "threshold_metrics",
    "top_k_metrics",
)
TUNING_DATASET_NAME = "tuning_trials"
GOLD_DATASET_NAMES = ("model_scorecard", "daily_risk_kpis", "investigation_queue")


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
        "models",
        "gold",
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

    models_cfg = config["models"]
    model_storage_mode = models_cfg.get("storage_mode")
    if model_storage_mode not in VALID_STORAGE_MODES:
        raise ValueError(f"models.storage_mode must be one of {sorted(VALID_STORAGE_MODES)}")
    model_write_mode = models_cfg.get("write_mode")
    if model_write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"models.write_mode must be one of {sorted(VALID_WRITE_MODES)}")
    if model_storage_mode == "path" and not models_cfg.get("base_path"):
        raise ValueError("models.base_path is required when models.storage_mode=path")
    for dataset_name in MODEL_DATASET_NAMES:
        table_name = models_cfg.get("table_names", {}).get(dataset_name)
        if not table_name:
            raise ValueError(f"models.table_names.{dataset_name} is required.")
        validate_identifier(
            table_name,
            label=f"models.table_names.{dataset_name}",
            allow_placeholder=False,
        )

    thresholds = models_cfg.get("thresholds")
    if not isinstance(thresholds, list) or not thresholds:
        raise ValueError("models.thresholds must be a non-empty list.")
    if any(not isinstance(value, (int, float)) or not 0 < value < 1 for value in thresholds):
        raise ValueError("Every models.thresholds value must be between 0 and 1.")
    if thresholds != sorted(set(thresholds)):
        raise ValueError("models.thresholds must be unique and sorted ascending.")

    top_k_fractions = models_cfg.get("top_k_fractions")
    if not isinstance(top_k_fractions, list) or not top_k_fractions:
        raise ValueError("models.top_k_fractions must be a non-empty list.")
    if any(
        not isinstance(value, (int, float)) or not 0 < value <= 1
        for value in top_k_fractions
    ):
        raise ValueError("Every models.top_k_fractions value must be in (0, 1].")
    if top_k_fractions != sorted(set(top_k_fractions)):
        raise ValueError("models.top_k_fractions must be unique and sorted ascending.")

    seed = models_cfg.get("seed")
    if not isinstance(seed, int):
        raise ValueError("models.seed must be an integer.")
    if not isinstance(models_cfg.get("use_class_weights"), bool):
        raise ValueError("models.use_class_weights must be true or false.")

    logistic_cfg = models_cfg.get("logistic_regression", {})
    if not isinstance(logistic_cfg.get("max_iter"), int) or logistic_cfg["max_iter"] <= 0:
        raise ValueError("models.logistic_regression.max_iter must be a positive integer.")
    reg_param = logistic_cfg.get("reg_param")
    if not isinstance(reg_param, (int, float)) or reg_param < 0:
        raise ValueError("models.logistic_regression.reg_param must be non-negative.")
    elastic_net_param = logistic_cfg.get("elastic_net_param")
    if not isinstance(elastic_net_param, (int, float)) or not 0 <= elastic_net_param <= 1:
        raise ValueError(
            "models.logistic_regression.elastic_net_param must be between 0 and 1."
        )

    forest_cfg = models_cfg.get("random_forest", {})
    for key in ("num_trees", "max_depth", "max_bins"):
        value = forest_cfg.get(key)
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"models.random_forest.{key} must be a positive integer.")
    subsampling_rate = forest_cfg.get("subsampling_rate")
    if not isinstance(subsampling_rate, (int, float)) or not 0 < subsampling_rate <= 1:
        raise ValueError("models.random_forest.subsampling_rate must be in (0, 1].")

    tuning_cfg = models_cfg.get("tuning")
    if tuning_cfg is not None:
        if not isinstance(tuning_cfg.get("enabled"), bool):
            raise ValueError("models.tuning.enabled must be true or false.")
        if tuning_cfg.get("selection_metric") not in {"pr_auc", "roc_auc"}:
            raise ValueError("models.tuning.selection_metric must be pr_auc or roc_auc.")
        candidates = tuning_cfg.get("candidates")
        if not isinstance(candidates, list) or len(candidates) < 2:
            raise ValueError("models.tuning.candidates must contain at least two candidates.")
        candidate_names: list[str] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                raise ValueError("Each models.tuning candidate must be a mapping.")
            candidate_name = candidate.get("model_name")
            validate_identifier(
                candidate_name,
                label="models.tuning.candidates.model_name",
                allow_placeholder=False,
            )
            candidate_names.append(candidate_name)
            family = candidate.get("family")
            parameters = candidate.get("parameters")
            if family not in {"logistic_regression", "random_forest"}:
                raise ValueError("Tuning candidate family must be logistic_regression or random_forest.")
            if not isinstance(parameters, dict):
                raise ValueError("Tuning candidate parameters must be a mapping.")
            if family == "logistic_regression":
                if not isinstance(parameters.get("max_iter"), int) or parameters["max_iter"] <= 0:
                    raise ValueError("Tuned logistic_regression max_iter must be a positive integer.")
                if not isinstance(parameters.get("reg_param"), (int, float)) or parameters["reg_param"] < 0:
                    raise ValueError("Tuned logistic_regression reg_param must be non-negative.")
                elastic_net = parameters.get("elastic_net_param")
                if not isinstance(elastic_net, (int, float)) or not 0 <= elastic_net <= 1:
                    raise ValueError("Tuned logistic_regression elastic_net_param must be in [0, 1].")
            else:
                for key in ("num_trees", "max_depth", "max_bins"):
                    if not isinstance(parameters.get(key), int) or parameters[key] <= 0:
                        raise ValueError(f"Tuned random_forest {key} must be a positive integer.")
                candidate_subsampling = parameters.get("subsampling_rate")
                if not isinstance(candidate_subsampling, (int, float)) or not 0 < candidate_subsampling <= 1:
                    raise ValueError("Tuned random_forest subsampling_rate must be in (0, 1].")
        if len(candidate_names) != len(set(candidate_names)):
            raise ValueError("models.tuning candidate model_name values must be unique.")
        if tuning_cfg["enabled"]:
            tuning_table = models_cfg.get("table_names", {}).get(TUNING_DATASET_NAME)
            if not tuning_table:
                raise ValueError("models.table_names.tuning_trials is required when tuning is enabled.")
            validate_identifier(
                tuning_table,
                label="models.table_names.tuning_trials",
                allow_placeholder=False,
            )

    gold_cfg = config["gold"]
    gold_storage_mode = gold_cfg.get("storage_mode")
    if gold_storage_mode not in VALID_STORAGE_MODES:
        raise ValueError(f"gold.storage_mode must be one of {sorted(VALID_STORAGE_MODES)}")
    gold_write_mode = gold_cfg.get("write_mode")
    if gold_write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"gold.write_mode must be one of {sorted(VALID_WRITE_MODES)}")
    if gold_storage_mode == "path" and not gold_cfg.get("base_path"):
        raise ValueError("gold.base_path is required when gold.storage_mode=path")
    for dataset_name in GOLD_DATASET_NAMES:
        table_name = gold_cfg.get("table_names", {}).get(dataset_name)
        if not table_name:
            raise ValueError(f"gold.table_names.{dataset_name} is required.")
        validate_identifier(
            table_name,
            label=f"gold.table_names.{dataset_name}",
            allow_placeholder=False,
        )
    if gold_cfg.get("champion_metric") not in {"pr_auc", "roc_auc"}:
        raise ValueError("gold.champion_metric must be pr_auc or roc_auc.")
    if gold_cfg.get("evaluation_split") != "test":
        raise ValueError("gold.evaluation_split must remain test for final reporting.")
    queue_fraction = gold_cfg.get("investigation_queue_fraction")
    if not isinstance(queue_fraction, (int, float)) or not 0 < queue_fraction <= 1:
        raise ValueError("gold.investigation_queue_fraction must be in (0, 1].")
    priority_fractions = gold_cfg.get("priority_fractions")
    if not isinstance(priority_fractions, list) or len(priority_fractions) != 3:
        raise ValueError("gold.priority_fractions must contain three ordered values.")
    if (
        any(not isinstance(value, (int, float)) or not 0 < value <= 1 for value in priority_fractions)
        or priority_fractions != sorted(set(priority_fractions))
        or priority_fractions[-1] > queue_fraction
    ):
        raise ValueError(
            "gold.priority_fractions must be unique, ascending, and within the queue fraction."
        )


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
    models_cfg = resolved["models"]
    gold_cfg = resolved["gold"]

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

    for key in (
        "model_storage_mode",
        "model_write_format",
        "model_write_mode",
        "model_base_path",
    ):
        if key in overrides and overrides[key] not in (None, ""):
            models_cfg[key.removeprefix("model_")] = overrides[key]

    for key in (
        "gold_storage_mode",
        "gold_write_format",
        "gold_write_mode",
        "gold_base_path",
    ):
        if key in overrides and overrides[key] not in (None, ""):
            gold_cfg[key.removeprefix("gold_")] = overrides[key]

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


def model_table_name(config: dict[str, Any], dataset_name: str) -> str:
    """Return the fully-qualified Unity Catalog model-output table name."""
    if dataset_name not in (*MODEL_DATASET_NAMES, TUNING_DATASET_NAME):
        raise ValueError(f"Unsupported model dataset: {dataset_name}")
    catalog_name = config["databricks"]["catalog_name"]
    schema_name = config["databricks"]["schema_name"]
    table_name = config["models"]["table_names"][dataset_name]
    validate_identifier(catalog_name, label="catalog_name", allow_placeholder=False)
    validate_identifier(schema_name, label="schema_name", allow_placeholder=False)
    return f"{catalog_name}.{schema_name}.{table_name}"


def model_target_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the configured Delta path for a model output."""
    if dataset_name not in (*MODEL_DATASET_NAMES, TUNING_DATASET_NAME):
        raise ValueError(f"Unsupported model dataset: {dataset_name}")
    overrides = config["models"].get("table_path_overrides", {})
    if dataset_name in overrides and overrides[dataset_name]:
        return overrides[dataset_name].rstrip("/")
    base_path = config["models"]["base_path"].rstrip("/")
    table_name = config["models"]["table_names"][dataset_name]
    return f"{base_path}/{table_name}"


def model_dataset_runtime(config: dict[str, Any], dataset_name: str) -> dict[str, str]:
    """Return the resolved target settings for a model output."""
    model_cfg = config["models"]
    runtime = {
        "dataset_name": dataset_name,
        "storage_mode": model_cfg["storage_mode"],
        "write_format": model_cfg["write_format"],
        "write_mode": model_cfg["write_mode"],
    }
    if runtime["storage_mode"] == "unity_catalog":
        runtime["target_table"] = model_table_name(config, dataset_name)
        runtime["target_path"] = ""
    else:
        runtime["target_table"] = ""
        runtime["target_path"] = model_target_path(config, dataset_name)
    return runtime


def gold_table_name(config: dict[str, Any], dataset_name: str) -> str:
    """Return the fully-qualified Unity Catalog Gold table name."""
    if dataset_name not in GOLD_DATASET_NAMES:
        raise ValueError(f"Unsupported Gold dataset: {dataset_name}")
    catalog_name = config["databricks"]["catalog_name"]
    schema_name = config["databricks"]["schema_name"]
    table_name = config["gold"]["table_names"][dataset_name]
    validate_identifier(catalog_name, label="catalog_name", allow_placeholder=False)
    validate_identifier(schema_name, label="schema_name", allow_placeholder=False)
    return f"{catalog_name}.{schema_name}.{table_name}"


def gold_target_path(config: dict[str, Any], dataset_name: str) -> str:
    """Return the configured Delta path for a Gold dataset."""
    if dataset_name not in GOLD_DATASET_NAMES:
        raise ValueError(f"Unsupported Gold dataset: {dataset_name}")
    overrides = config["gold"].get("table_path_overrides", {})
    if dataset_name in overrides and overrides[dataset_name]:
        return overrides[dataset_name].rstrip("/")
    base_path = config["gold"]["base_path"].rstrip("/")
    return f"{base_path}/{config['gold']['table_names'][dataset_name]}"


def gold_dataset_runtime(config: dict[str, Any], dataset_name: str) -> dict[str, str]:
    """Return resolved target settings for a Gold dataset."""
    gold_cfg = config["gold"]
    runtime = {
        "dataset_name": dataset_name,
        "storage_mode": gold_cfg["storage_mode"],
        "write_format": gold_cfg["write_format"],
        "write_mode": gold_cfg["write_mode"],
    }
    if runtime["storage_mode"] == "unity_catalog":
        runtime["target_table"] = gold_table_name(config, dataset_name)
        runtime["target_path"] = ""
    else:
        runtime["target_table"] = ""
        runtime["target_path"] = gold_target_path(config, dataset_name)
    return runtime
