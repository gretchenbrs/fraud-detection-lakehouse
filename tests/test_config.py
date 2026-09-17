"""Configuration validation and target-construction tests."""

from __future__ import annotations

import sys
import unittest
from copy import deepcopy
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.config import (
    apply_runtime_overrides,
    bronze_dataset_runtime,
    bronze_table_name,
    bronze_target_path,
    build_raw_data_path,
    gold_dataset_runtime,
    gold_table_name,
    model_dataset_runtime,
    model_table_name,
    validate_identifier,
    validate_project_config,
)


def _base_config() -> dict:
    return {
        "project": {"name": "fraud-risk-lakehouse", "layer": "bronze", "owner": "gretchen"},
        "databricks": {
            "catalog_name": "demo_catalog",
            "schema_name": "demo_schema",
            "volume_name": "demo_volume",
            "raw_data_subdirectory": "fraud_raw",
            "raw_data_path": "/Volumes/demo_catalog/demo_schema/demo_volume/fraud_raw",
            "create_catalog_if_missing": False,
            "create_schema_if_missing": False,
        },
        "bronze": {
            "storage_mode": "unity_catalog",
            "write_format": "delta",
            "write_mode": "errorifexists",
            "table_names": {
                "transactions": "bronze_transactions",
                "users": "bronze_users",
                "cards": "bronze_cards",
                "fraud_labels": "bronze_fraud_labels",
                "mcc_codes": "bronze_mcc_codes",
            },
            "base_path": "dbfs:/tmp/fraud-risk-lakehouse/bronze",
            "table_path_overrides": {},
        },
        "silver": {
            "storage_mode": "unity_catalog",
            "write_format": "delta",
            "write_mode": "overwrite",
            "table_names": {
                "users": "silver_users",
                "cards": "silver_cards",
                "fraud_labels": "silver_fraud_labels",
                "mcc_codes": "silver_mcc_codes",
                "transactions": "silver_transactions",
            },
            "base_path": "dbfs:/tmp/fraud-risk-lakehouse/silver",
            "table_path_overrides": {},
            "night_start_hour": 0,
            "night_end_hour": 6,
        },
        "features": {
            "storage_mode": "unity_catalog",
            "write_format": "delta",
            "write_mode": "overwrite",
            "table_names": {
                "split_assignments": "feature_split_assignments",
                "mcc_fraud_rates": "feature_mcc_fraud_rates",
                "model_features": "feature_model_features",
            },
            "base_path": "dbfs:/tmp/fraud-risk-lakehouse/features",
            "table_path_overrides": {},
            "train_end_date": "2017-12-31",
            "validation_end_date": "2018-12-31",
            "mcc_smoothing_alpha": 100.0,
        },
        "models": {
            "storage_mode": "unity_catalog",
            "write_format": "delta",
            "write_mode": "overwrite",
            "table_names": {
                "scored_predictions": "model_scored_predictions",
                "overall_metrics": "model_overall_metrics",
                "threshold_metrics": "model_threshold_metrics",
                "top_k_metrics": "model_top_k_metrics",
            },
            "base_path": "dbfs:/tmp/fraud-risk-lakehouse/models",
            "table_path_overrides": {},
            "seed": 42,
            "use_class_weights": True,
            "thresholds": [0.1, 0.5, 0.9],
            "top_k_fractions": [0.01, 0.05],
            "logistic_regression": {
                "max_iter": 40,
                "reg_param": 0.05,
                "elastic_net_param": 0.0,
            },
            "random_forest": {
                "num_trees": 40,
                "max_depth": 8,
                "max_bins": 128,
                "subsampling_rate": 0.7,
            },
        },
        "gold": {
            "storage_mode": "unity_catalog",
            "write_format": "delta",
            "write_mode": "overwrite",
            "table_names": {
                "model_scorecard": "gold_model_scorecard",
                "daily_risk_kpis": "gold_daily_risk_kpis",
                "investigation_queue": "gold_investigation_queue",
            },
            "base_path": "dbfs:/tmp/fraud-risk-lakehouse/gold",
            "table_path_overrides": {},
            "champion_metric": "pr_auc",
            "evaluation_split": "test",
            "investigation_queue_fraction": 0.01,
            "priority_fractions": [0.001, 0.005, 0.01],
        },
        "raw_sources": {
            "expected_files": {
                "transactions": "transactions_data.csv",
                "users": "users_data.csv",
                "cards": "cards_data.csv",
                "fraud_labels": "train_fraud_labels.json",
                "mcc_codes": "mcc_codes.json",
            }
        },
    }


class ConfigValidationTests(unittest.TestCase):
    def test_validate_project_config_accepts_valid_config(self) -> None:
        validate_project_config(_base_config())

    def test_build_raw_data_path_uses_volume_structure(self) -> None:
        self.assertEqual(
            build_raw_data_path("cat", "sch", "vol", "fraud_raw"),
            "/Volumes/cat/sch/vol/fraud_raw",
        )

    def test_validate_identifier_rejects_unsafe_value(self) -> None:
        with self.assertRaises(ValueError):
            validate_identifier("demo-schema", label="schema_name")

    def test_bronze_table_name_is_fully_qualified(self) -> None:
        self.assertEqual(
            bronze_table_name(_base_config(), "transactions"),
            "demo_catalog.demo_schema.bronze_transactions",
        )

    def test_path_mode_runtime_builds_target_path(self) -> None:
        config = deepcopy(_base_config())
        config["bronze"]["storage_mode"] = "path"
        runtime = bronze_dataset_runtime(config, "cards")
        self.assertEqual(runtime["target_path"], "dbfs:/tmp/fraud-risk-lakehouse/bronze/bronze_cards")
        self.assertEqual(runtime["target_table"], "")

    def test_bronze_target_path_honors_override(self) -> None:
        config = deepcopy(_base_config())
        config["bronze"]["table_path_overrides"]["transactions"] = "dbfs:/custom/bronze_transactions"
        self.assertEqual(bronze_target_path(config, "transactions"), "dbfs:/custom/bronze_transactions")

    def test_runtime_override_recomputes_raw_data_path(self) -> None:
        config = _base_config()
        updated = apply_runtime_overrides(
            config,
            overrides={"catalog_name": "prodcat", "schema_name": "fraud", "volume_name": "rawvol", "raw_data_path": ""},
        )
        self.assertEqual(
            updated["databricks"]["raw_data_path"],
            "/Volumes/prodcat/fraud/rawvol/fraud_raw",
        )

    def test_build_raw_data_path_allows_volume_root(self) -> None:
        self.assertEqual(
            build_raw_data_path("cat", "sch", "vol", ""),
            "/Volumes/cat/sch/vol",
        )

    def test_silver_config_requires_all_output_tables(self) -> None:
        config = _base_config()
        del config["silver"]["table_names"]["transactions"]
        with self.assertRaises(ValueError):
            validate_project_config(config)

    def test_feature_split_dates_must_be_chronological(self) -> None:
        config = _base_config()
        config["features"]["train_end_date"] = "2019-01-01"
        config["features"]["validation_end_date"] = "2018-12-31"
        with self.assertRaises(ValueError):
            validate_project_config(config)

    def test_model_thresholds_must_be_sorted_and_unique(self) -> None:
        config = _base_config()
        config["models"]["thresholds"] = [0.5, 0.1, 0.5]
        with self.assertRaises(ValueError):
            validate_project_config(config)

    def test_model_table_name_is_fully_qualified(self) -> None:
        self.assertEqual(
            model_table_name(_base_config(), "overall_metrics"),
            "demo_catalog.demo_schema.model_overall_metrics",
        )

    def test_model_runtime_uses_managed_table(self) -> None:
        runtime = model_dataset_runtime(_base_config(), "top_k_metrics")
        self.assertEqual(
            runtime["target_table"],
            "demo_catalog.demo_schema.model_top_k_metrics",
        )
        self.assertEqual(runtime["target_path"], "")

    def test_model_hyperparameters_are_validated(self) -> None:
        config = _base_config()
        config["models"]["random_forest"]["subsampling_rate"] = 1.5
        with self.assertRaises(ValueError):
            validate_project_config(config)

    def test_gold_table_name_is_fully_qualified(self) -> None:
        self.assertEqual(
            gold_table_name(_base_config(), "investigation_queue"),
            "demo_catalog.demo_schema.gold_investigation_queue",
        )

    def test_gold_runtime_uses_managed_table(self) -> None:
        runtime = gold_dataset_runtime(_base_config(), "daily_risk_kpis")
        self.assertEqual(
            runtime["target_table"],
            "demo_catalog.demo_schema.gold_daily_risk_kpis",
        )
        self.assertEqual(runtime["target_path"], "")

    def test_gold_queue_fraction_must_cover_priority_cutoffs(self) -> None:
        config = _base_config()
        config["gold"]["investigation_queue_fraction"] = 0.004
        with self.assertRaises(ValueError):
            validate_project_config(config)


if __name__ == "__main__":
    unittest.main()
