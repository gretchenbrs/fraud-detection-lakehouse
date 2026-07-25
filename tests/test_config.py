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


if __name__ == "__main__":
    unittest.main()
