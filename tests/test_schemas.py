"""Schema-registry tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.schemas import DATASET_SCHEMAS, dataset_names, expected_source_file_name


class SchemaRegistryTests(unittest.TestCase):
    def test_expected_dataset_keys_exist(self) -> None:
        self.assertEqual(
            set(DATASET_SCHEMAS),
            {"transactions", "users", "cards", "fraud_labels", "mcc_codes"},
        )

    def test_dataset_order_is_stable(self) -> None:
        self.assertEqual(
            dataset_names(),
            ("transactions", "users", "cards", "fraud_labels", "mcc_codes"),
        )

    def test_csv_schemas_have_unique_column_names(self) -> None:
        for dataset_name in ("transactions", "users", "cards"):
            columns = DATASET_SCHEMAS[dataset_name].raw_columns
            self.assertEqual(len(columns), len(set(columns)), dataset_name)

    def test_source_file_names_are_deterministic(self) -> None:
        self.assertEqual(expected_source_file_name("transactions"), "transactions_data.csv")
        self.assertEqual(expected_source_file_name("fraud_labels"), "train_fraud_labels.json")

    def test_fraud_labels_json_root_key_is_target(self) -> None:
        fraud_schema = DATASET_SCHEMAS["fraud_labels"]
        self.assertEqual(fraud_schema.source_format, "json")
        self.assertEqual(fraud_schema.json_root_key, "target")
        self.assertEqual(fraud_schema.bronze_columns, ("transaction_id", "is_fraud"))

    def test_mcc_codes_bronze_columns_are_defined(self) -> None:
        mcc_schema = DATASET_SCHEMAS["mcc_codes"]
        self.assertEqual(mcc_schema.source_format, "json")
        self.assertEqual(mcc_schema.bronze_columns, ("mcc", "mcc_category"))


if __name__ == "__main__":
    unittest.main()
