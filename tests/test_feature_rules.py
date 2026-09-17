"""Pure-Python tests for leakage-aware feature contracts."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.features.engineering import smoothed_binary_rate
from src.features.splits import VALID_SPLITS, validate_split_dates
from src.utils.config import feature_dataset_runtime, feature_table_name

from test_config import _base_config


class FeatureRuleTests(unittest.TestCase):
    def test_split_names_are_stable(self) -> None:
        self.assertEqual(VALID_SPLITS, ("train", "validation", "test"))

    def test_split_dates_are_ordered(self) -> None:
        validate_split_dates("2017-12-31", "2018-12-31")
        with self.assertRaises(ValueError):
            validate_split_dates("2019-01-01", "2018-12-31")

    def test_smoothed_rate_shrinks_toward_global_rate(self) -> None:
        rate = smoothed_binary_rate(
            positive_count=1,
            observation_count=2,
            global_rate=0.01,
            alpha=100.0,
        )
        self.assertGreater(rate, 0.01)
        self.assertLess(rate, 0.5)

    def test_feature_table_name_is_fully_qualified(self) -> None:
        self.assertEqual(
            feature_table_name(_base_config(), "model_features"),
            "demo_catalog.demo_schema.feature_model_features",
        )

    def test_feature_runtime_uses_managed_table(self) -> None:
        runtime = feature_dataset_runtime(_base_config(), "mcc_fraud_rates")
        self.assertEqual(
            runtime["target_table"],
            "demo_catalog.demo_schema.feature_mcc_fraud_rates",
        )
        self.assertEqual(runtime["target_path"], "")


if __name__ == "__main__":
    unittest.main()
