"""Pure-Python tests for Gold prioritization rules."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.gold.rules import priority_tier_for_fraction, validate_priority_fractions
from src.utils.config import gold_dataset_runtime, gold_table_name

from test_config import _base_config


class GoldRuleTests(unittest.TestCase):
    def test_priority_tiers_use_population_rank_only(self) -> None:
        cutoffs = [0.001, 0.005, 0.01]
        self.assertEqual(priority_tier_for_fraction(0.0005, cutoffs), "critical")
        self.assertEqual(priority_tier_for_fraction(0.003, cutoffs), "high")
        self.assertEqual(priority_tier_for_fraction(0.008, cutoffs), "medium")
        self.assertEqual(priority_tier_for_fraction(0.02, cutoffs), "standard")

    def test_priority_fractions_must_fit_inside_queue(self) -> None:
        validate_priority_fractions([0.001, 0.005, 0.01], 0.01)
        with self.assertRaises(ValueError):
            validate_priority_fractions([0.001, 0.005, 0.02], 0.01)

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


if __name__ == "__main__":
    unittest.main()
