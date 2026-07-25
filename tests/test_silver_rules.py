"""Pure-Python tests for Silver feature contracts."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.silver.transactions import KNOWN_ERROR_TOKENS, US_LOCATION_CODES
from src.utils.config import silver_dataset_runtime, silver_table_name

from test_config import _base_config


class SilverRuleTests(unittest.TestCase):
    def test_domestic_codes_include_observed_us_values(self) -> None:
        self.assertTrue({"CA", "DC", "AA", "NY"}.issubset(US_LOCATION_CODES))

    def test_country_names_are_not_classified_as_domestic_codes(self) -> None:
        self.assertNotIn("Canada", US_LOCATION_CODES)
        self.assertNotIn("United Kingdom", US_LOCATION_CODES)

    def test_interpretable_error_tokens_are_registered(self) -> None:
        self.assertIn("Bad CVV", KNOWN_ERROR_TOKENS)
        self.assertIn("Bad PIN", KNOWN_ERROR_TOKENS)
        self.assertIn("Insufficient Balance", KNOWN_ERROR_TOKENS)
        self.assertIn("Technical Glitch", KNOWN_ERROR_TOKENS)

    def test_silver_table_name_is_fully_qualified(self) -> None:
        self.assertEqual(
            silver_table_name(_base_config(), "transactions"),
            "demo_catalog.demo_schema.silver_transactions",
        )

    def test_silver_runtime_uses_managed_table(self) -> None:
        runtime = silver_dataset_runtime(_base_config(), "cards")
        self.assertEqual(runtime["target_table"], "demo_catalog.demo_schema.silver_cards")
        self.assertEqual(runtime["target_path"], "")


if __name__ == "__main__":
    unittest.main()
