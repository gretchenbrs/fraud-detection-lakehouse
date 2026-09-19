"""Pure-Python tests for reporting dimension guardrails."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.gold.reporting import REPORTING_SEGMENT_DIMENSIONS, validate_segment_dimension


class ReportingRuleTests(unittest.TestCase):
    def test_expected_reporting_dimensions_are_registered(self) -> None:
        self.assertEqual(
            REPORTING_SEGMENT_DIMENSIONS,
            (
                "priority_tier",
                "mcc_category",
                "merchant_location_category",
                "transaction_type",
            ),
        )

    def test_arbitrary_reporting_dimension_is_rejected(self) -> None:
        validate_segment_dimension("mcc_category")
        with self.assertRaises(ValueError):
            validate_segment_dimension("actual_is_fraud")


if __name__ == "__main__":
    unittest.main()
