"""Required-column validation tests."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.schemas import DATASET_SCHEMAS, validate_required_columns


class RequiredColumnValidationTests(unittest.TestCase):
    def test_validate_required_columns_accepts_complete_transaction_header(self) -> None:
        transaction_columns = list(DATASET_SCHEMAS["transactions"].raw_columns)
        validate_required_columns(
            actual_columns=transaction_columns,
            required_columns=transaction_columns,
            dataset_name="transactions",
        )

    def test_validate_required_columns_raises_for_missing_transaction_column(self) -> None:
        transaction_columns = list(DATASET_SCHEMAS["transactions"].raw_columns)
        incomplete_columns = transaction_columns[:-1]

        with self.assertRaises(ValueError) as exc_info:
            validate_required_columns(
                actual_columns=incomplete_columns,
                required_columns=transaction_columns,
                dataset_name="transactions",
            )

        self.assertIn("errors", str(exc_info.exception))
        self.assertIn("transactions", str(exc_info.exception))

    def test_cards_and_users_headers_are_non_empty(self) -> None:
        self.assertGreater(len(DATASET_SCHEMAS["cards"].raw_columns), 0)
        self.assertGreater(len(DATASET_SCHEMAS["users"].raw_columns), 0)


if __name__ == "__main__":
    unittest.main()
