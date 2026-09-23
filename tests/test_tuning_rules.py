"""Pure-Python guardrails for leakage-safe tuning configuration."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.models.tuning import tuning_candidates


class TuningRulesTests(unittest.TestCase):
    def test_enabled_tuning_returns_configured_candidates(self) -> None:
        model_config = {
            "tuning": {
                "enabled": True,
                "candidates": [
                    {"model_name": "baseline", "family": "logistic_regression", "parameters": {}}
                ],
            }
        }
        self.assertEqual(tuning_candidates(model_config)[0]["model_name"], "baseline")

    def test_disabled_tuning_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            tuning_candidates({"tuning": {"enabled": False, "candidates": []}})


if __name__ == "__main__":
    unittest.main()
