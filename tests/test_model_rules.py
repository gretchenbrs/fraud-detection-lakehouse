"""Pure-Python tests for model-training and evaluation contracts."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.models.evaluation import (
    binary_metrics_from_counts,
    validate_thresholds,
    validate_top_k_fractions,
)
from src.models.training import MODEL_NAMES, balanced_class_weights


class ModelRuleTests(unittest.TestCase):
    def test_two_required_model_families_are_registered(self) -> None:
        self.assertEqual(MODEL_NAMES, ("logistic_regression", "random_forest"))

    def test_balanced_weights_equalize_total_class_contribution(self) -> None:
        positive_weight, negative_weight = balanced_class_weights(100, 900)
        self.assertAlmostEqual(100 * positive_weight, 900 * negative_weight)

    def test_balanced_weights_require_both_classes(self) -> None:
        with self.assertRaises(ValueError):
            balanced_class_weights(0, 100)

    def test_binary_metrics_match_confusion_counts(self) -> None:
        metrics = binary_metrics_from_counts(
            true_positive=80,
            false_positive=20,
            true_negative=880,
            false_negative=20,
        )
        self.assertAlmostEqual(metrics["precision"], 0.8)
        self.assertAlmostEqual(metrics["recall"], 0.8)
        self.assertAlmostEqual(metrics["f1"], 0.8)
        self.assertAlmostEqual(metrics["false_positive_rate"], 20 / 900)

    def test_threshold_grid_rejects_unsorted_or_boundary_values(self) -> None:
        validate_thresholds([0.1, 0.5, 0.9])
        with self.assertRaises(ValueError):
            validate_thresholds([0.5, 0.1])
        with self.assertRaises(ValueError):
            validate_thresholds([0.0, 0.5])

    def test_top_k_grid_rejects_duplicates(self) -> None:
        validate_top_k_fractions([0.001, 0.01, 0.05])
        with self.assertRaises(ValueError):
            validate_top_k_fractions([0.01, 0.01])


if __name__ == "__main__":
    unittest.main()
