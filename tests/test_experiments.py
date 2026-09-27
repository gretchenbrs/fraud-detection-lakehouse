"""Pure checks for experiment scope, ablations, and normalized class weights."""

import unittest
from src.models.experiments import CANDIDATES, FOLDS, experimental_weights, validate_fold
from src.features.point_in_time import SNAPSHOT_NUMERIC


class ExperimentTests(unittest.TestCase):
    def test_weights_have_unit_average(self):
        for power in (0, 0.5, 1):
            positive, negative = experimental_weights(10, 9990, power)
            self.assertAlmostEqual((positive * 10 + negative * 9990) / 10000, 1)
        self.assertEqual(experimental_weights(10, 9990, 0), (1, 1))
        positive, negative = experimental_weights(10, 9990, 1)
        self.assertAlmostEqual(positive / negative, 999)

    def test_weights_reject_empty_classes(self):
        for values in ((0, 10, 1), (10, 0, 1), (1, 10, -1), (1, 10, 2)):
            with self.assertRaises(ValueError):
                experimental_weights(*values)

    def test_test_year_is_excluded(self):
        for _, end, validation in FOLDS:
            validate_fold(end, validation)
        for dates in (("2018-12-31", "2019-12-31"), ("2018-12-31", "2018-01-01")):
            with self.assertRaises(ValueError):
                validate_fold(*dates)

    def test_only_explicit_ablation_contains_snapshots(self):
        for name, numeric, _ in CANDIDATES:
            self.assertEqual(bool(set(numeric) & set(SNAPSHOT_NUMERIC)), name == "snapshot_ablation")

    def test_candidate_names_are_unique(self):
        names = [name for name, _, _ in CANDIDATES]
        self.assertEqual(len(names), len(set(names)))

