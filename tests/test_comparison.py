import unittest

from src.models.comparison import FAMILIES, rank_candidates


class SelectionTests(unittest.TestCase):
    def records(self):
        return [dict(model_name=name, fold=fold, pr_auc=0.1, recall_at_one_percent=0.3)
                for name in FAMILIES for fold in ("2017", "2018")]

    def test_review_capture_precedes_auc(self):
        rows = self.records()
        rows[0].update(pr_auc=0.01, recall_at_one_percent=0.8)
        self.assertEqual(rank_candidates(rows)[0]["model_name"], "logistic_regression")

    def test_missing_duplicate_and_test_results_rejected(self):
        rows = self.records()
        for bad in (rows[:-1], rows + [rows[0]], rows + [dict(rows[0], fold="2019")]):
            with self.assertRaises(ValueError):
                rank_candidates(bad)

    def test_nonfinite_metric_rejected(self):
        rows = self.records()
        rows[0]["pr_auc"] = float("nan")
        with self.assertRaises(ValueError):
            rank_candidates(rows)
