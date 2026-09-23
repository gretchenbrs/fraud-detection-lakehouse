"""Structural checks for the deployable end-to-end Databricks workflow."""

from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPO_ROOT / "resources" / "fraud_risk_pipeline.job.yml"


class WorkflowDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_workflow_has_the_expected_ordered_tasks(self) -> None:
        expected_keys = [
            "environment_setup",
            "bronze_ingestion",
            "bronze_validation",
            "raw_data_profiling",
            "silver_pipeline",
            "feature_engineering",
            "model_training",
            "model_tuning",
            "gold_risk_analytics",
            "business_reporting",
        ]

        task_positions = [self.workflow_text.index(f"task_key: {task_key}") for task_key in expected_keys]
        self.assertEqual(task_positions, sorted(task_positions))

    def test_each_downstream_task_depends_on_its_predecessor(self) -> None:
        expected_dependencies = [
            "environment_setup",
            "bronze_ingestion",
            "bronze_validation",
            "raw_data_profiling",
            "silver_pipeline",
            "feature_engineering",
            "model_training",
            "model_tuning",
            "gold_risk_analytics",
        ]
        for task_key in expected_dependencies:
            self.assertIn(f"depends_on:\n            - task_key: {task_key}", self.workflow_text)

    def test_tasks_use_serverless_environment_and_deployed_config_path(self) -> None:
        self.assertIn('environment_version: "6"', self.workflow_text)
        self.assertEqual(self.workflow_text.count("environment_key: standard_v6"), 11)
        self.assertEqual(self.workflow_text.count("project_root: ${workspace.file_path}"), 10)
        self.assertEqual(
            self.workflow_text.count("config_path: ${workspace.file_path}/config/project_config.yml"),
            10,
        )

    def test_workflow_is_single_concurrent_run_and_queued(self) -> None:
        self.assertIn("max_concurrent_runs: 1", self.workflow_text)
        self.assertIn("queue:\n        enabled: true", self.workflow_text)
        self.assertNotIn("trigger:", self.workflow_text)


if __name__ == "__main__":
    unittest.main()
