"""Checks for model-release identity and safe artifact locations."""

import json
import tempfile
import unittest
from pathlib import Path

from src.models.lifecycle import (
    APPROVED_MODEL, APPROVED_PARAMETERS, APPROVED_THRESHOLD, RELEASE,
    artifact_locations, read_release_metadata,
)


class ModelLifecycleTests(unittest.TestCase):
    def test_artifacts_use_separate_volume(self):
        volume, path, table = artifact_locations({
            "databricks": {"catalog_name": "workspace", "schema_name": "fraud_detection"}
        })
        self.assertEqual(volume, "workspace.fraud_detection.model_artifacts")
        self.assertTrue(path.startswith("/Volumes/workspace/fraud_detection/model_artifacts/"))
        self.assertEqual(table, "workspace.fraud_detection.model_batch_scores")

    def test_rejects_unsafe_namespace(self):
        with self.assertRaises(ValueError):
            artifact_locations({
                "databricks": {"catalog_name": "workspace;DROP", "schema_name": "fraud_detection"}
            })

    def test_rejects_different_parameters_or_threshold(self):
        metadata = {
            "release": RELEASE, "model_name": APPROVED_MODEL,
            "parameters": APPROVED_PARAMETERS, "threshold": APPROVED_THRESHOLD,
        }
        with tempfile.TemporaryDirectory() as path:
            manifest = Path(path, "release.json")
            manifest.write_text(json.dumps(metadata))
            self.assertEqual(read_release_metadata(path), metadata)
            for key, value in (
                ("threshold", 0.5), ("parameters", {"max_iter": 1}),
                ("release", "unknown"), ("model_name", "random_forest"),
            ):
                manifest.write_text(json.dumps({**metadata, key: value}))
                with self.assertRaises(ValueError):
                    read_release_metadata(path)
