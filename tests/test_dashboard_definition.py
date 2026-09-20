"""Structural guardrails for the version-controlled AI/BI dashboard."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = REPO_ROOT / "dashboards" / "fraud_risk_analytics.lvdash.json"


class DashboardDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))

    def test_dashboard_has_named_datasets_and_canvas_pages(self) -> None:
        datasets = self.dashboard["datasets"]
        pages = self.dashboard["pages"]

        self.assertEqual(len({dataset["name"] for dataset in datasets}), len(datasets))
        self.assertGreaterEqual(len(pages), 3)
        for page in pages:
            self.assertEqual(page["pageType"], "PAGE_TYPE_CANVAS")
            self.assertEqual(page["layoutVersion"], "GRID_V1")
            self.assertTrue(page["layout"])

    def test_query_lines_are_safe_for_concatenation_and_portable(self) -> None:
        qualified_table = re.compile(r"\bFROM\s+\w+\.\w+\.", re.IGNORECASE)

        for dataset in self.dashboard["datasets"]:
            lines = dataset["queryLines"]
            self.assertTrue(lines)
            self.assertTrue(
                all(line.endswith((" ", "\n")) for line in lines[:-1]),
                msg=f"Dataset {dataset['name']} has concatenation-unsafe queryLines",
            )
            query = "".join(lines)
            self.assertIsNone(
                qualified_table.search(query),
                msg=f"Dataset {dataset['name']} hard-codes a catalog and schema",
            )

    def test_widget_dataset_and_field_bindings_are_valid(self) -> None:
        dataset_names = {dataset["name"] for dataset in self.dashboard["datasets"]}

        for page in self.dashboard["pages"]:
            for layout_item in page["layout"]:
                widget = layout_item["widget"]
                for query_wrapper in widget.get("queries", []):
                    query = query_wrapper["query"]
                    self.assertIn(query["datasetName"], dataset_names)
                    field_names = {field["name"] for field in query["fields"]}
                    spec_text = json.dumps(widget.get("spec", {}))
                    for field_name in re.findall(r'"fieldName":\s*"([^"]+)"', spec_text):
                        self.assertIn(
                            field_name,
                            field_names,
                            msg=f"Widget {widget['name']} references an unbound field",
                        )

    def test_operational_page_does_not_expose_outcome_labels(self) -> None:
        operational_page = next(
            page for page in self.dashboard["pages"] if page["name"] == "investigation_operations"
        )
        operational_text = json.dumps(operational_page).lower()

        self.assertNotIn("actual_is_fraud", operational_text)
        self.assertNotIn("captured_fraud_count", operational_text)

    def test_outcome_label_is_confined_to_retrospective_dataset(self) -> None:
        datasets_with_label = [
            dataset["name"]
            for dataset in self.dashboard["datasets"]
            if "actual_is_fraud" in "".join(dataset["queryLines"]).lower()
        ]

        self.assertEqual(datasets_with_label, ["queue_audit"])


if __name__ == "__main__":
    unittest.main()
