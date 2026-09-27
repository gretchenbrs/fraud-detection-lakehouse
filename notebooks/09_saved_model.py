# Databricks notebook source
# MAGIC %md
# MAGIC # Saved Model Lifecycle
# MAGIC Fit the validation-selected configuration once, then load it in a separate
# MAGIC session for historical test replay. This is not a live transaction feed.
# MAGIC The frozen release includes fitted preprocessing and its replay input.
# MAGIC Existing evaluation and Gold tables remain the audit record of the tuning run.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("project_root", os.path.abspath(os.path.join(os.getcwd(), "..")))
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "fraud_detection")
dbutils.widgets.dropdown("mode", "score", ["fit", "score"])
root = dbutils.widgets.get("project_root")
sys.path.insert(0, root)

from src.bronze.ingest import resolve_runtime_project_config
from src.models.lifecycle import replay_saved_model, save_reviewed_model

config = resolve_runtime_project_config(
    dbutils.widgets.get("config_path") or os.path.join(root, "config", "project_config.yml"),
    overrides={
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
    },
)
mode = dbutils.widgets.get("mode")
if mode == "fit":
    result = save_reviewed_model(spark, config)
elif mode == "score":
    result = replay_saved_model(spark, config)
else:
    raise ValueError(f"Unsupported lifecycle mode: {mode}")
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))

