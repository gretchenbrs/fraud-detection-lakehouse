# Databricks notebook source
# MAGIC %md
# MAGIC # V2 Point-in-Time Feature Experiments
# MAGIC Rolling validation on 2017 and 2018; 2019 test is excluded.
# MAGIC Assumed fraud-label delay: 30 days. No automatic model promotion.
# MAGIC History includes unlabeled prior transactions and excludes same-time peers.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("project_root", os.path.abspath(os.path.join(os.getcwd(), "..")))
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "fraud_detection")
dbutils.widgets.dropdown("mode", "checks", ["checks", "prepare", "2017", "2018", "summarize", "reference"])
root = dbutils.widgets.get("project_root")
sys.path.insert(0, root)

from src.bronze.ingest import resolve_runtime_project_config
from src.features.point_in_time import run_spark_guardrail_checks
from src.models.experiments import prepare_experiment, run_fold, summarize_experiment, audit_legacy_validation

config = resolve_runtime_project_config(
    dbutils.widgets.get("config_path") or os.path.join(root, "config", "project_config.yml"),
    overrides={
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
    },
)
mode = dbutils.widgets.get("mode")
if mode in ("checks", "prepare"):
    result = run_spark_guardrail_checks(spark)
    if mode == "prepare":
        result.update(prepare_experiment(spark, config))
elif mode in ("2017", "2018"):
    result = run_fold(spark, config, mode)
elif mode == "summarize":
    result = summarize_experiment(spark, config)
elif mode == "reference":
    result = audit_legacy_validation(spark, config)
else:
    raise ValueError(mode)
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
