# Databricks notebook source
# MAGIC %md
# MAGIC # Model-family comparison and frozen historical test
# MAGIC Choose using 2017/2018 validation; 2019 is a previously inspected historical test.

# COMMAND ----------
import json
import os
import sys

dbutils.widgets.text("project_root", os.path.abspath(os.path.join(os.getcwd(), "..")))
dbutils.widgets.text("mode", "freeze")
dbutils.widgets.text("fold", "2018")
dbutils.widgets.text("family", "logistic_regression")
dbutils.widgets.text("run_tag", "")
root = dbutils.widgets.get("project_root")
sys.path.insert(0, root)
from src.bronze.ingest import resolve_runtime_project_config
from src.models.comparison import run_validation, freeze_selection, evaluate_frozen_test
config = resolve_runtime_project_config(os.path.join(root, "config", "project_config.yml"),
    overrides={"catalog_name": "workspace", "schema_name": "fraud_detection"})
mode, run_tag = dbutils.widgets.get("mode"), dbutils.widgets.get("run_tag")
if not run_tag.isdigit():
    raise ValueError("A unique numeric run_tag is required.")
if mode == "validate":
    result = run_validation(spark, config, dbutils.widgets.get("fold"), dbutils.widgets.get("family"), run_tag)
elif mode == "freeze":
    result = freeze_selection(spark, config, run_tag)
elif mode == "test":
    result = evaluate_frozen_test(spark, config, run_tag)
else:
    raise ValueError(mode)
dbutils.notebook.exit(json.dumps(result))
