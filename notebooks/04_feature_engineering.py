# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Engineering
# MAGIC
# MAGIC This notebook creates chronological train, validation, and test assignments,
# MAGIC then fits smoothed MCC fraud rates from the training split only.

# COMMAND ----------

import os
import sys

# COMMAND ----------

default_project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
default_config_path = os.path.join(default_project_root, "config", "project_config.yml")

if "dbutils" in globals():
    dbutils.widgets.text("project_root", default_project_root, "project_root")
    dbutils.widgets.text("config_path", default_config_path, "config_path")
    dbutils.widgets.text("catalog_name", "workspace", "catalog_name")
    dbutils.widgets.text("schema_name", "fraud_detection", "schema_name")
    dbutils.widgets.dropdown(
        "feature_storage_mode",
        "unity_catalog",
        ["unity_catalog", "path"],
        "feature_storage_mode",
    )
    dbutils.widgets.dropdown(
        "feature_write_mode",
        "overwrite",
        ["overwrite", "append", "ignore", "errorifexists"],
        "feature_write_mode",
    )
    dbutils.widgets.dropdown("write_output", "true", ["true", "false"], "write_output")
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "feature_storage_mode": dbutils.widgets.get("feature_storage_mode"),
        "feature_write_mode": dbutils.widgets.get("feature_write_mode"),
    }
    write_output = dbutils.widgets.get("write_output") == "true"
else:
    project_root = default_project_root
    config_path = default_config_path
    runtime_overrides = {}
    write_output = True

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from pyspark.sql import functions as F

from src.bronze.ingest import resolve_runtime_project_config
from src.features.pipeline import (
    load_feature_dataset,
    run_feature_pipeline,
)
from src.features.splits import split_distribution

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
print("Resolved feature runtime config:")
print(project_config["databricks"])
print(project_config["features"])

# COMMAND ----------

feature_summary_df = run_feature_pipeline(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)
display(feature_summary_df.orderBy("dataset_name"))

# COMMAND ----------

if write_output:
    model_features = load_feature_dataset(
        spark,
        project_config,
        "model_features",
    )
    display(split_distribution(model_features))
else:
    print("Split validation skipped because write_output=false.")

# COMMAND ----------

if write_output:
    mcc_fraud_rates = load_feature_dataset(
        spark,
        project_config,
        "mcc_fraud_rates",
    )
    display(mcc_fraud_rates.orderBy(F.desc("training_mcc_count")).limit(20))
else:
    print("MCC mapping preview skipped because write_output=false.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leakage and Distribution Guardrails
# MAGIC
# MAGIC - Only labeled transactions enter model feature tables.
# MAGIC - The split is chronological: train through 2017, validation in 2018, test in 2019.
# MAGIC - Validation and test retain their natural fraud prevalence.
# MAGIC - MCC smoothed fraud rates are fitted from train only.
# MAGIC - Unseen validation or test MCC values receive the global training fraud rate.
# MAGIC - No class balancing is performed in this pipeline.
