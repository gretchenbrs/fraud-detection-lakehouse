# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Pipeline
# MAGIC
# MAGIC This notebook cleans the five Bronze datasets and writes typed, joined Silver Delta tables.
# MAGIC It preserves unlabeled transactions and does not perform class balancing or target encoding.

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
        "silver_storage_mode",
        "unity_catalog",
        ["unity_catalog", "path"],
        "silver_storage_mode",
    )
    dbutils.widgets.dropdown(
        "silver_write_mode",
        "overwrite",
        ["overwrite", "append", "ignore", "errorifexists"],
        "silver_write_mode",
    )
    dbutils.widgets.dropdown("write_output", "true", ["true", "false"], "write_output")
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "silver_storage_mode": dbutils.widgets.get("silver_storage_mode"),
        "silver_write_mode": dbutils.widgets.get("silver_write_mode"),
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

from src.bronze.ingest import resolve_runtime_project_config
from src.silver.pipeline import run_silver_pipeline, silver_join_quality_summary

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
print("Resolved Silver runtime config:")
print(project_config["databricks"])
print(project_config["silver"])

# COMMAND ----------

silver_summary_df = run_silver_pipeline(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)
display(silver_summary_df.orderBy("dataset_name"))

# COMMAND ----------

if write_output:
    display(silver_join_quality_summary(spark, project_config))
else:
    print("Join-quality summary skipped because write_output=false.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardrails
# MAGIC
# MAGIC - Missing fraud labels remain null and are not converted to non-fraud.
# MAGIC - No rows are sampled or balanced in Silver.
# MAGIC - No MCC fraud-rate encoding is calculated here. That feature must be fit on training data only.
# MAGIC - Raw card number and CVV remain in Bronze and are intentionally excluded from Silver.
