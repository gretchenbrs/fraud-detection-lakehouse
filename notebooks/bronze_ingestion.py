# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC This notebook runs the Bronze ingestion layer only. It preserves raw source columns and writes Delta outputs.

# COMMAND ----------

import os
import sys

# COMMAND ----------

default_project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
default_config_path = os.path.join(default_project_root, "config", "project_config.yml")

if "dbutils" in globals():
    dbutils.widgets.text("project_root", default_project_root, "project_root")
    dbutils.widgets.text("config_path", default_config_path, "config_path")
    dbutils.widgets.text("catalog_name", "", "catalog_name")
    dbutils.widgets.text("schema_name", "", "schema_name")
    dbutils.widgets.text("volume_name", "", "volume_name")
    dbutils.widgets.text("raw_data_path", "", "raw_data_path")
    dbutils.widgets.dropdown("storage_mode", "unity_catalog", ["unity_catalog", "path"], "storage_mode")
    dbutils.widgets.dropdown("write_mode", "errorifexists", ["errorifexists", "append", "overwrite", "ignore"], "write_mode")
    dbutils.widgets.dropdown("write_output", "true", ["true", "false"], "write_output")
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "raw_data_path": dbutils.widgets.get("raw_data_path"),
        "storage_mode": dbutils.widgets.get("storage_mode"),
        "write_mode": dbutils.widgets.get("write_mode"),
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

from src.bronze.ingest import resolve_runtime_project_config, run_bronze_ingestion

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
print("Resolved Bronze runtime config:")
print(project_config["databricks"])
print(project_config["bronze"])

# COMMAND ----------

bronze_summary_df = run_bronze_ingestion(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)

# COMMAND ----------

display(bronze_summary_df.orderBy("dataset_name"))