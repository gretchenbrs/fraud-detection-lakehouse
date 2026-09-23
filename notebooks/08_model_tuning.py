# Databricks notebook source
# MAGIC %md
# MAGIC # Validation-Only Model Tuning
# MAGIC
# MAGIC This notebook fits a small, reproducible candidate set using **training rows only**.
# MAGIC Candidates are ranked by validation PR-AUC; only the validation champion is scored on
# MAGIC the held-out test split. This is tuning, not cost optimization.

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
        "model_storage_mode",
        "unity_catalog",
        ["unity_catalog", "path"],
        "model_storage_mode",
    )
    dbutils.widgets.dropdown(
        "model_write_mode",
        "overwrite",
        ["overwrite", "append", "ignore", "errorifexists"],
        "model_write_mode",
    )
    dbutils.widgets.dropdown("write_output", "true", ["true", "false"], "write_output")
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "model_storage_mode": dbutils.widgets.get("model_storage_mode"),
        "model_write_mode": dbutils.widgets.get("model_write_mode"),
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
from src.models.pipeline import load_model_dataset
from src.models.tuning import run_tuning_pipeline
from src.utils.config import TUNING_DATASET_NAME

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
print("Resolved tuning configuration:")
print(project_config["models"]["tuning"])

# COMMAND ----------

tuning_summary_df = run_tuning_pipeline(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)
display(tuning_summary_df.orderBy("dataset_name"))

# COMMAND ----------

if write_output:
    tuning_trials = load_model_dataset(spark, project_config, TUNING_DATASET_NAME)
    print("Validation-only tuning leaderboard")
    display(tuning_trials)

# COMMAND ----------

if write_output:
    overall_metrics = load_model_dataset(spark, project_config, "overall_metrics")
    print("Validation candidates plus held-out final test result for the champion only")
    display(overall_metrics.orderBy("evaluation_split", F.desc("pr_auc"), "model_name"))

# COMMAND ----------

if write_output:
    threshold_metrics = load_model_dataset(spark, project_config, "threshold_metrics")
    print("Validation-selected thresholds and the champion final-test confusion matrix")
    display(
        threshold_metrics.filter(F.col("is_selected_threshold"))
        .orderBy("evaluation_split", "model_name")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardrails
# MAGIC
# MAGIC - The preprocessor and class weights are fitted on training rows only.
# MAGIC - Candidate selection uses validation PR-AUC only, with deterministic tie-breaking.
# MAGIC - Test rows are not used to rank candidates or select a threshold.
# MAGIC - Only the validation champion is evaluated on test, preserving it as a final holdout.
# MAGIC - Gold is refreshed after this notebook so its scorecard and investigation queue use the tuned champion.
