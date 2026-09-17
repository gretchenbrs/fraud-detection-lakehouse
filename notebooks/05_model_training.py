# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Model Training and Evaluation
# MAGIC
# MAGIC This notebook trains a weighted logistic-regression baseline and a random-forest
# MAGIC comparison model. Preprocessing and class weights are fitted on training only.
# MAGIC Thresholds are compared on validation; test is used only for final evaluation.
# MAGIC Serverless execution requires environment version 4 or newer for `pyspark.ml`.

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
from src.models.pipeline import load_model_dataset, run_model_pipeline

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
print("Resolved model runtime config:")
print(project_config["databricks"])
print(project_config["models"])

# COMMAND ----------

# MAGIC %md
# MAGIC The first full run can take substantially longer than Bronze, Silver, or Features.
# MAGIC Both models train on roughly 7.2 million chronological training rows. Scored validation
# MAGIC and test rows are materialized before repeated evaluation because Serverless does not
# MAGIC support DataFrame cache APIs.

# COMMAND ----------

model_summary_df = run_model_pipeline(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)
display(model_summary_df.orderBy("dataset_name"))

# COMMAND ----------

if write_output:
    overall_metrics = load_model_dataset(
        spark,
        project_config,
        "overall_metrics",
    )
    display(overall_metrics.orderBy("evaluation_split", F.desc("pr_auc")))
else:
    print("Persisted metric preview skipped because write_output=false.")

# COMMAND ----------

if write_output:
    threshold_metrics = load_model_dataset(
        spark,
        project_config,
        "threshold_metrics",
    )
    print("Validation-selected thresholds and final test confusion matrices")
    display(
        threshold_metrics.filter(F.col("is_selected_threshold"))
        .orderBy("model_name", "evaluation_split")
    )

# COMMAND ----------

if write_output:
    print("Full validation threshold comparison")
    display(
        threshold_metrics.filter(F.col("evaluation_split") == "validation")
        .orderBy("model_name", "threshold")
    )

# COMMAND ----------

if write_output:
    top_k_metrics = load_model_dataset(
        spark,
        project_config,
        "top_k_metrics",
    )
    display(top_k_metrics.orderBy("evaluation_split", "model_name", "top_fraction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation Guardrails
# MAGIC
# MAGIC - Validation and test retain their natural fraud prevalence.
# MAGIC - Inverse-frequency class weights are calculated from training labels only.
# MAGIC - MCC target encoding and preprocessing statistics are fitted from training only.
# MAGIC - The selected threshold maximizes validation F1; this is not cost optimization.
# MAGIC - Test metrics are reported only at the validation-selected threshold.
# MAGIC - Fraud-amount capture uses transaction amount as a proxy, not confirmed financial loss.
