# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Risk Analytics and Investigation Queue
# MAGIC
# MAGIC This notebook selects the champion model using validation PR-AUC, publishes a
# MAGIC business-facing scorecard and daily test KPIs, and creates a ranked investigation
# MAGIC queue. True test outcomes are retained only for retrospective audit and do not
# MAGIC participate in priority ranking.

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
        "gold_storage_mode",
        "unity_catalog",
        ["unity_catalog", "path"],
        "gold_storage_mode",
    )
    dbutils.widgets.dropdown(
        "gold_write_mode",
        "overwrite",
        ["overwrite", "append", "ignore", "errorifexists"],
        "gold_write_mode",
    )
    dbutils.widgets.dropdown("write_output", "true", ["true", "false"], "write_output")

    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "gold_storage_mode": dbutils.widgets.get("gold_storage_mode"),
        "gold_write_mode": dbutils.widgets.get("gold_write_mode"),
    }
    write_output = dbutils.widgets.get("write_output") == "true"
else:
    project_root = default_project_root
    config_path = default_config_path
    runtime_overrides = {}
    write_output = False

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.gold.pipeline import load_gold_dataset, run_gold_pipeline
from src.utils.config import resolve_runtime_project_config

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
gold_summary_df = run_gold_pipeline(
    spark=spark,
    project_config=project_config,
    write_output=write_output,
)
display(gold_summary_df.orderBy("dataset_name"))

# COMMAND ----------

if write_output:
    model_scorecard = load_gold_dataset(spark, project_config, "model_scorecard")
    display(
        model_scorecard.orderBy(
            "evaluation_split",
            "model_name",
        )
    )
else:
    print("Gold output preview skipped because write_output=false.")

# COMMAND ----------

if write_output:
    daily_risk_kpis = load_gold_dataset(spark, project_config, "daily_risk_kpis")
    display(daily_risk_kpis.orderBy("transaction_date"))

# COMMAND ----------

if write_output:
    investigation_queue = load_gold_dataset(spark, project_config, "investigation_queue")
    display(investigation_queue.orderBy("investigation_rank").limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardrails
# MAGIC
# MAGIC - Champion selection uses validation metrics only.
# MAGIC - The selected operating threshold comes from validation only.
# MAGIC - Queue ranking uses model score, amount, and transaction ID tie-breaking only.
# MAGIC - `actual_is_fraud` is retained for retrospective audit and never drives priority.
# MAGIC - Amount-based capture remains a proxy because no confirmed loss or cost field exists.
