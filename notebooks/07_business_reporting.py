# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Risk Business Reporting
# MAGIC
# MAGIC This notebook reads the persisted Gold outputs and prepares dashboard-ready views.
# MAGIC It does not retrain models or rewrite pipeline tables.

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
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
    }
else:
    project_root = default_project_root
    config_path = default_config_path
    runtime_overrides = {}

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.bronze.ingest import resolve_runtime_project_config
from src.gold.pipeline import load_gold_dataset
from src.gold.reporting import (
    build_executive_summary,
    build_monthly_risk_trend,
    build_queue_audit_summary,
    build_queue_segment_summary,
)

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
model_scorecard = load_gold_dataset(spark, project_config, "model_scorecard")
daily_risk_kpis = load_gold_dataset(spark, project_config, "daily_risk_kpis")
investigation_queue = load_gold_dataset(spark, project_config, "investigation_queue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

executive_summary = build_executive_summary(model_scorecard, daily_risk_kpis)
display(executive_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison
# MAGIC
# MAGIC The champion flag is determined from validation PR-AUC. Test metrics are final
# MAGIC evaluation evidence and do not select the model.

# COMMAND ----------

display(model_scorecard.orderBy("evaluation_split", "model_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Risk Trend

# COMMAND ----------

monthly_risk_trend = build_monthly_risk_trend(daily_risk_kpis)
display(monthly_risk_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigation Queue by Priority

# COMMAND ----------

priority_summary = build_queue_segment_summary(investigation_queue, "priority_tier")
display(priority_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigation Queue by Merchant Category

# COMMAND ----------

mcc_summary = build_queue_segment_summary(investigation_queue, "mcc_category")
display(mcc_summary.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigation Queue by Channel and Geography

# COMMAND ----------

display(build_queue_segment_summary(investigation_queue, "transaction_type"))
display(build_queue_segment_summary(investigation_queue, "merchant_location_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Investigation Cases
# MAGIC
# MAGIC This operational view intentionally excludes `actual_is_fraud`.

# COMMAND ----------

display(
    investigation_queue.select(
        "investigation_rank",
        "priority_tier",
        "transaction_id",
        "transaction_timestamp",
        "score",
        "amount_abs",
        "is_above_selected_threshold",
        "transaction_type",
        "merchant_location_category",
        "mcc_category",
        "risk_reason_codes",
    )
    .orderBy("investigation_rank")
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrospective Queue Audit
# MAGIC
# MAGIC This section uses known test outcomes only to measure historical capture. Outcomes
# MAGIC were not used to rank transactions or assign priority tiers.

# COMMAND ----------

display(build_queue_audit_summary(investigation_queue))
