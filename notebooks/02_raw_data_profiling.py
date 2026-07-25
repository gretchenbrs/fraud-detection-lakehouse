# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Raw Data Profiling
# MAGIC
# MAGIC This notebook is exploratory only. It profiles the actual raw data already landed in Bronze and highlights observations that should inform Silver design.

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
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "raw_data_path": dbutils.widgets.get("raw_data_path"),
        "storage_mode": dbutils.widgets.get("storage_mode"),
    }
else:
    project_root = default_project_root
    config_path = default_config_path
    runtime_overrides = {}

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from pyspark.sql import functions as F

from src.bronze.ingest import resolve_runtime_project_config
from src.bronze.profiling import (
    exploratory_fraud_relationships,
    fraud_label_overview,
    key_quality_overview,
    load_profile_inputs,
    raw_missingness,
    top_frequency,
    transactions_overview,
)

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)
datasets = load_profile_inputs(spark, project_config)
transactions = datasets["transactions"]
users = datasets["users"]
cards = datasets["cards"]
fraud_labels = datasets["fraud_labels"]
mcc_codes = datasets["mcc_codes"]

# COMMAND ----------

print("Transactions overview")
display(transactions_overview(transactions))

print("Transactions missingness")
display(spark.createDataFrame(raw_missingness(transactions)).orderBy(F.desc("null_or_blank_count")))

print("Transaction type distribution")
display(top_frequency(transactions, "use_chip"))

print("Merchant state raw distribution")
display(top_frequency(transactions, "merchant_state"))

print("Transaction error raw distribution")
display(top_frequency(transactions, "errors"))

print("MCC raw frequency")
display(top_frequency(transactions, "mcc"))

# COMMAND ----------

print("Fraud labels overview")
display(spark.createDataFrame([fraud_label_overview(transactions, fraud_labels)]))

print("Cards key quality")
display(spark.createDataFrame([key_quality_overview(cards, "id")]))

print("Users key quality")
display(spark.createDataFrame([key_quality_overview(users, "id")]))

print("Cards to users potential one-to-many profile")
display(
    cards.groupBy("client_id")
    .agg(F.countDistinct("id").alias("distinct_cards_per_user"))
    .groupBy("distinct_cards_per_user")
    .count()
    .orderBy("distinct_cards_per_user")
)

# COMMAND ----------

print("Preliminary fraud relationships")
print("These tables are exploratory only. They may be leakage-prone if later reused directly as model features.")
fraud_relationships = exploratory_fraud_relationships(transactions, fraud_labels, mcc_codes)

for section_name, section_df in fraud_relationships.items():
    print(section_name)
    display(section_df.limit(30))
