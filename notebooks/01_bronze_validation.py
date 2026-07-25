# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Validation
# MAGIC
# MAGIC Run this notebook immediately after Bronze ingestion. It validates counts, keys, nulls, source files, and preliminary match rates without implementing Silver joins.

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

from src.bronze.base import load_bronze_dataset
from src.bronze.ingest import resolve_runtime_project_config
from src.bronze.validation import dataset_validation_summary, preliminary_match_rates
from src.utils.schemas import dataset_names, primary_key_columns

# COMMAND ----------

project_config = resolve_runtime_project_config(config_path, overrides=runtime_overrides)

# COMMAND ----------

summary_rows = [dataset_validation_summary(spark, project_config, dataset_name) for dataset_name in dataset_names()]
display(spark.createDataFrame(summary_rows).orderBy("dataset_name"))

# COMMAND ----------

for dataset_name in dataset_names():
    print(f"=== {dataset_name} ===")
    dataset_df = load_bronze_dataset(spark, project_config, dataset_name)
    dataset_df.printSchema()
    display(dataset_df.limit(10))

    primary_keys = primary_key_columns(dataset_name)
    print(f"Primary key candidate: {primary_keys}")
    duplicate_count = dataset_df.count() - dataset_df.select(*primary_keys).distinct().count()
    print(f"Duplicate key count: {duplicate_count}")

    if dataset_name == "transactions":
        print("Transaction raw examples:")
        display(dataset_df.select("id", "amount", "date", "merchant_state", "errors", "mcc", "card_id", "client_id").limit(10))
        display(dataset_df.groupBy("merchant_state").count().orderBy(F.desc("count")).limit(20))
        display(dataset_df.groupBy("errors").count().orderBy(F.desc("count")).limit(20))
        display(dataset_df.groupBy("mcc").count().orderBy(F.desc("count")).limit(20))

# COMMAND ----------

display(preliminary_match_rates(spark, project_config))

# COMMAND ----------

transactions = load_bronze_dataset(spark, project_config, "transactions")
fraud_labels = load_bronze_dataset(spark, project_config, "fraud_labels")

label_availability = (
    transactions.select(F.col("id").alias("transaction_id")).distinct()
    .join(fraud_labels.select("transaction_id").distinct(), on="transaction_id", how="left")
    .withColumn("has_fraud_label", F.col("transaction_id").isNotNull())
)

print("Fraud-label availability is evaluated only as a relationship check here, not as a Silver-enriched output.")
display(
    transactions.select(F.col("id").alias("transaction_id")).distinct()
    .join(
        fraud_labels.select("transaction_id").withColumn("has_fraud_label", F.lit(1)),
        on="transaction_id",
        how="left",
    )
    .groupBy("has_fraud_label")
    .count()
)
