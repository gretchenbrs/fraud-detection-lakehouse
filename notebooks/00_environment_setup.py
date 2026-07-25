# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Environment Setup
# MAGIC
# MAGIC Edit the catalog, schema, volume, and raw path values below before running Bronze ingestion.
# MAGIC This notebook does not upload local files automatically.

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
    dbutils.widgets.dropdown("allow_create_catalog", "false", ["false", "true"], "allow_create_catalog")
    dbutils.widgets.dropdown("allow_create_schema", "false", ["false", "true"], "allow_create_schema")
    project_root = dbutils.widgets.get("project_root")
    config_path = dbutils.widgets.get("config_path")
    runtime_overrides = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "raw_data_path": dbutils.widgets.get("raw_data_path"),
        "create_catalog_if_missing": dbutils.widgets.get("allow_create_catalog") == "true",
        "create_schema_if_missing": dbutils.widgets.get("allow_create_schema") == "true",
    }
else:
    project_root = default_project_root
    config_path = default_config_path
    runtime_overrides = {}

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from src.utils.config import apply_runtime_overrides, is_placeholder, load_project_config, raw_source_paths
from src.utils.schemas import dataset_names

# COMMAND ----------

base_config = load_project_config(config_path)
project_config = apply_runtime_overrides(base_config, overrides=runtime_overrides)

print("Resolved Databricks configuration:")
print(project_config["databricks"])
print("Resolved Bronze configuration:")
print(project_config["bronze"])

# COMMAND ----------

raw_data_path = project_config["databricks"]["raw_data_path"]
expected_sources = raw_source_paths(project_config)

print(f"Checking raw path: {raw_data_path}")

raw_path_exists = False
present_file_names = set()
missing_file_names = []

if "dbutils" in globals():
    try:
        raw_listing = dbutils.fs.ls(raw_data_path)
        raw_path_exists = True
        present_file_names = {item.name.rstrip("/") for item in raw_listing}
    except Exception as exc:
        print(f"Raw path check failed: {exc}")
else:
    print("dbutils is not available in this environment, so the Volume path cannot be listed here.")

for dataset_name in dataset_names():
    file_name = project_config["raw_sources"]["expected_files"][dataset_name]
    if file_name not in present_file_names:
        missing_file_names.append(file_name)

print(f"raw_path_exists={raw_path_exists}")
print("Expected files:")
for dataset_name, source_path in expected_sources.items():
    print(f"  - {dataset_name}: {source_path}")

print("Present files:")
for file_name in sorted(present_file_names):
    print(f"  - {file_name}")

print("Missing files:")
for file_name in missing_file_names:
    print(f"  - {file_name}")

# COMMAND ----------

catalog_name = project_config["databricks"]["catalog_name"]
schema_name = project_config["databricks"]["schema_name"]

if "dbutils" in globals():
    if project_config["databricks"]["create_catalog_if_missing"] and not is_placeholder(catalog_name):
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        print(f"Ensured catalog exists: {catalog_name}")
    else:
        print("Catalog creation skipped.")

    if project_config["databricks"]["create_schema_if_missing"] and not (
        is_placeholder(catalog_name) or is_placeholder(schema_name)
    ):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
        print(f"Ensured schema exists: {catalog_name}.{schema_name}")
    else:
        print("Schema creation skipped.")
else:
    print("Namespace creation skipped because this is not running inside Databricks.")
