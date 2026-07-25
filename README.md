# Financial Transaction Risk Analytics Lakehouse

This repository rebuilds an existing credit-card fraud analysis project as a clean, end-to-end Databricks Lakehouse portfolio project. The implementation is being rewritten from the raw source files in PySpark instead of extending the original exploratory Pandas notebooks.

## Current Phase

The project is intentionally paused at the Bronze and raw-profiling stage.

This phase includes:

- Databricks environment setup guidance
- configurable raw-data path and Bronze namespaces
- executable Bronze ingestion for all five raw datasets
- Bronze validation notebooks
- raw-data profiling notebooks
- a placeholder human-readable raw-data profile report

This phase does **not** implement Silver transformations, Gold tables, or fraud modeling yet.

## Verified Raw Inputs

Only these files are treated as pipeline inputs:

- `transactions_data.csv`
- `users_data.csv`
- `cards_data.csv`
- `train_fraud_labels.json`
- `mcc_codes.json`

The following generated files are explicitly excluded from the new pipeline:

- `cleaned_transactions.csv`
- `encoded_transactions.csv`
- `train_data.csv`
- `test_data.csv`

## Databricks Storage Strategy

The repository is configured to prefer Unity Catalog managed Bronze tables, with raw source files uploaded to a Unity Catalog Volume path like:

```text
/Volumes/<catalog>/<schema>/<volume>/fraud_raw/
```

If your workspace does not support that pattern, the Bronze layer can also write path-based Delta outputs under a configurable base path.

All namespace settings live in [config/project_config.yml](config/project_config.yml).

## Bronze Tables

The intended Bronze table names are:

- `bronze_transactions`
- `bronze_users`
- `bronze_cards`
- `bronze_fraud_labels`
- `bronze_mcc_codes`

## Notebook Run Order

1. [notebooks/00_environment_setup.py](notebooks/00_environment_setup.py)
2. [notebooks/bronze_ingestion.py](notebooks/bronze_ingestion.py)
3. [notebooks/01_bronze_validation.py](notebooks/01_bronze_validation.py)
4. [notebooks/02_raw_data_profiling.py](notebooks/02_raw_data_profiling.py)

## Repository Layout

```text
fraud-risk-lakehouse/
├── README.md
├── requirements.txt
├── databricks.yml
├── config/
├── docs/
├── notebooks/
├── src/
│   ├── bronze/
│   ├── features/
│   ├── gold/
│   ├── models/
│   ├── silver/
│   └── utils/
└── tests/
```

## What The Bronze Layer Does

- reads each raw source separately with explicit schemas or verified JSON parsing
- preserves raw source columns rather than applying Silver logic
- adds `_ingested_at`, `_source_file`, and `_ingestion_date`
- validates required CSV columns before writing
- writes Delta outputs either to Unity Catalog managed tables or configurable Delta paths
- defaults to `errorifexists` rather than destructive overwrite

## Supporting Documents

- [docs/architecture.md](docs/architecture.md)
- [docs/data_dictionary.md](docs/data_dictionary.md)
- [docs/raw_data_profile.md](docs/raw_data_profile.md)

## Local Tests

Run the lightweight non-Spark tests from the repository root:

```bash
python3 -m unittest discover -s tests
```

These tests validate configuration structure, table-name construction, schema registry consistency, and required-column checks. They do not prove that Spark ingestion succeeds inside Databricks.
