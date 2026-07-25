# Financial Transaction Risk Analytics Lakehouse

This repository rebuilds an existing credit-card fraud analysis project as a clean, end-to-end Databricks Lakehouse portfolio project. The implementation is being rewritten from the raw source files in PySpark instead of extending the original exploratory Pandas notebooks.

## Current Phase

Bronze ingestion is complete and the first Silver pipeline is implemented.

This phase includes:

- Databricks environment setup and five-source Bronze ingestion
- Bronze schema, key, and relationship validation
- Spark-native amount, timestamp, error, and location cleaning
- typed user, card, fraud-label, and MCC Silver dimensions
- a left-joined Silver transaction fact with explicit match-quality flags
- managed Delta outputs in Unity Catalog

This phase does **not** implement Gold tables, modeling, class balancing, target encoding,
dashboards, workflows, or deployment.

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

The current project defaults to this Unity Catalog Volume:

```text
/Volumes/workspace/fraud_detection/fraud_detection_raw
```

Bronze and Silver can also write path-based Delta outputs through configuration.

All namespace settings live in [config/project_config.yml](config/project_config.yml).

## Bronze Tables

The intended Bronze table names are:

- `bronze_transactions`
- `bronze_users`
- `bronze_cards`
- `bronze_fraud_labels`
- `bronze_mcc_codes`

## Silver Tables

- `silver_users`
- `silver_cards`
- `silver_fraud_labels`
- `silver_mcc_codes`
- `silver_transactions`

`silver_transactions` preserves every Bronze transaction through left joins. Missing labels
remain null, so Silver does not silently turn unlabeled rows into non-fraud examples.

## Notebook Run Order

1. [notebooks/00_environment_setup.py](notebooks/00_environment_setup.py)
2. [notebooks/bronze_ingestion.py](notebooks/bronze_ingestion.py)
3. [notebooks/01_bronze_validation.py](notebooks/01_bronze_validation.py)
4. [notebooks/02_raw_data_profiling.py](notebooks/02_raw_data_profiling.py)
5. [notebooks/03_silver_pipeline.py](notebooks/03_silver_pipeline.py)

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

## Layer Responsibilities

- Bronze preserves raw values, validates source structure, adds ingestion metadata, and
  writes replayable Delta tables.
- Silver standardizes types, creates label-independent risk signals, and enriches
  transactions using the verified transaction, user, card, and MCC keys.
- Raw card numbers, CVVs, and user street addresses stay in Bronze and are excluded from
  Silver outputs.
- MCC fraud-rate encoding is intentionally deferred to training-only feature logic.

## Supporting Documents

- [docs/architecture.md](docs/architecture.md)
- [docs/data_dictionary.md](docs/data_dictionary.md)
- [docs/raw_data_profile.md](docs/raw_data_profile.md)

## Local Tests

Run the lightweight non-Spark tests from the repository root:

```bash
python3 -m unittest discover -s tests
```

These tests validate configuration, source schemas, output names, and Silver rule contracts.
The Databricks notebook run is the integration test for Spark and Unity Catalog behavior.
