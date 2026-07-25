# Architecture Overview

## Objective

The project target is a Databricks Lakehouse that can ingest raw card-transaction data, standardize fraud-relevant signals, train and compare fraud models, and publish investigation-focused business outputs.

The current implementation includes:

- Databricks environment setup
- Bronze ingestion
- Bronze validation
- raw-data profiling
- Silver cleaning and conformed dimensions
- a Silver enriched transaction fact

Gold and modeling remain intentionally out of scope for this phase.

## Source System Boundary

Only five raw files are in scope:

- `transactions_data.csv`
- `users_data.csv`
- `cards_data.csv`
- `train_fraud_labels.json`
- `mcc_codes.json`

The original notebooks are treated as reference evidence only. They are not source code for the rebuilt pipeline.

## Databricks Runtime Design

### Preferred raw landing location

Upload the raw files to a Unity Catalog Volume path such as:

```text
/Volumes/<catalog>/<schema>/<volume>/fraud_raw/
```

### Preferred Bronze output

Use Unity Catalog managed Delta tables:

- `<catalog>.<schema>.bronze_transactions`
- `<catalog>.<schema>.bronze_users`
- `<catalog>.<schema>.bronze_cards`
- `<catalog>.<schema>.bronze_fraud_labels`
- `<catalog>.<schema>.bronze_mcc_codes`

### Fallback Bronze output

If managed tables are not available or not desired, the Bronze layer can write Delta files under a configurable base path instead.

## Medallion Design

### Bronze

Purpose:

- land raw sources with explicit schemas
- validate expected columns and root JSON structure
- preserve original raw values for replay and auditability
- add ingestion metadata
- create a trustworthy base for validation and profiling

Current Bronze metadata fields:

- `_ingested_at`
- `_source_file`
- `_ingestion_date`

### Silver

Implemented responsibilities:

- join fraud labels to transactions by transaction ID
- join users by `transactions.client_id -> users.id`
- join cards by `transactions.card_id -> cards.id`
- map MCC to merchant category
- clean amounts and expose negative-amount flags
- parse transaction timestamps and temporal fraud features
- normalize error strings into interpretable flags
- group merchant geography into domestic, international, or unknown
- preserve all transaction rows with left joins and expose match-quality flags
- preserve missing fraud labels as null
- exclude raw card number, CVV, and street address from Silver

### Gold

Planned responsibilities only, not yet implemented:

- model-ready feature tables
- fraud KPI marts
- investigation-prioritization tables
- threshold-comparison outputs for business review

## Dataset Relationship Assumptions And Status

The following relationships are supported by raw-schema inspection and reference-notebook evidence:

- `transactions.id -> fraud_labels.transaction_id`
- `transactions.client_id -> users.id`
- `transactions.card_id -> cards.id`
- `cards.client_id -> users.id`
- `transactions.mcc -> mcc_codes.mcc`

Verification status:

- raw field names were confirmed directly
- a 200,000-row transaction sample matched the expected user and card keys
- full Bronze ingestion completed successfully in Databricks
- Silver exposes unmatched-key counts for integration validation

## JSON Ingestion Strategy

Both JSON inputs are top-level objects rather than newline-delimited records.

- `train_fraud_labels.json` contains a top-level `target` map of `transaction_id -> label`
- `mcc_codes.json` contains a top-level `mcc -> merchant_category` map

The Bronze layer reads these files as whole-text JSON payloads and explodes them into tabular outputs using Spark-native expressions.

## Execution Pattern

- reusable logic lives under `src/`
- notebooks are orchestration and inspection layers
- configuration lives in `config/project_config.yml`
- local tests cover only non-Spark logic

## Notebook Sequence

1. `00_environment_setup.py`
2. `bronze_ingestion.py`
3. `01_bronze_validation.py`
4. `02_raw_data_profiling.py`
5. `03_silver_pipeline.py`

## Silver Guardrails

- no sampling or class balancing
- no time split yet
- no target-conditioned aggregations
- no MCC fraud-rate encoding
- no conversion of missing labels to zero

## Open Questions

- whether `merchant_id` is stable across time and unique across merchant geographies
- whether `num_cards_issued` is per card product, per account, or historical issuance count
- whether the fraud labels cover every eligible training-period transaction or only a curated subset
