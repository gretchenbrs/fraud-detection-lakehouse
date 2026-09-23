# Financial Transaction Risk Analytics Lakehouse

This repository rebuilds an existing credit-card fraud analysis project as a clean, end-to-end Databricks Lakehouse portfolio project. The implementation is being rewritten from the raw source files in PySpark instead of extending the original exploratory Pandas notebooks.

## Current Phase

Bronze, Silver, leakage-aware feature engineering, model evaluation, Gold risk
analytics, and a version-controlled AI/BI dashboard are implemented for Databricks.

This phase includes:

- Databricks environment setup and five-source Bronze ingestion
- Bronze schema, key, and relationship validation
- Spark-native amount, timestamp, error, and location cleaning
- typed user, card, fraud-label, and MCC Silver dimensions
- a left-joined Silver transaction fact with explicit match-quality flags
- chronological train, validation, and test assignments
- training-only smoothed MCC fraud-rate encoding
- training-only preprocessing and inverse-frequency class weights
- logistic-regression baseline and random-forest comparison
- validation threshold grid with F1-based selection
- PR-AUC, ROC-AUC, confusion matrix, and top-k capture outputs
- validation-only champion-model selection
- a Gold model scorecard, daily risk KPI mart, and ranked investigation queue
- dashboard-ready business reporting views and Databricks SQL queries
- a three-page Databricks AI/BI dashboard managed as a Bundle resource
- managed Delta outputs in Unity Catalog

This phase includes a deployable, manually triggered end-to-end Workflow. It is managed by
the Databricks Bundle, has one concurrent run, and deliberately has no automatic schedule so
the static portfolio data does not retrain unnecessarily. Dashboard deployment is also
declarative through the Bundle and requires an authenticated CLI session.

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

## Feature Tables

- `feature_split_assignments`
- `feature_mcc_fraud_rates`
- `feature_model_features`

The model feature table contains labeled rows only. Splits are chronological, and the MCC
rate mapping is fitted from training rows only before being applied to validation and test.

## Model Outputs

- `model_scored_predictions`
- `model_overall_metrics`
- `model_threshold_metrics`
- `model_top_k_metrics`

The baseline is logistic regression and the tree-based comparison is a random forest.
Class weights are calculated from training rows only. Candidate thresholds are evaluated
on validation, and test confusion metrics use only the validation-selected threshold.

## Gold Outputs

- `gold_model_scorecard`
- `gold_daily_risk_kpis`
- `gold_investigation_queue`

The champion model is selected by validation PR-AUC. Investigation priority is based on
model score with deterministic business-field tie breaking. The true test outcome is
retained only for retrospective audit and never influences queue rank.

## Notebook Run Order

1. [notebooks/00_environment_setup.py](notebooks/00_environment_setup.py)
2. [notebooks/bronze_ingestion.py](notebooks/bronze_ingestion.py)
3. [notebooks/01_bronze_validation.py](notebooks/01_bronze_validation.py)
4. [notebooks/02_raw_data_profiling.py](notebooks/02_raw_data_profiling.py)
5. [notebooks/03_silver_pipeline.py](notebooks/03_silver_pipeline.py)
6. [notebooks/04_feature_engineering.py](notebooks/04_feature_engineering.py)
7. [notebooks/05_model_training.py](notebooks/05_model_training.py)
8. [notebooks/06_gold_risk_analytics.py](notebooks/06_gold_risk_analytics.py)
9. [notebooks/07_business_reporting.py](notebooks/07_business_reporting.py)

## Deploy And Run The Workflow

Validate and deploy all Bundle-managed resources, including the AI/BI dashboard and the
end-to-end Lakeflow Job:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run the complete pipeline on demand after deployment:

```bash
databricks bundle run fraud_risk_lakehouse_pipeline -t dev
```

The job uses Serverless environment version 6, runs tasks in notebook order, and writes
reproducible `overwrite` outputs. Configure a schedule in the Databricks Job UI only when
the source-data refresh cadence is defined.

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
- Feature tables preserve natural validation and test prevalence; balancing remains a
  model-training concern.
- Validation F1 is an explicit threshold-selection rule, not a cost-based optimization.
- Top-k amount capture treats fraudulent transaction amount as a proxy and not confirmed loss.
- Gold champion selection uses validation PR-AUC; test labels remain evaluation evidence only.

## Supporting Documents

- [docs/architecture.md](docs/architecture.md)
- [docs/data_dictionary.md](docs/data_dictionary.md)
- [docs/raw_data_profile.md](docs/raw_data_profile.md)
- [docs/dashboard_guide.md](docs/dashboard_guide.md)

## Local Tests

Run the lightweight non-Spark tests from the repository root:

```bash
python3 -m unittest discover -s tests
```

These tests validate configuration, source schemas, output names, Silver and Gold rules,
chronological splits, MCC smoothing, class weights, and metric formulas. Databricks notebook
runs are the integration tests for Spark, Spark ML, and Unity Catalog behavior.
