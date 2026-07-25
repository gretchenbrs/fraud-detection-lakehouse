# Raw Data Profile

This report is intentionally split into two states:

- confirmed facts already known from the raw-file inspection completed on July 22, 2026
- placeholders that should be replaced with notebook outputs after the Databricks Bronze and profiling notebooks are run

Do not treat placeholder sections as final evidence.

## 1. Actual Schemas

Confirmed before Databricks execution:

- `transactions_data.csv` has 12 raw columns
- `users_data.csv` has 14 raw columns
- `cards_data.csv` has 13 raw columns
- `train_fraud_labels.json` is a top-level object with a `target` map
- `mcc_codes.json` is a top-level MCC-to-category mapping object

Notebook follow-up required:

- paste the exact `printSchema()` output for all five Bronze datasets here

## 2. Row Counts

Confirmed from local raw inspection:

- transactions: 13,305,915 rows
- users: 2,000 rows
- cards: 6,146 rows
- fraud labels: 8,914,963 entries
- MCC codes: 109 entries

Notebook follow-up required:

- record Bronze table row counts after Databricks ingestion
- confirm whether counts match the local pre-ingestion inspection

## 3. Key Relationships And Match Rates

Confirmed before Databricks execution:

- likely join: `transactions.id -> fraud_labels.transaction_id`
- likely join: `transactions.card_id -> cards.id`
- likely join: `transactions.client_id -> users.id`
- likely join: `cards.client_id -> users.id`
- likely join: `transactions.mcc -> mcc_codes.mcc`

Notebook follow-up required:

- paste preliminary distinct-key match rates from `01_bronze_validation.py`
- record duplicate-key counts for each likely primary key
- confirm whether any label IDs are unmatched to transaction IDs

## 4. Data-Quality Problems

Confirmed before Databricks execution:

- `merchant_state` is blank for many transaction rows
- `zip` is stored as a string-like numeric value and includes trailing `.0` patterns
- `errors` is mostly blank but can contain multiple comma-delimited values
- amount fields are stored as raw currency strings

Notebook follow-up required:

- paste null-rate summaries for important columns
- record any ingestion-time parse failures or malformed records
- note any unexpected duplicate primary keys or blank identifiers

## 5. Confirmed Raw Formats

Confirmed before Databricks execution:

- transaction timestamps are stored as strings like `YYYY-MM-DD HH:MM:SS`
- transaction amounts are raw currency strings like `$14.57` and `$-77.00`
- users and cards contain additional raw currency strings
- fraud labels use `Yes` and `No`
- the known `errors` tokens include:
  - `Bad CVV`
  - `Bad Card Number`
  - `Bad Expiration`
  - `Bad PIN`
  - `Bad Zipcode`
  - `Insufficient Balance`
  - `Technical Glitch`

Notebook follow-up required:

- add examples of raw amount formats that fail the exploratory parser, if any
- confirm whether any additional error strings appear after full Databricks profiling

## 6. Feature Ideas Supported By The Data

Supported in principle, pending notebook confirmation:

- fraud-label join by transaction ID
- MCC enrichment by raw MCC code
- raw amount cleaning and negative-amount detection
- transaction timestamp parsing into hour, weekday, weekend, and date features
- multi-label error normalization
- merchant location grouping using raw merchant state presence and value patterns

Notebook follow-up required:

- paste exploratory fraud-rate breakdowns by hour, weekday, weekend, transaction type, error category, MCC, location category, and amount bucket
- mark which of those breakdowns should be treated as leakage-prone descriptive analysis only

## 7. Ideas That Look Unsupported, Redundant, Or Risky

Known risks before Databricks execution:

- direct reuse of target-conditioned aggregations as production features would leak label information
- `merchant_state` should not be assumed to be a clean US-only field
- premature balancing in Bronze or Silver would distort the natural fraud distribution

Notebook follow-up required:

- confirm whether there are any additional high-cardinality or unstable raw fields that should be excluded from Silver
- note any raw fields that appear redundant once join keys are validated

## 8. Recommended Silver Transformations

Recommended, but not yet implemented:

- parse timestamps using Spark-native functions
- standardize amount strings and expose parse-failure diagnostics
- normalize `errors` into multi-hot interpretable flags
- preserve merchant raw geography while creating a cautious location category
- validate joins and coverage before any feature table is built
- restrict any target encoding or fraud-rate history features to training-only logic in a later modeling phase

## 9. Questions For User Review

- Which catalog and schema should become the long-term home of the Bronze tables?
- Should Bronze remain as Unity Catalog managed tables, or do you want path-based Delta outputs for development?
- Do you want the profiling notebook outputs copied into this document manually after your first Databricks run, or should the report remain a human-curated summary?
- If unmatched fraud-label IDs appear, should we treat them as data exclusions or investigate them as a separate dataset issue before Silver work starts?
