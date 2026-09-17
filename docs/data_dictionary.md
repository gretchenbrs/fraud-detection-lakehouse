# Data Dictionary

This document records what was verified directly from the raw files and what still needs confirmation. Bronze types below are intentionally conservative and preserve raw fidelity.

## transactions_data.csv

Observed row count: 13,305,915 data rows.

Observed notes:

- `use_chip` verified values: `Chip Transaction`, `Online Transaction`, `Swipe Transaction`
- `errors` is usually blank and otherwise contains one or more comma-delimited error tokens
- `merchant_state` is blank for many rows and has 199 distinct non-blank values, so it should not be assumed to be a clean US-state-only field

| Field | Observed raw type | Bronze type | Notes | Status |
| --- | --- | --- | --- | --- |
| `id` | string-like integer | string | Candidate transaction primary key | Verified |
| `date` | timestamp string | string | Example: `2010-01-01 00:01:00`; parse in Silver | Verified |
| `client_id` | string-like integer | string | Expected to join to `users.id` | Verified header, full referential validation pending |
| `card_id` | string-like integer | string | Expected to join to `cards.id` | Verified header, full referential validation pending |
| `amount` | currency-formatted string | string | Includes `$`, negative signs, and potentially other formatting artifacts | Verified |
| `use_chip` | categorical string | string | Three verified values observed in the full file scan | Verified |
| `merchant_id` | string-like integer | string | Likely merchant surrogate key; business semantics still unclear | Needs confirmation |
| `merchant_city` | free-text string | string | Merchant location text | Verified |
| `merchant_state` | free-text location code | string | Blank for many rows; not safe to assume strict US-state encoding | Partially verified |
| `zip` | numeric-looking string | string | Example values include trailing `.0`; keep as string in Bronze | Verified |
| `mcc` | string-like code | string | Expected to join to MCC mapping JSON | Verified |
| `errors` | free-text categorical string | string | Blank or comma-delimited error categories | Verified |

## users_data.csv

Observed row count: 2,000 data rows.

| Field | Observed raw type | Bronze type | Notes | Status |
| --- | --- | --- | --- | --- |
| `id` | string-like integer | string | User primary key candidate | Verified |
| `current_age` | integer-like string | string | Numeric coercion deferred to Silver | Verified |
| `retirement_age` | integer-like string | string | Numeric coercion deferred to Silver | Verified |
| `birth_year` | integer-like string | string | Numeric coercion deferred to Silver | Verified |
| `birth_month` | integer-like string | string | Numeric coercion deferred to Silver | Verified |
| `gender` | categorical string | string | Raw categorical field | Verified |
| `address` | free-text string | string | PII-like address field | Verified |
| `latitude` | decimal-like string | string | Geographic coordinate stored as string in Bronze | Verified |
| `longitude` | decimal-like string | string | Geographic coordinate stored as string in Bronze | Verified |
| `per_capita_income` | currency-formatted string | string | Remove symbols later in Silver | Verified |
| `yearly_income` | currency-formatted string | string | Remove symbols later in Silver | Verified |
| `total_debt` | currency-formatted string | string | Remove symbols later in Silver | Verified |
| `credit_score` | integer-like string | string | Numeric coercion deferred to Silver | Verified |
| `num_credit_cards` | integer-like string | string | Needs business-definition confirmation | Partially verified |

## cards_data.csv

Observed row count: 6,146 data rows.

Observed notes:

- `id` is unique in the raw file
- `client_id` covers all 2,000 raw users
- `cvv` should remain a string because sample values are not consistently zero-padded to three visible digits

| Field | Observed raw type | Bronze type | Notes | Status |
| --- | --- | --- | --- | --- |
| `id` | string-like integer | string | Card primary key candidate | Verified |
| `client_id` | string-like integer | string | Expected to join to `users.id` | Verified header, full referential validation pending |
| `card_brand` | categorical string | string | Example: Visa, Mastercard | Verified |
| `card_type` | categorical string | string | Includes values like `Debit`, `Credit`, `Debit (Prepaid)` | Verified |
| `card_number` | long numeric-looking string | string | Sensitive identifier; keep raw in Bronze | Verified |
| `expires` | `MM/YYYY` string | string | Parse later if needed | Verified |
| `cvv` | numeric-looking string | string | Preserve as string | Verified |
| `has_chip` | yes-no string | string | Example values `YES` and `NO` | Verified |
| `num_cards_issued` | integer-like string | string | Business meaning still needs confirmation | Needs confirmation |
| `credit_limit` | currency-formatted string | string | Remove symbols later in Silver | Verified |
| `acct_open_date` | `MM/YYYY` string | string | Parse later if needed | Verified |
| `year_pin_last_changed` | year string | string | Numeric coercion deferred to Silver | Verified |
| `card_on_dark_web` | yes-no string | string | Source and definition need confirmation | Partially verified |

## train_fraud_labels.json

Observed top-level structure:

- root object
- key: `target`
- value: map of `transaction_id -> "Yes" / "No"`

Observed entry count: 8,914,963 labels.

| Bronze field | Observed raw type | Bronze type | Notes | Status |
| --- | --- | --- | --- | --- |
| `transaction_id` | JSON object key | string | Expected to join to `transactions.id` | Verified header equivalent, full join coverage pending |
| `is_fraud` | categorical string | string | Values observed: `Yes`, `No` | Verified |

## mcc_codes.json

Observed top-level structure:

- root object
- map of `mcc -> merchant_category`

Observed entry count: 109 mappings.

| Bronze field | Observed raw type | Bronze type | Notes | Status |
| --- | --- | --- | --- | --- |
| `mcc` | JSON object key | string | Merchant category code | Verified |
| `mcc_category` | string | string | Merchant category description | Verified |

## Verified Error Tokens In transactions.errors

The full transaction-file scan observed these normalized tokens:

- `Bad CVV`
- `Bad Card Number`
- `Bad Expiration`
- `Bad PIN`
- `Bad Zipcode`
- `Insufficient Balance`
- `Technical Glitch`

Silver normalization should treat the raw field as a multi-valued categorical string rather than a single-label column.

## Silver Outputs

### silver_transactions

This is the analysis-ready transaction fact. It retains all Bronze transactions and uses
left joins, so enrichment failures do not remove rows.

Key derived fields:

| Field | Type | Definition |
| --- | --- | --- |
| `transaction_timestamp` | timestamp | Parsed from raw `date` |
| `transaction_date` | date | Calendar date of the transaction |
| `transaction_hour` | integer | Hour from 0 through 23 |
| `transaction_weekday` | string | Full weekday name |
| `is_weekend` | boolean | Saturday or Sunday |
| `is_night` | boolean | Hour in configured interval `[0, 6)` |
| `amount` | decimal(18,2) | Currency symbols, commas, spaces, and parentheses normalized |
| `amount_abs` | decimal(18,2) | Absolute transaction amount |
| `is_negative_amount` | boolean | Parsed amount is below zero |
| `amount_parse_failed` | boolean | Non-blank raw amount failed numeric parsing |
| `error_tokens` | array<string> | Trimmed comma-delimited error values |
| `error_bad_cvv` | boolean | Contains `Bad CVV` |
| `error_bad_pin` | boolean | Contains `Bad PIN` |
| `error_insufficient_balance` | boolean | Contains `Insufficient Balance` |
| `error_technical_glitch` | boolean | Contains `Technical Glitch` |
| `merchant_location_category` | string | `domestic`, `international`, or `unknown` |
| `mcc_category` | string | Category joined from `mcc_codes.json` |
| `is_fraud` | integer | 1/0 for labeled rows; null for unlabeled rows |
| `card_record_matched` | boolean | Card enrichment key matched |
| `user_record_matched` | boolean | User enrichment key matched |
| `mcc_record_matched` | boolean | MCC enrichment key matched |
| `fraud_label_matched` | boolean | Fraud label exists for this transaction |
| `card_user_matches_transaction_user` | boolean | Card owner agrees with transaction user |

Location classification is supported by a full raw-domain scan: observed two-letter US
state or military codes are domestic, observed country names are international, and blanks
are unknown. Blank state values are not assumed to be international.

### silver_users, silver_cards, silver_fraud_labels, silver_mcc_codes

These tables provide typed conformed dimensions. Currency fields are decimal, numeric
fields use numeric Spark types, and Yes/No fields become booleans or binary labels.
Raw card number, CVV, and user street address remain available only in Bronze.

No Silver table contains class-balanced data or training-only fraud-rate encodings.

## Feature Outputs

### feature_split_assignments

Contains one row per labeled transaction with its immutable transaction timestamp, date,
binary label, and chronological `data_split`.

- `train`: transaction date through `2017-12-31`
- `validation`: `2018-01-01` through `2018-12-31`
- `test`: transaction date from `2019-01-01` onward

Unlabeled transactions remain in Silver and are not silently treated as negative examples.

### feature_mcc_fraud_rates

Contains MCC counts and additive-smoothed fraud rates fitted only from `train` rows:

```text
(training_mcc_fraud_count + alpha * training_global_fraud_rate)
/
(training_mcc_count + alpha)
```

The configured `alpha` is `100.0`. MCC values absent from training receive the global
training fraud rate when model features are assembled.

### feature_model_features

Contains labeled, chronologically split rows with:

- transaction amount, time, channel, location, and error signals
- age at transaction, account age, and PIN-change age
- card and user snapshot attributes with a `snapshot_` prefix
- debt-to-income and amount-to-credit-limit ratios
- MCC category and the training-only smoothed MCC fraud rate

The snapshot prefix is intentional: the raw files do not provide effective-date history for
income, debt, credit score, or credit limit. These fields must not be described as strict
point-in-time values in model interpretation.

No class balancing occurs in these tables. Any balancing is restricted to the training input
inside the later model-training pipeline.
