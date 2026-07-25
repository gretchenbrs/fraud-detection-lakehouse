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
