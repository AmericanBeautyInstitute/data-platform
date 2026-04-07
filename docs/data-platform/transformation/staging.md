---
title: Staging Models
---

Staging models read from `raw` and apply type casting and field standardization. They contain no business logic.

## Models

| Model | Source | Grain | Kind |
|-------|--------|-------|------|
| `stg_facebook_ads__performance` | `raw.facebook_ads` | `(date, campaign_id)` | Incremental |
| `stg_google_ads__performance` | `raw.google_ads` | `(date, customer_id)` | Incremental |
| `stg_google_analytics__sessions` | `raw.google_analytics` | `(date, sessionSource, sessionMedium, country)` | Incremental |
| `stg_google_sheets__inventory` | `raw.google_sheets_inventory` | `(sku_id, snapshot_date)` | Incremental |
| `stg_google_sheets__programs` | `raw.google_sheets_programs` | `program_id` | Full |
| `stg_google_sheets__students` | `raw.google_sheets_students` | `student_id` | Incremental |
| `stg_paypal__transactions` | `raw.paypal_transactions` | `(transaction_date, transaction_id)` | Incremental |
| `stg_stripe__charges` | `raw.stripe_charges` | `(charge_date, charge_id)` | Incremental |

All staging models are `INCREMENTAL_BY_TIME_RANGE` except `stg_google_sheets__programs`, which is `FULL` (refreshed completely each run since it is a small dimension table).

## Typical Transformations

- Casting strings to `DATE` or `TIMESTAMP`.
- Converting microcents to dollars (`cost_micros / 1000000`).
- Calculating derived fields like `cost_per_conversion_usd` using `SAFE_DIVIDE`.
- Standardizing field names and types across sources.
