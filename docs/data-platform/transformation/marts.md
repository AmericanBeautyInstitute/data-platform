---
title: Mart Models
---

Mart models join staging tables and aggregate data to answer business questions.

## `mart_enrollment__ad_attribution`

Correlates daily student enrollments with Google Ads spend and Google Analytics paid sessions. Calculates cost-per-enrollment and cost-per-ad-conversion.

- **Grain:** `date`
- **Sources:** `stg_google_sheets__students`, `stg_google_ads__performance`, `stg_google_analytics__sessions`

## `mart_finance__revenue_by_program`

Consolidates Stripe and PayPal revenue by month and program. Matches transactions to programs using a fuzzy `LIKE` match on description/subject. Calculates gross revenue, fees, net revenue, and average net per transaction.

- **Grain:** `(month, program_id, payment_source)`
- **Sources:** `stg_stripe__charges`, `stg_paypal__transactions`, `stg_google_sheets__programs`

## `mart_inventory__stock_levels`

Calculates current inventory status per SKU. Projects daily usage based on active student count and units-per-student, then derives days of stock remaining and a stock status label (`ok`, `reorder_soon`, `reorder_now`, `stockout`).

- **Grain:** `(sku_id, snapshot_date)`
- **Sources:** `stg_google_sheets__inventory`, `stg_google_sheets__students`
