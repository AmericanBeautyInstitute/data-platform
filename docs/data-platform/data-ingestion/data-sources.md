---
title: Data Sources
---

The pipeline ingests data from six sources daily. Each source runs as a partitioned Dagster asset, scheduled at 6 AM Eastern. Assets retry up to three times with exponential backoff and jitter.

All extractors follow the same flow: fetch data from the API, validate it with Pydantic models, convert it to a PyArrow table, then load it to GCS and BigQuery.

## Stripe

Pulls charge records from the Stripe API using the Python SDK. Date filtering uses Unix timestamps. Amounts are stored in cents and converted to USD during extraction. Cursor-based pagination fetches all charges for the partition date.

| Field | Type | Notes |
|---|---|---|
| `charge_id` | string | |
| `charge_date` | date | Converted from Unix timestamp |
| `gross_amount_usd` | float | Cents divided by 100 |
| `amount_captured_usd` | float | Cents divided by 100 |
| `fee_usd` | float | From expanded balance transaction |
| `net_usd` | float | From expanded balance transaction |
| `currency` | string | |
| `status` | string | |
| `description` | string | |
| `customer_email` | string | |
| `customer_name` | string | |
| `payment_intent_id` | string | |

**BigQuery table:** `raw.stripe_charges`

## PayPal

Pulls transaction records from the PayPal Reporting API (`/v1/reporting/transactions`). The client authenticates with OAuth 2.0 client credentials and caches the bearer token. Pagination fetches 500 records per page.

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string | |
| `transaction_date` | date | Parsed from ISO 8601 |
| `gross_amount_usd` | float | |
| `fee_amount_usd` | float | |
| `net_amount_usd` | float | |
| `currency_code` | string | |
| `transaction_status` | string | |
| `transaction_subject` | string | |
| `payer_email` | string | |
| `payer_name` | string | |

**BigQuery table:** `raw.paypal_transactions`

## Google Ads

Queries campaign performance metrics through the Google Ads API using GAQL (Google Ads Query Language). Authenticates with a service account. Cost is stored in microcents (divide by 1,000,000 for USD).

| Field | Type | Notes |
|---|---|---|
| `date` | date | |
| `customer_id` | string | |
| `clicks` | int | |
| `impressions` | int | |
| `cost_micros` | int | Microcents; divide by 1,000,000 for USD |
| `conversions` | float | |

**BigQuery table:** `raw.google_ads`

## Google Analytics

Fetches session and pageview data from the GA4 Data API (`v1beta`). Authenticates with a service account. The report configuration specifies which dimensions and metrics to pull.

| Field | Type | Notes |
|---|---|---|
| `date` | date | Parsed from `YYYYMMDD` format |
| `dimensions` | dict | Source, medium, country |
| `metrics` | dict | Sessions, pageviews, bounce rate, conversions |

**BigQuery table:** `raw.google_analytics`

## Google Sheets

Reads entire sheets through the Google Sheets API v4 with read-only scope. No pagination — each sheet is fetched in a single call. The first row is treated as headers. Three sheets are ingested as separate assets: students, programs, and inventory.

| Field | Type | Notes |
|---|---|---|
| `data` | dict | Header-to-value mapping; all values are strings |

**BigQuery tables:** `raw.google_sheets_students`, `raw.google_sheets_programs`, `raw.google_sheets_inventory`

## Facebook Ads

Pulls campaign-level insights from the Facebook Marketing API using the Business SDK. Authenticates with a System User access token. Actions (link clicks, leads, conversions) are extracted from a nested array in the API response.

| Field | Type | Notes |
|---|---|---|
| `date` | date | |
| `campaign_id` | string | |
| `campaign_name` | string | |
| `impressions` | int | |
| `clicks` | int | |
| `reach` | int | |
| `spend_usd` | float | |
| `frequency` | float | |
| `link_clicks` | int | Extracted from actions array |
| `leads` | int | Extracted from actions array |
| `conversions` | int | Extracted from actions array |

**BigQuery table:** `raw.facebook_ads`
