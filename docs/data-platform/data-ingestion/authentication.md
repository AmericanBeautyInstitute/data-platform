---
title: Authentication
---

Each data source uses a different authentication mechanism. All credentials are stored as environment variables and never committed to the repository.

In production, credentials are stored in GCP Secret Manager and accessed at runtime. Locally, credentials are loaded from a `.env` file which is gitignored.

## Google Sheets, Google Analytics, Google Ads

These three sources authenticate using a GCP service account JSON key file. The path to the key file is provided via environment variable.

| Variable | Description |
|---|---|
| `GOOGLE_SHEETS_CREDENTIALS_PATH` | Path to service account JSON |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | Target spreadsheet ID |
| `GOOGLE_ANALYTICS_CREDENTIALS_PATH` | Path to service account JSON |
| `GOOGLE_ANALYTICS_PROPERTY_ID` | GA4 property ID |
| `GOOGLE_ADS_CREDENTIALS_PATH` | Path to Google Ads YAML credentials |
| `GOOGLE_ADS_CUSTOMER_ID` | Google Ads customer ID |

## Facebook Ads

Facebook Ads authenticates using a System User access token generated from Meta Business Manager. No browser login or OAuth flow is required at runtime.

| Variable | Description |
|---|---|
| `FACEBOOK_ADS_ACCESS_TOKEN` | System user long-lived access token |
| `FACEBOOK_ADS_ACCOUNT_ID` | Ad account ID in `act_XXXXXXXXX` format |

## PayPal

PayPal authenticates using OAuth 2.0 client credentials. The client exchanges `client_id` and `client_secret` for a short-lived access token automatically on each run.

| Variable | Description |
|---|---|
| `PAYPAL_CLIENT_ID` | PayPal app client ID |
| `PAYPAL_CLIENT_SECRET` | PayPal app client secret |

## Stripe

Stripe authenticates using a secret key. No token exchange is required — the key is passed directly to the Stripe SDK.

| Variable | Description |
|---|---|
| `STRIPE_SECRET_KEY` | Stripe secret key (`sk_live_...`) |

## GCP Infrastructure

The Dagster assets themselves use two additional environment variables for GCP infrastructure access.

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID |
| `GCS_BUCKET` | GCS bucket name for raw Parquet files |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON for BigQuery and GCS |
