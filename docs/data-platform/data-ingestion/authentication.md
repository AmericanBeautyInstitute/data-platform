---
title: Authentication
---

Each data source authenticates differently, but all credentials follow the same two-tier pattern:

- **Production** — the VM startup script pulls secrets from GCP Secret Manager and writes them to a `.env` file at `/home/dagster/data-platform/.env`.
- **Local development** — you create the `.env` file manually. It is gitignored.

Dagster resources read these values through `EnvVar` bindings, so asset code never accesses environment variables directly.

## Google Sheets, Google Analytics, Google Ads

These three sources authenticate with a GCP service account JSON key file. In production, the key lives at `/etc/gcp/service-account.json` on the VM.

| Variable | Description |
|---|---|
| `GOOGLE_SHEETS_CREDENTIALS_PATH` | Path to service account JSON |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | Target spreadsheet ID |
| `GOOGLE_ANALYTICS_CREDENTIALS_PATH` | Path to service account JSON |
| `GOOGLE_ANALYTICS_PROPERTY_ID` | GA4 property ID |
| `GOOGLE_ADS_CREDENTIALS_PATH` | Path to service account JSON |
| `GOOGLE_ADS_CUSTOMER_ID` | Google Ads customer ID |

## Facebook Ads

Authenticates with a System User access token from Meta Business Manager. No browser login or OAuth flow runs at runtime.

| Variable | Description |
|---|---|
| `FACEBOOK_ADS_ACCESS_TOKEN` | System user long-lived access token |
| `FACEBOOK_ADS_ACCOUNT_ID` | Ad account ID (`act_XXXXXXXXX` format) |

## PayPal

Authenticates with OAuth 2.0 client credentials. The client exchanges its ID and secret for a short-lived access token on each run.

| Variable | Description |
|---|---|
| `PAYPAL_CLIENT_ID` | PayPal app client ID |
| `PAYPAL_CLIENT_SECRET` | PayPal app client secret |

## Stripe

Authenticates with a secret key passed directly to the Stripe SDK. No token exchange required.

| Variable | Description |
|---|---|
| `STRIPE_SECRET_KEY` | Stripe secret key (`sk_live_...`) |

## GCP Infrastructure

The `IngestionConfig` Dagster resource provides GCP project and bucket values to all ingestion assets. These are bound to environment variables at resource definition time, not inside asset code.

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID (used by `IngestionConfig`, GCS, and BigQuery resources) |
| `GCS_BUCKET` | GCS bucket for raw Parquet files (used by `IngestionConfig`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON for BigQuery and GCS |
