---
title: Secret Management
---

API credentials and sensitive configuration are stored in [GCP Secret Manager](https://cloud.google.com/secret-manager) and pulled onto the VM at startup. See [Authentication](../data-ingestion/authentication.md) for details on each source's credentials.

## How It Works

1. Secrets are stored as individual entries in Secret Manager under the `american-beauty-institute` project.
2. On first boot, the VM startup script calls `gcloud secrets versions access latest` for each secret and writes the values into `/home/dagster/data-platform/.env`.
3. The `.env` file is owned by `dagster:dagster` with `chmod 600` (readable only by the dagster user).
4. Dagster services load the `.env` file via their systemd `EnvironmentFile` directive.

## Secrets in Secret Manager

| Secret Name | Used By |
|-------------|---------|
| `stripe-secret-key` | Stripe extractor |
| `paypal-client-id` | PayPal extractor |
| `paypal-client-secret` | PayPal extractor |
| `facebook-ads-access-token` | Facebook Ads extractor |
| `facebook-ads-account-id` | Facebook Ads extractor |
| `google-ads-customer-id` | Google Ads extractor |
| `google-analytics-property-id` | Google Analytics extractor |
| `google-sheets-spreadsheet-id` | Google Sheets extractor |

## Adding a New Secret

1. Create the secret in Secret Manager:

    ```bash
    gcloud secrets create my-new-secret \
      --project=american-beauty-institute \
      --replication-policy=automatic
    ```

2. Add a version with the secret value:

    ```bash
    echo -n "the-secret-value" | gcloud secrets versions add my-new-secret \
      --project=american-beauty-institute \
      --data-file=-
    ```

3. Add a line to the startup script (`infra/files/startup.sh`) to pull the secret into the `.env` file:

    ```bash
    MY_NEW_SECRET=$(gcloud secrets versions access latest --secret=my-new-secret --project=$${PROJECT_ID})
    ```

4. Add the variable to `.env.example` for local development documentation.

5. On the VM, either re-run the startup script or manually add the value to `/home/dagster/data-platform/.env` and restart services.

## Rotating a Secret

1. Add a new version in Secret Manager:

    ```bash
    echo -n "new-value" | gcloud secrets versions add my-secret \
      --project=american-beauty-institute \
      --data-file=-
    ```

2. SSH into the VM and update the `.env` file, or restart the VM to trigger the startup script. Then restart the Dagster services:

    ```bash
    sudo systemctl restart dagster-code
    sudo systemctl restart dagster
    ```

The startup script always pulls the `latest` version, so previous versions remain available in Secret Manager for rollback.

## Local Development

For local development, copy `.env.example` to `.env` and fill in the values manually. The `.env` file is gitignored and should never be committed.

```bash
cp .env.example .env
```

## Service Account Key

The GCP service account JSON key is **not** stored in Secret Manager. It must be copied to the VM manually after first boot:

```bash
gcloud compute scp key.json dagster-daemon:/etc/gcp/service-account.json \
  --zone=us-east1-b --tunnel-through-iap --project=american-beauty-institute
```

This key is used by Google Sheets, Google Analytics, Google Ads, BigQuery, and GCS.
