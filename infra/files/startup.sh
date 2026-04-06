#!/bin/bash
set -euo pipefail

# --- Install system dependencies ---
apt-get update
apt-get install -y git curl

# --- Create dagster service user ---
useradd --system --create-home --shell /bin/bash dagster

# --- Install uv (as dagster user) ---
su - dagster -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'

# --- Clone repo ---
su - dagster -c "git clone --branch ${branch} ${repo_url} /home/dagster/data-platform"

# --- Install Python dependencies ---
su - dagster -c 'cd /home/dagster/data-platform && /home/dagster/.local/bin/uv sync --no-dev'

# --- Set up DAGSTER_HOME ---
mkdir -p /var/dagster/home
cp /home/dagster/data-platform/infra/files/dagster.yaml /var/dagster/home/dagster.yaml
chown -R dagster:dagster /var/dagster

# --- Write .env with Secret Manager references ---
PROJECT_ID="american-beauty-institute"

cat > /home/dagster/data-platform/.env << ENV
DAGSTER_HOME=/var/dagster/home
GCP_PROJECT_ID=$${PROJECT_ID}
GCS_BUCKET=american-beauty-institute-raw
GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/service-account.json
GOOGLE_SHEETS_CREDENTIALS_PATH=/etc/gcp/service-account.json
GOOGLE_ANALYTICS_CREDENTIALS_PATH=/etc/gcp/service-account.json
GOOGLE_ADS_CREDENTIALS_PATH=/etc/gcp/service-account.json
SQLMESH_PROJECT_DIR=/home/dagster/data-platform/transform
SQLMESH_GATEWAY=bigquery
STRIPE_SECRET_KEY=$(gcloud secrets versions access latest --secret=stripe-secret-key --project=$${PROJECT_ID})
PAYPAL_CLIENT_ID=$(gcloud secrets versions access latest --secret=paypal-client-id --project=$${PROJECT_ID})
PAYPAL_CLIENT_SECRET=$(gcloud secrets versions access latest --secret=paypal-client-secret --project=$${PROJECT_ID})
FACEBOOK_ADS_ACCESS_TOKEN=$(gcloud secrets versions access latest --secret=facebook-ads-access-token --project=$${PROJECT_ID})
FACEBOOK_ADS_ACCOUNT_ID=$(gcloud secrets versions access latest --secret=facebook-ads-account-id --project=$${PROJECT_ID})
GOOGLE_ADS_CUSTOMER_ID=$(gcloud secrets versions access latest --secret=google-ads-customer-id --project=$${PROJECT_ID})
GOOGLE_ANALYTICS_PROPERTY_ID=$(gcloud secrets versions access latest --secret=google-analytics-property-id --project=$${PROJECT_ID})
GOOGLE_SHEETS_SPREADSHEET_ID=$(gcloud secrets versions access latest --secret=google-sheets-spreadsheet-id --project=$${PROJECT_ID})
ENV

chown dagster:dagster /home/dagster/data-platform/.env
chmod 600 /home/dagster/data-platform/.env

# --- Place service account credentials ---
mkdir -p /etc/gcp
chmod 700 /etc/gcp
# NOTE: Copy your service account JSON to /etc/gcp/service-account.json
# after VM creation via:
# gcloud compute scp gcp_service_account_key.json dagster-daemon:/etc/gcp/service-account.json \
#   --zone=us-east1-b --tunnel-through-iap --project=american-beauty-institute
# Then run: chown dagster:dagster /etc/gcp/service-account.json

# --- Install systemd services and health check ---
cp /home/dagster/data-platform/infra/files/dagster.service /etc/systemd/system/dagster.service
cp /home/dagster/data-platform/infra/files/dagster-code.service /etc/systemd/system/dagster-code.service
cp /home/dagster/data-platform/infra/files/dagster-healthcheck.service /etc/systemd/system/dagster-healthcheck.service
cp /home/dagster/data-platform/infra/files/dagster-healthcheck.timer /etc/systemd/system/dagster-healthcheck.timer
cp /home/dagster/data-platform/infra/files/dagster-healthcheck.sh /usr/local/bin/dagster-healthcheck.sh
chmod +x /usr/local/bin/dagster-healthcheck.sh

systemctl daemon-reload
systemctl enable dagster-code dagster dagster-healthcheck.timer
systemctl start dagster-code
systemctl start dagster
systemctl start dagster-healthcheck.timer
