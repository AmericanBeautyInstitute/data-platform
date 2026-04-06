#!/bin/bash
set -euo pipefail

# --- Install system dependencies ---
apt-get update
apt-get install -y git curl

# --- Install uv ---
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$${PATH}"

# --- Clone repo ---
git clone --branch ${branch} ${repo_url} /opt/data-platform
cd /opt/data-platform

# --- Install Python dependencies ---
/root/.local/bin/uv sync --no-dev

# --- Set up DAGSTER_HOME ---
mkdir -p /var/dagster/home
cp /opt/data-platform/infra/files/dagster.yaml /var/dagster/home/dagster.yaml

# --- Place service account credentials ---
mkdir -p /etc/gcp
chmod 700 /etc/gcp
# NOTE: Copy your service account JSON to /etc/gcp/service-account.json
# after VM creation via:
# gcloud compute scp gcp_service_account_key.json dagster-daemon:/etc/gcp/service-account.json \
#   --zone=us-east1-b --tunnel-through-iap --project=american-beauty-institute

# --- Write .env ---
cat > /opt/data-platform/.env << 'ENV'
DAGSTER_HOME=/var/dagster/home
GCP_PROJECT_ID=american-beauty-institute
GCS_BUCKET=american-beauty-institute-raw
GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/service-account.json
GOOGLE_SHEETS_CREDENTIALS_PATH=/etc/gcp/service-account.json
GOOGLE_ANALYTICS_CREDENTIALS_PATH=/etc/gcp/service-account.json
GOOGLE_ADS_CREDENTIALS_PATH=/etc/gcp/service-account.json
SQLMESH_PROJECT_DIR=/opt/data-platform/transform
SQLMESH_GATEWAY=bigquery
ENV

# --- Install and enable systemd service ---
cp /opt/data-platform/infra/files/dagster.service /etc/systemd/system/dagster.service
systemctl daemon-reload
systemctl enable dagster
systemctl start dagster
