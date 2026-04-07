---
title: Infrastructure
---

All infrastructure is managed with Terraform and lives in the `infra/` directory. The platform runs entirely on Google Cloud Platform.

## GCP Resources

Terraform provisions the following resources in the `american-beauty-institute` GCP project (`us-east1` region):

| Resource | Name | Purpose |
|----------|------|---------|
| Compute Engine VM | `dagster-daemon` | Runs the Dagster daemon and code server (`e2-micro`, Debian 12) |
| GCS Bucket | `american-beauty-institute-raw` | Raw data landing zone (versioned) |
| GCS Bucket | `american-beauty-institute-tf-state` | Terraform remote state (versioned) |
| Service Account | `dagster-daemon` | Identity for the Dagster VM |
| Firewall Rule | `allow-iap-ssh` | Allows SSH through Identity-Aware Proxy |

The VM has no external IP. All SSH access goes through IAP, which restricts inbound traffic to Google's IAP range (`35.235.240.0/20`) on port 22.

## Service Account & IAM

The `dagster-daemon` service account has five IAM bindings:

| Role | Scope | Purpose |
|------|-------|---------|
| `roles/bigquery.dataEditor` | Project | Read and write BigQuery tables |
| `roles/bigquery.jobUser` | Project | Run BigQuery jobs |
| `roles/storage.objectAdmin` | Raw bucket | Read, write, and delete objects in the raw data bucket |
| `roles/secretmanager.secretAccessor` | Project | Fetch API credentials from Secret Manager at startup |
| `roles/iap.tunnelResourceAccessor` | Project | Allow SSH tunneling through IAP |

## Terraform State

State is stored remotely in GCS:

```hcl
backend "gcs" {
  bucket = "american-beauty-institute-tf-state"
  prefix = "terraform/state"
}
```

The state bucket has versioning enabled, so previous state versions can be recovered if needed.

## Terraform Layout

```
infra/
├── main.tf           # All resource definitions
├── variables.tf      # Input variables
├── outputs.tf        # Output values
├── versions.tf       # Provider and backend configuration
└── files/
    ├── startup.sh    # VM first-boot script
    ├── dagster.yaml  # Dagster daemon configuration
    ├── dagster.service
    ├── dagster-code.service
    ├── dagster-healthcheck.service
    ├── dagster-healthcheck.timer
    └── dagster-healthcheck.sh
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `project` | `american-beauty-institute` | GCP project ID |
| `region` | `us-east1` | GCP region |
| `zone` | `us-east1-b` | GCP zone for the VM |
| `credentials_file` | _(required)_ | Path to a GCP service account JSON key |
| `dagster_repo_url` | `https://github.com/americanbeautyinstitute/data-platform.git` | Repo cloned onto the VM |
| `dagster_branch` | `main` | Branch checked out on the VM |

## Applying Changes

Requires Terraform >= 1.7 and the Google provider ~> 5.0.

```bash
cd infra
terraform init
terraform plan -var="credentials_file=path/to/key.json"
terraform apply -var="credentials_file=path/to/key.json"
```

## VM First Boot

When the VM starts for the first time, the startup script (`infra/files/startup.sh`) runs the following steps:

1. Installs system dependencies (`git`, `curl`).
2. Creates a `dagster` system user.
3. Installs `uv` as the `dagster` user.
4. Clones the repo and installs Python dependencies with `uv sync --no-dev`.
5. Sets up `DAGSTER_HOME` at `/var/dagster/home` and copies `dagster.yaml`.
6. Pulls secrets from GCP Secret Manager and writes them to `/home/dagster/data-platform/.env`.
7. Copies systemd unit files and enables the Dagster services and health check timer.

After the startup script completes, the service account JSON key must be copied to the VM manually:

```bash
gcloud compute scp key.json dagster-daemon:/etc/gcp/service-account.json \
  --zone=us-east1-b --tunnel-through-iap --project=american-beauty-institute
```

Then set ownership on the VM:

```bash
sudo chown dagster:dagster /etc/gcp/service-account.json
```

## Outputs

After `terraform apply`, the following outputs are available:

| Output | Description |
|--------|-------------|
| `dagster_vm_name` | Name of the Dagster VM |
| `dagster_vm_zone` | Zone of the Dagster VM |
| `raw_bucket_name` | Name of the raw data GCS bucket |
| `tf_state_bucket_name` | Name of the Terraform state bucket |
| `dagster_service_account_email` | Email of the Dagster service account |
