---
title: Deployment
---

The platform runs on a single GCP Compute Engine VM. Deployments pull the latest code from `main` and restart the Dagster services.

## How to Deploy

From your local machine, run:

```bash
make deploy
```

This executes over an IAP-tunneled SSH connection and runs the following steps on the VM:

1. `git pull` — pulls the latest code from the `main` branch.
2. `uv sync` — installs any new or updated dependencies.
3. `systemctl restart dagster-code` — restarts the gRPC code server.
4. `systemctl restart dagster` — restarts the Dagster daemon.

Both `git pull` and `uv sync` run as the `dagster` user. The service restarts run as root via `sudo`.

## Prerequisites

- **Google Cloud SDK** installed and authenticated (`gcloud auth login`).
- **IAP permissions** — your Google account needs the `IAP-secured Tunnel User` role on the project.
- Changes must be merged to `main` before deploying. The VM always tracks the `main` branch.

## SSH Access

To open an interactive shell on the VM:

```bash
make ssh
```

This tunnels through IAP. The VM has no external IP, so direct SSH is not possible.

## Rollback

There is no automated rollback. To revert a deployment:

1. SSH into the VM: `make ssh`
2. Switch to the dagster user: `sudo -u dagster bash`
3. Revert to the previous commit:

    ```bash
    cd /home/dagster/data-platform
    git log --oneline -5        # find the commit to revert to
    git checkout <commit-sha>
    ```

4. Restart services:

    ```bash
    sudo systemctl restart dagster-code
    sudo systemctl restart dagster
    ```

5. Verify services are running:

    ```bash
    sudo systemctl status dagster-code dagster
    ```

Once the fix is merged to `main`, run `make deploy` again to move back to tracking HEAD.

## What Gets Deployed

The `deploy` target only updates application code and Python dependencies. It does **not** update:

- **Infrastructure** — Terraform changes require `terraform apply` (see [Infrastructure](../infrastructure/index.md)).
- **Systemd unit files** — changes to service definitions in `infra/files/` require manually copying them to `/etc/systemd/system/` and running `systemctl daemon-reload`.
- **Dagster configuration** — changes to `dagster.yaml` require copying to `/var/dagster/home/` and restarting services.
