---
title: Operations & Monitoring
---

Dagster runs as two systemd services on the `dagster-daemon` VM. A timer-based health check monitors both services.

## Services

| Unit | Type | Description |
|------|------|-------------|
| `dagster-code` | `simple` | gRPC code server on `127.0.0.1:4266`. Loads asset definitions from `assets.definitions`. |
| `dagster` | `simple` | Dagster daemon. Schedules and executes runs. Depends on `dagster-code`. |
| `dagster-healthcheck` | `oneshot` | Checks that both services are active. |
| `dagster-healthcheck.timer` | `timer` | Triggers the health check every 5 minutes (first run 2 minutes after boot). |

Both long-running services run as the `dagster` user, read environment variables from `/home/dagster/data-platform/.env`, and restart on failure with a 10-second delay.

### Service Dependencies

```
dagster-code  ←  dagster  (Requires + After)
```

`dagster` will not start until `dagster-code` is running. If `dagster-code` stops, `dagster` is also stopped.

## Common Commands

Check status of both services:

```bash
sudo systemctl status dagster-code dagster
```

Restart services (always restart `dagster-code` first):

```bash
sudo systemctl restart dagster-code
sudo systemctl restart dagster
```

Stop everything:

```bash
sudo systemctl stop dagster
sudo systemctl stop dagster-code
```

Check the health check timer:

```bash
systemctl list-timers dagster-healthcheck.timer
```

## Viewing Logs

All services log to the systemd journal.

Recent logs for the daemon:

```bash
journalctl -u dagster -n 100 --no-pager
```

Recent logs for the code server:

```bash
journalctl -u dagster-code -n 100 --no-pager
```

Follow logs in real time:

```bash
journalctl -u dagster -u dagster-code -f
```

Health check results:

```bash
journalctl -u dagster-healthcheck -n 20 --no-pager
```

## Health Check

The health check script (`/usr/local/bin/dagster-healthcheck.sh`) runs every 5 minutes via `dagster-healthcheck.timer`. It checks that both `dagster` and `dagster-code` are active and logs one of:

- `OK: all Dagster services running`
- `UNHEALTHY: <service> is not running`

The check exits with code 1 if any service is down. Failed runs are visible in the journal.

## Dagster Configuration

Dagster is configured via `/var/dagster/home/dagster.yaml`:

```yaml
storage:
  sqlite:
    base_dir: /var/dagster/home

code_server:
  grpc:
    host: 127.0.0.1
    port: 4266

telemetry:
  enabled: false
```

- **Storage** — run metadata is stored in SQLite under `/var/dagster/home`.
- **Code server** — the daemon connects to the gRPC server on localhost port 4266.
- **Telemetry** — disabled.

There is no webserver. All interaction with Dagster is through the CLI or the daemon's scheduled runs.

## Troubleshooting

### A service won't start

Check the journal for error details:

```bash
journalctl -u dagster-code -n 50 --no-pager
```

Common causes:

- **Missing `.env` file** — the service won't start without it. Check that `/home/dagster/data-platform/.env` exists and is owned by `dagster`.
- **Port conflict** — if something else is using port 4266, `dagster-code` will fail. Check with `ss -tlnp | grep 4266`.
- **Bad dependency install** — run `sudo -u dagster /home/dagster/.local/bin/uv sync` to reinstall.

### Health check reports UNHEALTHY

SSH into the VM and check which service is down:

```bash
sudo systemctl status dagster-code dagster
```

Restart the failed service. If the code server is down, restart both (the daemon depends on it).

### Run metadata is corrupted

Dagster stores run data in SQLite. If the database becomes corrupted:

1. Stop both services.
2. Back up `/var/dagster/home/`.
3. Delete the SQLite files and restart. Run history will be lost but the pipeline will resume on the next schedule.
