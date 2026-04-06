#!/bin/bash
# Checks that both Dagster services are running.
# Called by dagster-healthcheck.timer via systemd.
# Failures are logged to journal: journalctl -u dagster-healthcheck

set -euo pipefail

failed=0

for service in dagster dagster-code; do
    if ! systemctl is-active --quiet "$service"; then
        echo "UNHEALTHY: $service is not running"
        failed=1
    fi
done

if [ "$failed" -eq 0 ]; then
    echo "OK: all Dagster services running"
fi

exit $failed
