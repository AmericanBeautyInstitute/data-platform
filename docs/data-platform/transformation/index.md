---
title: Data Transformation
---

Data transformation is handled by [SQLMesh](https://sqlmesh.com/) and runs inside BigQuery. All transformation code lives in the `transform/` directory.

## Layout

```
transform/
├── config.yaml                          # SQLMesh gateway and model defaults
├── metadata.py                          # Incremental model utilities
├── macros/
├── audits/
│   └── assert_no_nulls.sql              # Reusable null-check audit
└── models/
    ├── staging/
    │   ├── stg_facebook_ads__performance.sql
    │   ├── stg_google_ads__performance.sql
    │   ├── stg_google_analytics__sessions.sql
    │   ├── stg_google_sheets__inventory.sql
    │   ├── stg_google_sheets__programs.sql
    │   ├── stg_google_sheets__students.sql
    │   ├── stg_paypal__transactions.sql
    │   └── stg_stripe__charges.sql
    └── marts/
        ├── mart_enrollment__ad_attribution.sql
        ├── mart_finance__revenue_by_program.sql
        └── mart_inventory__stock_levels.sql
```

## Configuration

SQLMesh connects to BigQuery via a service account:

```yaml
gateways:
  bigquery:
    connection:
      type: bigquery
      method: service-account
      project: american-beauty-institute
      keyfile: ${GOOGLE_APPLICATION_CREDENTIALS}
      location: US

model_defaults:
  dialect: bigquery
  start: '2024-01-01'
  cron: '@daily'
```

All models default to the BigQuery dialect, start backfilling from 2024-01-01, and run on a daily schedule.

## Datasets

Models are organized into three BigQuery datasets:

| Dataset | Purpose |
|---------|---------|
| `raw` | Landing zone. Tables written by the extract-load pipeline. |
| `staging` | Type-cast and cleaned versions of raw tables. |
| `marts` | Business-level aggregations answering specific questions. |

## Incremental Processing

Most models use `INCREMENTAL_BY_TIME_RANGE`. SQLMesh passes `@start_date` and `@end_date` variables to each model's `WHERE` clause, so only new partitions are processed on each run. The `loaded_at` column (added by `transform/metadata.py`) tracks when each row was last written.
