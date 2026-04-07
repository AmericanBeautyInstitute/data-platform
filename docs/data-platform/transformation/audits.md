---
title: Audits & Linting
---

## Audits

Every model includes an `assert_no_nulls` audit on its primary key column. This runs after each model evaluation and fails the pipeline if any null keys are found.

```sql
AUDIT (
  name assert_no_nulls,
  dialect bigquery,
);

SELECT * FROM @this_model WHERE @column IS NULL;
```

## Linting

SQLMesh's built-in linter is enabled with three rules:

- `ambiguousorinvalidcolumn` — flags column references that can't be resolved.
- `invalidselectstarexpansion` — prevents `SELECT *` in models.
- `noambiguousprojections` — requires unambiguous column names in joins.
