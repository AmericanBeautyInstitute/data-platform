MODEL (
  name staging.stg_google_sheets__inventory,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column snapshot_date
  ),
  grain (sku_id, snapshot_date),
  cron '@daily',
  audits (assert_no_nulls(column := sku_id))
);

SELECT
  CAST(sku_id             AS STRING)  AS sku_id,
  CAST(sku_name           AS STRING)  AS sku_name,
  CAST(program_id         AS STRING)  AS program_id,
  CAST(quantity_on_hand   AS INT64)   AS quantity_on_hand,
  CAST(reorder_threshold  AS INT64)   AS reorder_threshold,
  CAST(reorder_quantity   AS INT64)   AS reorder_quantity,
  CAST(unit_cost_usd      AS NUMERIC) AS unit_cost_usd,
  CAST(units_per_student  AS NUMERIC) AS units_per_student,  -- expected usage per student per program
  DATE(snapshot_date)                 AS snapshot_date,
  CAST(loaded_at          AS TIMESTAMP) AS loaded_at
FROM raw.google_sheets_inventory
WHERE
  snapshot_date BETWEEN @start_date AND @end_date
