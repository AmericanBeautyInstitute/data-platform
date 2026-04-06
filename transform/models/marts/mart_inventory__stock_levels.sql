MODEL (
  name marts.mart_inventory__stock_levels,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column snapshot_date
  ),
  grain (sku_id, snapshot_date),
  cron '@daily',
  audits (assert_no_nulls(column := sku_id))
);

WITH active_students AS (
  SELECT
    program_id,
    COUNT(*) AS active_student_count
  FROM staging.stg_google_sheets__students
  WHERE enrollment_status = 'active'
  GROUP BY program_id
),

projected_usage AS (
  SELECT
    i.sku_id,
    i.sku_name,
    i.program_id,
    i.quantity_on_hand,
    i.reorder_threshold,
    i.reorder_quantity,
    i.unit_cost_usd,
    i.units_per_student,
    i.snapshot_date,
    COALESCE(a.active_student_count, 0)        AS active_student_count,
    ROUND(
      i.units_per_student * COALESCE(a.active_student_count, 0), 2
    )                                           AS projected_daily_usage,
    SAFE_DIVIDE(
      i.quantity_on_hand,
      NULLIF(i.units_per_student * COALESCE(a.active_student_count, 0), 0)
    )                                           AS days_of_stock_remaining
  FROM staging.stg_google_sheets__inventory AS i
  LEFT JOIN active_students AS a
    ON i.program_id = a.program_id
  WHERE
    i.snapshot_date BETWEEN @start_date AND @end_date
)

SELECT
  sku_id,
  sku_name,
  program_id,
  snapshot_date,
  quantity_on_hand,
  reorder_threshold,
  reorder_quantity,
  unit_cost_usd,
  units_per_student,
  active_student_count,
  projected_daily_usage,
  ROUND(days_of_stock_remaining, 1)             AS days_of_stock_remaining,
  CASE
    WHEN quantity_on_hand <= 0                  THEN 'stockout'
    WHEN quantity_on_hand <= reorder_threshold  THEN 'reorder_now'
    WHEN days_of_stock_remaining <= 14          THEN 'reorder_soon'
    ELSE                                             'ok'
  END                                           AS stock_status,
  ROUND(reorder_quantity * unit_cost_usd, 2)    AS reorder_cost_usd
FROM projected_usage
