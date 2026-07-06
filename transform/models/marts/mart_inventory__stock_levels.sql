with active_students as (
  select
    program_id,
    count(*) as active_student_count
  from {{ ref('stg_google_sheets__students') }}
  where enrollment_status = 'active'
  group by program_id
),

projected_usage as (
  select
    i.sku_id,
    i.sku_name,
    i.program_id,
    i.quantity_on_hand,
    i.reorder_threshold,
    i.reorder_quantity,
    i.unit_cost_usd,
    i.units_per_student,
    i.snapshot_date,
    coalesce(a.active_student_count, 0) as active_student_count,
    round(
      i.units_per_student * coalesce(a.active_student_count, 0), 2
    ) as projected_daily_usage,
    safe_divide(
      i.quantity_on_hand,
      nullif(i.units_per_student * coalesce(a.active_student_count, 0), 0)
    ) as days_of_stock_remaining
  from {{ ref('stg_google_sheets__inventory') }} as i
  left join active_students as a
    on i.program_id = a.program_id
)

select
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
  round(days_of_stock_remaining, 1) as days_of_stock_remaining,
  case
    when quantity_on_hand <= 0 then 'stockout'
    when quantity_on_hand <= reorder_threshold then 'reorder_now'
    when days_of_stock_remaining <= 14 then 'reorder_soon'
    else 'ok'
  end as stock_status,
  round(reorder_quantity * unit_cost_usd, 2) as reorder_cost_usd
from projected_usage
