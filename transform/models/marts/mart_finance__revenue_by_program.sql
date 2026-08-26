with payments as (
  select
    payment_date,
    gross_amount_usd,
    fee_amount_usd,
    net_amount_usd,
    is_successful,
    transaction_subject,
    payment_source
  from {{ ref('stg_stripe__charges') }}

  union all

  select
    payment_date,
    gross_amount_usd,
    fee_amount_usd,
    net_amount_usd,
    is_successful,
    transaction_subject,
    payment_source
  from {{ ref('stg_paypal__transactions') }}
),

monthly_revenue as (
  select
    date_trunc(payment_date, month) as month,
    payment_source,
    transaction_subject,
    sum(gross_amount_usd) as gross_revenue,
    sum(fee_amount_usd) as total_fees,
    sum(net_amount_usd) as net_revenue,
    count(*) as transaction_count
  from payments
  where is_successful
  group by
    date_trunc(payment_date, month),
    payment_source,
    transaction_subject
),

joined_to_programs as (
  select
    r.month,
    r.payment_source,
    r.transaction_subject,
    coalesce(p.program_id, 'unknown') as program_id,
    coalesce(p.program_name, 'unknown') as program_name,
    r.gross_revenue,
    r.total_fees,
    r.net_revenue,
    r.transaction_count
  from monthly_revenue as r
  left join {{ ref('stg_google_sheets__programs') }} as p
    on lower(r.transaction_subject)
      like concat('%', lower(p.program_name), '%')
)

select
  month,
  program_id,
  program_name,
  payment_source,
  transaction_subject,
  sum(gross_revenue) as gross_revenue,
  sum(total_fees) as total_fees,
  sum(net_revenue) as net_revenue,
  sum(transaction_count) as transaction_count,
  safe_divide(
    sum(net_revenue),
    nullif(sum(transaction_count), 0)
  ) as avg_net_per_transaction
from joined_to_programs
group by
  month,
  program_id,
  program_name,
  payment_source,
  transaction_subject
