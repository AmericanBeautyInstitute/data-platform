with stripe_revenue as (
  select
    date_trunc(charge_date, month) as month,
    description as transaction_subject,
    sum(gross_amount_usd) as gross_revenue,
    sum(fee_usd) as total_fees,
    sum(net_usd) as net_revenue,
    count(*) as transaction_count,
    'stripe' as payment_source
  from {{ ref('stg_stripe__charges') }}
  where status = 'succeeded'
  group by date_trunc(charge_date, month), description
),

paypal_revenue as (
  select
    date_trunc(transaction_date, month) as month,
    transaction_subject,
    sum(gross_amount_usd) as gross_revenue,
    sum(fee_amount_usd) * -1 as total_fees,
    sum(net_amount_usd) as net_revenue,
    count(*) as transaction_count,
    'paypal' as payment_source
  from {{ ref('stg_paypal__transactions') }}
  where transaction_status = 'S'
  group by date_trunc(transaction_date, month), transaction_subject
),

combined as (
  select * from stripe_revenue
  union all
  select * from paypal_revenue
),

joined_to_programs as (
  select
    c.month,
    c.payment_source,
    c.transaction_subject,
    coalesce(p.program_id, 'unknown') as program_id,
    coalesce(p.program_name, 'unknown') as program_name,
    c.gross_revenue,
    c.total_fees,
    c.net_revenue,
    c.transaction_count
  from combined as c
  left join {{ ref('stg_google_sheets__programs') }} as p
    on lower(c.transaction_subject) like concat('%', lower(p.program_name), '%')
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
group by month, program_id, program_name, payment_source, transaction_subject
