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
)

select
  month,
  payment_source,
  transaction_subject,
  gross_revenue,
  total_fees,
  net_revenue,
  transaction_count,
  safe_divide(
    net_revenue,
    nullif(transaction_count, 0)
  ) as avg_net_per_transaction
from monthly_revenue
