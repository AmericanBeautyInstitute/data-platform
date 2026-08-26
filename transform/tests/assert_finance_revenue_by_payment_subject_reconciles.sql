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

expected as (
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

actual as (
  select
    month,
    payment_source,
    transaction_subject,
    gross_revenue,
    total_fees,
    net_revenue,
    transaction_count
  from {{ ref('mart_finance__revenue_by_payment_subject') }}
)

select
  coalesce(e.month, a.month) as month,
  coalesce(e.payment_source, a.payment_source) as payment_source,
  coalesce(
    e.transaction_subject,
    a.transaction_subject
  ) as transaction_subject
from expected as e
full outer join actual as a
  on e.month = a.month
  and e.payment_source = a.payment_source
  and coalesce(e.transaction_subject, '')
    = coalesce(a.transaction_subject, '')
where
  e.month is null
  or a.month is null
  or abs(e.gross_revenue - a.gross_revenue) > 0.01
  or abs(e.total_fees - a.total_fees) > 0.01
  or abs(e.net_revenue - a.net_revenue) > 0.01
  or e.transaction_count != a.transaction_count
