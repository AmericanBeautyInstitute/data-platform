select
  payment_id,
  gross_amount_usd,
  fee_amount_usd,
  net_amount_usd
from {{ ref('stg_stripe__charges') }}
where
  fee_amount_usd < 0
  or gross_amount_usd - fee_amount_usd != net_amount_usd
