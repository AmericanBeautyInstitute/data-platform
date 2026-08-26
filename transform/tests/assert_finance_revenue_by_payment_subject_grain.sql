select
  month,
  payment_source,
  transaction_subject,
  count(*) as row_count
from {{ ref('mart_finance__revenue_by_payment_subject') }}
group by
  month,
  payment_source,
  transaction_subject
having count(*) > 1
