MODEL (
  name marts.mart_finance__revenue_by_program,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column month
  ),
  grain (month, program_id, payment_source),
  cron '@daily',
  audits (assert_no_nulls(column := month))
);

WITH stripe_revenue AS (
  SELECT
    DATE_TRUNC(charge_date, MONTH)      AS month,
    description                         AS transaction_subject,
    SUM(gross_amount_usd)               AS gross_revenue,
    SUM(fee_usd)                        AS total_fees,
    SUM(net_usd)                        AS net_revenue,
    COUNT(*)                            AS transaction_count,
    'stripe'                            AS payment_source
  FROM staging.stg_stripe__charges
  WHERE
    status = 'succeeded'
    AND charge_date BETWEEN @start_date AND @end_date
  GROUP BY DATE_TRUNC(charge_date, MONTH), description
),

paypal_revenue AS (
  SELECT
    DATE_TRUNC(transaction_date, MONTH) AS month,
    transaction_subject,
    SUM(gross_amount_usd)               AS gross_revenue,
    SUM(fee_amount_usd) * -1            AS total_fees,
    SUM(net_amount_usd)                 AS net_revenue,
    COUNT(*)                            AS transaction_count,
    'paypal'                            AS payment_source
  FROM staging.stg_paypal__transactions
  WHERE
    transaction_status = 'S'
    AND transaction_date BETWEEN @start_date AND @end_date
  GROUP BY DATE_TRUNC(transaction_date, MONTH), transaction_subject
),

combined AS (
  SELECT * FROM stripe_revenue
  UNION ALL
  SELECT * FROM paypal_revenue
),

joined_to_programs AS (
  SELECT
    c.month,
    c.payment_source,
    c.transaction_subject,
    COALESCE(p.program_id, 'unknown')   AS program_id,
    COALESCE(p.program_name, 'unknown') AS program_name,
    c.gross_revenue,
    c.total_fees,
    c.net_revenue,
    c.transaction_count
  FROM combined AS c
  LEFT JOIN staging.stg_google_sheets__programs AS p
    ON LOWER(c.transaction_subject) LIKE CONCAT('%', LOWER(p.program_name), '%')
)

SELECT
  month,
  program_id,
  program_name,
  payment_source,
  transaction_subject,
  SUM(gross_revenue)      AS gross_revenue,
  SUM(total_fees)         AS total_fees,
  SUM(net_revenue)        AS net_revenue,
  SUM(transaction_count)  AS transaction_count,
  SAFE_DIVIDE(
    SUM(net_revenue),
    NULLIF(SUM(transaction_count), 0)
  )                       AS avg_net_per_transaction
FROM joined_to_programs
WHERE
  month BETWEEN @start_date AND @end_date
GROUP BY month, program_id, program_name, payment_source, transaction_subject
