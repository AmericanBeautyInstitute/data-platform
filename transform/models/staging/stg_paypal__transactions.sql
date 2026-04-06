MODEL (
  name staging.stg_paypal__transactions,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column transaction_date
  ),
  grain (transaction_date, transaction_id),
  cron '@daily',
  audits (assert_no_nulls(column := transaction_id))
);

SELECT
  CAST(transaction_id      AS STRING)  AS transaction_id,
  DATE(transaction_date)               AS transaction_date,
  CAST(gross_amount_usd    AS NUMERIC) AS gross_amount_usd,
  CAST(currency_code       AS STRING)  AS currency_code,
  CAST(transaction_status  AS STRING)  AS transaction_status,
  CAST(transaction_subject AS STRING)  AS transaction_subject,
  CAST(payer_email         AS STRING)  AS payer_email,
  CAST(payer_name          AS STRING)  AS payer_name,
  CAST(fee_amount_usd      AS NUMERIC) AS fee_amount_usd,
  CAST(net_amount_usd      AS NUMERIC) AS net_amount_usd,
  CAST(loaded_at           AS TIMESTAMP) AS loaded_at
FROM raw.paypal_transactions
WHERE
  transaction_date BETWEEN @start_date AND @end_date
