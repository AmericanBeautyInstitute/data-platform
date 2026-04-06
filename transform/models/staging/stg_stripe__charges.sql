MODEL (
  name staging.stg_stripe__charges,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column charge_date
  ),
  grain (charge_date, charge_id),
  cron '@daily',
  audits (assert_no_nulls(column := charge_id))
);

SELECT
  CAST(charge_id           AS STRING)  AS charge_id,
  DATE(charge_date)                    AS charge_date,
  CAST(gross_amount_usd    AS NUMERIC) AS gross_amount_usd,
  CAST(amount_captured_usd AS NUMERIC) AS amount_captured_usd,
  CAST(fee_usd             AS NUMERIC) AS fee_usd,
  CAST(net_usd             AS NUMERIC) AS net_usd,
  CAST(currency            AS STRING)  AS currency,
  CAST(status              AS STRING)  AS status,
  CAST(description         AS STRING)  AS description,
  CAST(customer_email      AS STRING)  AS customer_email,
  CAST(customer_name       AS STRING)  AS customer_name,
  CAST(payment_intent_id   AS STRING)  AS payment_intent_id,
  CAST(loaded_at           AS TIMESTAMP) AS loaded_at
FROM raw.stripe_charges
WHERE
  charge_date BETWEEN @start_date AND @end_date
