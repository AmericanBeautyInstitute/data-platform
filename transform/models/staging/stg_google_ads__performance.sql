MODEL (
  name staging.stg_google_ads__performance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date
  ),
  grain (date, customer_id),
  cron '@daily',
  audits (assert_no_nulls(column := date))
);

SELECT
  DATE(date)                    AS date,
  CAST(customer_id  AS STRING)  AS customer_id,
  CAST(clicks       AS INT64)   AS clicks,
  CAST(impressions  AS INT64)   AS impressions,
  ROUND(cost_micros / 1000000, 2) AS cost_usd,
  CAST(conversions  AS FLOAT64) AS conversions,
  SAFE_DIVIDE(
    ROUND(cost_micros / 1000000, 2),
    NULLIF(CAST(conversions AS FLOAT64), 0)
  )                             AS cost_per_conversion_usd
FROM raw.google_ads
WHERE
  date BETWEEN @start_date AND @end_date
