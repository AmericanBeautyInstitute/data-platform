MODEL (
  name staging.stg_facebook_ads__performance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date
  ),
  grain (date, campaign_id),
  cron '@daily',
  audits (assert_no_nulls(column := date))
);

SELECT
  DATE(date)                        AS date,
  CAST(campaign_id      AS STRING)  AS campaign_id,
  CAST(campaign_name    AS STRING)  AS campaign_name,
  CAST(impressions      AS INT64)   AS impressions,
  CAST(clicks           AS INT64)   AS clicks,
  CAST(spend_usd        AS NUMERIC) AS spend_usd,
  CAST(reach            AS INT64)   AS reach,
  CAST(frequency        AS FLOAT64) AS frequency,
  CAST(link_clicks      AS INT64)   AS link_clicks,
  CAST(leads            AS INT64)   AS leads,
  CAST(conversions      AS INT64)   AS conversions,
  CAST(loaded_at        AS TIMESTAMP) AS loaded_at
FROM raw.facebook_ads
WHERE
  date BETWEEN @start_date AND @end_date
