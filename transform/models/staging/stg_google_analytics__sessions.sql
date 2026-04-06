MODEL (
  name staging.stg_google_analytics__sessions,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date
  ),
  grain (date, sessionSource, sessionMedium, country),
  cron '@daily',
  audits (assert_no_nulls(column := date))
);

SELECT
  DATE(date)                        AS date,
  CAST(sessionSource    AS STRING)  AS session_source,
  CAST(sessionMedium    AS STRING)  AS session_medium,
  CAST(country          AS STRING)  AS country,
  CAST(sessions         AS INT64)   AS sessions,
  CAST(screenPageViews  AS INT64)   AS page_views,
  CAST(bounceRate       AS FLOAT64) AS bounce_rate,
  CAST(conversions      AS FLOAT64) AS conversions
FROM raw.google_analytics
WHERE
  date BETWEEN @start_date AND @end_date
