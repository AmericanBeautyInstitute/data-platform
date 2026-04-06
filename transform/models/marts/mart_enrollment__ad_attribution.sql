MODEL (
  name marts.mart_enrollment__ad_attribution,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column date
  ),
  grain date,
  cron '@daily',
  audits (assert_no_nulls(column := date))
);

WITH daily_enrollments AS (
  SELECT
    enrolled_at       AS date,
    program_id,
    COUNT(*)          AS new_enrollments
  FROM staging.stg_google_sheets__students
  WHERE
    enrolled_at BETWEEN @start_date AND @end_date
  GROUP BY enrolled_at, program_id
),

daily_ads AS (
  SELECT
    date,
    SUM(cost_usd)               AS total_spend_usd,
    SUM(clicks)                 AS total_clicks,
    SUM(impressions)            AS total_impressions,
    SUM(conversions)            AS total_ad_conversions
  FROM staging.stg_google_ads__performance
  WHERE
    date BETWEEN @start_date AND @end_date
  GROUP BY date
),

daily_sessions AS (
  SELECT
    date,
    SUM(sessions)               AS total_sessions,
    SUM(page_views)             AS total_page_views
  FROM staging.stg_google_analytics__sessions
  WHERE
    session_source IN ('google', 'cpc', 'paid')
    AND date BETWEEN @start_date AND @end_date
  GROUP BY date
)

SELECT
  e.date,
  e.program_id,
  e.new_enrollments,
  COALESCE(a.total_spend_usd, 0)        AS total_spend_usd,
  COALESCE(a.total_clicks, 0)           AS total_clicks,
  COALESCE(a.total_impressions, 0)      AS total_impressions,
  COALESCE(a.total_ad_conversions, 0)   AS total_ad_conversions,
  COALESCE(s.total_sessions, 0)         AS total_paid_sessions,
  COALESCE(s.total_page_views, 0)       AS total_paid_page_views,
  SAFE_DIVIDE(
    COALESCE(a.total_spend_usd, 0),
    NULLIF(e.new_enrollments, 0)
  )                                     AS cost_per_enrollment_usd,
  SAFE_DIVIDE(
    COALESCE(a.total_spend_usd, 0),
    NULLIF(COALESCE(a.total_ad_conversions, 0), 0)
  )                                     AS cost_per_ad_conversion_usd
FROM daily_enrollments AS e
LEFT JOIN daily_ads AS a
  ON e.date = a.date
LEFT JOIN daily_sessions AS s
  ON e.date = s.date
