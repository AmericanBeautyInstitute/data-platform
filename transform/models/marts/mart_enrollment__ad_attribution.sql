with daily_enrollments as (
  select
    enrolled_at as date,
    count(*) as new_enrollments
  from {{ ref('stg_google_sheets__students') }}
  group by enrolled_at
),

daily_ads as (
  select
    date,
    sum(cost_usd) as total_spend_usd,
    sum(clicks) as total_clicks,
    sum(impressions) as total_impressions,
    sum(conversions) as total_ad_conversions
  from {{ ref('stg_google_ads__performance') }}
  group by date
),

daily_sessions as (
  select
    date,
    sum(sessions) as total_sessions,
    sum(page_views) as total_page_views
  from {{ ref('stg_google_analytics__sessions') }}
  where session_source in ('google', 'cpc', 'paid')
  group by date
),

date_spine as (
  select date from daily_enrollments
  union distinct
  select date from daily_ads
  union distinct
  select date from daily_sessions
)

select
  d.date,
  coalesce(e.new_enrollments, 0) as new_enrollments,
  coalesce(a.total_spend_usd, 0) as total_spend_usd,
  coalesce(a.total_clicks, 0) as total_clicks,
  coalesce(a.total_impressions, 0) as total_impressions,
  coalesce(a.total_ad_conversions, 0) as total_ad_conversions,
  coalesce(s.total_sessions, 0) as total_paid_sessions,
  coalesce(s.total_page_views, 0) as total_paid_page_views,
  safe_divide(
    coalesce(a.total_spend_usd, 0),
    nullif(coalesce(e.new_enrollments, 0), 0)
  ) as cost_per_enrollment_usd,
  safe_divide(
    coalesce(a.total_spend_usd, 0),
    nullif(coalesce(a.total_ad_conversions, 0), 0)
  ) as cost_per_ad_conversion_usd
from date_spine as d
left join daily_enrollments as e
  on d.date = e.date
left join daily_ads as a
  on d.date = a.date
left join daily_sessions as s
  on d.date = s.date
