with expected_enrollments as (
  select
    enrolled_at as date,
    count(*) as new_enrollments
  from {{ ref('stg_google_sheets__students') }}
  group by enrolled_at
),

expected_ads as (
  select
    date,
    sum(cost_usd) as total_spend_usd,
    sum(clicks) as total_clicks,
    sum(impressions) as total_impressions,
    sum(conversions) as total_ad_conversions
  from {{ ref('stg_google_ads__performance') }}
  group by date
),

expected_sessions as (
  select
    date,
    sum(sessions) as total_paid_sessions,
    sum(page_views) as total_paid_page_views
  from {{ ref('stg_google_analytics__sessions') }}
  where session_source in ('google', 'cpc', 'paid')
  group by date
),

expected_dates as (
  select date from expected_enrollments
  union distinct
  select date from expected_ads
  union distinct
  select date from expected_sessions
),

expected as (
  select
    d.date,
    coalesce(e.new_enrollments, 0) as new_enrollments,
    coalesce(a.total_spend_usd, 0) as total_spend_usd,
    coalesce(a.total_clicks, 0) as total_clicks,
    coalesce(a.total_impressions, 0) as total_impressions,
    coalesce(a.total_ad_conversions, 0) as total_ad_conversions,
    coalesce(s.total_paid_sessions, 0) as total_paid_sessions,
    coalesce(s.total_paid_page_views, 0) as total_paid_page_views
  from expected_dates as d
  left join expected_enrollments as e
    on d.date = e.date
  left join expected_ads as a
    on d.date = a.date
  left join expected_sessions as s
    on d.date = s.date
),

actual as (
  select
    date,
    new_enrollments,
    total_spend_usd,
    total_clicks,
    total_impressions,
    total_ad_conversions,
    total_paid_sessions,
    total_paid_page_views
  from {{ ref('mart_enrollment__ad_attribution') }}
)

select
  coalesce(e.date, a.date) as date
from expected as e
full outer join actual as a
  on e.date = a.date
where
  e.date is null
  or a.date is null
  or e.new_enrollments != a.new_enrollments
  or abs(e.total_spend_usd - a.total_spend_usd) > 0.01
  or e.total_clicks != a.total_clicks
  or e.total_impressions != a.total_impressions
  or e.total_ad_conversions != a.total_ad_conversions
  or e.total_paid_sessions != a.total_paid_sessions
  or e.total_paid_page_views != a.total_paid_page_views
