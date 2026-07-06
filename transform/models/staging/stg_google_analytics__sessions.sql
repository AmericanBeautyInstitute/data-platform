with source as (
    select * from {{ source('raw', 'google_analytics') }}
),

renamed as (
    select
        date,
        session_source,
        session_medium,
        country,
        sessions,
        screen_page_views as page_views,
        bounce_rate,
        conversions
    from source
)

select * from renamed
