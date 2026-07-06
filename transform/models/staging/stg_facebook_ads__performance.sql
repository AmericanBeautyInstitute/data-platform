with source as (
    select * from {{ source('raw', 'facebook_ads') }}
),

renamed as (
    select
        date,
        campaign_id,
        campaign_name,
        impressions,
        clicks,
        spend_usd,
        reach,
        frequency,
        link_clicks,
        leads,
        conversions
    from source
)

select * from renamed
