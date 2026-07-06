with source as (
    select * from {{ source('raw', 'google_ads') }}
),

renamed as (
    select
        date,
        customer_id,
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        round(cost_micros / 1000000, 2) as cost_usd,
        conversions,
        safe_divide(
            round(cost_micros / 1000000, 2),
            nullif(conversions, 0)
        ) as cost_per_conversion_usd
    from source
)

select * from renamed
