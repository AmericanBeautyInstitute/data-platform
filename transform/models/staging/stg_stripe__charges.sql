with source as (
    select * from {{ source('raw', 'stripe_charges') }}
),

renamed as (
    select
        charge_id,
        charge_date,
        gross_amount_usd,
        amount_captured_usd,
        fee_usd,
        net_usd,
        currency,
        status,
        description,
        customer_email,
        customer_name,
        payment_intent_id
    from source
)

select * from renamed
