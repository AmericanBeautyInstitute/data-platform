with source as (
    select * from {{ source('raw', 'paypal_transactions') }}
),

renamed as (
    select
        transaction_id,
        transaction_date,
        gross_amount_usd,
        currency_code,
        transaction_status,
        transaction_subject,
        payer_email,
        payer_name,
        fee_amount_usd,
        net_amount_usd
    from source
)

select * from renamed
