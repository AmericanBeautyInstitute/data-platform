-- fee_amount_usd is a positive expense for every provider.
with source as (
    select * from {{ source('raw', 'stripe_charges') }}
),

conformed as (
    select
        charge_id as payment_id,
        charge_date as payment_date,
        gross_amount_usd,
        fee_usd as fee_amount_usd,
        net_usd as net_amount_usd,
        upper(currency) as currency_code,
        status as source_status,
        case
            when status = 'succeeded' then true
            else false
        end as is_successful,
        nullif(trim(description), '') as transaction_subject,
        customer_email,
        customer_name,
        payment_intent_id,
        'stripe' as payment_source
    from source
)

select * from conformed
