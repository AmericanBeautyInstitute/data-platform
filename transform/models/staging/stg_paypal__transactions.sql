-- fee_amount_usd is a positive expense for every provider.
with source as (
    select * from {{ source('raw', 'paypal_transactions') }}
),

conformed as (
    select
        transaction_id as payment_id,
        paypal_reference_id,
        transaction_date as payment_date,
        gross_amount_usd,
        -fee_amount_usd as fee_amount_usd,
        net_amount_usd,
        upper(currency_code) as currency_code,
        transaction_status as source_status,
        case
            when transaction_status = 'S' then true
            else false
        end as is_successful,
        nullif(trim(transaction_subject), '') as transaction_subject,
        payer_email,
        payer_name,
        'paypal' as payment_source
    from source
)

select * from conformed
