-- Retains the merge-keyed daily history at (sku_id, snapshot_date) grain.
with source as (
    select * from {{ source('raw', 'google_sheets_inventory') }}
),

renamed as (
    select
        cast(sku_id as string) as sku_id,
        cast(sku_name as string) as sku_name,
        cast(program_id as string) as program_id,
        cast(quantity_on_hand as int64) as quantity_on_hand,
        cast(reorder_threshold as int64) as reorder_threshold,
        cast(reorder_quantity as int64) as reorder_quantity,
        cast(unit_cost_usd as numeric) as unit_cost_usd,
        cast(units_per_student as numeric) as units_per_student,
        cast(snapshot_date as date) as snapshot_date
    from source
)

select * from renamed
