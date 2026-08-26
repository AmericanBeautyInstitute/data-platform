-- Keeps only the most recent snapshot from the merge-keyed daily history.
-- Each snapshot_date is a full copy; programs are a current-state dimension.
with source as (
    select * from {{ source('raw', 'google_sheets_programs') }}
),

renamed as (
    select
        cast(program_id as string) as program_id,
        cast(program_name as string) as program_name,
        cast(program_code as string) as program_code,
        cast(duration_weeks as int64) as duration_weeks,
        cast(max_enrollment as int64) as max_enrollment,
        cast(is_active as bool) as is_active,
        cast(snapshot_date as date) as snapshot_date
    from source
    qualify row_number() over (
        partition by program_id order by snapshot_date desc
    ) = 1
)

select * from renamed
