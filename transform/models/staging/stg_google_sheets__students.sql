-- Keeps only the most recent snapshot: the sheet is append-loaded, so each
-- snapshot_date is a full copy. Students are a current-state dimension.
with source as (
    select * from {{ source('raw', 'google_sheets_students') }}
),

renamed as (
    select
        cast(student_id as string) as student_id,
        cast(first_name as string) as first_name,
        cast(last_name as string) as last_name,
        cast(email as string) as email,
        cast(phone as string) as phone,
        cast(program_id as string) as program_id,
        cast(enrollment_status as string) as enrollment_status,
        cast(enrolled_at as date) as enrolled_at,
        cast(expected_grad_date as date) as expected_grad_date,
        cast(actual_grad_date as date) as actual_grad_date,
        cast(snapshot_date as date) as snapshot_date
    from source
    qualify row_number() over (
        partition by student_id order by snapshot_date desc
    ) = 1
)

select * from renamed
