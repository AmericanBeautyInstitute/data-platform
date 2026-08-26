select program_id, snapshot_date, count(*) as row_count
from {{ source('raw', 'google_sheets_programs') }}
group by program_id, snapshot_date
having count(*) > 1
