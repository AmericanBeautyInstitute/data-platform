select student_id, snapshot_date, count(*) as row_count
from {{ source('raw', 'google_sheets_students') }}
group by student_id, snapshot_date
having count(*) > 1
