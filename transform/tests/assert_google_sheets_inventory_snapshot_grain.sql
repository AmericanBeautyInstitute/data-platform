select sku_id, snapshot_date, count(*) as row_count
from {{ source('raw', 'google_sheets_inventory') }}
group by sku_id, snapshot_date
having count(*) > 1
