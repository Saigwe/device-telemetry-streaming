select
    device_id,
    site_id
from {{ source('bronze', 'devices') }}
