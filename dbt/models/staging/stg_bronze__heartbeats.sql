-- The source delivers at least once, so keep one row per event_id (the first one ingested).
select
    event_id,
    device_id,
    event_ts,
    status,
    battery_pct,
    ingested_ts
from {{ source('bronze', 'heartbeats') }}
qualify row_number() over (partition by event_id order by ingested_ts) = 1
