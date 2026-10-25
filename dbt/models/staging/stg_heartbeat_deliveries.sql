select
    ingest_seq,
    event_id,
    device_id,
    site,
    event_ts,
    event_ts - event_ts % {{ var('window_seconds') }}    as window_start,
    status,
    delivery_delay_s,
    is_redelivery
from {{ source('bronze', 'heartbeat_events') }}
