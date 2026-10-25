-- Batch reference: the "true" answer with all data present. Duplicates removed (first delivery wins),
-- every event counted in its event-time window regardless of how late it arrived.
with events as (
    select *
    from {{ ref('stg_heartbeat_deliveries') }}
    qualify row_number() over (partition by event_id order by ingest_seq) = 1
)

select
    site,
    window_start,
    count(*)                                                        as events,
    count(distinct device_id)                                       as devices,
    count(distinct case when status = 'failed' then device_id end)  as failed_devices,
    count(distinct case when status = 'degraded' then device_id end) as degraded_devices
from events
group by 1, 2
