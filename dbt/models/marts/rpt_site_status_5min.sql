-- Batch reference for the streaming job: 5-minute windows per site on event time.
-- Late events (ingested after window end + watermark) are counted but would be dropped by the stream.
with e as (
    select
        h.*,
        d.site_id,
        time_bucket(interval '5 minutes', h.event_ts) as window_start
    from {{ ref('stg_bronze__heartbeats') }} h
    join {{ ref('stg_bronze__devices') }} d on d.device_id = h.device_id
)

select
    site_id,
    window_start,
    window_start + interval '5 minutes'                                         as window_end,
    count(*)                                                                    as events,
    count(distinct case when status = 'failed' then device_id end)              as failed_devices,
    count(*) filter (where ingested_ts > window_start + interval '5 minutes'
                     + to_minutes(cast({{ var('watermark_minutes') }} as bigint)))    as late_events
from e
group by 1, 2
