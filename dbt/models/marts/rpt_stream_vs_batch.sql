-- Stream output against the batch reference, per site and window. The stream publishes a window once
-- the watermark passes, so events arriving later go to the late side output instead. Reconciled:
-- batch events = stream events + late events; failed devices may only differ by late failures.
with late as (
    select
        site,
        window_start,
        count(*)                                                        as late_events,
        count(distinct case when status = 'failed' then device_id end)  as late_failed_devices
    from {{ source('stream', 'late_events') }}
    group by 1, 2
)

select
    coalesce(b.site, s.site)                                        as site,
    coalesce(b.window_start, s.window_start)                        as window_start,
    to_timestamp(coalesce(b.window_start, s.window_start))          as window_start_ts,
    coalesce(b.events, 0)                                           as batch_events,
    coalesce(s.events, 0)                                           as stream_events,
    coalesce(l.late_events, 0)                                      as late_events,
    coalesce(b.events, 0) - coalesce(s.events, 0) - coalesce(l.late_events, 0) as unexplained_events,
    coalesce(b.failed_devices, 0)                                   as batch_failed_devices,
    coalesce(s.failed_devices, 0)                                   as stream_failed_devices,
    coalesce(l.late_failed_devices, 0)                              as late_failed_devices,
    s.emitted_at_seq
from {{ ref('rpt_site_status_5min_batch') }} b
full outer join {{ source('stream', 'window_results') }} s
    on s.site = b.site and s.window_start = b.window_start
left join late l
    on l.site = coalesce(b.site, s.site) and l.window_start = coalesce(b.window_start, s.window_start)
