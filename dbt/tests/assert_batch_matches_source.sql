-- End-to-end check against the source: after deduplication, each window holds exactly one heartbeat per
-- drive-day, and its failed devices equal that datacenter-day's failures in the replayed data.
select coalesce(s.site, b.site) as site, coalesce(s.window_start, b.window_start) as window_start,
       s.drive_days, b.events, s.failures, b.failed_devices
from {{ ref('rpt_source_failures_by_site_day') }} s
full outer join {{ ref('rpt_site_status_5min_batch') }} b
    on b.site = s.site and b.window_start = s.window_start
where coalesce(s.drive_days, -1) <> coalesce(b.events, -1)
   or coalesce(s.failures, -1) <> coalesce(b.failed_devices, -1)
