-- Every event is accounted for: in each window, batch events = stream events + late events.
-- Failed devices: the stream can only be short by devices whose failure heartbeat arrived late.
select *
from {{ ref('rpt_stream_vs_batch') }}
where unexplained_events <> 0
   or stream_failed_devices > batch_failed_devices
   or batch_failed_devices > stream_failed_devices + late_failed_devices
