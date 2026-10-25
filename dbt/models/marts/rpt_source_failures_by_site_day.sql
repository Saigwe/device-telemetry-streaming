-- Failures per datacenter and day straight from the replayed drive-days, mapped to the stream window
-- that represents that day. The stream (plus its late output) must reproduce these exactly.
select
    datacenter                                                      as site,
    cast(epoch(cast('{{ var("sim_start") }}' as timestamp)) as bigint)
        + date_diff('day', (select min(date) from {{ source('bronze', 'source_drive_days') }}), date)
          * {{ var('window_seconds') }}                             as window_start,
    count(*)                                                        as drive_days,
    count(*) filter (where failure = 1)                             as failures
from {{ source('bronze', 'source_drive_days') }}
group by 1, 2
