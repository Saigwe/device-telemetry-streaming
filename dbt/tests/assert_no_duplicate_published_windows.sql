-- Effectively-once output: each site-window is published exactly once in the serving table.
select site, window_start, count(*) as n
from {{ source('stream', 'window_results') }}
group by 1, 2
having count(*) > 1
