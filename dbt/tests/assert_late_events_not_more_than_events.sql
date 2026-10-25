-- A window's late events are a subset of its events.
select * from {{ ref('rpt_site_status_5min') }} where late_events > events
