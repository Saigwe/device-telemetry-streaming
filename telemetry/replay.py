"""Turn drive-days into a heartbeat stream with realistic delivery problems, as `bronze.heartbeat_events`.

Each source row (one drive on one day) becomes one heartbeat. Time is compressed: Backblaze day d
maps to the 5-minute window starting `sim_start + d x 5 min`, so one stream window per site equals
one real day per datacenter, and the stream's failure counts can be checked against the source.

Delivery is made messy on purpose, deterministically (hash-based, no RNG state):
- every event arrives 0-20 s after it happened (normal jitter, so arrival order != event order)
- `late_share` of events arrive 150-600 s late, beyond the 120 s allowed lateness
- `duplicate_share` of events are delivered twice, 1-60 s apart (at-least-once delivery)

The table is the "topic": rows in `ingest_seq` order are what a consumer receives.
"""
from __future__ import annotations

import duckdb

SIM_START = "2026-01-01 00:00:00"


def build_event_log(con: duckdb.DuckDBPyConnection, days: int, late_share: float = 0.01,
                    duplicate_share: float = 0.005) -> int:
    con.execute(f"""
        create or replace table bronze.heartbeat_events as
        with src as (
            select
                serial_number,
                datacenter,
                date,
                failure,
                coalesce(smart_5_raw, 0) + coalesce(smart_197_raw, 0) as warn_count,
                date_diff('day', (select min(date) from bronze.source_drive_days), date) as d,
                (hash(serial_number || cast(date as varchar))       % 1000000) / 1e6 as u1,
                (hash(cast(date as varchar) || serial_number || 'a') % 1000000) / 1e6 as u2,
                (hash(serial_number || 'b' || cast(date as varchar)) % 1000000) / 1e6 as u3
            from bronze.source_drive_days
            where date < (select min(date) from bronze.source_drive_days) + interval {int(days)} day
        ),
        events as (
            select
                serial_number || ':' || cast(date as varchar)                         as event_id,
                serial_number                                                          as device_id,
                datacenter                                                             as site,
                epoch(timestamp '{SIM_START}') + d * 300 + cast(floor(u1 * 300) as integer) as event_ts,
                case when failure = 1 then 'failed' when warn_count > 0 then 'degraded' else 'ok' end as status,
                case when u2 < {late_share} then 150 + u3 * 450 else u3 * 20 end      as delay_s,
                false                                                                  as is_redelivery,
                u2, u3
            from src
        ),
        deliveries as (
            select event_id, device_id, site, event_ts, status, delay_s, is_redelivery from events
            union all
            select event_id, device_id, site, event_ts, status, delay_s + 1 + u2 * 59, true
            from events
            where u3 < {duplicate_share}
        )
        select
            row_number() over (order by event_ts + delay_s, event_id, is_redelivery) as ingest_seq,
            event_id, device_id, site, event_ts, status,
            round(delay_s, 3)                                                       as delivery_delay_s,
            event_ts + delay_s                                                      as ingest_ts,
            is_redelivery
        from deliveries
    """)
    return con.execute("select count(*) from bronze.heartbeat_events").fetchone()[0]


def iter_events(con: duckdb.DuckDBPyConnection, batch_rows: int = 500_000):
    """Yield (ingest_seq, event_id, device_id, site, event_ts, status, ingest_ts) in arrival order.
    Reads through its own cursor, so sink writes on `con` mid-stream don't invalidate the read."""
    cur = con.cursor().execute("""select ingest_seq, event_id, device_id, site, event_ts, status, ingest_ts
                         from bronze.heartbeat_events order by ingest_seq""")
    while True:
        batch = cur.fetchmany(batch_rows)
        if not batch:
            return
        yield from batch
