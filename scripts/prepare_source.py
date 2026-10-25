"""Build `bronze.source_drive_days` (the drive-days the stream replays) and the heartbeat event log.

Real data: copy from project 5's warehouse (Backblaze Drive Stats, loaded by device-fleet-reliability):
    python scripts/prepare_source.py --db warehouse/telemetry.duckdb --fleet-db ../device-fleet-reliability/warehouse/fleet.duckdb --days 90
CI fixture: synthetic drive-days in the same shape:
    python scripts/prepare_source.py --db warehouse/fixture.duckdb --fixture --days 10
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry.replay import build_event_log  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--fleet-db", help="DuckDB file with bronze.drive_days (Backblaze)")
    src.add_argument("--fixture", action="store_true")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--fixture-drives", type=int, default=4000)
    args = ap.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(args.db) as con:
        con.execute("create schema if not exists bronze")
        if args.fleet_db:
            con.execute(f"attach '{Path(args.fleet_db).as_posix()}' as fleet (read_only)")
            con.execute(f"""
                create or replace table bronze.source_drive_days as
                select date, serial_number, datacenter, failure, smart_5_raw, smart_197_raw
                from fleet.bronze.drive_days
                where date < (select min(date) from fleet.bronze.drive_days) + interval {args.days} day""")
            con.execute("detach fleet")
        else:
            con.execute(f"""
                create or replace table bronze.source_drive_days as
                with drives as (
                    select 'FX' || lpad(cast(i as varchar), 6, '0') as serial_number,
                           ['sac1', 'phx1', 'ams5'][1 + i % 3] as datacenter
                    from range({args.fixture_drives}) t(i)
                ),
                days as (select date '2026-01-01' + cast(d as integer) as date from range({args.days}) t(d)),
                grid as (
                    select d.serial_number, d.datacenter, days.date,
                           (hash(d.serial_number || cast(days.date as varchar)) % 10000) / 1e4 as u
                    from drives d cross join days
                )
                select date, serial_number, datacenter,
                       case when u < 0.0015 then 1 else 0 end as failure,
                       case when u > 0.99 then 3 else 0 end as smart_5_raw,
                       0 as smart_197_raw
                from grid""")
            # a drive that fails leaves service: drop its later days, as Backblaze does
            con.execute("""
                delete from bronze.source_drive_days s
                using (select serial_number, min(date) as fd from bronze.source_drive_days where failure = 1 group by 1) f
                where s.serial_number = f.serial_number and s.date > f.fd""")
        rows = con.execute("select count(*) from bronze.source_drive_days").fetchone()[0]
        events = build_event_log(con, args.days)
        stats = con.execute("""select count(*) filter (where is_redelivery), count(*) filter (where delivery_delay_s > 120)
                               from bronze.heartbeat_events""").fetchone()
    print(f"bronze.source_drive_days {rows:,} rows -> bronze.heartbeat_events {events:,} deliveries "
          f"({stats[0]:,} redeliveries, {stats[1]:,} delayed > 120 s)")


if __name__ == "__main__":
    main()
