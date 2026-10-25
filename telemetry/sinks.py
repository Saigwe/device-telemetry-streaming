"""Sinks for the stream's output.

- DuckDBSink: the "live" serving tables, upserted by key, so a replay after a crash rewrites the
  same rows instead of adding new ones (idempotent, which is what makes results effectively-once).
- IcebergSink: append-only history of every emitted window in an Apache Iceberg table (pyiceberg,
  local SQL catalog), so the full record of what was published, and when, is kept.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from telemetry.processor import WindowResult

RESULT_COLS = ["site", "window_start", "window_end", "events", "devices", "failed_devices",
               "degraded_devices", "first_arrival_seq", "emitted_at_seq"]


def _rows(results: list[WindowResult]) -> list[tuple]:
    return [tuple(getattr(r, c) for c in RESULT_COLS) for r in results]


class DuckDBSink:
    def __init__(self, con: duckdb.DuckDBPyConnection, run_mode: str):
        self.con = con
        self.mode = run_mode
        con.execute("create schema if not exists stream")
        con.execute("""create table if not exists stream.window_results (
            site varchar, window_start bigint, window_end bigint, events bigint, devices bigint,
            failed_devices bigint, degraded_devices bigint, first_arrival_seq bigint, emitted_at_seq bigint,
            run_mode varchar, primary key (site, window_start))""")
        con.execute("""create table if not exists stream.alerts (
            site varchar, window_start bigint, failed_devices bigint, run_mode varchar,
            primary key (site, window_start))""")
        con.execute("""create table if not exists stream.late_events (
            event_id varchar primary key, device_id varchar, site varchar, event_ts bigint, status varchar,
            window_start bigint, arrival_seq bigint, run_mode varchar)""")

    def reset(self) -> None:
        for t in ("window_results", "alerts", "late_events"):
            self.con.execute(f"delete from stream.{t}")

    def _upsert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        """Bulk upsert through an Arrow table (row-by-row executemany is ~100x slower in DuckDB)."""
        if not rows:
            return
        batch = pa.table({c: list(v) for c, v in zip(columns, zip(*rows))})
        self.con.register("_batch", batch)
        self.con.execute(f"insert or replace into stream.{table} select {', '.join(columns)} from _batch")
        self.con.unregister("_batch")

    def write(self, results: list[WindowResult], alerts: list[WindowResult], late: list[tuple]) -> None:
        self._upsert("window_results", RESULT_COLS + ["run_mode"], [r + (self.mode,) for r in _rows(results)])
        self._upsert("alerts", ["site", "window_start", "failed_devices", "run_mode"],
                     [(a.site, a.window_start, a.failed_devices, self.mode) for a in alerts])
        self._upsert("late_events", ["event_id", "device_id", "site", "event_ts", "status", "window_start",
                                     "arrival_seq", "run_mode"], [e + (self.mode,) for e in late])


class IcebergSink:
    def __init__(self, warehouse_dir: Path, table: str = "telemetry.window_history"):
        from pyiceberg.catalog.sql import SqlCatalog

        warehouse_dir.mkdir(parents=True, exist_ok=True)
        # A plain path, not a file:// URI: pyiceberg turns file:///C:/... into an invalid path on Windows.
        self.catalog = SqlCatalog("local", uri=f"sqlite:///{(warehouse_dir / 'catalog.db').as_posix()}",
                                  warehouse=warehouse_dir.resolve().as_posix())
        ns, name = table.split(".")
        self.catalog.create_namespace_if_not_exists(ns)
        self.schema = pa.schema([("site", pa.string()), ("window_start", pa.int64()), ("window_end", pa.int64()),
                                 ("events", pa.int64()), ("devices", pa.int64()), ("failed_devices", pa.int64()),
                                 ("degraded_devices", pa.int64()), ("first_arrival_seq", pa.int64()),
                                 ("emitted_at_seq", pa.int64()), ("run_id", pa.string())])
        if self.catalog.table_exists(table):
            self.catalog.drop_table(table)
        self.table = self.catalog.create_table(table, schema=self.schema)
        self.commits = 0

    def append(self, results: list[WindowResult], run_id: str) -> None:
        if not results:
            return
        cols = list(zip(*_rows(results)))
        data = {c: list(v) for c, v in zip(RESULT_COLS, cols)}
        data["run_id"] = [run_id] * len(results)
        self.table.append(pa.table(data, schema=self.schema))
        self.commits += 1

    def snapshot_count(self) -> int:
        return len(self.table.refresh().snapshots())
