"""Run the stream processor over the heartbeat stream and write results to the sinks.

Sources
- replay : read `bronze.heartbeat_events` in arrival order (no broker; used for the full real-data run)
- kafka  : consume the topic produced by scripts/produce_kafka.py (used in CI against Redpanda)

Both feed the same WindowAggregator. Results go to the DuckDB serving tables (idempotent upsert) and
an Iceberg history table, in micro-batches. With Kafka, offsets are committed only after the sink
write, so a crash replays input rather than losing it (at-least-once in, effectively-once out).

Usage
    python scripts/run_stream.py --db warehouse/telemetry.duckdb --source replay
    python scripts/run_stream.py --db warehouse/fixture.duckdb --source kafka --bootstrap localhost:9092
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from telemetry.processor import WindowAggregator  # noqa: E402
from telemetry.replay import iter_events  # noqa: E402
from telemetry.sinks import DuckDBSink, IcebergSink  # noqa: E402

MICRO_BATCH = 50_000


def kafka_events(consumer, idle_s: float, start_timeout_s: float = 120.0):
    """Yield events from the topic until it has been idle for `idle_s` (bounded run for CI)."""
    last = time.time()
    started = False
    seq = 0
    while time.time() - last < (idle_s if started else start_timeout_s):
        msgs = consumer.consume(num_messages=5000, timeout=1.0)
        if not msgs:
            continue
        started = True
        last = time.time()
        for m in msgs:
            if m.error():
                raise RuntimeError(m.error())
            e = json.loads(m.value())
            seq += 1
            yield (seq, e["event_id"], e["device_id"], e["site"], e["event_ts"], e["status"], e["produced_at"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--source", choices=["replay", "kafka"], default="replay")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="device.heartbeats")
    ap.add_argument("--idle-seconds", type=float, default=10.0)
    ap.add_argument("--alert-threshold", type=int, default=6)
    ap.add_argument("--iceberg-dir", default=None)
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:12]
    con = duckdb.connect(args.db)
    sink = DuckDBSink(con, args.source)
    sink.reset()
    iceberg = IcebergSink(Path(args.iceberg_dir or Path(args.db).with_suffix("")) / "iceberg")
    agg = WindowAggregator(alert_threshold=args.alert_threshold)

    pending_results, pending_alerts = [], []
    late_written = 0
    e2e_latency: list[float] = []
    consumer = None
    if args.source == "kafka":
        from confluent_kafka import Consumer

        consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"telemetry-{run_id}",
                             "auto.offset.reset": "earliest", "enable.auto.commit": False})
        consumer.subscribe([args.topic])

    def flush_batch():
        nonlocal pending_results, pending_alerts, late_written
        new_late = agg.late_events[late_written:]
        sink.write(pending_results, pending_alerts, new_late)
        iceberg.append(pending_results, run_id)
        late_written = len(agg.late_events)
        pending_results, pending_alerts = [], []
        if consumer is not None:            # commit offsets only after results are durable
            try:
                consumer.commit(asynchronous=False)
            except Exception as exc:        # nothing consumed since the last commit
                if "NO_OFFSET" not in str(exc):
                    raise

    events = iter_events(con) if args.source == "replay" else kafka_events(consumer, args.idle_seconds)
    t0 = time.perf_counter()
    n = 0
    for seq, event_id, device_id, site, event_ts, status, arrived in events:
        closed = agg.process(event_id, device_id, site, int(event_ts), status)
        if closed:
            pending_results.extend(closed)
            pending_alerts.extend(r for r in closed if r.failed_devices >= agg.alert_threshold)
            if args.source == "kafka":
                now = time.time()
                e2e_latency.extend(now - arrived for _ in closed)
        n += 1
        if n % MICRO_BATCH == 0:
            flush_batch()
    closed = agg.flush()
    pending_results.extend(closed)
    pending_alerts.extend(r for r in closed if r.failed_devices >= agg.alert_threshold)
    flush_batch()
    elapsed = time.perf_counter() - t0
    if consumer is not None:
        consumer.close()

    s = agg.stats
    summary = {
        "run_id": run_id, "source": args.source, "events_received": s.received, "counted": s.counted,
        "duplicates_dropped": s.duplicates, "late_to_side_output": s.late, "windows_emitted": s.windows_emitted,
        "alerts": s.alerts, "max_open_windows": s.max_open_windows, "max_dedup_ids": s.max_dedup_ids,
        "seconds": round(elapsed, 1), "events_per_second": round(s.received / elapsed) if elapsed else None,
        "iceberg_snapshots": iceberg.snapshot_count(),
    }
    if e2e_latency:
        lat = sorted(e2e_latency)
        summary["latency_p50_s"] = round(lat[len(lat) // 2], 3)
        summary["latency_p95_s"] = round(lat[int(len(lat) * 0.95)], 3)
    con.execute("create table if not exists stream.run_log (run_id varchar, summary json, finished_at timestamp)")
    con.execute("insert into stream.run_log values (?, ?, now())", [run_id, json.dumps(summary)])
    con.close()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
