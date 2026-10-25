"""Publish the heartbeat stream to Kafka (Redpanda in CI), in arrival order, keyed by device.

- Creates the topic (3 partitions) if needed and registers the JSON schema with the schema registry.
- Keying by device keeps each device's heartbeats in order within a partition; across partitions the
  consumer sees a different interleaving than the replay, which is exactly what the watermark handles.
- Every message is checked against the schema's required fields before sending.

Usage
    python scripts/produce_kafka.py --db warehouse/fixture.duckdb --bootstrap localhost:9092 --schema-registry http://localhost:8081
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from telemetry.replay import iter_events  # noqa: E402

SCHEMA = json.loads((ROOT / "schemas" / "heartbeat.schema.json").read_text(encoding="utf-8"))


def register_schema(registry: str, subject: str) -> int:
    body = json.dumps({"schemaType": "JSON", "schema": json.dumps(SCHEMA)}).encode()
    req = urllib.request.Request(f"{registry}/subjects/{subject}/versions", data=body, method="POST",
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["id"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="device.heartbeats")
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--schema-registry", default=None)
    ap.add_argument("--create-topic-only", action="store_true",
                    help="create the topic and register the schema, then exit (run before starting consumers)")
    args = ap.parse_args()

    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.topic not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(args.topic, num_partitions=args.partitions, replication_factor=1)])[args.topic].result()
    if args.schema_registry:
        print(f"registered schema id {register_schema(args.schema_registry, args.topic + '-value')}")
    if args.create_topic_only:
        print(f"topic {args.topic} ready")
        return

    required = set(SCHEMA["required"])
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20, "enable.idempotence": True})
    t0 = time.perf_counter()
    n = 0
    with duckdb.connect(args.db, read_only=True) as con:
        for _, event_id, device_id, site, event_ts, status, _ in iter_events(con):
            msg = {"event_id": event_id, "device_id": device_id, "site": site, "event_ts": int(event_ts),
                   "status": status, "produced_at": time.time()}
            assert required <= msg.keys()
            while True:
                try:
                    producer.produce(args.topic, key=device_id, value=json.dumps(msg))
                    break
                except BufferError:
                    producer.poll(0.5)
            n += 1
            if n % 10000 == 0:
                producer.poll(0)
    producer.flush(60)
    print(f"produced {n:,} messages to {args.topic} in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
