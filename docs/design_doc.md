# Design doc: Real-Time Device Telemetry Stream

| | |
|---|---|
| **Author** | Stanley Igwe |
| **Status** | Implemented (see README for measured results) |
| **Last updated** | 2026-10-25 |

## 1. Problem and users
- **Users:** fleet operations (alerts within minutes), and reliability analysts (exact daily and weekly numbers).
- **Need:** know within minutes when a site's devices start failing, without waiting for the overnight batch report, and without the live numbers disagreeing with the official report later.

## 2. Requirements
| Type | Requirement |
|---|---|
| Functional | Failed and degraded device counts per site per 5-minute window; an alert at a threshold |
| Correctness | No double counting under redelivery; every event accounted for; live numbers reconcile to the batch reference |
| Latency | A window is published within about 2 minutes of event time ending (the allowed lateness) |
| Scale | 30.7M heartbeats replayed in minutes on a laptop; partitioned by device for horizontal scale |
| Operability | Results written idempotently; a crash or replay must not change published numbers |

## 3. Design
- **Event time, not processing time.** Windows are on when a heartbeat happened. Arrival order differs, because of network jitter, partitions and retries.
- **Watermark** = max event time seen − 120 s. A window is published once the watermark passes its end. A smaller allowance gives faster results but sends more events to the late output; a larger one is slower and more complete.
- **Late events** are never added to a window that has already been published, because that would silently change a number someone may have acted on. They go to a side output (`stream.late_events`), and the batch reference absorbs them.
- **Deduplication** on `event_id`, with state kept for 3 windows after close, so memory stays bounded.
- **Effectively-once output** comes from three pieces:
  - at-least-once input: Kafka offsets are committed only after the sink write
  - deduplication
  - an idempotent sink: an upsert keyed by (site, window_start)

  A crash replays input but rewrites the same rows.
- **Lambda-style check:** dbt computes the batch answer from the full event log. A test enforces stream + late = batch for every window, so the two paths can't drift apart unnoticed.

## 4. Options considered
| Decision | Options | Chosen | Why |
|---|---|---|---|
| Broker | Kafka / Redpanda / Kinesis / Pub/Sub | Kafka API on Redpanda | One binary with a built-in schema registry; Kafka-compatible client code |
| Processing | Flink / Spark Structured Streaming / Python | A small Python engine | The watermark, windows and dedup are explicit and unit-tested; Flink is the next step at scale (it needs a JVM cluster) |
| Late data | Update published windows / drop / side output | Side output + batch reconciliation | Published numbers stay stable, and nothing is lost |
| Delivery semantics | Exactly-once transactions / at-least-once + idempotence | At-least-once + dedup + idempotent upsert | Simpler, works with any sink that can upsert, same observable result |
| History | Iceberg / Delta / Parquet files | Iceberg (pyiceberg) | Snapshots of every micro-batch commit; engine-neutral (Spark, Trino, DuckDB can read it) |
| Live serving | ClickHouse + Grafana / DuckDB + Streamlit | DuckDB + Streamlit here | No Docker on the build machine. The serving table is the same shape ClickHouse would hold |

## 5. Risks and what I'd change at 10× scale
- **One process.** The engine runs single-threaded. At 10× I'd move to Flink (or Kafka Streams) with keyed state per site, or run one consumer per partition with per-partition watermarks merged by min().
- **Watermark from one fast partition** can close windows early for slower partitions. Per-partition watermarks fix this. Today it shows up as more late events in Kafka mode than in the ordered replay.
- **Dedup state in memory.** At scale it goes in RocksDB or Flink state, with a TTL.
- **Alert threshold** is a fixed count. A rate per 1,000 devices on site would compare sites of different sizes fairly.
