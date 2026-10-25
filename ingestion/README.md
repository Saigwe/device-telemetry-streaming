# Ingestion

There's no separate loader here; the stream's input is built by `scripts/prepare_source.py`:

1. **Copy the drive-days to replay** into `bronze.source_drive_days`. They come from project 5's warehouse ([device-fleet-reliability](https://github.com/Saigwe/device-fleet-reliability), Backblaze Drive Stats); `--fixture` makes synthetic ones for CI instead.
2. **Turn them into deliveries** in `bronze.heartbeat_events`, in arrival order, using `telemetry/replay.py`. Delivery is deliberately messy: jitter, late arrivals and redeliveries.

`scripts/produce_kafka.py` publishes those deliveries to Kafka; the `replay` source reads them directly.
