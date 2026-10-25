# Ingestion (roadmap)

Loaders for the real data go here and write to the same `bronze` tables as the fixture.

A Python producer replays [Backblaze](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) data or simulated heartbeats at speed, with duplicate and out-of-order events on purpose. CI checks the batch reference implementation on a synthetic fixture with planted duplicates and late events.
