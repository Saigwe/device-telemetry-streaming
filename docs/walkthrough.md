# 3-minute walkthrough: Real-Time Device Telemetry Stream

A timed script for a Loom recording of about 3 minutes (screen plus camera bubble, 1080p). Say it in your own words; the lines are a guide, not a teleprompter.

**Open these tabs before you press record:** README, telemetry/, app/, docs/design_doc.md, GitHub Actions (Kafka job).

| Time | On screen | What to say |
|---|---|---|
| 0:00–0:20 | Portfolio card, then the README title | A daily report tells operations about failing devices a day late. This is the streaming version: 30.7 million real heartbeats at 148k events a second, and every window reconciles to a batch reference. |
| 0:20–0:45 | README intro and the Data section | It's the streaming follow-on to the device-estate reporting I led at a workplace-communications hardware company. The data is the Backblaze fleet, replayed with jitter, late arrivals and duplicates. No employer data is used. |
| 0:45–1:10 | README architecture: Kafka → stream processor → serving table + Iceberg history | Five-minute event-time windows per site, a watermark with 120 seconds of allowed lateness, dedup on event id, and a side output for late events. |
| 1:10–1:35 | `telemetry/` engine and the Streamlit view | Results are effectively once: at-least-once input, dedup and an idempotent upsert keyed by site and window. |
| 1:35–2:00 | The CI job that runs through real Kafka (Redpanda) | In CI it runs end to end through Redpanda with a schema registry, and offsets are committed only after the sink write. |
| 2:00–2:30 | Reconciliation in Numbers | All 540 windows reconcile: stream plus late equals batch, with 0 unexplained. The Kafka run found a real bug: the watermark followed the fastest partition and pushed 59% of events late. Per-partition watermarks cut that to 0.3%. |
| 2:30–2:45 | `docs/design_doc.md`, the options table | The decision I'd defend in an interview: Lateness versus latency: 120 seconds of allowed lateness keeps the median latency at 0.41 seconds while almost nothing arrives late. |
| 2:45–3:00 | Back to the README top | For operations, that means alerts in minutes that can be trusted, and replays that never double-count. The repo, numbers and design doc are linked under the video. |

## Recording checklist
- Close email, chat and notifications. Zoom the browser to 125% so code is readable.
- Run `make fixture-all` once before recording, so CI-style output is fresh if you show it.
- Keep to one take per section; Loom lets you trim. Aim for 2:45 to 3:10.
- Show real numbers on screen as you say them; never estimate on camera.
- Say "synthetic" out loud wherever the README labels data as synthetic.

## After recording
1. Copy the Loom share link.
2. In `README.md`, replace the roadmap line with `- [x] [3-minute Loom walkthrough](<your Loom link>)` and add the link under the title.
3. Add the link to this project's card on saigwe.github.io.
