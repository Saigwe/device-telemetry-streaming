"""Event-time tumbling windows per site, with a watermark, deduplication and threshold alerts.

The same processor runs whether events come from Kafka or from an in-memory replay; it only sees
(event_id, device_id, site, event_ts, status) in arrival order.

Semantics
- Window: [start, start + window_s) on *event time* (when the heartbeat happened), not arrival time.
- Watermark: max event time seen so far minus `allowed_lateness_s`. A window is closed and emitted
  once the watermark passes its end. Events for an already-closed window are *late*: they are not
  added to the emitted result (that would change a published number); they go to the late output,
  so nothing is silently lost.
- Deduplication: the source delivers at least once. An event id seen before is dropped. Ids are kept
  until `dedup_retention_windows` windows after their window closed, so state stays bounded.
- Alerts: a window whose count of distinct failed devices reaches `alert_threshold` raises an alert.
- Effectively-once: with at-least-once delivery, dedup and an idempotent sink keyed by
  (site, window_start), replaying input after a crash produces the same results.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class WindowResult:
    site: str
    window_start: int           # epoch seconds
    window_end: int
    events: int
    devices: int
    failed_devices: int
    degraded_devices: int
    first_arrival_seq: int
    emitted_at_seq: int         # arrival sequence number at which the window was closed


@dataclass
class _Window:
    events: int = 0
    devices: set = field(default_factory=set)
    failed: set = field(default_factory=set)
    degraded: set = field(default_factory=set)
    first_seq: int = 0


@dataclass
class Stats:
    received: int = 0
    duplicates: int = 0
    late: int = 0
    counted: int = 0
    windows_emitted: int = 0
    alerts: int = 0
    max_open_windows: int = 0
    max_dedup_ids: int = 0


class WindowAggregator:
    def __init__(self, window_s: int = 300, allowed_lateness_s: int = 120, alert_threshold: int = 6,
                 dedup_retention_windows: int = 3):
        self.window_s = window_s
        self.lateness = allowed_lateness_s
        self.alert_threshold = alert_threshold
        self.retention = dedup_retention_windows * window_s
        self.open: dict[tuple[str, int], _Window] = {}
        self.seen: dict[str, int] = {}          # event_id -> event_ts
        self.max_event_ts: int | None = None
        self.closed_before = -1                 # every window starting before this is closed
        self.seq = 0
        self.stats = Stats()
        self.results: list[WindowResult] = []
        self.alerts: list[WindowResult] = []
        self.late_events: list[tuple] = []

    @property
    def watermark(self) -> int | None:
        return None if self.max_event_ts is None else self.max_event_ts - self.lateness

    def process(self, event_id: str, device_id: str, site: str, event_ts: int, status: str) -> list[WindowResult]:
        """Handle one event in arrival order. Returns windows closed by this event (possibly none)."""
        self.seq += 1
        st = self.stats
        st.received += 1
        if event_id in self.seen:
            st.duplicates += 1
            return []
        self.seen[event_id] = event_ts

        start = event_ts - event_ts % self.window_s
        if start <= self.closed_before:
            st.late += 1
            self.late_events.append((event_id, device_id, site, event_ts, status, start, self.seq))
            return []

        w = self.open.get((site, start))
        if w is None:
            w = self.open[(site, start)] = _Window(first_seq=self.seq)
        w.events += 1
        w.devices.add(device_id)
        if status == "failed":
            w.failed.add(device_id)
        elif status == "degraded":
            w.degraded.add(device_id)
        st.counted += 1

        if self.max_event_ts is None or event_ts > self.max_event_ts:
            self.max_event_ts = event_ts
        st.max_open_windows = max(st.max_open_windows, len(self.open))
        return self._close_ready()

    def _close_ready(self, force: bool = False) -> list[WindowResult]:
        wm = self.watermark
        if wm is None:
            return []
        # Highest window start whose end is at or before the watermark.
        ready_before = (wm - self.window_s) - (wm - self.window_s) % self.window_s if not force else 1 << 62
        if not force and ready_before <= self.closed_before:
            return []
        closed = []
        for key in sorted(k for k in self.open if force or k[1] <= ready_before):
            w = self.open.pop(key)
            r = WindowResult(key[0], key[1], key[1] + self.window_s, w.events, len(w.devices),
                             len(w.failed), len(w.degraded), w.first_seq, self.seq)
            closed.append(r)
            self.results.append(r)
            self.stats.windows_emitted += 1
            if r.failed_devices >= self.alert_threshold:
                self.alerts.append(r)
                self.stats.alerts += 1
        if not force:
            self.closed_before = max(self.closed_before, ready_before)
            self._prune(wm)
        return closed

    def _prune(self, wm: int) -> None:
        if len(self.seen) < 200_000:
            self.stats.max_dedup_ids = max(self.stats.max_dedup_ids, len(self.seen))
            return
        cutoff = wm - self.retention
        self.stats.max_dedup_ids = max(self.stats.max_dedup_ids, len(self.seen))
        self.seen = {k: ts for k, ts in self.seen.items() if ts >= cutoff}

    def flush(self) -> list[WindowResult]:
        """End of input: close every open window (a bounded replay has no more events coming)."""
        return self._close_ready(force=True)
