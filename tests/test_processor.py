import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry.processor import WindowAggregator  # noqa: E402

T0 = 1_767_225_600  # 2026-01-01 00:00 UTC, a window boundary


def agg(**kw):
    return WindowAggregator(window_s=300, allowed_lateness_s=120, alert_threshold=2, **kw)


def test_window_closes_only_after_watermark_passes_its_end():
    a = agg()
    assert a.process("e1", "d1", "s1", T0 + 10, "ok") == []
    assert a.process("e2", "d2", "s1", T0 + 400, "ok") == []     # watermark = T0+280 < T0+300
    out = a.process("e3", "d3", "s1", T0 + 421, "ok")              # watermark = T0+301 >= end
    assert [(r.window_start, r.events) for r in out] == [(T0, 1)]


def test_out_of_order_event_within_lateness_is_counted():
    a = agg()
    a.process("e1", "d1", "s1", T0 + 290, "ok")
    a.process("e2", "d2", "s1", T0 + 350, "ok")     # next window; watermark T0+230
    a.process("e3", "d3", "s1", T0 + 100, "ok")     # older but its window is still open
    a.flush()
    first = [r for r in a.results if r.window_start == T0][0]
    assert first.events == 2 and a.stats.late == 0


def test_event_after_window_closed_goes_to_late_output_not_the_result():
    a = agg()
    a.process("e1", "d1", "s1", T0 + 10, "ok")
    a.process("e2", "d2", "s1", T0 + 500, "ok")     # closes window T0
    a.process("e3", "d3", "s1", T0 + 20, "failed")  # too late
    a.flush()
    first = [r for r in a.results if r.window_start == T0][0]
    assert first.events == 1 and first.failed_devices == 0
    assert a.stats.late == 1 and a.late_events[0][0] == "e3"


def test_duplicates_are_dropped():
    a = agg()
    for _ in range(3):
        a.process("e1", "d1", "s1", T0 + 10, "ok")
    a.flush()
    assert a.results[0].events == 1 and a.stats.duplicates == 2


def test_sites_are_windowed_separately_and_failed_devices_are_distinct():
    a = agg()
    a.process("e1", "d1", "s1", T0 + 1, "failed")
    a.process("e2", "d1", "s1", T0 + 2, "failed")   # same device again: one failed device
    a.process("e3", "d9", "s2", T0 + 3, "degraded")
    a.flush()
    by_site = {r.site: r for r in a.results}
    assert by_site["s1"].failed_devices == 1 and by_site["s1"].events == 2
    assert by_site["s2"].degraded_devices == 1


def test_alert_when_failed_devices_reach_threshold():
    a = agg()
    a.process("e1", "d1", "s1", T0 + 1, "failed")
    a.process("e2", "d2", "s1", T0 + 2, "failed")
    a.process("e3", "d3", "s2", T0 + 3, "failed")
    a.flush()
    assert [(r.site, r.failed_devices) for r in a.alerts] == [("s1", 2)]


def test_replaying_the_same_input_gives_the_same_results():
    events = [("e%d" % i, "d%d" % (i % 7), "s%d" % (i % 3), T0 + (i * 37) % 2000, "ok") for i in range(500)]
    runs = []
    for _ in range(2):
        a = agg()
        for e in events + events[:50]:   # at-least-once: part of the input delivered twice
            a.process(*e)
        a.flush()
        runs.append(sorted((r.site, r.window_start, r.events) for r in a.results))
    assert runs[0] == runs[1]


def test_counted_plus_late_plus_duplicates_equals_received():
    a = agg()
    for i in range(1000):
        a.process("e%d" % (i % 900), "d", "s", T0 + (i * 53) % 3000, "ok")
    a.flush()
    s = a.stats
    assert s.counted + s.late + s.duplicates == s.received
    assert sum(r.events for r in a.results) == s.counted
