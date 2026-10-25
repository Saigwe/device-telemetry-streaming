"""Live view of the stream's output (a Grafana stand-in that needs no Docker). Refreshes every 5 seconds.

Run:  streamlit run app/dashboard.py     (warehouse path: TELEMETRY_DUCKDB_PATH, default warehouse/telemetry.duckdb)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = os.environ.get("TELEMETRY_DUCKDB_PATH", str(ROOT / "warehouse" / "telemetry.duckdb"))

st.set_page_config(page_title="Device Telemetry Stream", layout="wide")
st.title("Device telemetry stream")


def read(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except (duckdb.IOException, duckdb.CatalogException):
        return pd.DataFrame()


@st.fragment(run_every="5s")
def live() -> None:
    runs = read("select summary from stream.run_log order by finished_at desc limit 1")
    if runs.empty:
        st.info("No stream run yet. Run `make stream` (or the Kafka consumer).")
        return
    s = json.loads(runs.iloc[0]["summary"])
    c = st.columns(6)
    c[0].metric("Events received", f"{s['events_received']:,}")
    c[1].metric("Duplicates dropped", f"{s['duplicates_dropped']:,}")
    c[2].metric("Late (side output)", f"{s['late_to_side_output']:,}")
    c[3].metric("Windows published", f"{s['windows_emitted']:,}")
    c[4].metric("Alerts", f"{s['alerts']:,}")
    c[5].metric("Throughput", f"{s['events_per_second']:,}/s" if s.get("events_per_second") else "n/a")

    w = read("select site, to_timestamp(window_start) as window, failed_devices, events "
             "from stream.window_results order by window_start")
    if not w.empty:
        st.subheader("Failed devices per 5-minute window, by site")
        st.line_chart(w.pivot_table(index="window", columns="site", values="failed_devices", aggfunc="sum"))

    a = read("select site, to_timestamp(window_start) as window, failed_devices from stream.alerts "
             "order by window_start desc")
    st.subheader(f"Alerts ({len(a)})")
    st.dataframe(a, use_container_width=True, hide_index=True)

    rec = read("select count(*) as windows, sum(case when unexplained_events <> 0 then 1 else 0 end) as unexplained "
               "from gold.rpt_stream_vs_batch")
    if not rec.empty and rec.iloc[0]["windows"]:
        if rec.iloc[0]["unexplained"] == 0:
            st.success(f"Reconciled to the batch reference: all {int(rec.iloc[0]['windows'])} windows account "
                       "for every event (stream + late = batch), as of the last dbt build.")
        else:
            st.error(f"{int(rec.iloc[0]['unexplained'])} windows don't reconcile to the batch reference.")


live()
