"""Guard — the chart's missing-data visualisation is RETAINED.

Phase 47 (RMV-02) removes the *historical 1-minute bar archive*: the
Massive/Polygon/Yahoo/Binance harvesters that filled ``minute_data`` in
``historical.duckdb``, plus their CLI and dashboard job.

It does NOT remove the tick-lake gap visualisation. Owner decision 2026-10-05:
the shaded regions that show where ticks are missing, the continuity ribbons
and the quiet-interval analysis are wanted and stay. These tests fail if any
part of that stack is removed by accident.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import socket
import threading
import time

import pytest
import requests

from src.dashboard.server import create_dashboard_server
from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from tests.fixtures.deterministic_quotes import QuoteTick

REPO_ROOT = Path(__file__).resolve().parents[2]
STATIC = REPO_ROOT / "src" / "dashboard" / "static"

LAKE_DATE = "2026-10-02"
# 2026-10-02 is a Friday; 13:30 UTC = 09:30 ET (EDT), inside the regular session.
SESSION_START_UTC = datetime(2026, 10, 2, 13, 30, 0)
GAP_START_UTC = datetime(2026, 10, 2, 13, 36, 0)
GAP_END_UTC = datetime(2026, 10, 2, 13, 55, 0)


def _minute_ticks(start: datetime, minutes: int, offset: int) -> list:
    return [
        QuoteTick(
            timestamp=start + timedelta(minutes=i),
            symbol="AAPL",
            price=220.0 + (offset + i) * 0.05,
            volume=100.0,
            bid=219.95 + (offset + i) * 0.05,
            ask=220.05 + (offset + i) * 0.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"gap_{offset}_{i:04d}",
        )
        for i in range(minutes)
    ]


@pytest.fixture
def gapped_lake_server(tmp_path, monkeypatch):
    """Dashboard server over a lake with a deliberate 19-minute hole in the session."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = _minute_ticks(SESSION_START_UTC, 6, 0) + _minute_ticks(GAP_END_UTC, 6, 100)
    with LakePublisher(root=lake_root, writer_id="w_gap_guard") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_gap_guard", sequence=1)

    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))
    monkeypatch.setenv("DATA_DIR", str(lake_root))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.12)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


def test_gap_shading_plugin_is_defined_and_attached_to_the_streaming_chart() -> None:
    js = (STATIC / "js" / "chart.js").read_text(encoding="utf-8")
    for cls in ("GapShadingRenderer", "GapShadingPaneView", "GapShadingPlugin"):
        assert f"class {cls}" in js, f"{cls} was removed from chart.js"

    assert "streamingCandleSeries.attachPrimitive(gapShadingPlugin)" in js, (
        "the gap shading plugin is no longer attached to the streaming candle series"
    )
    assert "setGaps" in js, "the plugin no longer receives gap data"


def test_candle_payload_carries_the_gaps_that_drive_the_shading(gapped_lake_server) -> None:
    resp = requests.get(
        f"{gapped_lake_server}/api/candles?symbol=AAPL&date={LAKE_DATE}", timeout=15
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("candles"), "no candles returned for the gapped lake"

    gaps = data.get("gaps")
    assert gaps, f"the 19-minute hole produced no gaps: {data.get('summary')}"
    durations = [g["duration"] for g in gaps]
    assert max(durations) >= 19, f"the session hole was not reported: {gaps}"


def test_quiet_interval_analysis_survives_and_reads_the_lake() -> None:
    import src.utils.integrity as integrity

    assert hasattr(integrity, "detect_stream_quiet_intervals"), (
        "detect_stream_quiet_intervals was removed with the bar subsystem; it is "
        "lake-backed and still required"
    )
    source = (REPO_ROOT / "src" / "utils" / "integrity.py").read_text(encoding="utf-8")
    assert "src.database" not in source


def test_continuity_ribbon_markup_survives() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    for element_id in (
        "continuity-ribbon-canvas",
        "continuity-ribbon-view",
        "continuity-incident-summary",
        "continuity-week-select",
        "continuity-extended-toggle",
    ):
        assert f'id="{element_id}"' in html, f"continuity element removed: {element_id}"


def test_continuity_routes_survive() -> None:
    source = (REPO_ROOT / "src" / "dashboard" / "server.py").read_text(encoding="utf-8")
    assert '"/api/streaming/continuity"' in source
    assert '"/api/streaming/continuity/weeks"' in source
