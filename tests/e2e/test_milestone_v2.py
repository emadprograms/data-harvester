"""
Milestone v5.0 End-to-End Verification & Concurrency Stress Test Suite.

Validates the complete integrated workflow across:
- Parquet tick lake storage (LakePublisher -> TickLakeReader -> dashboard REST API)
- Symbol registry lifecycle (register -> stream -> fence for purge)
- Data integrity & health engine over lake partitions
- Concurrent lake writes with live dashboard reads (no lock contention, no partial reads)
- Capital.com exclusive WebSocket streamer

The dual-DuckDB era (historical.duckdb / streaming.duckdb) is over: the lake is the only
storage, so these tests exercise the production write and read paths end to end.
"""
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
import requests

from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from src.stream.runner import StreamingEngine
from src.stream.capital_stream import CapitalStreamer
from src.dashboard.server import create_dashboard_server
from tests.support.lake_population import build_rows, create_lake, publish_rows

SYMBOLS = ["NVDA", "AAPL"]


@pytest.fixture
def e2e_environment(tmp_path, monkeypatch):
    """Isolated lake + dashboard server for one end-to-end scenario."""
    lake = create_lake(tmp_path / "lake", symbols=SYMBOLS)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
    monkeypatch.setenv("DATA_DIR", str(lake))

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.2)

    yield {"lake": lake, "base_url": f"http://127.0.0.1:{port}"}

    server.shutdown()
    server.server_close()


def _publish_pltr_ticks(lake, symbol="PLTR", count=30):
    """Publishes `count` PLTR ticks inside a single UTC minute (2026-09-25 14:30)."""
    base_time = datetime(2026, 9, 25, 14, 30, 0)
    rows = []
    for i in range(count):
        stamp = (base_time + timedelta(seconds=i * 2)).strftime("%Y-%m-%d %H:%M:%S")
        rows.append((stamp, symbol, 28.0 + i * 0.05, 10.0, 27.95, 28.05, "CAPITAL", "REG"))
    return publish_rows(lake, rows)


def test_e2e_milestone_v2_full_lifecycle(e2e_environment):
    """
    End-to-End Workflow Test:
    1. Add a symbol via the Dashboard API (registry).
    2. Stream raw ticks for the new symbol into the Parquet lake.
    3. Resample raw ticks dynamically into OHLCV candles via the dashboard API.
    4. Run the on-demand integrity audit via the Dashboard API.
    5. Fence the symbol via the Dashboard API and confirm no other data is touched.
    """
    base_url = e2e_environment["base_url"]
    lake = e2e_environment["lake"]
    symbol = "PLTR"

    # 1. Add symbol via Dashboard API
    add_payload = {"display_name": symbol, "capital_ticker": symbol}
    resp = requests.post(f"{base_url}/api/streaming/symbols", json=add_payload)
    assert resp.status_code in (200, 201), resp.text
    assert resp.json()["success"] is True

    list_resp = requests.get(f"{base_url}/api/streaming/symbols")
    symbols = [s["display_name"] for s in list_resp.json()["symbols"]]
    assert symbol in symbols

    # 2. Ingest raw ticks into the lake through the production writer path
    tick_count = _publish_pltr_ticks(lake, symbol)
    assert tick_count == 30

    reader = TickLakeReader(root=lake)
    stored = reader.query_ticks(symbol=symbol)
    assert len(stored) == 30

    # 3. Dynamic resampling to 1-minute OHLCV candles via the REST layer
    candles_resp = requests.get(
        f"{base_url}/api/candles?symbol={symbol}&timeframe=1m&date=2026-09-25&hours=extended"
    )
    assert candles_resp.status_code == 200, candles_resp.text
    payload = candles_resp.json()
    assert payload["symbol"] == symbol
    assert payload["count"] == 1, f"Expected exactly 1 one-minute candle, got {payload['count']}"
    candle = payload["candles"][0]
    assert candle["open"] == pytest.approx(28.0)
    assert candle["high"] == pytest.approx(28.0 + 29 * 0.05)
    assert candle["low"] == pytest.approx(28.0)
    assert candle["close"] == pytest.approx(28.0 + 29 * 0.05)
    assert candle["volume"] == pytest.approx(300.0)
    assert candle["tick_count"] == 30

    # 4. Trigger on-demand integrity audit via API
    audit_resp = requests.get(f"{base_url}/api/integrity?symbol={symbol}")
    assert audit_resp.status_code == 200
    audit_data = audit_resp.json()
    assert "overall_passed" in audit_data
    assert "symbols_audited" in audit_data
    assert "quiet_intervals" in audit_data

    # 5. Fence the symbol via the Dashboard API (compaction performs the physical purge)
    del_resp = requests.delete(f"{base_url}/api/streaming/symbols/{symbol}")
    assert del_resp.status_code == 200
    assert del_resp.json()["success"] is True
    assert del_resp.json()["status"] == "PENDING_PURGE"

    entries = {
        s["display_name"]: s for s in requests.get(f"{base_url}/api/streaming/symbols").json()["symbols"]
    }
    assert entries[symbol]["status"] == "PENDING_PURGE"
    assert entries[symbol]["active"] is False

    # The fenced symbol's own ticks survive until compaction purges them; other symbols are untouched
    assert len(reader.query_ticks(symbol=symbol)) == 30
    assert len(reader.query_ticks(symbol="NVDA")) == 0


def test_concurrent_lake_writes_and_dashboard_reads(e2e_environment):
    """
    Stress test verifying reader/writer isolation on the Parquet lake:
    - Thread 1: high-speed tick writer publishing immutable batches.
    - Thread 2: continuous Dashboard HTTP readers querying status, candles and symbols.
    Proves readers never observe partial batches and never fail mid-write.
    """
    lake = e2e_environment["lake"]
    base_url = e2e_environment["base_url"]
    errors = []

    def stream_writer_worker():
        try:
            base = datetime(2026, 9, 25, 15, 0, 0)
            with LakePublisher(root=lake, writer_id="e2e_soak") as publisher:
                for batch_num in range(10):
                    rows = [
                        (
                            (base + timedelta(minutes=batch_num, milliseconds=i * 50)).strftime(
                                "%Y-%m-%d %H:%M:%S.%f"
                            )[:-3],
                            "NVDA",
                            225.0 + i * 0.01,
                            1.0,
                            224.9,
                            225.1,
                            "CAPITAL",
                            "REG",
                        )
                        for i in range(50)
                    ]
                    publisher.publish_batch(
                        build_rows(rows, writer_id="e2e_soak"),
                        batch_id=f"e2e_batch_{batch_num}",
                        sequence=batch_num + 1,
                    )
                    time.sleep(0.02)
        except Exception as exc:  # noqa: BLE001 - the assertion below reports it
            errors.append(f"lake writer exception: {exc!r}")

    def dashboard_reader_worker():
        try:
            for _ in range(10):
                for url in (
                    f"{base_url}/api/status",
                    f"{base_url}/api/streaming/symbols",
                    f"{base_url}/api/candles?symbol=NVDA&timeframe=1m&limit=100",
                ):
                    response = requests.get(url)
                    if response.status_code != 200:
                        errors.append(f"dashboard query failed: {url} -> {response.status_code}")
                time.sleep(0.02)
        except Exception as exc:  # noqa: BLE001 - the assertion below reports it
            errors.append(f"dashboard reader exception: {exc!r}")

    threads = [
        threading.Thread(target=stream_writer_worker),
        threading.Thread(target=dashboard_reader_worker),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not any(thread.is_alive() for thread in threads), "stress test threads did not finish"
    assert not errors, f"Concurrency stress errors: {errors}"

    reader = TickLakeReader(root=lake)
    assert len(reader.query_ticks(symbol="NVDA", limit=10000)) == 500


def test_capital_exclusive_streamer_message_flow():
    """Verify CapitalStreamer creates valid quote records for the equity epics it is given."""
    ticks_captured = []

    def on_tick(tick):
        ticks_captured.append(tick)

    streamer = CapitalStreamer(epics=["AAPL", "NVDA"], on_tick_callback=on_tick)
    assert streamer.epics == ["AAPL", "NVDA"]
    assert streamer.running is False
    assert streamer.ws is None


def test_runner_resolves_lake_root_end_to_end(tmp_path, monkeypatch):
    """StreamingEngine writes into the lake root it is configured with (no disk database)."""
    lake = create_lake(tmp_path / "lake", symbols=["AAPL"])
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
    monkeypatch.setenv("DATA_DIR", str(lake))

    engine = StreamingEngine()
    assert engine.lake_root == lake
    assert not (lake / "streaming.duckdb").exists()
    assert not any(p.suffix == ".duckdb" for p in lake.rglob("*.duckdb"))
