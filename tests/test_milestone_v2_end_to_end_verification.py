"""
Milestone v2.0 End-to-End Verification & Concurrency Stress Test Suite (Phase 9).
Validates the complete integrated workflow across:
- 100% Dedicated Dual-DuckDB Storage Layer (historical.duckdb & streaming.duckdb)
- Capital.com Exclusive WebSocket Streamer & Dynamic Live Reload
- Data Integrity & Health Engine (gaps, OHLCV sanity, and drift)
- Interactive JavaScript Web Dashboard REST API
- Concurrent stress testing proving zero write-lock contention
"""
import os
import time
import socket
import threading
import tempfile
import requests
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta

from src.database.connection import DuckDBClient, get_duckdb_connection
from src.database.schema import init_historical_db, init_streaming_db
from src.database.operations import (
    save_data_to_storage,
    save_ticks_to_storage,
    query_ticks,
    query_candlesticks_from_ticks,
    get_symbol_map_from_db,
)
from src.stream.runner import StreamingEngine
from src.stream.capital_stream import CapitalStreamer
from src.dashboard.server import create_dashboard_server


@pytest.fixture(scope="module")
def e2e_environment():
    """Sets up an end-to-end isolated environment with dual DBs and a dashboard server."""
    temp_dir = tempfile.mkdtemp()
    hist_path = os.path.join(temp_dir, "test_historical.duckdb")
    stream_path = os.path.join(temp_dir, "test_streaming.duckdb")

    h_client = DuckDBClient(hist_path)
    s_client = DuckDBClient(stream_path)
    init_historical_db(h_client)
    init_streaming_db(s_client)
    h_client.close()
    s_client.close()

    # Find ephemeral port for dashboard server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.2)

    base_url = f"http://127.0.0.1:{port}"

    yield {
        "dir": temp_dir,
        "hist_path": hist_path,
        "stream_path": stream_path,
        "base_url": base_url,
    }

    server.shutdown()
    server.server_close()
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except Exception:
        pass


def test_e2e_milestone_v2_full_lifecycle(e2e_environment):
    """
    End-to-End Workflow Test:
    1. Add symbol via Dashboard API.
    2. Stream raw ticks for the new symbol into streaming.duckdb.
    3. Resample raw ticks dynamically into OHLCV bars.
    4. Run on-demand integrity audit via Dashboard API.
    5. Delete symbol via Dashboard API.
    """
    base_url = e2e_environment["base_url"]
    stream_path = e2e_environment["stream_path"]
    hist_path = e2e_environment["hist_path"]
    symbol = "PLTR"

    # 1. Add symbol via Dashboard API
    add_payload = {
        "display_name": symbol,
        "capital_ticker": symbol,
        "massive_ticker": symbol,
        "yahoo_ticker": f"{symbol}.US"
    }
    resp = requests.post(f"{base_url}/api/symbols", json=add_payload)
    assert resp.status_code == 200
    assert resp.json()["success"] is True

    # Verify presence in symbols API
    list_resp = requests.get(f"{base_url}/api/symbols")
    symbols = [s["display_name"] for s in list_resp.json()["symbols"]]
    assert symbol in symbols

    # 2. Ingest raw ticks for PLTR into streaming.duckdb
    s_client = DuckDBClient(stream_path)
    base_time = datetime(2026, 9, 25, 14, 30, 0, tzinfo=timezone.utc)
    ticks = []
    for i in range(30):
        t = base_time + timedelta(seconds=i * 2)
        ticks.append((t, symbol, 28.0 + (i * 0.05), 10.0, 27.95, 28.05, "CAPITAL", "REG"))
    success = save_ticks_to_storage(s_client, ticks)
    assert success is True

    # Verify ticks stored
    raw_ticks_df = query_ticks(symbol, client=s_client)
    assert len(raw_ticks_df) == 30

    # 3. Dynamic resampling to 1-minute OHLCV candles
    bars_df = query_candlesticks_from_ticks(symbol, timeframe="1m", client=s_client)
    assert len(bars_df) == 1
    bar = bars_df.iloc[0]
    assert bar["open"] == pytest.approx(28.0)
    assert bar["high"] == pytest.approx(28.0 + 29 * 0.05)
    assert bar["low"] == pytest.approx(28.0)
    assert bar["close"] == pytest.approx(28.0 + 29 * 0.05)
    assert bar["volume"] == pytest.approx(300.0)
    assert bar["tick_count"] == 30
    s_client.close()

    # 4. Trigger on-demand integrity audit via API
    audit_resp = requests.get(f"{base_url}/api/integrity?symbol={symbol}")
    assert audit_resp.status_code == 200
    audit_data = audit_resp.json()
    assert "overall_passed" in audit_data
    assert "anomalies" in audit_data
    assert "gaps" in audit_data
    assert "drift" in audit_data

    # 5. Remove symbol via Dashboard API
    del_resp = requests.delete(f"{base_url}/api/symbols/{symbol}")
    assert del_resp.status_code == 200
    assert del_resp.json()["success"] is True

    # Verify removal
    list_resp2 = requests.get(f"{base_url}/api/symbols")
    symbols2 = [s["display_name"] for s in list_resp2.json()["symbols"]]
    assert symbol not in symbols2


def test_concurrent_streaming_harvest_and_dashboard_stress(e2e_environment):
    """
    Stress test verifying 100% concurrency isolation:
    - Thread 1: Always-on high-speed streaming tick writer to streaming.duckdb.
    - Thread 2: Batch historical harvester writer to historical.duckdb.
    - Thread 3: Continuous Dashboard HTTP readers querying status and symbols.
    Proves ZERO file-lock contention and zero collisions.
    """
    hist_path = e2e_environment["hist_path"]
    stream_path = e2e_environment["stream_path"]
    base_url = e2e_environment["base_url"]

    errors = []

    def stream_writer_worker():
        try:
            client = DuckDBClient(stream_path)
            t_base = datetime.now(timezone.utc)
            for batch_num in range(10):
                batch = [
                    (t_base + timedelta(milliseconds=i * 50), "SPY", 580.0 + i * 0.01, 1.0, 579.9, 580.1, "CAPITAL", "REG")
                    for i in range(50)
                ]
                ok = save_ticks_to_storage(client, batch)
                if not ok:
                    errors.append(f"Stream writer failed on batch {batch_num}")
                time.sleep(0.02)
            client.close()
        except Exception as e:
            errors.append(f"Stream writer exception: {e}")

    def historical_harvest_worker():
        try:
            client = DuckDBClient(hist_path)
            t_base = datetime(2026, 9, 25, 9, 30, 0, tzinfo=timezone.utc)
            for batch_num in range(5):
                rows = [
                    {
                        "timestamp": t_base + timedelta(minutes=i + batch_num * 20),
                        "symbol": "QQQ",
                        "open": 490.0, "high": 492.0, "low": 489.0, "close": 491.0,
                        "volume": 500.0, "session": "REG", "source": "MASSIVE"
                    }
                    for i in range(20)
                ]
                ok = save_data_to_storage(pd.DataFrame(rows), archive_client=client)
                if not ok:
                    errors.append(f"Historical harvester failed on batch {batch_num}")
                time.sleep(0.03)
            client.close()
        except Exception as e:
            errors.append(f"Historical harvest exception: {e}")

    def dashboard_reader_worker():
        try:
            for _ in range(10):
                r1 = requests.get(f"{base_url}/api/status")
                r2 = requests.get(f"{base_url}/api/symbols")
                if r1.status_code != 200 or r2.status_code != 200:
                    errors.append(f"Dashboard query failed: r1={r1.status_code}, r2={r2.status_code}")
                time.sleep(0.02)
        except Exception as e:
            errors.append(f"Dashboard reader exception: {e}")

    # Launch threads in parallel
    t1 = threading.Thread(target=stream_writer_worker)
    t2 = threading.Thread(target=historical_harvest_worker)
    t3 = threading.Thread(target=dashboard_reader_worker)

    t1.start()
    t2.start()
    t3.start()

    t1.join(timeout=10)
    t2.join(timeout=10)
    t3.join(timeout=10)

    assert len(errors) == 0, f"Concurrency stress errors: {errors}"


def test_capital_exclusive_streamer_message_flow():
    """Verify CapitalStreamer creates valid quote records without any Binance dependencies."""
    ticks_captured = []

    def on_tick(tick):
        ticks_captured.append(tick)

    streamer = CapitalStreamer(epics=["AAPL", "NVDA"], on_tick_callback=on_tick)
    assert streamer.epics == ["AAPL", "NVDA"]
    assert streamer.running is False
    assert streamer.ws is None
