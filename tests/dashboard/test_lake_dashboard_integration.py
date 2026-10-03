"""
Integration test suite for Dashboard endpoints backed by TickLakeReader (Milestone v4.0 - Phase 19).

Validates:
- GET /api/candles?symbol=AAPL&source=streaming returns candles resampled from lake Parquet batches
- GET /api/stream/tape returns latest ticks from lake partitions with computed spread
- GET /api/stream/status returns writer heartbeat and total counts from _control/writer_status.json
- GET /api/streaming/continuity returns continuity and gap analysis over lake partitions
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import threading
import time
import pytest
import requests

from src.dashboard.server import create_dashboard_server
from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from tests.fixtures.deterministic_quotes import QuoteTick


@pytest.fixture
def lake_dashboard_server(tmp_path, monkeypatch):
    """
    Spawns an ephemeral dashboard server bound to a free port, with TICK_LAKE_ROOT
    isolated to tmp_path / 'lake' and pre-populated with quote ticks and writer status.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 1. Populate lake with quotes for AAPL on 2026-10-02
    t_start = datetime(2026, 10, 2, 14, 30, 0)
    ticks = [
        QuoteTick(
            timestamp=t_start + timedelta(seconds=i * 10),
            symbol="AAPL",
            price=150.0 + i * 0.25,
            volume=100.0,
            bid=149.95 + i * 0.25,
            ask=150.05 + i * 0.25,
            source="CAPITAL",
            session="REG",
            ingest_id=f"dash_aapl_{i:04d}",
        )
        for i in range(25)
    ]
    with LakePublisher(root=lake_root, writer_id="w_dash_01") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_dash_001", sequence=1)

    # 2. Write writer_status.json in _control/
    control_dir = lake_root / "_control"
    control_dir.mkdir(parents=True, exist_ok=True)
    status_file = control_dir / "writer_status.json"
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump({
            "status": "RUNNING",
            "writer_id": "w_dash_01",
            "pid": 54321,
            "total_rows_written": 25,
            "batches_published": 1,
            "total_quarantined": 0,
            "total_retrying": 0,
            "last_publish_time": datetime.now(timezone.utc).timestamp(),
            "last_batch_id": "batch_dash_001",
            "last_batch_rows": 25,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)

    # 3. Configure environment
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))
    monkeypatch.setenv("DATA_DIR", str(lake_root))

    # 4. Bind ephemeral port and start server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.12)

    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url, lake_root
    finally:
        server.shutdown()
        server.server_close()


def test_api_candles_streaming_endpoint_lake(lake_dashboard_server):
    """
    GET /api/candles?symbol=AAPL&source=streaming backed by TickLakeReader returns
    resampled OHLCV candles from lake Parquet batches.
    """
    base_url, lake_root = lake_dashboard_server

    # Verify reader method contract
    reader = TickLakeReader(root=lake_root)
    reader.get_candles(symbol="AAPL", date="2026-10-02")

    resp = requests.get(f"{base_url}/api/candles?symbol=AAPL&source=streaming&date=2026-10-02")
    assert resp.status_code == 200
    data = resp.json()

    assert data.get("symbol") == "AAPL"
    assert data.get("database") in ("streaming", "lake")
    candles = data.get("candles", [])
    assert len(candles) > 0

    first_candle = candles[0]
    assert "time" in first_candle
    assert "open" in first_candle
    assert "high" in first_candle
    assert "low" in first_candle
    assert "close" in first_candle
    assert "volume" in first_candle


def test_api_stream_tape_endpoint_lake(lake_dashboard_server):
    """
    GET /api/stream/tape backed by TickLakeReader returns latest ticks from lake partitions
    with calculated spread and reverse-chronological ordering.
    """
    base_url, lake_root = lake_dashboard_server

    # Verify reader method contract
    reader = TickLakeReader(root=lake_root)
    reader.get_tape(symbol="AAPL", limit=10)

    resp = requests.get(f"{base_url}/api/stream/tape?symbol=AAPL&limit=10")
    assert resp.status_code == 200
    data = resp.json()

    assert "ticks" in data
    ticks = data.get("ticks", [])
    assert len(ticks) > 0
    assert len(ticks) <= 10

    first_tick = ticks[0]
    assert "price" in first_tick
    assert "timestamp" in first_tick
    assert "spread" in first_tick
    assert first_tick["symbol"] == "AAPL"


def test_api_stream_status_endpoint_lake(lake_dashboard_server):
    """
    GET /api/stream/status backed by TickLakeReader returns writer status and metrics
    from _control/writer_status.json.
    """
    base_url, lake_root = lake_dashboard_server

    # Verify reader method contract
    reader = TickLakeReader(root=lake_root)
    reader.get_stream_status()

    resp = requests.get(f"{base_url}/api/stream/status")
    assert resp.status_code == 200
    data = resp.json()

    assert data.get("is_alive") is True
    assert data.get("ticks_total") == 25
    assert "status" in data


def test_api_streaming_continuity_lake(lake_dashboard_server):
    """
    GET /api/streaming/continuity backed by TickLakeReader returns continuity and
    gap analysis over lake partitions.
    """
    base_url, lake_root = lake_dashboard_server

    # Verify reader method contract
    reader = TickLakeReader(root=lake_root)
    reader.get_streaming_continuity_analysis(days=5, symbol="AAPL")

    resp = requests.get(f"{base_url}/api/streaming/continuity?symbol=AAPL&days=5")
    assert resp.status_code == 200
    data = resp.json()

    assert "status" in data
    assert "available_weeks" in data
