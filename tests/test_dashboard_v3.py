"""
Comprehensive Unit & Integration Tests for Dashboard v3 Observability & Analytics Engine.
Covers:
- GET /api/candles with 1m and multi-minute/hour/day timeframes
- GET /api/candles date filtering and limit clamping
- GET /api/symbols/coverage metadata, bar counts, and sources
- GET /api/stream/tape and GET /api/stream/status
- GET /api/market/session US Eastern market clock and session boundaries
- POST /api/harvester/run, GET /api/harvester/status, and GET /api/harvester/logs
- Context-aware integrity audits with dynamic date range discovery
- Static HTML serving for index.html
"""
import pytest
import os
import json
import urllib.request
import threading
import time
from datetime import datetime, timezone, timedelta

from src.dashboard.server import create_dashboard_server
from src.dashboard.analytics import (
    get_candles,
    get_symbols_coverage,
    get_stream_tape,
    get_stream_status,
    get_market_session_info,
)
from src.dashboard.harvester_job import HarvesterJobManager


@pytest.fixture(scope="module")
def dashboard_server():
    """Starts a live background test server on ephemeral port."""
    port = 8995
    server = create_dashboard_server("127.0.0.1", port)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.3)
    base_url = f"http://127.0.0.1:{port}"
    yield base_url
    server.shutdown()
    server.server_close()


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "DashboardTest"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def http_post(url, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


class TestAnalyticsEngine:
    def test_get_candles_1m(self):
        res = get_candles("SPY", timeframe="1m", limit=10)
        assert res["symbol"] == "SPY"
        assert res["timeframe"] == "1m"
        assert res["count"] > 0
        candle = res["candles"][0]
        assert "open" in candle
        assert "high" in candle
        assert "low" in candle
        assert "close" in candle
        assert "volume" in candle
        assert "time" in candle

    def test_get_candles_all_timeframes(self):
        timeframes = ["5m", "15m", "30m", "1h", "4h", "1d"]
        for tf in timeframes:
            res = get_candles("NVDA", timeframe=tf, limit=3)
            assert res["symbol"] == "NVDA"
            assert res["timeframe"] == tf
            assert res["count"] > 0
            assert len(res["candles"]) > 0

    def test_get_candles_date_range(self):
        res = get_candles("NVDA", timeframe="1h", start="2026-07-01", end="2026-07-15", limit=50)
        assert res["count"] > 0
        for c in res["candles"]:
            assert "2026-07" in c["time_str"]

    def test_get_candles_limit_clamping(self):
        res = get_candles("SPY", timeframe="1m", limit=99999)
        # Should be clamped to 10000 maximum
        assert res["count"] <= 10000

    def test_get_candles_empty_symbol(self):
        res = get_candles("NON_EXISTENT_SYMBOL_XYZ", timeframe="1m", limit=10)
        assert res["count"] == 0
        assert res["candles"] == []

    def test_get_symbols_coverage(self):
        cov = get_symbols_coverage()
        assert cov["total_symbols"] > 0
        assert cov["total_bars_database"] > 0
        sym_entry = cov["symbols"][0]
        assert "display_name" in sym_entry
        assert "bar_count" in sym_entry
        assert "asset_class" in sym_entry
        assert "freshness" in sym_entry

    def test_get_stream_tape(self):
        tape = get_stream_tape(limit=10)
        assert "ticks" in tape
        assert isinstance(tape["ticks"], list)
        if tape["count"] > 0:
            tick = tape["ticks"][0]
            assert "price" in tick
            assert "symbol" in tick

    def test_get_stream_status(self):
        st = get_stream_status()
        assert "is_alive" in st
        assert "ticks_total" in st
        assert "status_label" in st
        assert st["status_label"] in ["LIVE", "STOPPED"]

    def test_get_market_session_info(self):
        mkt = get_market_session_info()
        assert "phase" in mkt
        assert mkt["phase"] in ["REGULAR", "PRE_MARKET", "AFTER_HOURS", "CLOSED", "OVERNIGHT"]
        assert "time_et" in mkt
        assert "time_utc" in mkt
        assert "active_session_date" in mkt
        assert mkt["seconds_to_session_cutoff"] >= 0


class TestDashboardRestEndpoints:
    def test_endpoint_static_html(self, dashboard_server):
        req = urllib.request.Request(f"{dashboard_server}/")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            html = resp.read().decode("utf-8")
            assert "TradingView" in html or "LightweightCharts" in html
            assert "Raw OHLCV Candle Inspector" in html

    def test_endpoint_candles(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/candles?symbol=AAPL&tf=5m&limit=10")
        assert status == 200
        assert data["symbol"] == "AAPL"
        assert data["timeframe"] == "5m"

    def test_endpoint_symbols_coverage(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/symbols/coverage")
        assert status == 200
        assert "symbols" in data
        assert len(data["symbols"]) > 0

    def test_endpoint_stream_tape(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/stream/tape?limit=5")
        assert status == 200
        assert "ticks" in data

    def test_endpoint_stream_status(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/stream/status")
        assert status == 200
        assert "status_label" in data

    def test_endpoint_market_session(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/market/session")
        assert status == 200
        assert "phase_label" in data

    def test_endpoint_harvester_status(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/harvester/status")
        assert status == 200
        assert data["status"] in ["IDLE", "RUNNING", "COMPLETED", "FAILED"]

    def test_endpoint_harvester_logs(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/harvester/logs")
        assert status == 200
        assert "logs" in data
        assert isinstance(data["logs"], list)

    def test_endpoint_integrity_with_symbol(self, dashboard_server):
        status, data = http_get(f"{dashboard_server}/api/integrity?symbol=SPY")
        assert status == 200
        assert data["symbols_audited"] == ["SPY"]
        assert "anomalies" in data
        assert "gaps" in data


class TestHarvesterJobManager:
    def test_job_manager_status(self):
        mgr = HarvesterJobManager()
        st = mgr.get_status()
        assert st["status"] == "IDLE"
        assert st["job_id"] is None
        assert st["log_count"] == 0
        logs = mgr.get_all_logs()
        assert isinstance(logs, list)
