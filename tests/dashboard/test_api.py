"""
Comprehensive Integration Tests for Dashboard REST API.
Validates separated database endpoints (historical vs streaming),
symbol inventory, coverage, market sessions, harvester controls, and error handlers.
"""
import socket
import threading
import time
import pytest
import requests

from src.dashboard.server import create_dashboard_server


@pytest.fixture(scope="module")
def api_test_server():
    """Starts an ephemeral ThreadedHTTPServer on an open port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.15)

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.shutdown()
    server.server_close()


def test_api_status(api_test_server):
    """GET /api/status returns overall status, historical health, and streaming health."""
    resp = requests.get(f"{api_test_server}/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "historical" in data
    assert "streaming" in data
    assert "size_mb" in data["historical"]
    assert "ticks_rows" in data["streaming"]


def test_api_symbols(api_test_server):
    """GET /api/symbols returns inventory list and count."""
    resp = requests.get(f"{api_test_server}/api/symbols")
    assert resp.status_code == 200
    data = resp.json()
    assert "symbols" in data
    assert "total" in data
    assert data["total"] == len(data["symbols"])
    names = [s.get("display_name") for s in data["symbols"]]
    assert "NVDA" in names


def test_api_symbols_coverage(api_test_server):
    """GET /api/symbols/coverage returns rich matrix with date spans, freshness, and sources."""
    resp = requests.get(f"{api_test_server}/api/symbols/coverage")
    assert resp.status_code == 200
    data = resp.json()
    assert "symbols" in data
    assert "total_symbols" in data
    assert "total_bars_database" in data
    assert data["total_symbols"] > 0
    assert data["total_bars_database"] > 0

    first = data["symbols"][0]
    assert "display_name" in first
    assert "bar_count" in first
    assert "freshness" in first
    assert "sources_list" in first


def test_api_market_session(api_test_server):
    """GET /api/market/session returns ET clock, session phase, and cutoff countdown."""
    resp = requests.get(f"{api_test_server}/api/market/session")
    assert resp.status_code == 200
    data = resp.json()
    assert "time_et" in data
    assert "phase" in data
    assert "is_regular_open" in data
    assert "active_session_date" in data
    assert "seconds_to_session_cutoff" in data
    assert isinstance(data["is_regular_open"], bool)


def test_api_stream_status(api_test_server):
    """GET /api/stream/status returns live streamer process health and rate."""
    resp = requests.get(f"{api_test_server}/api/stream/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "is_alive" in data
    assert "ticks_total" in data
    assert "ticks_last_minute" in data
    assert isinstance(data["is_alive"], bool)


def test_api_stream_tape(api_test_server):
    """GET /api/stream/tape returns recent quotes with limit parameter support."""
    resp = requests.get(f"{api_test_server}/api/stream/tape?limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert "ticks" in data
    assert isinstance(data["ticks"], list)
    assert len(data["ticks"]) <= 10


def test_api_historical_overview(api_test_server):
    """GET /api/historical/overview returns canonical DuckDB metrics and source breakdown."""
    resp = requests.get(f"{api_test_server}/api/historical/overview")
    assert resp.status_code == 200
    data = resp.json()
    assert "database" in data
    assert data["database"] == "data/historical.duckdb"
    assert "total_rows" in data
    assert "sources" in data
    assert data["total_rows"] > 0


def test_api_candles_historical_source(api_test_server):
    """GET /api/candles with source=historical queries historical.duckdb."""
    resp = requests.get(f"{api_test_server}/api/candles?symbol=NVDA&tf=1m&limit=5&source=historical")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "historical"
    assert data.get("symbol") == "NVDA"
    assert isinstance(data.get("candles"), list)


def test_api_candles_streaming_source(api_test_server):
    """GET /api/candles with source=streaming queries streaming.duckdb."""
    resp = requests.get(f"{api_test_server}/api/candles?symbol=NVDA&tf=1m&limit=5&source=streaming")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("database") == "streaming"
    assert data.get("symbol") == "NVDA"
    assert isinstance(data.get("candles"), list)


def test_api_dedicated_candles_endpoints(api_test_server):
    """GET /api/historical/candles and /api/streaming/candles return segregated data."""
    resp_h = requests.get(f"{api_test_server}/api/historical/candles?symbol=AAPL&tf=1m&limit=5")
    assert resp_h.status_code == 200
    data_h = resp_h.json()
    assert data_h.get("database") == "historical"

    resp_s = requests.get(f"{api_test_server}/api/streaming/candles?symbol=AAPL&tf=1m&limit=5")
    assert resp_s.status_code == 200
    data_s = resp_s.json()
    assert data_s.get("database") == "streaming"


def test_api_harvester_status_and_logs(api_test_server):
    """GET /api/harvester/status and /api/harvester/logs return manager telemetry."""
    resp_st = requests.get(f"{api_test_server}/api/harvester/status")
    assert resp_st.status_code == 200
    data_st = resp_st.json()
    assert "status" in data_st

    resp_lg = requests.get(f"{api_test_server}/api/harvester/logs")
    assert resp_lg.status_code == 200
    data_lg = resp_lg.json()
    assert "logs" in data_lg
    assert isinstance(data_lg["logs"], list)


def test_api_symbols_post_missing_field_400(api_test_server):
    """POST /api/symbols with missing display_name returns 400 Bad Request."""
    resp = requests.post(f"{api_test_server}/api/symbols", json={"capital_ticker": "ABC"})
    assert resp.status_code == 400
    data = resp.json()
    assert data.get("success") is False
    assert "display_name is required" in data.get("error", "")


def test_api_unknown_route_404(api_test_server):
    """GET to an unregistered API endpoint returns 404 with error message."""
    resp = requests.get(f"{api_test_server}/api/does_not_exist")
    assert resp.status_code == 404
    data = resp.json()
    assert data.get("error") == "Not Found"
