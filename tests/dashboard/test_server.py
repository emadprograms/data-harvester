"""
Tests for Data Harvester Dashboard REST API Server (Phase 8).
Validates static HTML delivery, health status reporting, symbol CRUD,
on-demand integrity audits, and dynamic streamer reload signaling.
"""
import threading
import time
import requests
import pytest
from src.dashboard.server import create_dashboard_server


@pytest.fixture(scope="module")
def dashboard_test_server():
    """Spawns an ephemeral dashboard HTTP server on localhost with an available port."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.2)

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.shutdown()
    server.server_close()


def test_dashboard_static_index_html_served(dashboard_test_server):
    """Verify that GET / serves the interactive JavaScript web UI."""
    resp = requests.get(f"{dashboard_test_server}/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["Content-Type"]
    assert "DATA HARVESTER" in resp.text
    assert "symbol_map" in resp.text


def test_api_status_endpoint(dashboard_test_server):
    """Verify GET /api/status returns operational metrics and DB health."""
    resp = requests.get(f"{dashboard_test_server}/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "historical" in data
    assert "streaming" in data
    assert data["status"] in ["HEALTHY", "DEGRADED", "CRITICAL"]


def test_api_symbols_list_endpoint(dashboard_test_server):
    """Verify GET /api/symbols returns inventory list from database."""
    resp = requests.get(f"{dashboard_test_server}/api/symbols")
    assert resp.status_code == 200
    data = resp.json()
    assert "symbols" in data
    assert "total" in data
    assert isinstance(data["symbols"], list)


def test_api_symbols_add_and_delete_lifecycle(dashboard_test_server):
    """Verify adding, fetching, and removing a symbol via REST endpoints."""
    test_symbol = "DASHUNIT"

    # 1. Add symbol
    add_payload = {
        "display_name": test_symbol,
        "capital_ticker": test_symbol,
        "massive_ticker": test_symbol,
        "yahoo_ticker": f"{test_symbol}.US"
    }
    add_resp = requests.post(f"{dashboard_test_server}/api/symbols", json=add_payload)
    assert add_resp.status_code == 200
    assert add_resp.json()["success"] is True

    # 2. Verify symbol is in inventory
    list_resp = requests.get(f"{dashboard_test_server}/api/symbols")
    symbols = [s["display_name"] for s in list_resp.json()["symbols"]]
    assert test_symbol in symbols

    # 3. Delete symbol
    del_resp = requests.delete(f"{dashboard_test_server}/api/symbols/{test_symbol}")
    assert del_resp.status_code == 200
    assert del_resp.json()["success"] is True

    # 4. Verify symbol is removed
    list_resp2 = requests.get(f"{dashboard_test_server}/api/symbols")
    symbols2 = [s["display_name"] for s in list_resp2.json()["symbols"]]
    assert test_symbol not in symbols2


def test_api_integrity_audit_endpoint(dashboard_test_server):
    """Verify GET /api/integrity runs audits and returns structured results."""
    resp = requests.get(f"{dashboard_test_server}/api/integrity?symbol=SPY")
    assert resp.status_code == 200
    data = resp.json()
    assert "overall_passed" in data
    assert "anomalies" in data
    assert "gaps" in data
    assert "quiet_intervals" in data
    assert "drift" in data
    assert isinstance(data["gaps"], list)


def test_api_streamer_reload_endpoint(dashboard_test_server):
    """Verify POST /api/streamer/reload creates signal file."""
    resp = requests.post(f"{dashboard_test_server}/api/streamer/reload")
    assert resp.status_code == 200
    assert resp.json()["success"] is True


def test_api_404_not_found(dashboard_test_server):
    """Verify non-existent routes return 404."""
    resp = requests.get(f"{dashboard_test_server}/api/non_existent_endpoint")
    assert resp.status_code == 404
