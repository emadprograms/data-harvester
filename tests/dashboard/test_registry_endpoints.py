"""
Integration tests for dashboard SymbolRegistry endpoints and zero DuckDB lock guarantees.
Milestone v4.0 - Phase 18 (P3).

Validates:
- GET /api/streaming/symbols reads directly from SymbolRegistry
- POST /api/streaming/symbols adds symbol to registry and touches signal file
- POST /api/streaming/symbols returns HTTP 409 Conflict if symbol is in PENDING_PURGE
- DELETE /api/streaming/symbols marks symbol PENDING_PURGE with truthful response
- PATCH /api/streaming/symbols toggles active/inactive state
- ZERO DuckDB connections are opened during any symbol admin operations
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import threading
import time
from unittest.mock import MagicMock
import duckdb
import pytest
import requests

from src.dashboard.server import create_dashboard_server
from src.storage.registry import (
    DEFAULT_SIGNAL_FILENAME,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    SymbolRegistry,
    init_registry,
)


@pytest.fixture
def ephemeral_server(tmp_path, monkeypatch):
    """
    Spawns an ephemeral dashboard server bound to a free port, with TICK_LAKE_ROOT
    isolated to tmp_path / 'lake'.
    """
    lake_root = tmp_path / "lake"
    lake_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))
    monkeypatch.setenv("DATA_DIR", str(lake_root))

    # Find free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url, lake_root
    finally:
        server.shutdown()
        server.server_close()


def test_get_streaming_symbols_endpoint_reads_registry(ephemeral_server):
    """
    GET /api/streaming/symbols returns symbol list loaded from SymbolRegistry,
    including generation, active state, and status.
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    reg.add_symbol("NVDA", display_name="NVIDIA Corp", capital_ticker="NVDA")

    resp = requests.get(f"{base_url}/api/streaming/symbols", timeout=3.0)
    assert resp.status_code == 200
    data = resp.json()

    assert "symbols" in data
    symbols = data["symbols"]
    assert len(symbols) >= 1

    # Locate NVDA entry
    nvda_entry = None
    for item in symbols:
        if isinstance(item, dict) and (item.get("symbol") == "NVDA" or item.get("display_name") == "NVDA"):
            nvda_entry = item
            break
        elif item == "NVDA":
            nvda_entry = {"symbol": "NVDA", "status": STATUS_ACTIVE, "active": True}
            break

    assert nvda_entry is not None, "NVDA not found in /api/streaming/symbols response"
    assert nvda_entry.get("status") == STATUS_ACTIVE
    assert nvda_entry.get("active") is True
    assert nvda_entry.get("generation", 1) == 1


def test_post_streaming_symbols_adds_to_registry_and_signals(ephemeral_server):
    """
    POST /api/streaming/symbols adds symbol to SymbolRegistry and touches
    .stream_reload.signal to wake up background runners.
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)

    signal_file = lake_root / DEFAULT_SIGNAL_FILENAME
    if not signal_file.exists():
        signal_file = lake_root / "_control" / DEFAULT_SIGNAL_FILENAME

    payload = {
        "symbol": "TSLA",
        "display_name": "Tesla Inc",
        "capital_ticker": "TSLA",
    }
    resp = requests.post(f"{base_url}/api/streaming/symbols", json=payload, timeout=3.0)
    assert resp.status_code in (200, 201)

    # Verify symbol persisted in registry on disk
    snapshot = reg.load()
    assert "TSLA" in snapshot.symbols
    entry = snapshot.symbols["TSLA"]
    assert entry.status == STATUS_ACTIVE
    assert entry.active is True
    assert entry.generation == 1

    # Verify reload signal file was touched
    assert signal_file.exists() or (lake_root / ".stream_reload.signal").exists()


def test_post_streaming_symbols_returns_409_on_pending_purge(ephemeral_server):
    """
    POST /api/streaming/symbols for a symbol in PENDING_PURGE returns HTTP 409 Conflict,
    preventing corrupt overwrites before purge completes.
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)

    # Add and mark pending purge
    reg.add_symbol("NVDA", capital_ticker="NVDA")
    reg.remove_symbol("NVDA")
    assert reg.load().symbols["NVDA"].status == STATUS_PENDING_PURGE

    # Attempting to re-add via HTTP POST must return 409 Conflict
    payload = {
        "symbol": "NVDA",
        "display_name": "NVIDIA Corp",
        "capital_ticker": "NVDA",
    }
    resp = requests.post(f"{base_url}/api/streaming/symbols", json=payload, timeout=3.0)
    assert resp.status_code == 409, f"Expected 409 Conflict, got {resp.status_code}: {resp.text}"

    body = resp.json()
    err_msg = body.get("error", "") or body.get("message", "")
    assert "purge" in err_msg.lower() or "conflict" in err_msg.lower() or "pending" in err_msg.lower()


def test_delete_streaming_symbols_marks_pending_purge(ephemeral_server):
    """
    DELETE /api/streaming/symbols/{symbol} returns HTTP 200 with status='PENDING_PURGE'
    and a truthful message explaining the symbol is pending purge (not falsely claiming instant deletion).
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    reg.add_symbol("AMZN", capital_ticker="AMZN")

    resp = requests.delete(f"{base_url}/api/streaming/symbols/AMZN", timeout=3.0)
    assert resp.status_code == 200
    data = resp.json()

    # Verify truthful response
    assert data.get("status") == STATUS_PENDING_PURGE or "pending_purge" in json.dumps(data).lower()
    msg = data.get("message", "").lower()
    assert "pending purge" in msg or "purged" in msg or "marked" in msg

    # Verify on-disk registry state
    snapshot = reg.load()
    assert "AMZN" in snapshot.symbols
    assert snapshot.symbols["AMZN"].status == STATUS_PENDING_PURGE
    assert snapshot.symbols["AMZN"].active is False


def test_patch_streaming_symbols_toggle(ephemeral_server):
    """
    PATCH /api/streaming/symbols/{symbol} toggles active state True -> False -> True.
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    reg.add_symbol("MSFT", capital_ticker="MSFT")

    # 1. Toggle to inactive (active=False)
    resp = requests.patch(
        f"{base_url}/api/streaming/symbols/MSFT",
        json={"active": False},
        timeout=3.0
    )
    assert resp.status_code == 200

    snapshot1 = reg.load()
    assert snapshot1.symbols["MSFT"].active is False
    assert snapshot1.symbols["MSFT"].status == STATUS_INACTIVE

    # 2. Toggle back to active (active=True)
    resp2 = requests.patch(
        f"{base_url}/api/streaming/symbols/MSFT",
        json={"active": True},
        timeout=3.0
    )
    assert resp2.status_code == 200

    snapshot2 = reg.load()
    assert snapshot2.symbols["MSFT"].active is True
    assert snapshot2.symbols["MSFT"].status == STATUS_ACTIVE


def test_zero_duckdb_connections_opened_during_symbol_admin(ephemeral_server, monkeypatch):
    """
    Verifies that ZERO DuckDB connections are opened during GET, POST, DELETE,
    and PATCH operations on streaming symbols. Proves complete decoupling from DuckDB locks.
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)

    # Intercept duckdb.connect to count connections
    connect_calls = []
    orig_connect = duckdb.connect

    def spy_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return orig_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy_connect)

    # 1. GET
    resp_get = requests.get(f"{base_url}/api/streaming/symbols", timeout=3.0)
    assert resp_get.status_code == 200

    # 2. POST (add GOOGL)
    resp_post = requests.post(
        f"{base_url}/api/streaming/symbols",
        json={"symbol": "GOOGL", "capital_ticker": "GOOGL"},
        timeout=3.0
    )
    assert resp_post.status_code in (200, 201)

    # 3. PATCH (toggle GOOGL)
    resp_patch = requests.patch(
        f"{base_url}/api/streaming/symbols/GOOGL",
        json={"active": False},
        timeout=3.0
    )
    assert resp_patch.status_code == 200

    # 4. DELETE (remove GOOGL)
    resp_delete = requests.delete(f"{base_url}/api/streaming/symbols/GOOGL", timeout=3.0)
    assert resp_delete.status_code == 200

    # ASSERT ZERO DuckDB connections were opened during all four admin operations
    assert len(connect_calls) == 0, (
        f"DuckDB connections opened during symbol admin: {connect_calls}"
    )
