"""Phase 46 DASH-01 — the dashboard serves one store: the Parquet tick lake.

The Historical Dashboard's routes and analytics functions are removed, and the
unified candle route serves the lake whether or not a source is named.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import socket
import threading
import time

import pytest
import requests

from src.dashboard.server import create_dashboard_server
from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from tests.fixtures.deterministic_quotes import QuoteTick

REMOVED_ROUTES = (
    "/api/historical/symbols",
    "/api/historical/symbols/AAPL",
    "/api/historical/candles?symbol=AAPL&date=2026-10-02",
    "/api/historical/overview",
    "/api/symbols/coverage",
)

REMOVED_ANALYTICS = (
    "get_historical_candles",
    "get_historical_overview",
    "get_symbols_coverage",
)

LAKE_DATE = "2026-10-02"


@pytest.fixture
def lake_server(tmp_path, monkeypatch):
    """Ephemeral dashboard server backed by a lake holding AAPL ticks."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

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
            ingest_id=f"po_{i:04d}",
        )
        for i in range(25)
    ]
    with LakePublisher(root=lake_root, writer_id="w_parquet_only") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_po_001", sequence=1)

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
        yield f"http://127.0.0.1:{port}", lake_root
    finally:
        server.shutdown()
        server.server_close()


def test_historical_routes_are_removed(lake_server) -> None:
    base_url, _ = lake_server
    for route in REMOVED_ROUTES:
        resp = requests.get(f"{base_url}{route}", timeout=10)
        assert resp.status_code == 404, f"{route} should be gone, got {resp.status_code}"


def test_historical_symbol_write_routes_are_removed(lake_server) -> None:
    base_url, _ = lake_server
    post = requests.post(
        f"{base_url}/api/historical/symbols", json={"display_name": "AAPL"}, timeout=10
    )
    assert post.status_code == 404, f"POST historical symbols should be gone, got {post.status_code}"

    delete = requests.delete(f"{base_url}/api/historical/symbols/AAPL", timeout=10)
    assert delete.status_code == 404, f"DELETE historical symbols should be gone, got {delete.status_code}"


def test_symbol_writes_never_name_a_historical_source(lake_server) -> None:
    base_url, _ = lake_server
    resp = requests.post(
        f"{base_url}/api/symbols?source=historical", json={"display_name": "ZZZZ"}, timeout=10
    )
    assert resp.status_code in (400, 404), (
        f"a historical source must not be writable, got {resp.status_code}: {resp.text[:200]}"
    )


def test_dashboard_server_does_not_import_the_disk_database_layer() -> None:
    import ast
    from pathlib import Path

    source_file = Path(__file__).resolve().parents[2] / "src" / "dashboard" / "server.py"
    tree = ast.parse(source_file.read_text(encoding="utf-8"))

    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)

    offenders = sorted(m for m in modules if m.startswith("src.database"))
    assert not offenders, f"dashboard server imports the disk database layer: {offenders}"


def test_historical_analytics_functions_are_removed() -> None:
    import src.dashboard.analytics as analytics

    remaining = [name for name in REMOVED_ANALYTICS if hasattr(analytics, name)]
    assert not remaining, f"historical analytics functions remain: {remaining}"


def test_candles_serve_the_lake_without_a_source_parameter(lake_server) -> None:
    base_url, _ = lake_server
    resp = requests.get(f"{base_url}/api/candles?symbol=AAPL&date={LAKE_DATE}", timeout=10)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("symbol") == "AAPL"
    assert data.get("candles"), "the lake-backed candle route returned no candles"


def test_candles_still_accept_a_legacy_source_parameter(lake_server) -> None:
    base_url, _ = lake_server
    for source in ("historical", "streaming", "live"):
        resp = requests.get(
            f"{base_url}/api/candles?symbol=AAPL&date={LAKE_DATE}&source={source}", timeout=10
        )
        assert resp.status_code == 200, f"source={source} should still resolve to the lake"
        assert resp.json().get("candles"), f"source={source} returned no candles"
