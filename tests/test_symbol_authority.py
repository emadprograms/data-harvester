"""SYMB-01 — `_control/registry.json` is the single symbol authority.

Three boundaries are asserted here:

1. **The registry holds exactly the approved ticket.** The 19 equities in
   `src/config.py` are the deployment scope; nothing else is even representable
   as a streaming target.
2. **The callback boundary rejects unsolicited symbols.** A tick for a symbol the
   engine was not told to stream never reaches the write queue, is counted, and
   is logged once.
3. **The UI cannot widen the scope.** `POST /api/streaming/symbols` refuses a
   symbol outside the approved ticket with HTTP 400 before touching the registry.
   The rest of the CRUD suite exercises synthetic names and runs with
   `ALLOW_UNSCOPED_SYMBOLS=1` (see `tests/conftest.py`); the production default is
   asserted below with the override removed.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import requests
import pytest

from src.config import APPROVED_EQUITY_SYMBOLS
from src.stream.runner import StreamingEngine
from src.storage.registry import SymbolRegistry, init_registry
from tests.support.lake_population import create_lake


def test_approved_ticket_is_nineteen_us_equities():
    assert len(APPROVED_EQUITY_SYMBOLS) == 19
    assert all(symbol.isalpha() and symbol.isupper() for symbol in APPROVED_EQUITY_SYMBOLS)
    assert "BTCUSDT" not in APPROVED_EQUITY_SYMBOLS and "SPY" not in APPROVED_EQUITY_SYMBOLS


def test_registry_contains_exactly_the_approved_ticket(tmp_path):
    lake = create_lake(tmp_path / "lake", symbols=APPROVED_EQUITY_SYMBOLS)
    registry = SymbolRegistry(root=lake)
    assert sorted(registry.load().symbols) == sorted(APPROVED_EQUITY_SYMBOLS)


# ---------------------------------------------------------- callback boundary


def test_unsolicited_tick_is_rejected_before_it_reaches_the_queue(tmp_path):
    lake = create_lake(tmp_path / "lake", symbols=["AAPL", "MSFT"])
    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(engine.reload_symbols())
        assert "AAPL" in engine.active_streaming_symbols

        tick = {
            "epic": "DOGEUSDT",
            "price": 1.0,
            "timestamp": datetime.now(timezone.utc),
            "bid": 0.99,
            "ask": 1.01,
        }
        asyncio.run(engine._handle_capital_tick(tick))
        assert engine.write_queue.qsize() == 0, "an unsolicited symbol was admitted"
        assert engine.ticks_rejected_out_of_scope == 1
    finally:
        engine.writer.close()
        engine.stop()


def test_known_symbols_still_pass_through_the_boundary(tmp_path):
    lake = create_lake(tmp_path / "lake", symbols=["AAPL", "MSFT"])
    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(engine.reload_symbols())
        for epic in ("AAPL", "MSFT"):
            asyncio.run(
                engine._handle_capital_tick(
                    {
                        "epic": epic,
                        "price": 150.0,
                        "timestamp": datetime.now(timezone.utc),
                        "bid": 149.9,
                        "ask": 150.1,
                    }
                )
            )
        assert engine.write_queue.qsize() == 2
        assert engine.ticks_rejected_out_of_scope == 0
    finally:
        engine.writer.close()
        engine.stop()


def test_the_registry_is_the_authority_before_the_first_reload(tmp_path):
    """No subscriptions loaded yet: the registry still fences unknown symbols."""
    lake = create_lake(tmp_path / "lake", symbols=["AAPL"])
    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(
            engine._handle_capital_tick(
                {
                    "epic": "TSLA",
                    "price": 200.0,
                    "timestamp": datetime.now(timezone.utc),
                    "bid": 199.9,
                    "ask": 200.1,
                }
            )
        )
        assert engine.write_queue.qsize() == 0
        assert engine.ticks_rejected_out_of_scope == 1
    finally:
        engine.writer.close()
        engine.stop()


def test_a_capital_ticker_is_mapped_to_its_display_symbol(tmp_path):
    lake = tmp_path / "lake"
    create_lake(lake, symbols=["AAPL"])
    registry = SymbolRegistry(root=lake)
    registry.add_symbol(symbol="NVDA", display_name="NVDA", capital_ticker="NVDA.US")

    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(engine.reload_symbols())
        asyncio.run(
            engine._handle_capital_tick(
                {
                    "epic": "NVDA.US",
                    "price": 100.0,
                    "timestamp": datetime.now(timezone.utc),
                    "bid": 99.9,
                    "ask": 100.1,
                }
            )
        )
        assert engine.write_queue.qsize() == 1
        queued = engine.write_queue.get_nowait()
        assert queued[1] == "NVDA", f"epic was not mapped to the display symbol: {queued}"
    finally:
        engine.writer.close()
        engine.stop()


def test_rejection_is_logged_once_per_symbol(tmp_path, caplog):
    lake = create_lake(tmp_path / "lake", symbols=["AAPL"])
    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(engine.reload_symbols())
        tick = {
            "epic": "PLTR",
            "price": 10.0,
            "timestamp": datetime.now(timezone.utc),
            "bid": 9.9,
            "ask": 10.1,
        }
        with caplog.at_level("WARNING"):
            for _ in range(5):
                asyncio.run(engine._handle_capital_tick(tick))
        assert engine.ticks_rejected_out_of_scope == 5
        warnings = [r for r in caplog.records if "out-of-scope" in r.getMessage()]
        assert len(warnings) == 1, "the rejection log should not spam one line per tick"
    finally:
        engine.writer.close()
        engine.stop()


def test_an_empty_registry_has_no_authority_to_enforce(tmp_path):
    """A bare engine (no registry entries) behaves as before: nothing to fence against."""
    lake = tmp_path / "lake"
    init_registry(lake)
    engine = StreamingEngine(lake_root=lake)
    try:
        asyncio.run(
            engine._handle_capital_tick(
                {
                    "epic": "AAPL",
                    "price": 150.0,
                    "timestamp": datetime.now(timezone.utc),
                    "bid": 149.9,
                    "ask": 150.1,
                }
            )
        )
        assert engine.write_queue.qsize() == 1
    finally:
        engine.writer.close()
        engine.stop()


# ---------------------------------------------------------------- UI boundary


@pytest.fixture
def scoped_server(tmp_path, monkeypatch):
    """An in-process dashboard over a lake seeded with the approved ticket."""
    import socket
    import threading

    from src.dashboard.server import create_dashboard_server

    lake = create_lake(tmp_path / "lake", symbols=APPROVED_EQUITY_SYMBOLS[:3])
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
    monkeypatch.setenv("DATA_DIR", str(lake))
    monkeypatch.delenv("ALLOW_UNSCOPED_SYMBOLS", raising=False)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = create_dashboard_server(host="127.0.0.1", port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", lake
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_the_ui_refuses_a_symbol_outside_the_ticket(scoped_server):
    base_url, lake = scoped_server
    resp = requests.post(
        f"{base_url}/api/streaming/symbols",
        json={"display_name": "PLTR", "capital_ticker": "PLTR"},
        timeout=5,
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert "approved equity scope" in (body.get("error") or "")

    registry = SymbolRegistry(root=lake)
    assert "PLTR" not in registry.load().symbols, "the refused symbol reached the registry"


def test_the_ui_still_accepts_an_approved_symbol(tmp_path, monkeypatch):
    import socket
    import threading

    from src.dashboard.server import create_dashboard_server

    lake = create_lake(tmp_path / "lake", symbols=[APPROVED_EQUITY_SYMBOLS[0]])
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
    monkeypatch.setenv("DATA_DIR", str(lake))
    monkeypatch.delenv("ALLOW_UNSCOPED_SYMBOLS", raising=False)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = create_dashboard_server(host="127.0.0.1", port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        candidate = APPROVED_EQUITY_SYMBOLS[1]
        resp = requests.post(
            f"http://127.0.0.1:{port}/api/streaming/symbols",
            json={"display_name": candidate, "capital_ticker": candidate},
            timeout=5,
        )
        assert resp.status_code in (200, 201), resp.text
        assert candidate in SymbolRegistry(root=lake).load().symbols
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_the_ticket_can_be_widened_in_one_place_only():
    """Documented operator escape hatch: editing config, not inventing a UI override."""
    import inspect
    from src.dashboard import server as server_module

    source = inspect.getsource(server_module._symbol_scope_rejection)
    assert "APPROVED_EQUITY_SYMBOLS" in source, "the scope check must read the single ticket"
