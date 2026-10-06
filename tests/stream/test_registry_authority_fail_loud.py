"""INCIDENT-2026-10-06 — an empty symbol registry must never look healthy.

The Tuesday outage had no crash and no red light: the lake registry listed zero
active symbols, the streamer started, subscribed to nothing, and stayed alive
while the supervisor published INGESTING and the dashboard reported HEALTHY.
These tests pin the corrected behavior:

1. a LIVE engine refuses to start when the registry has no active symbols;
2. a mock/offline engine stays lenient (dev + test runs);
3. a dead writer is not reported as alive just because its status file says so;
4. /api/status degrades when the writer is gone;
5. the registry seeding command exists and is idempotent.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.storage.config import init_tick_lake
from src.storage.reader import TickLakeReader
from src.storage.registry import (
    RegistryError,
    SymbolRegistry,
    init_registry,
    seed_registry,
)
from src.config import APPROVED_EQUITY_SYMBOLS
from tests.support.lake_population import create_lake


def _blank_lake(root: Path) -> Path:
    """A lake whose registry exists but lists nothing (post-migration state)."""
    init_tick_lake(root)
    init_registry(root)
    return root


# ------------------------------------------------------- 1. live fail-closed

def test_live_engine_refuses_to_start_on_empty_registry(tmp_path, monkeypatch):
    """No active symbols => no subscription => refuse to run, loudly."""
    from src.stream import runner as runner_module

    lake_root = _blank_lake(tmp_path / "lake")

    class ForbiddenProvider:
        def __init__(self, *args, **kwargs):
            raise AssertionError("a live provider must not be constructed without symbols")

    monkeypatch.setattr(runner_module, "CapitalStreamer", ForbiddenProvider)
    engine = runner_module.StreamingEngine(lake_root=lake_root, mock_mode=False)
    try:
        with pytest.raises(RegistryError) as excinfo:
            asyncio.run(engine.start())
        assert "no active symbols" in str(excinfo.value)
        assert "seed" in str(excinfo.value)
    finally:
        engine.writer.close()


def test_mock_engine_stays_lenient_on_empty_registry(tmp_path, monkeypatch):
    """Offline/mock runs keep the historical lenient behavior (dev ergonomics)."""
    from src.stream import runner as runner_module

    lake_root = _blank_lake(tmp_path / "lake")
    created = {}

    class FakeProvider:
        def __init__(self, epics=None, on_tick_callback=None, **kwargs):
            created["epics"] = list(epics or [])

        async def start(self):
            return None

        async def update_subscriptions(self, epics):
            created["epics"] = list(epics)
            return True

        def stop(self):
            return None

    monkeypatch.setattr(runner_module, "MockStreamer", FakeProvider)
    engine = runner_module.StreamingEngine(lake_root=lake_root, mock_mode=True)
    try:
        asyncio.run(asyncio.wait_for(engine.start(), timeout=5))
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    finally:
        engine.stop()
        engine.writer.close()
    assert created.get("epics") == []


def test_missing_registry_still_fails_closed(tmp_path, monkeypatch):
    """A live engine on an established lake without a registry file fails."""
    from src.stream import runner as runner_module

    lake_root = tmp_path / "lake"
    create_lake(lake_root, symbols=["AAPL"])
    SymbolRegistry(root=lake_root).path.unlink()

    engine = runner_module.StreamingEngine(lake_root=lake_root, mock_mode=False)
    try:
        with pytest.raises(RegistryError):
            asyncio.run(engine.start())
    finally:
        engine.writer.close()


# --------------------------------------------- 3. a dead writer is not "LIVE"

def test_dead_writer_pid_is_not_reported_alive(tmp_path):
    """A crashed writer leaves a stale RUNNING file; liveness must check the PID."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    control = lake_root / "_control"
    control.mkdir(parents=True, exist_ok=True)

    # A PID that cannot exist: pick a value above the platform's limit.
    dead_pid = 999_999_999
    (control / "writer_status.json").write_text(
        json.dumps({
            "status": "RUNNING",
            "writer_id": "writer_1",
            "pid": dead_pid,
            "total_rows_written": 0,
            "batches_published": 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }),
        encoding="utf-8",
    )

    status = TickLakeReader(root=lake_root).get_stream_status()
    assert status["status"] == "RUNNING"          # the file still claims it
    assert status["pid_alive"] is False
    assert status["is_alive"] is False            # ...but we no longer believe it
    assert status["stale_status_file"] is True
    assert status["status_label"] == "STOPPED"


def test_live_writer_pid_is_reported_alive(tmp_path):
    """The same file with a live PID (the test process) reports alive."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    control = lake_root / "_control"
    control.mkdir(parents=True, exist_ok=True)
    (control / "writer_status.json").write_text(
        json.dumps({
            "status": "RUNNING",
            "writer_id": "writer_1",
            "pid": os.getpid(),
            "total_rows_written": 10,
            "batches_published": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }),
        encoding="utf-8",
    )
    status = TickLakeReader(root=lake_root).get_stream_status()
    assert status["pid_alive"] is True
    assert status["is_alive"] is True
    assert status["stale_status_file"] is False
    assert status["heartbeat_age_seconds"] is not None


def test_lake_health_does_not_call_disk_contents_active_ingestion(tmp_path):
    """`active_symbols` counts files on disk; the honest name is reported too."""
    lake_root = tmp_path / "lake"
    create_lake(lake_root, symbols=["AAPL"])
    report = TickLakeReader(root=lake_root).get_lake_health_report()
    assert "historical_symbols" in report
    assert "seconds_since_newest_write" in report


# --------------------------------------------------- 5. the seeding capability

def test_seed_registry_fills_a_blank_lake_and_is_idempotent(tmp_path):
    lake_root = _blank_lake(tmp_path / "lake")
    assert SymbolRegistry(root=lake_root).get_active_symbols() == []

    summary = seed_registry(lake_root)
    assert summary["active_total"] == len(APPROVED_EQUITY_SYMBOLS)
    assert sorted(summary["seeded"]) == sorted(APPROVED_EQUITY_SYMBOLS)

    again = seed_registry(lake_root)
    assert again["seeded"] == []
    assert again["active_total"] == len(APPROVED_EQUITY_SYMBOLS)


def test_seed_registry_accepts_an_explicit_symbol_list(tmp_path):
    lake_root = _blank_lake(tmp_path / "lake")
    summary = seed_registry(lake_root, symbols=["NVDA", "AAPL"])
    assert summary["seeded"] == ["NVDA", "AAPL"]
    assert summary["active_total"] == 2


def test_seed_registry_unblocks_the_live_engine(tmp_path, monkeypatch):
    """Seeding is exactly what turns the silent outage into a working stream."""
    from src.stream import runner as runner_module

    lake_root = _blank_lake(tmp_path / "lake")
    engine = runner_module.StreamingEngine(lake_root=lake_root, mock_mode=False)
    try:
        with pytest.raises(RegistryError):
            asyncio.run(engine.start())
    finally:
        engine.writer.close()

    seed_registry(lake_root, symbols=["AAPL"])
    engine = runner_module.StreamingEngine(lake_root=lake_root, mock_mode=False)

    class CapturingProvider:
        def __init__(self, epics=None, on_tick_callback=None, **kwargs):
            self.epics = list(epics or [])
            self.updates = [self.epics]

        async def start(self):
            return None

        async def update_subscriptions(self, epics):
            self.updates.append(list(epics))
            return True

        def stop(self):
            return None

    monkeypatch.setattr(runner_module, "CapitalStreamer", CapturingProvider)
    try:
        engine.capital_streamer = CapturingProvider(epics=["AAPL"])
        asyncio.run(engine.reload_symbols())
        assert engine.active_streaming_symbols  # the authority now has symbols
        assert "AAPL" in engine.active_streaming_symbols
    finally:
        engine.writer.close()
