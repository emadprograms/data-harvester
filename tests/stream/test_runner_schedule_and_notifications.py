"""SCHED-03 + NOTIF-01 wiring: the runner enforces the window and tells the operator.

SCHED-03 — a manual start cannot bypass the schedule: outside 04:00–20:00 ET the
direct runner entry point refuses to authenticate and exits with
``EXIT_OUTSIDE_WINDOW``. ``--mock`` is exempt because it never subscribes to a
provider.

NOTIF-01 — the streamer reports session start, session stop and a failed start to
Discord, best-effort, without ever blocking the event loop.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.stream import runner as runner_module
from src.stream.runner import (
    EXIT_OUTSIDE_WINDOW,
    StreamingEngine,
    check_ingestion_window,
)
from src.utils import notifications
from tests.support.lake_population import create_lake

ET = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[2]


def et(*parts) -> datetime:
    return datetime(*parts, tzinfo=ET)


# ----------------------------------------------------- SCHED-03: the start gate


def test_window_gate_allows_the_open_hours():
    allowed, message = check_ingestion_window(now=et(2026, 10, 7, 12, 0))
    assert allowed is True
    assert "INGESTING" in message


@pytest.mark.parametrize(
    "moment",
    [
        et(2026, 10, 7, 3, 59),  # pre-open
        et(2026, 10, 7, 20, 0),  # exactly close
        et(2026, 10, 10, 12, 0),  # Saturday
    ],
)
def test_window_gate_refuses_outside_the_open_hours(moment):
    allowed, message = check_ingestion_window(now=moment)
    assert allowed is False
    assert "WAITING_FOR_WINDOW" in message


def test_mock_mode_is_exempt_because_it_never_subscribes():
    allowed, message = check_ingestion_window(now=et(2026, 10, 7, 3, 0), mock_mode=True)
    assert allowed is True
    assert "WAITING_FOR_WINDOW" in message  # the honest state is still reported


def test_cli_refuses_to_start_outside_the_window(tmp_path):
    """A manual real-mode start outside the window exits 3 without authenticating."""
    env = dict(os.environ)
    env["STREAM_NOW_OVERRIDE"] = "2026-10-07T03:30:00"  # Wednesday, 03:30 ET
    env.pop("CAPITAL_COM_X_CAP_API_KEY", None)
    env.pop("CAPITAL_COM_IDENTIFIER", None)
    env.pop("CAPITAL_COM_PASSWORD", None)

    proc = subprocess.run(
        [sys.executable, "-m", "src.stream.runner", "--lake-root", str(tmp_path / "lake")],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == EXIT_OUTSIDE_WINDOW, (
        f"expected the window gate ({EXIT_OUTSIDE_WINDOW}), got {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    combined = (proc.stdout + proc.stderr).lower()
    assert "window" in combined


def test_cli_mock_mode_starts_outside_the_window(tmp_path):
    """--mock produces synthetic ticks: it is allowed off-hours and still drains."""
    env = dict(os.environ)
    env["STREAM_NOW_OVERRIDE"] = "2026-10-10T12:00:00"  # Saturday
    lake_root = tmp_path / "lake"
    create_lake(lake_root, symbols=["AAPL", "MSFT"])

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "src.stream.runner",
            "--lake-root", str(lake_root),
            "--writer-id", "window_mock",
            "--mock",
            "--ticks-per-sec", "20",
            "--flush-interval", "0.2",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        time.sleep(3.0)
        assert proc.poll() is None, (
            "mock mode must keep running off-hours, but it exited early:\n"
            + (proc.stdout.read() if proc.stdout else "")
        )
    finally:
        proc.terminate()
        try:
            output, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - pathological
            proc.kill()
            output, _ = proc.communicate()
    assert proc.returncode == 0, f"graceful drain expected, got {proc.returncode}\n{output}"
    assert "window" not in output.lower() or "closes" in output.lower()


# ------------------------------------------- NOTIF-01: streamer lifecycle events


@pytest.fixture
def recorded_notifications(monkeypatch):
    calls = []
    monkeypatch.setattr(
        runner_module,
        "notify_detached",
        lambda event, detail=None, **kwargs: calls.append((event, detail, kwargs)),
    )
    return calls


def _events(calls):
    return [event for event, _detail, _kwargs in calls]


def test_session_started_is_reported(tmp_path, recorded_notifications):
    lake_root = create_lake(tmp_path / "lake", symbols=["AAPL", "MSFT"])

    async def run():
        engine = StreamingEngine(
            lake_root=lake_root, mock_mode=True, mock_ticks_per_sec=20, flush_interval=0.1
        )
        task = asyncio.create_task(engine.start())
        await asyncio.sleep(0.4)
        engine.stop()
        await asyncio.wait_for(task, timeout=15)

    asyncio.run(run())
    events = _events(recorded_notifications)
    assert notifications.SESSION_STARTED in events
    started = next(call for call in recorded_notifications if call[0] == notifications.SESSION_STARTED)
    assert "2" in str(started[1]) or (started[2].get("fields") or {}).get("Symbols")


def test_session_stopped_is_reported_after_a_clean_drain(tmp_path, recorded_notifications):
    lake_root = create_lake(tmp_path / "lake", symbols=["AAPL", "MSFT"])

    async def run():
        engine = StreamingEngine(
            lake_root=lake_root, mock_mode=True, mock_ticks_per_sec=20, flush_interval=0.1
        )
        task = asyncio.create_task(engine.start())
        await asyncio.sleep(0.3)
        engine.stop()
        await asyncio.wait_for(task, timeout=15)
        assert engine.drain_succeeded is True

    asyncio.run(run())
    assert notifications.SESSION_STOPPED in _events(recorded_notifications)


def test_drain_failure_is_reported_honestly(tmp_path, recorded_notifications):
    lake_root = create_lake(tmp_path / "lake", symbols=["AAPL"])

    async def run():
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)

        async def broken_close():
            raise RuntimeError("disk went away mid-drain")

        engine.writer.close_async = broken_close
        with pytest.raises(runner_module.DrainFailedError):
            await engine.shutdown(drain_timeout=1.0)
        return engine

    engine = asyncio.run(run())
    assert engine.drain_succeeded is False
    events = _events(recorded_notifications)
    assert notifications.DRAIN_FAILED in events
    assert notifications.SESSION_STOPPED not in events


def test_failed_start_is_reported(tmp_path, recorded_notifications, monkeypatch):
    """`_run_streamer` maps a startup exception to a notification and exit code 1."""
    lake_root = create_lake(tmp_path / "lake", symbols=["AAPL"])

    class ExplodingEngine:
        def __init__(self):
            self.running = False

        async def start(self):
            raise RuntimeError("Capital.com rejected the credentials")

    engine = ExplodingEngine()
    exit_code = runner_module._run_streamer(engine, lake_root=lake_root)

    assert exit_code == 1
    events = _events(recorded_notifications)
    assert notifications.SESSION_START_FAILED in events
    detail = next(call[1] for call in recorded_notifications if call[0] == notifications.SESSION_START_FAILED)
    assert "credentials" in detail
    assert notifications.SESSION_STARTED not in events


def test_drain_failure_maps_to_its_own_exit_code(tmp_path, recorded_notifications):
    class DrainingEngine:
        async def start(self):
            raise runner_module.DrainFailedError("5 accepted ticks never reached Parquet")

    exit_code = runner_module._run_streamer(DrainingEngine(), lake_root=tmp_path / "lake")
    assert exit_code == runner_module.EXIT_DRAIN_FAILED
    assert notifications.DRAIN_FAILED in _events(recorded_notifications)


def test_notifications_are_not_even_dispatched_without_a_webhook(monkeypatch):
    """No webhook configured → no thread churn on every session."""
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SKIP_DISCORD", raising=False)
    spawned = []
    monkeypatch.setattr(
        notifications.threading, "Thread", lambda *a, **k: spawned.append(k) or pytest.fail("thread spawned")
    )
    notifications.notify_detached(notifications.SESSION_STARTED, detail="no-op")
    assert spawned == []


# -------------------------------- SCHED-04: close means stop admitting, drain once


def test_window_watchdog_stops_the_runner_at_close():
    """The runner watches the window and asks itself to stop when it closes."""
    async def run():
        stop_event = asyncio.Event()
        inside = et(2026, 10, 6, 19, 59, 59)
        outside = et(2026, 10, 6, 20, 0, 0)
        seen = []

        def clock():
            seen.append(1)
            return inside if len(seen) <= 2 else outside

        await asyncio.wait_for(
            runner_module.window_watchdog(stop_event, clock=clock, poll_seconds=0.01),
            timeout=5,
        )
        assert stop_event.is_set(), "the watchdog never asked the runner to stop"
        assert len(seen) >= 3

    asyncio.run(run())


def test_window_watchdog_is_silent_while_the_window_is_open():
    async def run():
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            runner_module.window_watchdog(
                stop_event,
                clock=lambda: et(2026, 10, 6, 12, 0),
                poll_seconds=0.01,
            )
        )
        await asyncio.sleep(0.1)
        assert not stop_event.is_set()
        assert not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())


def test_admission_closes_with_the_window(tmp_path):
    """At 20:00 the engine stops accepting ticks: no queue growth after close."""
    lake_root = create_lake(tmp_path / "admission-lake", symbols=["AAPL"])
    engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
    try:
        tick = ("2026-10-06 19:59:00.000000", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        assert engine._enqueue_tick(tick) is True

        engine.close_admission(reason="window closed at 20:00 ET")
        assert engine.admission_open is False

        assert engine._enqueue_tick(tick) is False
        assert engine.write_queue.qsize() == 1, "a tick was accepted after the window closed"
        assert engine.ticks_dropped >= 1
    finally:
        engine.writer.close()
        engine.stop()


def test_async_admission_is_also_closed(tmp_path):
    async def run():
        lake_root = create_lake(tmp_path / "admission-async", symbols=["AAPL"])
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.05)
        try:
            engine.close_admission(reason="window closed")
            tick = ("2026-10-06 20:00:01.000000", "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
            await asyncio.wait_for(engine._enqueue_tick_async(tick), timeout=1.0)
            assert engine.write_queue.qsize() == 0
            assert engine.ticks_dropped >= 1
        finally:
            engine.writer.close()
            engine.stop()

    asyncio.run(run())


def test_the_close_path_drains_exactly_once(tmp_path):
    """The runner stops the engine once, drains once, and exits 0 on window close."""
    calls = {"stop": 0, "start": 0}

    class FakeEngine:
        drain_succeeded = True

        def __init__(self):
            self._stopped = None

        async def start(self):
            self._stopped = asyncio.Event()
            calls["start"] += 1
            await self._stopped.wait()

        def stop(self):
            calls["stop"] += 1
            if self._stopped is not None:
                self._stopped.set()

    engine = FakeEngine()
    ticks = [et(2026, 10, 6, 19, 59, 50)]

    def clock():
        # Inside for the first few polls, then past 20:00.
        ticks.append(ticks[-1] + timedelta(seconds=5))
        return ticks[-1]

    exit_code = runner_module._run_streamer(
        engine, lake_root=tmp_path / "lake", clock=clock, watchdog_poll_seconds=0.01
    )
    assert exit_code == 0
    assert calls == {"stop": 1, "start": 1}, f"expected exactly one drain, got {calls}"


def test_manual_stop_still_drains_once(tmp_path):
    """A child that stops itself inside the window is drained once, not twice."""
    calls = {"stop": 0}

    class SelfStoppingEngine:
        drain_succeeded = True

        def __init__(self):
            self._stopped = None

        async def start(self):
            self._stopped = asyncio.Event()
            await asyncio.sleep(0.05)
            self.stop()
            await self._stopped.wait()

        def stop(self):
            calls["stop"] += 1
            if self._stopped is not None:
                self._stopped.set()

    exit_code = runner_module._run_streamer(
        SelfStoppingEngine(),
        lake_root=tmp_path / "lake",
        clock=lambda: et(2026, 10, 6, 12, 0),
        watchdog_poll_seconds=0.01,
    )
    assert exit_code == 0
    assert calls["stop"] == 1
