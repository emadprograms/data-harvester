"""SCHED-02 — the supervisor owns the service lifecycle.

The supervisor is the only component allowed to decide whether the streamer is
running, so it must know the ingestion window (weekdays 04:00–20:00 ET) and
publish its state:

``WAITING_FOR_WINDOW`` -> ``STARTING`` -> ``INGESTING`` -> ``DRAINING`` ->
``WAITING_FOR_WINDOW``, with ``MAINTENANCE`` for off-hours compaction and
``ERROR`` when the child keeps crashing.

Two behaviours matter most and are asserted directly:

- **No provider work outside the window.** Off-hours the supervisor must not
  launch (or keep alive) the streamer, because starting it means authenticating
  and subscribing to Capital.com.
- **An intentional off-hours stop is not a crash.** The child exiting 0 because
  the window closed must not increment the crash counter, must not trigger
  backoff, and must not raise max-restart errors.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.utils.lifecycle import LifecycleRecorder, LifecycleState
from tools.service_supervisor import ProcessSupervisor

ET = ZoneInfo("America/New_York")


def et(*parts) -> datetime:
    return datetime(*parts, tzinfo=ET)


class FakeChild:
    """Stands in for a subprocess.Popen without spawning a real process."""

    def __init__(self, pid=4242):
        self.pid = pid
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode


class FakeClock:
    def __init__(self, moment):
        self.moment = moment

    def __call__(self):
        return self.moment

    def advance(self, **kwargs):
        self.moment = self.moment + timedelta(**kwargs)

    def set(self, moment):
        self.moment = moment


@pytest.fixture
def supervisor(tmp_path):
    """A window-enforcing supervisor with no real child process."""
    clock = FakeClock(et(2026, 10, 6, 3, 30))  # Tuesday, pre-open
    sup = ProcessSupervisor(
        name="streamer",
        module="src.stream.runner",
        poll_interval=0.01,
        backoff_factor=0.001,
        max_backoff=0.001,
        log_dir=tmp_path / "logs",
        handle_signals=False,
        enforce_window=True,
        clock=clock,
    )
    spawned = []

    def fake_start():
        child = FakeChild(pid=1000 + len(spawned))
        sup.process = child
        sup.last_child_exit_code = None
        sup._child_start_time = time.monotonic()  # a fresh child, not one started at boot
        spawned.append(child)
        return child

    def fake_stop(timeout=15.0):
        child = sup.process
        if child is not None:
            child.returncode = 0
            sup.last_child_exit_code = 0
        sup.process = None
        return sup.last_child_exit_code

    sup._start_child = fake_start
    sup._stop_child = fake_stop
    sup.spawned = spawned
    sup.clock = clock
    return sup


# --------------------------------------------------------------- state machine


def test_lifecycle_states_are_the_documented_set():
    assert {state.value for state in LifecycleState} == {
        "WAITING_FOR_WINDOW",
        "STARTING",
        "INGESTING",
        # STALLED: the child is alive inside the window but writing no rows
        # (INCIDENT-2026-10-06 — "process alive" was reported as "ingesting").
        "STALLED",
        "DRAINING",
        "MAINTENANCE",
        "ERROR",
    }


def test_closed_window_waits_and_never_launches(supervisor):
    assert supervisor.tick() == LifecycleState.WAITING_FOR_WINDOW
    assert supervisor.spawned == [], "a child was launched outside the window"
    assert supervisor.process is None
    assert supervisor.lifecycle_state is LifecycleState.WAITING_FOR_WINDOW


def test_window_open_starts_the_child_and_reports_ingesting(supervisor):
    supervisor.clock.set(et(2026, 10, 6, 4, 0))
    assert supervisor.tick() == LifecycleState.STARTING
    assert len(supervisor.spawned) == 1

    assert supervisor.tick() == LifecycleState.INGESTING
    # A second tick must not spawn a second child.
    assert len(supervisor.spawned) == 1


def test_close_of_window_drains_once_then_waits(supervisor):
    supervisor.clock.set(et(2026, 10, 6, 12, 0))
    supervisor.tick()
    supervisor.tick()
    assert supervisor.lifecycle_state is LifecycleState.INGESTING

    supervisor.clock.set(et(2026, 10, 6, 20, 0))  # exactly at close
    assert supervisor.tick() == LifecycleState.DRAINING
    assert supervisor.process is None
    assert supervisor.spawned[0].terminated is False  # stops via _stop_child, not kill

    assert supervisor.tick() == LifecycleState.WAITING_FOR_WINDOW
    assert len(supervisor.spawned) == 1, "the streamer was restarted after close"


def test_weekend_is_not_a_start_signal(supervisor):
    supervisor.clock.set(et(2026, 10, 10, 12, 0))  # Saturday
    supervisor.tick()
    assert supervisor.spawned == []
    assert supervisor.lifecycle_state is LifecycleState.WAITING_FOR_WINDOW


def test_intentional_off_hours_stop_is_not_counted_as_a_crash(supervisor):
    supervisor.clock.set(et(2026, 10, 6, 12, 0))
    supervisor.tick()
    supervisor.tick()
    supervisor.clock.set(et(2026, 10, 6, 20, 0))
    supervisor.tick()  # DRAINING
    supervisor.tick()  # WAITING_FOR_WINDOW

    assert supervisor.consecutive_crashes == 0
    assert supervisor.lifecycle_state is LifecycleState.WAITING_FOR_WINDOW
    # The next morning it starts again, with no backoff delay.
    supervisor.clock.set(et(2026, 10, 7, 4, 0))
    supervisor.tick()
    assert len(supervisor.spawned) == 2
    assert supervisor.lifecycle_state is LifecycleState.STARTING


def test_child_crash_inside_the_window_is_a_crash(supervisor):
    supervisor.clock.set(et(2026, 10, 6, 12, 0))
    supervisor.tick()  # STARTING
    child = supervisor.process
    child.returncode = 137  # killed by another process
    supervisor.tick()
    assert supervisor.consecutive_crashes == 1
    assert supervisor.lifecycle_state in (LifecycleState.ERROR, LifecycleState.STARTING)


def test_max_restarts_lands_in_error(supervisor):
    supervisor.max_restarts = 2
    supervisor.clock.set(et(2026, 10, 6, 12, 0))
    for _ in range(6):
        supervisor.tick()
        if supervisor.process is not None:
            supervisor.process.returncode = 1
            supervisor.tick()
        if supervisor.lifecycle_state is LifecycleState.ERROR:
            break
    assert supervisor.lifecycle_state is LifecycleState.ERROR
    assert supervisor.running is False


# ------------------------------------------------------- recorder / state file


def test_recorder_writes_and_reads_a_state_file(tmp_path):
    recorder = LifecycleRecorder(tmp_path / "logs", name="streamer")
    payload = recorder.transition(LifecycleState.WAITING_FOR_WINDOW, detail="pre-open")
    assert payload["state"] == "WAITING_FOR_WINDOW"
    assert payload["name"] == "streamer"
    assert payload["detail"] == "pre-open"
    assert "updated_at" in payload

    on_disk = json.loads((tmp_path / "logs" / "streamer.state.json").read_text())
    assert on_disk["state"] == "WAITING_FOR_WINDOW"

    recorder.transition(LifecycleState.INGESTING, detail="window open at 04:00")
    assert recorder.read()["state"] == "INGESTING"


def test_recorder_transitions_are_atomic(tmp_path):
    """No partial JSON: the file is always readable, even mid-write."""
    recorder = LifecycleRecorder(tmp_path / "logs", name="streamer")
    for state in LifecycleState:
        recorder.transition(state, detail=f"to {state.value}")
        assert json.loads(recorder.path.read_text())["state"] == state.value


def test_supervisor_publishes_its_state_for_the_status_script(supervisor, tmp_path):
    supervisor.tick()
    state_file = tmp_path / "logs" / "streamer.state.json"
    assert state_file.is_file(), "the supervisor did not publish a state file"
    payload = json.loads(state_file.read_text())
    assert payload["state"] == "WAITING_FOR_WINDOW"
    assert payload["name"] == "streamer"
    assert "window" in payload, "the state file should carry the window description"


# ------------------------------------------------------------- maintenance hook


def test_maintenance_state_is_owned_and_reported(supervisor):
    """Off-hours work (compaction) runs under the supervisor's MAINTENANCE state."""
    ran = []

    def compaction():
        ran.append(True)
        return {"status": "NOOP"}

    supervisor.maintenance_hook = compaction
    supervisor.clock.set(et(2026, 10, 6, 21, 0))  # closed window
    assert supervisor.tick() == LifecycleState.MAINTENANCE
    assert ran == [True]
    assert supervisor.tick() == LifecycleState.WAITING_FOR_WINDOW


def test_maintenance_hook_failure_is_reported_and_not_fatal(supervisor):
    def broken():
        raise RuntimeError("compaction exploded")

    supervisor.maintenance_hook = broken
    supervisor.clock.set(et(2026, 10, 6, 21, 0))
    assert supervisor.tick() == LifecycleState.ERROR
    # The next tick returns to waiting; a maintenance bug does not stop the service.
    assert supervisor.tick() == LifecycleState.WAITING_FOR_WINDOW


def test_maintenance_never_runs_while_the_window_is_open(supervisor):
    ran = []
    supervisor.maintenance_hook = lambda: ran.append(True)
    supervisor.clock.set(et(2026, 10, 6, 12, 0))
    supervisor.tick()
    supervisor.tick()
    assert ran == [], "compaction must never run during ingestion"


def test_window_enforcement_is_opt_in_for_non_ingestion_services(tmp_path):
    """The dashboard has no window: it stays up 24/7."""
    sup = ProcessSupervisor(
        name="dashboard",
        module="src.dashboard.server",
        poll_interval=0.01,
        log_dir=tmp_path / "logs",
        handle_signals=False,
        enforce_window=False,
        clock=FakeClock(et(2026, 10, 6, 3, 0)),
    )
    started = []
    sup._start_child = lambda: started.append(True) or FakeChild()
    sup._stop_child = lambda timeout=15.0: 0
    sup.tick()
    assert started == [True]
    assert sup.lifecycle_state is not LifecycleState.WAITING_FOR_WINDOW
