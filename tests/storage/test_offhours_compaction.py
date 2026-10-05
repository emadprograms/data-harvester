"""SCHED-05 — unattended off-hours compaction.

The compactor must be safe to invite to the party every night, from a cron job,
from the supervisor, or twice by accident:

- once per closed interval, idempotent, and a no-op counts as success;
- one launcher at a time (a lease), with stale leases reclaimable;
- blocked honestly when the drain failed or a writer is still live;
- failures reported, never fatal, and retryable next interval.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.storage.offhours import (
    ALREADY_COMPLETED,
    BLOCKED_DRAIN_FAILED,
    COMPLETED,
    FAILED,
    LEASE_HELD,
    NOT_ELIGIBLE,
    acquire_lease,
    closed_interval_id,
    completed_intervals,
    release_lease,
    run_scheduled_compaction,
)
from tests.support.lake_population import create_lake, publish_rows

ET = ZoneInfo("America/New_York")


def et(*parts) -> datetime:
    return datetime(*parts, tzinfo=ET)


class StubCompactor:
    def __init__(self, outcome=None, error=None):
        self.outcome = outcome or {"status": "NOOP", "compacted_partitions": 0, "message": "nothing to do"}
        self.error = error
        self.calls = 0

    def compact(self, symbol=None, date_str=None, force=False):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.outcome


@pytest.fixture
def lake(tmp_path):
    return create_lake(tmp_path / "lake", symbols=["AAPL", "MSFT"])


@pytest.fixture
def notified():
    calls = []

    def notifier(event, detail=None, **kwargs):
        calls.append((event, detail, kwargs))

    notifier.calls = calls
    return notifier


# ------------------------------------------------------------ interval identity


def test_the_interval_is_stable_across_the_whole_closed_gap():
    evening = et(2026, 10, 6, 21, 0)
    night = et(2026, 10, 7, 2, 30)
    just_before_open = et(2026, 10, 7, 3, 59, 59)
    assert closed_interval_id(evening) == closed_interval_id(night) == closed_interval_id(just_before_open)
    assert closed_interval_id(evening) == et(2026, 10, 7, 4, 0).isoformat()


def test_the_interval_changes_once_the_next_window_opens():
    friday_night = et(2026, 10, 9, 21, 0)
    assert closed_interval_id(friday_night) == et(2026, 10, 12, 4, 0).isoformat()
    assert closed_interval_id(et(2026, 10, 12, 21, 0)) == et(2026, 10, 13, 4, 0).isoformat()
    # While a window is open there is no closed interval to key on: the id is the
    # current instant, and `run_scheduled_compaction` refuses to run anyway.
    monday_morning = et(2026, 10, 12, 4, 30)
    assert closed_interval_id(monday_morning) == monday_morning.isoformat()


# --------------------------------------------------------------- idempotence


def test_compaction_runs_once_per_interval(lake):
    compactor = StubCompactor({"status": "COMPACTED", "compacted_partitions": 3, "message": "3 partitions"})
    factory = lambda root: compactor

    first = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=factory)
    assert first["status"] == COMPLETED
    assert first["compacted"] == 3

    second = run_scheduled_compaction(lake, now=et(2026, 10, 7, 2, 30), compactor_factory=factory)
    assert second["status"] == ALREADY_COMPLETED
    assert second["previous"]["compacted_partitions"] == 3
    assert compactor.calls == 1, "compaction ran twice in the same closed interval"

    # The ledger records the interval on disk, so a restarted supervisor agrees.
    recorded = completed_intervals(lake)
    assert closed_interval_id(et(2026, 10, 6, 21, 0)) in recorded


def test_a_no_op_interval_counts_as_success(lake):
    compactor = StubCompactor({"status": "NOOP", "compacted_partitions": 0, "message": "No partitions qualify."})
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 22, 0), compactor_factory=lambda root: compactor)
    assert result["status"] == COMPLETED
    assert result["compacted"] == 0
    assert result["outcome"]["status"] == "NOOP"

    again = run_scheduled_compaction(lake, now=et(2026, 10, 6, 22, 5), compactor_factory=lambda root: compactor)
    assert again["status"] == ALREADY_COMPLETED, "a no-op interval must not be retried forever"
    assert compactor.calls == 1


def test_compaction_waits_while_the_window_is_open(lake):
    compactor = StubCompactor()
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 12, 0), compactor_factory=lambda root: compactor)
    assert result["status"] == NOT_ELIGIBLE
    assert "open" in result["message"]
    assert compactor.calls == 0, "compaction ran during ingestion"


# --------------------------------------------------------------------- lease


def test_a_held_lease_blocks_a_second_launcher(lake):
    assert acquire_lease(lake, owner="cron") is True
    try:
        compactor = StubCompactor()
        result = run_scheduled_compaction(
            lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: compactor
        )
        assert result["status"] == LEASE_HELD
        assert compactor.calls == 0
        # The second launcher did not complete the interval for the first one.
        assert closed_interval_id(et(2026, 10, 6, 21, 0)) not in completed_intervals(lake)
    finally:
        release_lease(lake)


def test_a_stale_lease_from_a_dead_process_is_reclaimed(lake):
    lease_path = lake / "_maintenance" / "compaction.lock"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    lease_path.write_text(
        json.dumps({"owner": "dead-cron", "pid": 999_999, "acquired_at": time.time() - 7200, "heartbeat": time.time() - 7200})
    )

    compactor = StubCompactor({"status": "NOOP", "compacted_partitions": 0})
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: compactor)
    assert result["status"] == COMPLETED, "a dead launcher's lease should not block compaction forever"
    assert compactor.calls == 1


def test_the_lease_is_released_after_every_outcome(lake):
    lease_path = lake / "_maintenance" / "compaction.lock"
    run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: StubCompactor())
    assert not lease_path.exists(), "the lease was not released after a successful run"

    failing = StubCompactor(error=RuntimeError("disk exploded"))
    run_scheduled_compaction(lake, now=et(2026, 10, 7, 1, 0), compactor_factory=lambda root: failing)
    assert not lease_path.exists(), "the lease was not released after a failure"


# ---------------------------------------------------------------- drain gate


def _write_writer_status(lake, status, heartbeat_age=None):
    control = lake / "_control"
    control.mkdir(parents=True, exist_ok=True)
    payload = {"status": status, "writer_id": "w1", "pid": os.getpid()}
    if heartbeat_age is not None:
        payload["heartbeat_monotonic"] = time.monotonic() - heartbeat_age
    (control / "writer_status.json").write_text(json.dumps(payload))


def test_a_failed_drain_blocks_compaction_and_is_reported(lake, notified):
    _write_writer_status(lake, "DRAIN_FAILED")
    compactor = StubCompactor()
    result = run_scheduled_compaction(
        lake,
        now=et(2026, 10, 6, 21, 0),
        compactor_factory=lambda root: compactor,
        notifier=notified,
    )
    assert result["status"] == BLOCKED_DRAIN_FAILED
    assert "DRAIN_FAILED" in result["message"]
    assert compactor.calls == 0, "compaction ran over un-drained accepted ticks"
    assert notified.calls and notified.calls[0][0] == "compaction_failed"

    # The interval is not marked complete, so a healthy retry can still run.
    assert closed_interval_id(et(2026, 10, 6, 21, 0)) not in completed_intervals(lake)


def test_a_live_writer_blocks_compaction(lake):
    _write_writer_status(lake, "RUNNING", heartbeat_age=1.0)
    compactor = StubCompactor()
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: compactor)
    assert result["status"] == BLOCKED_DRAIN_FAILED
    assert "live" in result["message"]
    assert compactor.calls == 0


def test_a_stale_live_status_does_not_block_forever(lake):
    """A crashed streamer leaves `RUNNING` behind; a stale heartbeat is not a live writer."""
    _write_writer_status(lake, "RUNNING", heartbeat_age=10_000.0)
    compactor = StubCompactor()
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: compactor)
    assert result["status"] == COMPLETED
    assert compactor.calls == 1


def test_a_clean_stop_does_not_block(lake):
    _write_writer_status(lake, "STOPPED")
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: StubCompactor())
    assert result["status"] == COMPLETED


# ------------------------------------------------------------------ failures


def test_a_compaction_failure_is_reported_not_fatal(lake, notified):
    failing = StubCompactor(error=RuntimeError("lake read-only"))
    result = run_scheduled_compaction(
        lake, now=et(2026, 10, 6, 21, 0), compactor_factory=lambda root: failing, notifier=notified
    )
    assert result["status"] == FAILED
    assert "lake read-only" in result["message"]
    assert notified.calls and notified.calls[0][0] == "compaction_failed"
    # Not recorded: the next launcher should retry rather than skip the interval.
    assert closed_interval_id(et(2026, 10, 6, 21, 0)) not in completed_intervals(lake)

    retry = run_scheduled_compaction(lake, now=et(2026, 10, 6, 22, 0), compactor_factory=lambda root: StubCompactor())
    assert retry["status"] == COMPLETED


def test_real_compactor_integration(lake):
    """The default factory drives the real LakeCompactor against a real lake."""
    publish_rows(
        lake,
        [
            ("2026-10-06 20:01:00.000000", "AAPL", 150.0, 1.0, None, None, "CAPITAL", "POST"),
            ("2026-10-06 20:01:00.500000", "AAPL", 150.2, 1.0, None, None, "CAPITAL", "POST"),
        ],
        writer_id="offhours_integration",
        sequence=1,
    )
    result = run_scheduled_compaction(lake, now=et(2026, 10, 6, 21, 0))
    assert result["status"] == COMPLETED, result
    assert result["outcome"]["status"] in ("NOOP", "COMPACTED", "COMPLETED")
