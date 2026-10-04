"""
Unit tests for tests/support/process_harness.py wait helpers.

Milestone v4.2 — Q02 (safe fixtures and independent oracles).

These helpers replace fixed ``time.sleep`` synchronization in the chaos/soak
suite. The port-conflict test in tests/integration/test_supervisor_chaos_soak.py
asserted after a hardcoded 0.8s sleep while the supervised child needed ~0.6s to
import and fail with EADDRINUSE, so crash detection raced the assertion and the
test failed on slower hosts. These tests pin the helpers' contract: bounded
waits, no early return before the predicate is true, no hang past the timeout,
and tolerance of transient predicate errors such as a child that has not yet
written its readiness file.
"""

import time

import pytest

from tests.support.process_harness import wait_until, wait_until_or_fail


def test_wait_until_returns_true_immediately_when_already_satisfied():
    started = time.monotonic()
    assert wait_until(lambda: True, timeout=5.0, interval=0.05) is True
    assert time.monotonic() - started < 1.0


def test_wait_until_polls_until_predicate_becomes_true():
    deadline = time.monotonic() + 0.4
    assert wait_until(lambda: time.monotonic() >= deadline, timeout=5.0, interval=0.02) is True


def test_wait_until_returns_false_after_timeout_without_predicate_success():
    started = time.monotonic()
    assert wait_until(lambda: False, timeout=0.3, interval=0.05) is False
    elapsed = time.monotonic() - started
    assert elapsed >= 0.3, f"returned early after {elapsed:.3f}s"
    assert elapsed < 2.0, f"overran timeout: {elapsed:.3f}s"


def test_wait_until_tolerates_predicates_that_raise_transiently():
    """A child's readiness file may not exist yet; the helper must keep polling."""
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise FileNotFoundError("readiness file not written yet")
        return True

    assert wait_until(flaky, timeout=5.0, interval=0.02) is True
    assert calls["n"] == 3


def test_wait_until_or_fail_returns_elapsed_time_on_success():
    elapsed = wait_until_or_fail(lambda: True, description="trivial condition", timeout=5.0)
    assert elapsed >= 0.0


def test_wait_until_or_fail_raises_with_timing_evidence():
    with pytest.raises(AssertionError) as excinfo:
        wait_until_or_fail(lambda: False, description="child process readiness", timeout=0.3, interval=0.05)

    message = str(excinfo.value)
    assert "child process readiness" in message
    assert "0.3s" in message
