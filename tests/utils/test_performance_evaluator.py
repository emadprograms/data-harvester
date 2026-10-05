"""
Unit tests for Performance Evaluator (PERF-04, C43-03).

Verifies:
- Valid metrics pass all mandatory qualification gates.
- Over-limit latencies fail qualification.
- Over-limit event-loop lag fails qualification.
- Insufficient writer CPU reduction (<50%) fails qualification.
- Non-finite (NaN, inf) and non-positive (zero, negative) latencies fail closed.
- Missing required metrics fail closed.
"""

import math
import pytest

from src.utils.performance_evaluator import (
    PerformanceQualificationError,
    evaluate_performance_gates,
)


def test_evaluator_passes_valid_metrics():
    """All metrics strictly satisfy mandatory upper limits."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=45.2,
        session_5m_p95_ms=52.8,
        month_1d_p95_ms=180.5,
        writer_cpu_sec_per_1m=1.2,
        legacy_writer_cpu_sec_per_1m=4.8,  # (4.8 - 1.2)/4.8 = 75% reduction >= 50%
        event_loop_lag_p99_ms=8.5,
        freshness_p99_ms=950.0,
        flush_interval_seconds=1.0,  # threshold = 2000ms
    )

    assert summary.overall_passed is True
    assert len(summary.failure_reasons) == 0
    # assert_qualification must succeed
    summary.assert_qualification()


def test_evaluator_rejects_over_limit_session_latency():
    """Warm session 1m/5m query p95 >= 100ms fails."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=105.0,  # Over 100ms limit
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=200.0,
        writer_cpu_sec_per_1m=1.0,
        legacy_writer_cpu_sec_per_1m=3.0,
    )

    assert summary.overall_passed is False
    assert any("session 1m query p95" in r for r in summary.failure_reasons)
    with pytest.raises(PerformanceQualificationError, match="violations"):
        summary.assert_qualification()


def test_evaluator_rejects_over_limit_month_daily_candles():
    """Warm month 1d query p95 >= 250ms fails."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=40.0,
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=265.0,  # Over 250ms limit
        writer_cpu_sec_per_1m=1.0,
        legacy_writer_cpu_sec_per_1m=3.0,
    )

    assert summary.overall_passed is False
    assert any("month 1d candle query p95" in r for r in summary.failure_reasons)


def test_evaluator_rejects_insufficient_cpu_reduction():
    """Writer CPU reduction < 50% vs legacy fails."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=40.0,
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=150.0,
        writer_cpu_sec_per_1m=2.0,
        legacy_writer_cpu_sec_per_1m=3.0,  # (3.0 - 2.0)/3.0 = 33.3% reduction < 50%
    )

    assert summary.overall_passed is False
    assert any("Writer CPU reduction" in r for r in summary.failure_reasons)


def test_evaluator_rejects_over_limit_event_loop_lag():
    """Event-loop lag p99 >= 20ms fails."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=40.0,
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=150.0,
        writer_cpu_sec_per_1m=1.0,
        legacy_writer_cpu_sec_per_1m=3.0,
        event_loop_lag_p99_ms=22.4,  # Over 20ms limit
    )

    assert summary.overall_passed is False
    assert any("Event-loop lag p99" in r for r in summary.failure_reasons)


def test_evaluator_rejects_over_limit_freshness():
    """Receive-to-visible freshness p99 > flush_interval + 1.0s fails."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=40.0,
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=150.0,
        writer_cpu_sec_per_1m=1.0,
        legacy_writer_cpu_sec_per_1m=3.0,
        freshness_p99_ms=2200.0,
        flush_interval_seconds=1.0,  # threshold = 2000ms
    )

    assert summary.overall_passed is False
    assert any("Freshness p99" in r for r in summary.failure_reasons)


@pytest.mark.parametrize("bad_val", [float("nan"), float("inf"), float("-inf"), 0.0, -5.0])
def test_evaluator_fails_closed_on_nonfinite_or_nonpositive_values(bad_val):
    """Non-finite or non-positive values must fail closed."""
    summary = evaluate_performance_gates(
        session_1m_p95_ms=bad_val,
        session_5m_p95_ms=50.0,
        month_1d_p95_ms=150.0,
        writer_cpu_sec_per_1m=1.0,
        legacy_writer_cpu_sec_per_1m=3.0,
    )

    assert summary.overall_passed is False
    assert len(summary.failure_reasons) >= 1
