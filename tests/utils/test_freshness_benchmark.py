"""
Unit tests for Arrival-to-Visible Freshness Measurement (PERF-03, C43-04).

Verifies:
- Healthy-load p99 latency <= configured flush interval + 1.0s.
- Injected artificial delay causes truthful SLA assertion failure.
"""

import pytest

from src.utils.freshness_benchmark import (
    FreshnessBenchmarkReport,
    measure_arrival_to_visible_freshness,
)


def test_arrival_to_visible_freshness_healthy_load_passes_sla(tmp_path):
    """Healthy load: arrival-to-visible p99 must be <= flush_interval + 1.0s."""
    lake_root = tmp_path / "freshness_lake"
    report = measure_arrival_to_visible_freshness(
        lake_root=lake_root,
        flush_interval_seconds=0.3,
        tick_count=30,
        symbols=["NVDA", "AAPL"],
        poll_interval_s=0.02,
        tick_interval_s=0.005,
        artificial_delay_s=0.0,
        timeout_seconds=8.0,
    )

    assert report.total_ticks == 30
    assert report.visible_ticks == 30
    assert report.sla_passed is True
    # Verify SLA threshold calculation
    assert report.sla_threshold_seconds == pytest.approx(1.3, rel=1e-3)
    assert report.sla_threshold_ms == pytest.approx(1300.0, rel=1e-3)
    assert report.p99_latency_ms <= report.sla_threshold_ms
    # assert_sla() must succeed without error
    report.assert_sla()


def test_arrival_to_visible_freshness_detects_artificial_delay_violation(tmp_path):
    """Injected artificial delay: proves that the test truthfully fails when latency violates SLA."""
    lake_root = tmp_path / "delayed_freshness_lake"
    # flush_interval=0.2s -> threshold = 1.2s (1200ms)
    # artificial_delay=1.5s -> latencies will be >= 1500ms > 1200ms
    report = measure_arrival_to_visible_freshness(
        lake_root=lake_root,
        flush_interval_seconds=0.2,
        tick_count=15,
        symbols=["NVDA"],
        poll_interval_s=0.02,
        tick_interval_s=0.005,
        artificial_delay_s=1.5,
        timeout_seconds=10.0,
    )

    assert report.visible_ticks > 0
    assert report.sla_passed is False
    assert report.p99_latency_ms > report.sla_threshold_ms

    with pytest.raises(AssertionError, match="Freshness SLA violation"):
        report.assert_sla()
