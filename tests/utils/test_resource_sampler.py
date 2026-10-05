"""
Unit tests for ResourceSampler (PERF-02).

Verifies:
- Sampler continuously tracks peak RSS in background and catches transient memory spikes
  that start/end point-in-time snapshots miss.
- Sampler catches transient queue backlog bursts.
- Lifecycle management (context manager, start, stop, duration).
- Multi-target and dynamic target registration.
"""

from collections import deque
import gc
import os
import time

import psutil
import pytest

from src.utils.resource_sampler import ResourceSampler


def test_sampler_catches_transient_memory_spike():
    """Verify that a temporary memory spike occurring mid-run is recorded as peak RSS."""
    process = psutil.Process()
    baseline_rss_mb = process.memory_info().rss / (1024 * 1024)

    sampler = ResourceSampler(pids=[os.getpid()], interval_seconds=0.02)
    with sampler:
        time.sleep(0.05)
        # Allocate ~40MB transient buffer
        spike_payload = bytearray(40 * 1024 * 1024)
        for i in range(0, len(spike_payload), 4096):
            spike_payload[i] = 1  # Force physical page faulting
        time.sleep(0.08)  # Allow background sampler to observe the spike
        # Release transient buffer and collect garbage
        del spike_payload
        gc.collect()
        time.sleep(0.05)

    summary = sampler.get_summary()
    assert summary.sample_count >= 5
    # The peak RSS captured must reflect the ~40MB spike
    assert summary.peak_rss_mb >= baseline_rss_mb + 25.0, (
        f"Peak RSS ({summary.peak_rss_mb:.1f}MB) did not capture transient spike over baseline ({baseline_rss_mb:.1f}MB)"
    )


def test_sampler_catches_transient_queue_backlog_burst():
    """Verify that a sudden queue backlog burst that drains before completion is captured."""
    simulated_queue = deque()

    sampler = ResourceSampler(
        pids=[os.getpid()],
        queue_source=lambda: len(simulated_queue),
        interval_seconds=0.02,
    )

    with sampler:
        assert len(simulated_queue) == 0
        time.sleep(0.04)
        # Transient burst: enqueue 450 items
        for i in range(450):
            simulated_queue.append(i)
        time.sleep(0.08)  # Let sampler tick
        # Drain completely before exiting
        simulated_queue.clear()
        time.sleep(0.04)

    summary = sampler.get_summary()
    assert summary.peak_queue_backlog >= 450, (
        f"Peak queue backlog ({summary.peak_queue_backlog}) failed to capture 450 item burst"
    )


def test_sampler_lifecycle_and_idempotence():
    """Verify start/stop idempotence and summary statistics structure."""
    sampler = ResourceSampler(pids=[os.getpid()], interval_seconds=0.02)
    sampler.start()
    sampler.start()  # Idempotent start
    time.sleep(0.06)
    summary1 = sampler.get_summary()
    assert summary1.sample_count >= 2
    assert summary1.duration_seconds > 0

    summary2 = sampler.stop()
    assert summary2.sample_count >= summary1.sample_count
    # Second stop is safe
    summary3 = sampler.stop()
    assert summary3.sample_count == summary2.sample_count


def test_sampler_dynamic_target_registration():
    """Verify dynamic target registration during execution."""
    sampler = ResourceSampler(pids=[os.getpid()], interval_seconds=0.02)
    with sampler:
        time.sleep(0.04)
        sampler.register_target("self_alias", os.getpid())
        time.sleep(0.06)

    summary = sampler.get_summary()
    assert "self_alias" in summary.per_process_peak_rss_mb
    assert summary.per_process_peak_rss_mb["self_alias"] > 0
