"""
Continuous Background Resource Sampler (PERF-02).

Replaces point-in-time max(start, end) snapshots with a dedicated background
thread continuously sampling per-process and aggregate RSS (MB), CPU%, and queue
backlog at high resolution (e.g. 50ms intervals).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import gc
import os
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import psutil


@dataclass
class ResourceSample:
    """Instantaneous snapshot of resource utilization across monitored entities."""
    timestamp: float
    aggregate_rss_mb: float
    aggregate_cpu_percent: float
    per_process_rss_mb: Dict[str, float]
    per_process_cpu_percent: Dict[str, float]
    queue_backlog: int = 0


@dataclass
class ResourceSummary:
    """Summary of peak and final resource consumption observed by the sampler."""
    peak_rss_mb: float
    peak_aggregate_rss_mb: float
    peak_cpu_percent: float
    peak_queue_backlog: int
    sample_count: int
    duration_seconds: float
    per_process_peak_rss_mb: Dict[str, float] = field(default_factory=dict)
    per_process_peak_cpu_percent: Dict[str, float] = field(default_factory=dict)
    start_rss_mb: float = 0.0
    end_rss_mb: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ResourceSampler:
    """High-resolution background sampler for memory, CPU, and queue depth.
    
    Guarantees:
    - Daemon thread sampling at configurable interval (default 50ms = 0.05s).
    - Tracks per-process and aggregate RSS across specified processes or child processes.
    - Tracks queue depth / backlog bursts that drain before execution ends.
    - Idempotent lifecycle (start, stop, context manager).
    """

    def __init__(
        self,
        pids: Optional[Sequence[int]] = None,
        named_targets: Optional[Dict[str, Union[int, subprocess.Popen, psutil.Process]]] = None,
        target_provider: Optional[Callable[[], Dict[str, Union[int, subprocess.Popen, psutil.Process]]]] = None,
        queue_source: Optional[Union[Callable[[], int], Any]] = None,
        interval_seconds: float = 0.05,
        include_children: bool = True,
        max_retained_samples: int = 5000,
    ) -> None:
        self.interval_seconds = max(0.005, float(interval_seconds))
        self.include_children = bool(include_children)
        self.max_retained_samples = int(max_retained_samples)
        self.queue_source = queue_source
        self.target_provider = target_provider

        self._pids: List[int] = list(pids) if pids is not None else [os.getpid()]
        self._named_targets: Dict[str, Any] = dict(named_targets or {})
        if not self._named_targets and self._pids:
            for pid in self._pids:
                self._named_targets[str(pid)] = pid

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._start_time: float = 0.0
        self._stop_time: float = 0.0
        self._start_rss_mb: float = 0.0
        self._end_rss_mb: float = 0.0

        self._peak_aggregate_rss_mb: float = 0.0
        self._peak_aggregate_cpu_percent: float = 0.0
        self._peak_queue_backlog: int = 0
        self._per_process_peak_rss_mb: Dict[str, float] = {}
        self._per_process_peak_cpu: Dict[str, float] = {}
        self._sample_count: int = 0
        self._samples: List[ResourceSample] = []

    def register_target(self, name: str, target: Union[int, subprocess.Popen, psutil.Process]) -> None:
        """Dynamically add a target to be monitored by the sampler."""
        with self._lock:
            self._named_targets[str(name)] = target

    def _resolve_pid(self, target: Any) -> Optional[int]:
        if isinstance(target, int):
            return target
        if hasattr(target, "pid"):
            return target.pid
        return None

    def _sample_queue_backlog(self) -> int:
        if self.queue_source is None:
            return 0
        try:
            if callable(self.queue_source):
                return int(self.queue_source())
            if hasattr(self.queue_source, "qsize"):
                return int(self.queue_source.qsize())
            if hasattr(self.queue_source, "__len__"):
                return len(self.queue_source)
        except Exception:
            pass
        return 0

    def _sample_once(self) -> None:
        now = time.monotonic()
        targets: Dict[str, Any] = {}
        with self._lock:
            targets.update(self._named_targets)

        if self.target_provider is not None:
            try:
                provided = self.target_provider()
                if isinstance(provided, dict):
                    targets.update(provided)
                elif isinstance(provided, (list, tuple)):
                    for item in provided:
                        pid = self._resolve_pid(item)
                        if pid is not None:
                            targets[str(pid)] = pid
            except Exception:
                pass

        total_rss_bytes = 0
        total_cpu_pct = 0.0
        rss_per_target: Dict[str, float] = {}
        cpu_per_target: Dict[str, float] = {}

        seen_pids = set()
        seen_cpu_pids = set()

        for name, target in list(targets.items()):
            pid = self._resolve_pid(target)
            if pid is None:
                continue

            try:
                p = psutil.Process(pid)
                if not p.is_running():
                    continue

                # Sample memory and CPU per target
                mem = p.memory_info()
                rss_mb = mem.rss / (1024 * 1024)
                rss_per_target[name] = round(rss_mb, 2)

                # Avoid double-counting aggregate RSS
                if pid not in seen_pids:
                    seen_pids.add(pid)
                    total_rss_bytes += mem.rss

                    # Child processes if requested
                    if self.include_children:
                        try:
                            for child in p.children(recursive=True):
                                if child.pid not in seen_pids and child.is_running():
                                    seen_pids.add(child.pid)
                                    c_mem = child.memory_info()
                                    total_rss_bytes += c_mem.rss
                                    try:
                                        total_cpu_pct += child.cpu_percent(interval=None)
                                    except Exception:
                                        pass
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass

                # psutil cpu_percent (non-blocking)
                try:
                    cpu_pct = p.cpu_percent(interval=None)
                    cpu_per_target[name] = round(cpu_pct, 2)
                    if pid not in seen_cpu_pids:
                        seen_cpu_pids.add(pid)
                        total_cpu_pct += cpu_pct
                except Exception:
                    pass

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        agg_rss_mb = round(total_rss_bytes / (1024 * 1024), 2)
        agg_cpu_pct = round(total_cpu_pct, 2)
        backlog = self._sample_queue_backlog()

        with self._lock:
            self._sample_count += 1
            if agg_rss_mb > self._peak_aggregate_rss_mb:
                self._peak_aggregate_rss_mb = agg_rss_mb
            if agg_cpu_pct > self._peak_aggregate_cpu_percent:
                self._peak_aggregate_cpu_percent = agg_cpu_pct
            if backlog > self._peak_queue_backlog:
                self._peak_queue_backlog = backlog

            for name, rss in rss_per_target.items():
                if rss > self._per_process_peak_rss_mb.get(name, 0.0):
                    self._per_process_peak_rss_mb[name] = rss

            for name, cpu_val in cpu_per_target.items():
                if cpu_val > self._per_process_peak_cpu.get(name, 0.0):
                    self._per_process_peak_cpu[name] = cpu_val

            if len(self._samples) < self.max_retained_samples:
                self._samples.append(
                    ResourceSample(
                        timestamp=now,
                        aggregate_rss_mb=agg_rss_mb,
                        aggregate_cpu_percent=agg_cpu_pct,
                        per_process_rss_mb=rss_per_target,
                        per_process_cpu_percent=cpu_per_target,
                        queue_backlog=backlog,
                    )
                )

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._sample_once()
            self._stop_event.wait(timeout=self.interval_seconds)
        # Final sample upon stop
        self._sample_once()

    def start(self) -> ResourceSampler:
        """Start the background sampler thread."""
        if self._thread is not None and self._thread.is_alive():
            return self

        self._start_time = time.monotonic()
        self._stop_event.clear()

        # Prime initial snapshot
        self._sample_once()
        self._start_rss_mb = self._peak_aggregate_rss_mb

        self._thread = threading.Thread(target=self._run, daemon=True, name="ResourceSamplerThread")
        self._thread.start()
        return self

    def stop(self) -> ResourceSummary:
        """Stop background sampling and return the calculated summary."""
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._stop_time = time.monotonic()
        self._end_rss_mb = self._peak_aggregate_rss_mb
        return self.get_summary()

    def get_summary(self) -> ResourceSummary:
        """Obtain current resource metrics summary without stopping."""
        with self._lock:
            duration = (self._stop_time or time.monotonic()) - (self._start_time or time.monotonic())
            return ResourceSummary(
                peak_rss_mb=self._peak_aggregate_rss_mb,
                peak_aggregate_rss_mb=self._peak_aggregate_rss_mb,
                peak_cpu_percent=self._peak_aggregate_cpu_percent,
                peak_queue_backlog=self._peak_queue_backlog,
                sample_count=self._sample_count,
                duration_seconds=round(max(0.0, duration), 3),
                per_process_peak_rss_mb=dict(self._per_process_peak_rss_mb),
                per_process_peak_cpu_percent=dict(self._per_process_peak_cpu),
                start_rss_mb=round(self._start_rss_mb, 2),
                end_rss_mb=round(self._end_rss_mb, 2),
            )

    def __enter__(self) -> ResourceSampler:
        return self.start()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()
