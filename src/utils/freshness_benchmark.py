"""
Real Receive-to-Visible Freshness Measurement (PERF-03, C43-04).

Measures arrival-to-visible latency per tick ID through actual StreamingEngine callbacks
and an independent in-memory TickLakeReader process using monotonic time.
Enforces healthy-load p99 latency <= configured flush interval + 1.0s.
Supports injecting artificial delays to demonstrate truthful SLA failure.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import duckdb

from src.storage.config import init_tick_lake
from src.storage.registry import SymbolRegistry, init_registry
from src.stream.runner import StreamingEngine

logger = logging.getLogger("freshness_benchmark")


@dataclass
class FreshnessBenchmarkReport:
    """Consolidated freshness measurement report and SLA assertion."""
    total_ticks: int
    visible_ticks: int
    flush_interval_seconds: float
    sla_threshold_seconds: float
    sla_threshold_ms: float
    p50_latency_ms: float
    p90_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    sla_passed: bool
    latencies_ms: List[float] = field(default_factory=list)
    host_environment: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def assert_sla(self) -> None:
        """Assert healthy-load p99 latency <= flush_interval + 1.0s.
        
        Fail-closed: requires at least one visible measurement, non-zero and finite latency.
        """
        assert self.visible_ticks > 0, "Zero ticks were visible to the independent reader"
        assert self.p99_latency_ms > 0.0, f"Non-positive p99 latency: {self.p99_latency_ms}"
        assert self.p99_latency_ms <= self.sla_threshold_ms, (
            f"Freshness SLA violation: p99 latency {self.p99_latency_ms:.2f}ms exceeds "
            f"threshold {self.sla_threshold_ms:.2f}ms (flush_interval {self.flush_interval_seconds}s + 1.0s)"
        )


def _compute_percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    idx = (pct / 100.0) * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    weight = idx - lower
    return round(sorted_vals[lower] * (1.0 - weight) + sorted_vals[upper] * weight, 3)


def _independent_reader_worker(
    lake_root_str: str,
    tracked_ids: List[str],
    result_queue: mp.Queue,
    stop_event: mp.Event,
    poll_interval_s: float = 0.02,
    artificial_delay_s: float = 0.0,
) -> None:
    """Worker function executed in an independent process querying Parquet via in-memory DuckDB."""
    lake_root = Path(lake_root_str)
    ticks_dir = lake_root / "ticks"
    remaining = set(tracked_ids)
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET threads = 2")

    try:
        while not stop_event.is_set() and remaining:
            parquet_files = [str(f) for f in ticks_dir.glob("symbol=*/date=*/*.parquet")]
            if parquet_files:
                try:
                    res = con.execute(
                        "SELECT ingest_id FROM read_parquet(?, hive_partitioning=false)",
                        [parquet_files],
                    ).fetchall()
                    now_m = time.monotonic()
                    if artificial_delay_s > 0.0:
                        time.sleep(artificial_delay_s)
                        now_m = time.monotonic()

                    for (seen_id,) in res:
                        if seen_id in remaining:
                            remaining.remove(seen_id)
                            result_queue.put((seen_id, now_m))
                except Exception:
                    pass

            time.sleep(poll_interval_s)
    finally:
        con.close()


async def _run_freshness_ingest(
    engine: StreamingEngine,
    symbols: List[str],
    tracked_ids: List[str],
    tick_interval_s: float,
) -> Dict[str, float]:
    """Feed ticks through real StreamingEngine callbacks, recording monotonic arrival times."""
    arrival_times: Dict[str, float] = {}
    base_time = datetime(2026, 10, 2, 13, 30, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    for i, tick_id in enumerate(tracked_ids):
        sym = symbols[i % len(symbols)]
        tick_payload = {
            "epic": sym,
            "symbol": sym,
            "price": 100.0 + (i % 20) * 0.5,
            "volume": 1.0,
            "bid": 99.9,
            "ask": 100.1,
            "timestamp": base_time,
            "source": "CAPITAL",
            "session": "REG",
            "ingest_id": tick_id,
        }
        arrival_m = time.monotonic()
        arrival_times[tick_id] = arrival_m
        await engine._handle_capital_tick(tick_payload)
        if tick_interval_s > 0:
            await asyncio.sleep(tick_interval_s)

    return arrival_times


def measure_arrival_to_visible_freshness(
    lake_root: Union[str, Path],
    flush_interval_seconds: float = 0.5,
    tick_count: int = 100,
    symbols: Optional[Sequence[str]] = None,
    poll_interval_s: float = 0.02,
    tick_interval_s: float = 0.005,
    artificial_delay_s: float = 0.0,
    timeout_seconds: float = 15.0,
) -> FreshnessBenchmarkReport:
    """Orchestrates real arrival-to-visible freshness measurement.
    
    1. Sets up isolated lake and registers symbols.
    2. Initializes and starts StreamingEngine with given flush_interval_seconds.
    3. Spawns independent reader process executing in-memory DuckDB queries.
    4. Submits ticks via StreamingEngine._handle_capital_tick callback.
    5. Measures monotonic delta until row is returned by reader query.
    6. Returns FreshnessBenchmarkReport with p50, p90, p95, p99, and SLA validation.
    """
    import uuid
    lake_path = Path(lake_root).resolve()
    init_tick_lake(lake_path)
    init_registry(lake_path)
    test_symbols = list(symbols or ["NVDA", "AAPL", "MSFT"])

    # Ensure symbols are registered in lake registry
    reg = SymbolRegistry(lake_path)
    for s in test_symbols:
        try:
            reg.add_symbol(s, capital_ticker=s)
        except Exception:
            pass

    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    stop_event = ctx.Event()

    engine = StreamingEngine(
        lake_root=lake_path,
        flush_interval=flush_interval_seconds,
        max_batch_rows=50,
        writer_id="freshness_engine_writer",
        mock_mode=True,
        mock_ticks_per_sec=0.0,
    )
    # Ensure active symbols are recognized
    for s in test_symbols:
        engine.active_streaming_symbols.add(s)
        engine.epic_to_display[s] = s

    run_token = uuid.uuid4().hex[:8]
    tracked_ids = [f"fresh_sla_{os.getpid()}_{run_token}_{i:05d}" for i in range(tick_count)]

    # Start reader subprocess
    reader_proc = ctx.Process(
        target=_independent_reader_worker,
        args=(
            str(lake_path),
            tracked_ids,
            result_queue,
            stop_event,
            poll_interval_s,
            artificial_delay_s,
        ),
        daemon=True,
    )
    reader_proc.start()

    arrival_times: Dict[str, float] = {}
    visible_times: Dict[str, float] = {}

    async def _async_runner():
        nonlocal arrival_times
        engine_task = asyncio.create_task(engine.start())
        # Wait for engine to start
        for _ in range(100):
            if engine.running:
                break
            await asyncio.sleep(0.01)

        # Ingest ticks
        arrival_times = await _run_freshness_ingest(
            engine=engine,
            symbols=test_symbols,
            tracked_ids=tracked_ids,
            tick_interval_s=tick_interval_s,
        )
        # Await ticks visibility or timeout
        deadline = time.monotonic() + timeout_seconds
        while len(visible_times) < len(arrival_times) and time.monotonic() < deadline:
            while not result_queue.empty():
                try:
                    seen_id, vis_m = result_queue.get_nowait()
                    visible_times[seen_id] = vis_m
                except Exception:
                    break
            await asyncio.sleep(0.02)

        # Stop engine cleanly
        await engine.shutdown()
        engine_task.cancel()
        try:
            await engine_task
        except asyncio.CancelledError:
            pass

    try:
        asyncio.run(_async_runner())
    finally:
        stop_event.set()
        reader_proc.join(timeout=2.0)
        if reader_proc.is_alive():
            reader_proc.terminate()

    # Drain any remaining results in queue
    while not result_queue.empty():
        try:
            seen_id, vis_m = result_queue.get_nowait()
            visible_times[seen_id] = vis_m
        except Exception:
            break

    # Calculate latencies
    latencies_ms: List[float] = []
    for tid, arr_time in arrival_times.items():
        if tid in visible_times:
            lat = max(0.0, (visible_times[tid] - arr_time) * 1000.0)
            latencies_ms.append(lat)

    sorted_lats = sorted(latencies_ms)
    sla_thresh_ms = (flush_interval_seconds + 1.0) * 1000.0
    p50 = _compute_percentile(sorted_lats, 50.0)
    p90 = _compute_percentile(sorted_lats, 90.0)
    p95 = _compute_percentile(sorted_lats, 95.0)
    p99 = _compute_percentile(sorted_lats, 99.0)
    max_lat = round(max(sorted_lats), 3) if sorted_lats else 0.0
    sla_passed = bool(sorted_lats and p99 <= sla_thresh_ms)

    return FreshnessBenchmarkReport(
        total_ticks=len(arrival_times),
        visible_ticks=len(latencies_ms),
        flush_interval_seconds=flush_interval_seconds,
        sla_threshold_seconds=flush_interval_seconds + 1.0,
        sla_threshold_ms=sla_thresh_ms,
        p50_latency_ms=p50,
        p90_latency_ms=p90,
        p95_latency_ms=p95,
        p99_latency_ms=p99,
        max_latency_ms=max_lat,
        sla_passed=sla_passed,
        latencies_ms=sorted_lats,
        host_environment={
            "cpu_count": os.cpu_count(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )
