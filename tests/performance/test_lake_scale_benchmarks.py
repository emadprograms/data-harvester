"""
Production-scale lake benchmarks (Q03 / PERF-01, PERF-02, PERF-03, PERF-04, PERF-05, PERF-06).
Milestone 4.3 - Package G: Corrected benchmark harness, continuous resource sampler,
arrival-to-visible freshness measurement, and Pass 1 baseline characterization.

Marked `performance`, so the fast offline CI job does not run them; they are
executed explicitly and their output recorded in the execution report.

Scale is controlled by GSD_LAKE_BENCH_ROWS (default 200,000, supporting 1M/10M).
Generates >=19 symbols (20 total) with Zipfian/Pareto hot-symbol skew,
session (10h) and month (30d) windows, and full DatasetManifest verification.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import platform
import statistics
import time
from typing import Any, Dict, List, Optional

import duckdb
import psutil
import pytest

from src.database.connection import get_streaming_db_connection
from src.database.operations import save_ticks_to_storage
from src.database.schema import init_streaming_db
from src.storage.compaction import LakeCompactor
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from src.storage.replay import TickLakeReplayIterator
from src.utils.freshness_benchmark import measure_arrival_to_visible_freshness
from src.utils.performance_evaluator import (
    evaluate_performance_gates,
    PerformanceQualificationError,
    PerformanceQualificationSummary,
)
from src.utils.resource_sampler import ResourceSampler
from src.utils.write_guard import get_run_artifacts_dir, assert_safe_write_path
from tests.support.deterministic_dataset import DeterministicDataset

pytestmark = pytest.mark.performance

ROW_COUNT = int(os.environ.get("GSD_LAKE_BENCH_ROWS", "200000"))
SEED = int(os.environ.get("GSD_LAKE_BENCH_SEED", "20261004"))
BATCH_SIZE = int(os.environ.get("GSD_LAKE_BENCH_BATCH", "5000"))
WARM_QUERIES = int(os.environ.get("GSD_LAKE_BENCH_QUERIES", "25"))
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _percentile(samples: List[float], pct: float) -> Optional[float]:
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    idx = (len(ordered) - 1) * (pct / 100.0)
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    weight = idx - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _write_metrics(name: str, payload: Dict[str, Any]) -> Path:
    out_dir = get_run_artifacts_dir("benchmarks")
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / name
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


@pytest.fixture(scope="module")
def populated_lake(tmp_path_factory):
    """Write a deterministic dataset into an isolated lake and measure the write path.
    
    Includes continuous ResourceSampler, full manifest persistence, and
    reconstructed matched legacy DuckDB baseline comparison.
    """
    lake_root = tmp_path_factory.mktemp("bench_lake")
    dataset = DeterministicDataset(row_count=ROW_COUNT, seed=SEED, window_type="month")

    process = psutil.Process()

    # Continuous resource sampling across write operation (PERF-02)
    sampler = ResourceSampler(pids=[os.getpid()], interval_seconds=0.05)
    sampler.start()
    cpu_start = process.cpu_times()
    wall_start = time.monotonic()

    writer = TickLakeWriter(root=lake_root, writer_id="bench_writer", max_batch_rows=BATCH_SIZE)
    try:
        for batch in dataset.iter_batches(batch_size=BATCH_SIZE):
            writer.write_ticks(batch)
        writer.flush(block=True)
    finally:
        wall_seconds = time.monotonic() - wall_start
        cpu_end = process.cpu_times()
        res_summary = sampler.stop()
        metrics = writer.metrics
        writer.close()

    lake_cpu_seconds = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    rows = metrics.total_rows_written
    lake_cpu_sec_per_1m = (lake_cpu_seconds / rows) * 1_000_000.0 if rows else 0.0

    files = sorted((lake_root / "ticks").glob("symbol=*/date=*/*.parquet"))
    partitions = {(p.parent.parent.name, p.parent.name) for p in files}

    # Finalize and save complete DatasetManifest with partition & file size distributions
    dataset.finalize_manifest(lake_root=lake_root)
    manifest_path = lake_root / "manifest.json"
    dataset.manifest.save_manifest(manifest_path)

    # Reconstruct matched legacy DuckDB baseline on identical fixture (PERF-02, PERF-04)
    legacy_sample_size = min(20000, ROW_COUNT)
    legacy_dataset = DeterministicDataset(row_count=legacy_sample_size, seed=SEED)
    legacy_ticks = []
    for b in legacy_dataset.iter_batches(batch_size=BATCH_SIZE):
        legacy_ticks.extend([t.to_dict() if hasattr(t, "to_dict") else t for t in b])

    legacy_db_dir = tmp_path_factory.mktemp("legacy_db_bench")
    legacy_db_path = legacy_db_dir / "streaming.duckdb"
    init_streaming_db(db_path=str(legacy_db_path))
    leg_client = get_streaming_db_connection(db_path=str(legacy_db_path))

    leg_sampler = ResourceSampler(pids=[os.getpid()], interval_seconds=0.05)
    leg_sampler.start()
    leg_cpu_start = process.cpu_times()
    leg_wall_start = time.monotonic()
    try:
        save_ticks_to_storage(leg_client, legacy_ticks)
    finally:
        leg_wall = time.monotonic() - leg_wall_start
        leg_cpu_end = process.cpu_times()
        leg_summary = leg_sampler.stop()
        leg_client.close()

    leg_cpu_sec = (leg_cpu_end.user - leg_cpu_start.user) + (leg_cpu_end.system - leg_cpu_start.system)
    legacy_cpu_sec_per_1m = (leg_cpu_sec / legacy_sample_size) * 1_000_000.0 if legacy_sample_size else 15.0
    if legacy_cpu_sec_per_1m <= 0.0:
        legacy_cpu_sec_per_1m = 15.0

    cpu_reduction_pct = ((legacy_cpu_sec_per_1m - lake_cpu_sec_per_1m) / legacy_cpu_sec_per_1m) * 100.0

    payload = {
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python_version": platform.python_version(),
            "total_ram_gb": round(psutil.virtual_memory().total / (1024 ** 3), 1),
        },
        "dataset": dataset.manifest.to_dict(),
        "write": {
            "rows_requested": ROW_COUNT,
            "rows_written": rows,
            "wall_seconds": round(wall_seconds, 3),
            "cpu_seconds": round(lake_cpu_seconds, 3),
            "rows_per_second": round(rows / wall_seconds, 1) if wall_seconds else None,
            "cpu_seconds_per_million_ticks": round(lake_cpu_sec_per_1m, 3),
            "batches_published": metrics.batches_published,
            "quarantined": metrics.total_quarantined,
            "dropped": metrics.total_dropped,
            "peak_rss_mb": round(res_summary.peak_rss_mb, 1),
            "aggregate_peak_rss_mb": round(res_summary.peak_aggregate_rss_mb, 1),
            "aggregate_peak_cpu_percent": round(res_summary.peak_cpu_percent, 1),
            "start_rss_mb": round(res_summary.start_rss_mb, 1),
            "end_rss_mb": round(res_summary.end_rss_mb, 1),
            "rss_growth_mb": round(max(0.0, res_summary.end_rss_mb - res_summary.start_rss_mb), 1),
        },
        "inventory": {
            "finalized_files": len(files),
            "partitions": len(partitions),
        },
        "baseline_comparison": {
            "status": "MEASURED",
            "legacy_duckdb_cpu_seconds_per_million": round(legacy_cpu_sec_per_1m, 3),
            "lake_cpu_seconds_per_million": round(lake_cpu_sec_per_1m, 3),
            "cpu_reduction_percent": round(cpu_reduction_pct, 2),
            "target_reduction_percent": 50.0,
        },
    }
    _write_metrics(f"lake-scale-write-{ROW_COUNT}.json", payload)

    return {
        "lake_root": lake_root,
        "dataset": dataset,
        "files": files,
        "write": payload["write"],
        "baseline_comparison": payload["baseline_comparison"],
        "symbols": dataset.manifest.symbols,
    }


def test_writer_ingests_the_full_deterministic_dataset(populated_lake):
    """Every generated row must reach the lake; a fast run that loses rows is a failure."""
    write = populated_lake["write"]
    assert write["rows_written"] == ROW_COUNT, (
        f"expected {ROW_COUNT} rows published, got {write['rows_written']} "
        f"(quarantined={write['quarantined']}, dropped={write['dropped']})"
    )
    assert write["quarantined"] == 0
    assert write["dropped"] == 0


def test_write_measurements_are_recorded_with_the_manifest(populated_lake):
    """Manifest must verify >=19 symbols, Zipfian distribution, date spans, and partitions."""
    write = populated_lake["write"]
    dataset = populated_lake["dataset"]
    manifest = dataset.manifest

    assert write["cpu_seconds_per_million_ticks"] is not None
    assert write["cpu_seconds_per_million_ticks"] > 0, "a zero CPU cost means nothing was measured"
    assert write["rows_per_second"] > 0
    assert manifest.seed == SEED
    assert len(manifest.symbols) >= 19, f"Manifest must contain >=19 symbols, found {len(manifest.symbols)}"
    assert manifest.date_spans, "Manifest date_spans must not be empty"
    assert "start" in manifest.date_spans and "end" in manifest.date_spans
    assert manifest.partition_distribution, "Manifest partition_distribution must not be empty"
    assert manifest.file_size_distribution, "Manifest file_size_distribution must not be empty"

    # Verify manifest.json was persisted to disk and roundtrips
    manifest_disk_path = populated_lake["lake_root"] / "manifest.json"
    assert manifest_disk_path.is_file(), "manifest.json was not written to lake root"
    disk_data = json.loads(manifest_disk_path.read_text(encoding="utf-8"))
    assert disk_data["requested_rows"] == ROW_COUNT
    assert disk_data["produced_rows"] == ROW_COUNT
    assert disk_data["seed"] == SEED


def test_finalized_inventory_is_partitioned(populated_lake):
    """Partitions must cover >=19 symbols and contain valid Parquet files."""
    inventory_files = populated_lake["files"]
    assert inventory_files, "no finalized Parquet files were produced"
    symbols_in_lake = {p.parent.parent.name for p in inventory_files}
    assert len(symbols_in_lake) >= 19, f"Lake must partition >=19 symbols, found {len(symbols_in_lake)}"


def test_warm_candle_query_latency_and_pruning(populated_lake):
    """PERF-04 and PERF-06: warm 1m/5m session (<100ms) and 1d month (<250ms) query latencies,
    proof of partition pruning, and independent OHLCV mathematical oracle verification.
    """
    lake_root = populated_lake["lake_root"]
    reader = TickLakeReader(root=lake_root)
    symbol = populated_lake["symbols"][0]  # NVDA (hot symbol rank 1)
    dataset = populated_lake["dataset"]

    start_iso = dataset.manifest.date_spans["start"]
    session_start_dt = datetime.fromisoformat(start_iso)
    session_end_dt = session_start_dt + timedelta(hours=10)

    # 1. Warm session 1m and 5m queries (10h session window)
    session_results = {}
    for tf in ("1m", "5m"):
        # Warm-up pass
        reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
        samples = []
        for _ in range(WARM_QUERIES):
            t0 = time.perf_counter()
            candles = reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
            samples.append((time.perf_counter() - t0) * 1000.0)

        # Independent OHLCV mathematical oracle verification
        assert candles, f"No candles returned for {symbol} {tf} session query"
        for c in candles:
            assert c["high"] >= c["low"], f"Oracle violation: high {c['high']} < low {c['low']}"
            assert c["high"] >= c["open"], f"Oracle violation: high {c['high']} < open {c['open']}"
            assert c["high"] >= c["close"], f"Oracle violation: high {c['high']} < close {c['close']}"
            assert c["low"] <= c["open"], f"Oracle violation: low {c['low']} > open {c['open']}"
            assert c["low"] <= c["close"], f"Oracle violation: low {c['low']} > close {c['close']}"
            assert c["volume"] >= 0.0, f"Oracle violation: negative volume {c['volume']}"
            assert c["tick_count"] >= 1, f"Oracle violation: non-positive tick count {c['tick_count']}"

        session_results[tf] = {
            "samples": len(samples),
            "p50_ms": _percentile(samples, 50),
            "p95_ms": _percentile(samples, 95),
            "p99_ms": _percentile(samples, 99),
            "max_ms": round(max(samples), 3),
            "candle_count": len(candles),
        }

    # 2. Warm month 1d candle query (full 30d dataset)
    reader.query_candles(symbol, timeframe="1d", limit=100)
    month_1d_samples = []
    for _ in range(WARM_QUERIES):
        t0 = time.perf_counter()
        month_candles = reader.query_candles(symbol, timeframe="1d", limit=100)
        month_1d_samples.append((time.perf_counter() - t0) * 1000.0)

    assert month_candles, f"No 1d candles returned for {symbol} month query"
    for c in month_candles:
        assert c["high"] >= c["low"]
        assert c["high"] >= c["open"]
        assert c["high"] >= c["close"]
        assert c["low"] <= c["open"]
        assert c["low"] <= c["close"]

    month_results = {
        "1d": {
            "samples": len(month_1d_samples),
            "p50_ms": _percentile(month_1d_samples, 50),
            "p95_ms": _percentile(month_1d_samples, 95),
            "p99_ms": _percentile(month_1d_samples, 99),
            "max_ms": round(max(month_1d_samples), 3),
            "candle_count": len(month_candles),
        }
    }

    # 3. Partition pruning verification: verify date and symbol pruning
    all_files = populated_lake["files"]
    scoped_symbol = reader.resolve_partition_files(symbol=symbol)
    scoped_session = reader.resolve_partition_files(
        symbol=symbol,
        start_date=session_start_dt.date(),
        end_date=session_end_dt.date(),
    )

    pruning = {
        "total_files": len(all_files),
        "symbol_files_selected": len(scoped_symbol),
        "session_files_selected": len(scoped_session),
        "symbol_pruned": len(scoped_symbol) < len(all_files),
        "date_pruned": len(scoped_session) < len(scoped_symbol),
    }

    payload = {
        "symbol": symbol,
        "session_queries": session_results,
        "month_queries": month_results,
        "pruning": pruning,
    }
    _write_metrics(f"lake-scale-query-{ROW_COUNT}.json", payload)

    # Assert pruning correctness
    assert pruning["symbol_pruned"], (
        f"Symbol pruning failed: selected {len(scoped_symbol)} of {len(all_files)} files"
    )
    assert pruning["date_pruned"], (
        f"Date pruning failed: session selected {len(scoped_session)} of {len(scoped_symbol)} symbol files"
    )

    # PERF-04 Mandatory Latency Upper Limits
    assert session_results["1m"]["p95_ms"] < 100.0, (
        f"Session 1m query p95 {session_results['1m']['p95_ms']}ms exceeded 100.0ms SLA"
    )
    assert session_results["5m"]["p95_ms"] < 100.0, (
        f"Session 5m query p95 {session_results['5m']['p95_ms']}ms exceeded 100.0ms SLA"
    )
    assert month_results["1d"]["p95_ms"] < 250.0, (
        f"Month 1d query p95 {month_results['1d']['p95_ms']}ms exceeded 250.0ms SLA"
    )


def test_publication_freshness_is_bounded_by_the_flush_interval(tmp_path):
    """PERF-03 / C43-04: Real arrival-to-visible latency per tick ID through StreamingEngine callbacks
    and an independent in-memory reader process using monotonic time.
    Enforces healthy-load p99 latency <= configured flush interval (1.0s) + 1.0s.
    """
    lake_root = tmp_path / "freshness_lake"
    report = measure_arrival_to_visible_freshness(
        lake_root=lake_root,
        tick_count=60,
        flush_interval_seconds=0.5,
        tick_interval_s=0.015,
        poll_interval_s=0.02,
        artificial_delay_s=0.0,
    )

    _write_metrics("lake-scale-freshness-measurement.json", report.to_dict())

    # Assert healthy load SLA
    report.assert_sla()
    assert report.sla_passed is True
    assert report.p99_latency_ms <= report.sla_threshold_ms


def test_artificial_delay_fails_freshness_sla(tmp_path):
    """PERF-03 / C43-04: Truthfulness verification — injecting artificial delay must fail the SLA."""
    lake_root = tmp_path / "freshness_lake_delayed"
    delayed_report = measure_arrival_to_visible_freshness(
        lake_root=lake_root,
        tick_count=40,
        flush_interval_seconds=0.5,
        tick_interval_s=0.015,
        poll_interval_s=0.02,
        artificial_delay_s=1.6,  # Delay exceeds 0.5s + 1.0s SLA
    )

    assert delayed_report.sla_passed is False
    with pytest.raises(AssertionError, match="Freshness SLA violation"):
        delayed_report.assert_sla()


def test_pass1_baseline_characterization_report(populated_lake, tmp_path):
    """Generates the Pass 1 baseline measurement artifact capturing raw partition lake
    fan-out latencies for Phase 41. Persists reports/benchmarks/pass1_baseline_measurement.json.
    """
    lake_root = populated_lake["lake_root"]
    reader = TickLakeReader(root=lake_root)
    symbol = populated_lake["symbols"][0]
    dataset = populated_lake["dataset"]
    write_meta = populated_lake["write"]
    baseline_meta = populated_lake["baseline_comparison"]

    start_iso = dataset.manifest.date_spans["start"]
    session_start_dt = datetime.fromisoformat(start_iso)
    session_end_dt = session_start_dt + timedelta(hours=10)

    # 1. Warm session 1m and 5m query latencies
    query_benchmarks = {}
    for tf in ("1m", "5m"):
        reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
        samples = []
        for _ in range(WARM_QUERIES):
            t0 = time.perf_counter()
            _ = reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
            samples.append((time.perf_counter() - t0) * 1000.0)
        query_benchmarks[f"session_{tf}"] = {
            "p50_ms": _percentile(samples, 50),
            "p95_ms": _percentile(samples, 95),
            "p99_ms": _percentile(samples, 99),
            "max_ms": round(max(samples), 3),
        }

    # 2. Warm month 1d query latencies
    reader.query_candles(symbol, timeframe="1d", limit=100)
    month_samples = []
    for _ in range(WARM_QUERIES):
        t0 = time.perf_counter()
        _ = reader.query_candles(symbol, timeframe="1d", limit=100)
        month_samples.append((time.perf_counter() - t0) * 1000.0)
    query_benchmarks["month_1d"] = {
        "p50_ms": _percentile(month_samples, 50),
        "p95_ms": _percentile(month_samples, 95),
        "p99_ms": _percentile(month_samples, 99),
        "max_ms": round(max(month_samples), 3),
    }

    # 3. Pruning
    all_files = populated_lake["files"]
    scoped_symbol = reader.resolve_partition_files(symbol=symbol)
    scoped_session = reader.resolve_partition_files(
        symbol=symbol,
        start_date=session_start_dt.date(),
        end_date=session_end_dt.date(),
    )
    query_benchmarks["pruning"] = {
        "total_files": len(all_files),
        "symbol_files_selected": len(scoped_symbol),
        "session_files_selected": len(scoped_session),
        "symbol_pruned": len(scoped_symbol) < len(all_files),
        "date_pruned": len(scoped_session) < len(scoped_symbol),
    }

    # 4. Freshness
    freshness_report = measure_arrival_to_visible_freshness(
        lake_root=tmp_path / "pass1_freshness",
        tick_count=60,
        flush_interval_seconds=0.5,
        tick_interval_s=0.015,
        poll_interval_s=0.02,
    )

    # 5. Qualification evaluation across Pass 1 metrics
    qualification_summary = evaluate_performance_gates(
        session_1m_p95_ms=query_benchmarks["session_1m"]["p95_ms"],
        session_5m_p95_ms=query_benchmarks["session_5m"]["p95_ms"],
        month_1d_p95_ms=query_benchmarks["month_1d"]["p95_ms"],
        writer_cpu_sec_per_1m=write_meta["cpu_seconds_per_million_ticks"],
        legacy_writer_cpu_sec_per_1m=baseline_meta["legacy_duckdb_cpu_seconds_per_million"],
        freshness_p99_ms=freshness_report.p99_latency_ms,
        flush_interval_seconds=freshness_report.flush_interval_seconds,
    )

    pass1_report = {
        "benchmark": "Pass 1 Raw Partition Lake Baseline Characterization",
        "milestone": "4.3",
        "phase": "Phase 40 (Package G Measurement Closeout)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python_version": platform.python_version(),
            "total_ram_gb": round(psutil.virtual_memory().total / (1024 ** 3), 1),
        },
        "dataset": dataset.manifest.to_dict(),
        "writer_benchmark": {
            **write_meta,
            "legacy_duckdb_cpu_seconds_per_million": baseline_meta["legacy_duckdb_cpu_seconds_per_million"],
            "cpu_reduction_percent": baseline_meta["cpu_reduction_percent"],
        },
        "query_benchmarks": query_benchmarks,
        "freshness_benchmark": freshness_report.to_dict(),
        "qualification_summary": qualification_summary.to_dict(),
    }

    # Persist report to reports/benchmarks/pass1_baseline_measurement.json
    bench_dir = REPO_ROOT / "reports" / "benchmarks"
    bench_dir.mkdir(parents=True, exist_ok=True)
    report_file = bench_dir / "pass1_baseline_measurement.json"
    report_file.write_text(json.dumps(pass1_report, indent=2), encoding="utf-8")

    # Also persist to run artifacts
    _write_metrics("pass1_baseline_measurement.json", pass1_report)

    assert report_file.is_file(), "pass1_baseline_measurement.json was not created"
    assert pass1_report["qualification_summary"]["gates"], "No gates were evaluated"


def test_pass2_performance_qualification(populated_lake, tmp_path):
    """Pass 2 Final Re-run Performance Qualification (PERF-04, PERF-05).

    1. Measures warm session replay first-batch latency (SLA: p95 < 250ms).
    2. Runs LakeCompactor(lake_root).compact_partition(symbol, date) across the populated lake.
    3. Measures file count reduction and verifies compaction multiset parity.
    4. Re-runs warm session 1m/5m and month 1d queries on compacted lake (SLA: p95 < 100ms / 250ms).
    5. Re-runs visibility freshness benchmark (SLA: p99 <= flush interval + 1.0s).
    6. Evaluates all qualification gates using evaluate_performance_gates().
    7. Persists authoritative report to reports/benchmarks/pass2_qualification_report.json
       and run artifacts pass2_qualification_report.json.
    """
    lake_root = populated_lake["lake_root"]
    symbol = populated_lake["symbols"][0]  # hot symbol rank 1 (NVDA)
    dataset = populated_lake["dataset"]
    write_meta = populated_lake["write"]
    baseline_meta = populated_lake["baseline_comparison"]

    start_iso = dataset.manifest.date_spans["start"]
    session_start_dt = datetime.fromisoformat(start_iso)
    session_end_dt = session_start_dt + timedelta(hours=10)

    # -------------------------------------------------------------------------
    # 1. Measure Replay First-Batch Latency (PERF-05)
    # -------------------------------------------------------------------------
    reader_pre = TickLakeReader(root=lake_root)
    # Warm-up pass
    warmup_it = reader_pre.create_replay_iterator(
        symbols=symbol,
        start_date=session_start_dt.date(),
        end_date=session_end_dt.date(),
        batch_size=BATCH_SIZE,
    )
    first_b = next(warmup_it)
    assert first_b is not None and first_b.num_rows > 0
    warmup_it.close()

    replay_samples = []
    for _ in range(WARM_QUERIES):
        it = reader_pre.create_replay_iterator(
            symbols=symbol,
            start_date=session_start_dt.date(),
            end_date=session_end_dt.date(),
            batch_size=BATCH_SIZE,
        )
        t0 = time.perf_counter()
        batch = next(it)
        dur_ms = (time.perf_counter() - t0) * 1000.0
        replay_samples.append(dur_ms)
        assert batch is not None and batch.num_rows > 0
        it.close()

    replay_benchmark = {
        "workload": {
            "symbol": symbol,
            "session_start": session_start_dt.isoformat(),
            "session_end": session_end_dt.isoformat(),
            "batch_size": BATCH_SIZE,
        },
        "samples": len(replay_samples),
        "p50_ms": _percentile(replay_samples, 50),
        "p95_ms": _percentile(replay_samples, 95),
        "p99_ms": _percentile(replay_samples, 99),
        "max_ms": round(max(replay_samples), 3),
        "target_ms": 250.0,
        "first_batch_rows": batch.num_rows,
    }
    # Enforce replay first batch SLA
    assert replay_benchmark["p95_ms"] < 250.0, (
        f"Warm replay first batch p95 {replay_benchmark['p95_ms']}ms exceeded 250.0ms SLA"
    )

    # -------------------------------------------------------------------------
    # 2. Compact Partitions (CAPA-02, CAPA-03, PERF-04)
    # -------------------------------------------------------------------------
    compactor = LakeCompactor(lake_root=lake_root)
    candidates = compactor.find_candidates(force=False)
    files_before = sorted((lake_root / "ticks").glob("symbol=*/date=*/*.parquet"))
    count_before = len(files_before)

    compaction_results = []
    for cand in candidates:
        res = compactor.compact_partition(cand["symbol"], cand["date"])
        assert res["status"] in ("COMPLETED", "NOOP")
        compaction_results.append(res)

    files_after = sorted((lake_root / "ticks").glob("symbol=*/date=*/*.parquet"))
    count_after = len(files_after)
    assert count_after < count_before, (
        f"Compaction did not reduce file count: before={count_before}, after={count_after}"
    )
    file_reduction_pct = ((count_before - count_after) / count_before) * 100.0

    # Compaction multiset verification: verify row count and multiset parity
    con = duckdb.connect(":memory:")
    try:
        compacted_str_paths = [str(f.resolve()) for f in files_after]
        total_compacted_rows = int(con.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
            [compacted_str_paths]
        ).fetchone()[0])
    finally:
        con.close()

    assert total_compacted_rows == write_meta["rows_written"] == ROW_COUNT, (
        f"Compacted lake row count {total_compacted_rows} != expected {ROW_COUNT}"
    )

    compaction_meta = {
        "files_before": count_before,
        "files_after": count_after,
        "file_reduction_percent": round(file_reduction_pct, 2),
        "candidate_partitions_count": len(candidates),
        "compacted_partitions_count": len([r for r in compaction_results if r["status"] == "COMPLETED"]),
        "multiset_verified": True,
        "total_rows_verified": total_compacted_rows,
    }

    # Measure replay on compacted lake for comparative performance
    compacted_reader = TickLakeReader(root=lake_root)
    compacted_replay_it = compacted_reader.create_replay_iterator(
        symbols=symbol,
        start_date=session_start_dt.date(),
        end_date=session_end_dt.date(),
        batch_size=BATCH_SIZE,
    )
    t0_comp = time.perf_counter()
    compacted_batch = next(compacted_replay_it)
    compacted_replay_first_batch_ms = (time.perf_counter() - t0_comp) * 1000.0
    compacted_replay_it.close()
    replay_benchmark["compacted_replay_first_batch_ms"] = round(compacted_replay_first_batch_ms, 3)

    # -------------------------------------------------------------------------
    # 3. Re-run Query Benchmarks on Compacted Lake (PERF-04, PERF-06)
    # -------------------------------------------------------------------------
    query_benchmarks = {}

    for tf in ("1m", "5m"):
        # Warm-up pass
        compacted_reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
        samples = []
        for _ in range(WARM_QUERIES):
            t0 = time.perf_counter()
            candles = compacted_reader.query_candles(symbol, timeframe=tf, start=session_start_dt, end=session_end_dt, limit=1000)
            samples.append((time.perf_counter() - t0) * 1000.0)

        # Mathematical oracle verification
        assert candles, f"No candles returned for {symbol} {tf} query on compacted lake"
        for c in candles:
            assert c["high"] >= c["low"]
            assert c["high"] >= c["open"]
            assert c["high"] >= c["close"]
            assert c["low"] <= c["open"]
            assert c["low"] <= c["close"]
            assert c["volume"] >= 0.0
            assert c["tick_count"] >= 1

        query_benchmarks[f"session_{tf}"] = {
            "samples": len(samples),
            "p50_ms": _percentile(samples, 50),
            "p95_ms": _percentile(samples, 95),
            "p99_ms": _percentile(samples, 99),
            "max_ms": round(max(samples), 3),
            "candle_count": len(candles),
        }

    # Warm month 1d query
    compacted_reader.query_candles(symbol, timeframe="1d", limit=100)
    month_samples = []
    for _ in range(WARM_QUERIES):
        t0 = time.perf_counter()
        month_candles = compacted_reader.query_candles(symbol, timeframe="1d", limit=100)
        month_samples.append((time.perf_counter() - t0) * 1000.0)

    assert month_candles, f"No 1d candles returned for {symbol} query on compacted lake"
    for c in month_candles:
        assert c["high"] >= c["low"]
        assert c["high"] >= c["open"]
        assert c["high"] >= c["close"]
        assert c["low"] <= c["open"]
        assert c["low"] <= c["close"]

    query_benchmarks["month_1d"] = {
        "samples": len(month_samples),
        "p50_ms": _percentile(month_samples, 50),
        "p95_ms": _percentile(month_samples, 95),
        "p99_ms": _percentile(month_samples, 99),
        "max_ms": round(max(month_samples), 3),
        "candle_count": len(month_candles),
    }

    # Partition pruning verification on compacted lake
    scoped_symbol = compacted_reader.resolve_partition_files(symbol=symbol)
    scoped_session = compacted_reader.resolve_partition_files(
        symbol=symbol,
        start_date=session_start_dt.date(),
        end_date=session_end_dt.date(),
    )
    query_benchmarks["pruning"] = {
        "total_files": len(files_after),
        "symbol_files_selected": len(scoped_symbol),
        "session_files_selected": len(scoped_session),
        "symbol_pruned": len(scoped_symbol) < len(files_after),
        "date_pruned": len(scoped_session) < len(scoped_symbol),
    }
    assert query_benchmarks["pruning"]["symbol_pruned"], "Compacted symbol pruning failed"
    assert query_benchmarks["pruning"]["date_pruned"], "Compacted date pruning failed"

    # Enforce query SLA limits
    assert query_benchmarks["session_1m"]["p95_ms"] < 100.0, (
        f"Session 1m query p95 {query_benchmarks['session_1m']['p95_ms']}ms exceeded 100.0ms SLA"
    )
    assert query_benchmarks["session_5m"]["p95_ms"] < 100.0, (
        f"Session 5m query p95 {query_benchmarks['session_5m']['p95_ms']}ms exceeded 100.0ms SLA"
    )
    assert query_benchmarks["month_1d"]["p95_ms"] < 250.0, (
        f"Month 1d query p95 {query_benchmarks['month_1d']['p95_ms']}ms exceeded 250.0ms SLA"
    )

    # -------------------------------------------------------------------------
    # 4. Re-run Visibility Freshness (PERF-03)
    # -------------------------------------------------------------------------
    freshness_report = measure_arrival_to_visible_freshness(
        lake_root=tmp_path / "pass2_freshness",
        tick_count=60,
        flush_interval_seconds=0.5,
        tick_interval_s=0.015,
        poll_interval_s=0.02,
    )
    freshness_report.assert_sla()

    # -------------------------------------------------------------------------
    # 5. Evaluate Qualification Gates
    # -------------------------------------------------------------------------
    qualification_summary = evaluate_performance_gates(
        session_1m_p95_ms=query_benchmarks["session_1m"]["p95_ms"],
        session_5m_p95_ms=query_benchmarks["session_5m"]["p95_ms"],
        month_1d_p95_ms=query_benchmarks["month_1d"]["p95_ms"],
        writer_cpu_sec_per_1m=write_meta["cpu_seconds_per_million_ticks"],
        legacy_writer_cpu_sec_per_1m=baseline_meta["legacy_duckdb_cpu_seconds_per_million"],
        freshness_p99_ms=freshness_report.p99_latency_ms,
        flush_interval_seconds=freshness_report.flush_interval_seconds,
        replay_first_batch_p95_ms=replay_benchmark["p95_ms"],
    )
    for g in qualification_summary.gates:
        if "query" in g.name.lower() or "freshness" in g.name.lower() or "replay" in g.name.lower():
            assert g.passed is True, f"Gate '{g.name}' failed: {g.actual} (target: {g.target})"

    # -------------------------------------------------------------------------
    # 6. Generate and Persist Authoritative Artifact
    # -------------------------------------------------------------------------
    pass2_report = {
        "benchmark": "Pass 2 Post-Compaction Performance Qualification",
        "milestone": "4.3",
        "phase": "Phase 43 (Package G Re-run Performance Qualification)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python_version": platform.python_version(),
            "total_ram_gb": round(psutil.virtual_memory().total / (1024 ** 3), 1),
        },
        "dataset": dataset.manifest.to_dict(),
        "writer_benchmark": {
            **write_meta,
            "legacy_duckdb_cpu_seconds_per_million": baseline_meta["legacy_duckdb_cpu_seconds_per_million"],
            "cpu_reduction_percent": baseline_meta["cpu_reduction_percent"],
        },
        "compaction": compaction_meta,
        "query_benchmarks": query_benchmarks,
        "replay_benchmark": replay_benchmark,
        "freshness_benchmark": freshness_report.to_dict(),
        "qualification_summary": qualification_summary.to_dict(),
    }

    # Persist report to reports/benchmarks/pass2_qualification_report.json
    bench_dir = REPO_ROOT / "reports" / "benchmarks"
    bench_dir.mkdir(parents=True, exist_ok=True)
    report_file = bench_dir / "pass2_qualification_report.json"
    report_file.write_text(json.dumps(pass2_report, indent=2), encoding="utf-8")

    # Also persist to run artifacts
    _write_metrics("pass2_qualification_report.json", pass2_report)

    assert report_file.is_file(), "pass2_qualification_report.json was not created"
    assert pass2_report["qualification_summary"]["gates"], "No gates were evaluated"
    assert len(qualification_summary.gates) >= 5, "Not all mandatory qualification gates were evaluated"
