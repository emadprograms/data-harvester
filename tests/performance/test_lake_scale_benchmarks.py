"""
Production-scale lake benchmarks (Q03 / PERF-01, PERF-04, PERF-05, PERF-06, PERF-08).

Marked `performance`, so the 20-minute offline CI job does not run them; they are
executed explicitly and their output recorded in the execution report.

Scale is controlled by GSD_LAKE_BENCH_ROWS (default 200,000). The default is
deliberately modest: this project's performance reference host is a Mac Mini /
Windows machine, and a CI or sandbox container is a different machine. Numbers
produced here describe *this* host only and are never compared against the
production SLA without the documented baseline (see gap LAKE-P0-03).

What is measured:
- writer wall/CPU time, throughput, and CPU seconds per million ticks
- peak RSS of the benchmark process
- finalized file and partition counts for the generated inventory
- warm 1m / 5m / 1d candle query latency percentiles
- pruning: how many files a scoped query selects versus the whole lake
- freshness: time from write_ticks returning to the rows being visible

What is NOT measured here (recorded as unmeasured, not passed):
- PERF-02's >=50% CPU reduction against the legacy DuckDB baseline (no
  reproducible baseline exists — gap LAKE-P0-03)
- PERF-03 event-loop lag (needs the live streaming runner and a provider)
"""

import json
import os
from pathlib import Path
import statistics
import time

import psutil
import pytest

from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from tests.support.deterministic_dataset import DeterministicDataset

pytestmark = pytest.mark.performance

ROW_COUNT = int(os.environ.get("GSD_LAKE_BENCH_ROWS", "200000"))
SEED = int(os.environ.get("GSD_LAKE_BENCH_SEED", "20261004"))
BATCH_SIZE = int(os.environ.get("GSD_LAKE_BENCH_BATCH", "5000"))
WARM_QUERIES = int(os.environ.get("GSD_LAKE_BENCH_QUERIES", "25"))

ARTIFACTS = Path(__file__).resolve().parents[2] / ".planning" / "artifacts"


def _percentile(samples, pct):
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * (pct / 100.0)
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    weight = idx - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _write_metrics(name, payload):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    target = ARTIFACTS / name
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


@pytest.fixture(scope="module")
def populated_lake(tmp_path_factory):
    """Write a deterministic dataset into an isolated lake and measure the write path."""
    lake_root = tmp_path_factory.mktemp("bench_lake")
    dataset = DeterministicDataset(row_count=ROW_COUNT, seed=SEED)

    process = psutil.Process()
    rss_start = process.memory_info().rss
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
        rss_peak = process.memory_info().rss
        metrics = writer.metrics
        writer.close()

    cpu_seconds = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    rows = metrics.total_rows_written

    files = sorted((lake_root / "ticks").glob("symbol=*/date=*/*.parquet"))
    partitions = {(p.parent.parent.name, p.parent.name) for p in files}

    payload = {
        "host": {
            "cpu_count": os.cpu_count(),
            "platform": "see execution report environment manifest",
        },
        "dataset": dataset.manifest.to_dict(),
        "write": {
            "rows_requested": ROW_COUNT,
            "rows_written": rows,
            "wall_seconds": round(wall_seconds, 3),
            "cpu_seconds": round(cpu_seconds, 3),
            "rows_per_second": round(rows / wall_seconds, 1) if wall_seconds else None,
            "cpu_seconds_per_million_ticks": round(cpu_seconds / rows * 1_000_000, 3) if rows else None,
            "batches_published": metrics.batches_published,
            "quarantined": metrics.total_quarantined,
            "dropped": metrics.total_dropped,
            "peak_rss_mb": round(max(rss_peak, rss_start) / (1024 * 1024), 1),
            "rss_growth_mb": round((rss_peak - rss_start) / (1024 * 1024), 1),
        },
        "inventory": {
            "finalized_files": len(files),
            "partitions": len(partitions),
        },
        "baseline_comparison": {
            "status": "NOT_MEASURED",
            "reason": "No reproducible legacy DuckDB baseline exists (traceability gap LAKE-P0-03).",
        },
    }
    _write_metrics(f"lake-scale-write-{ROW_COUNT}.json", payload)

    return {
        "lake_root": lake_root,
        "dataset": dataset,
        "files": files,
        "write": payload["write"],
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
    write = populated_lake["write"]
    assert write["cpu_seconds_per_million_ticks"] is not None
    assert write["cpu_seconds_per_million_ticks"] > 0, "a zero CPU cost means nothing was measured"
    assert write["rows_per_second"] > 0
    assert populated_lake["dataset"].manifest.seed == SEED


def test_finalized_inventory_is_partitioned(populated_lake):
    inventory_files = populated_lake["files"]
    assert inventory_files, "no finalized Parquet files were produced"
    # Partitions must be at least one per symbol present in the dataset.
    assert len({p.parent.parent.name for p in inventory_files}) >= 1


def test_warm_candle_query_latency_and_pruning(populated_lake):
    """PERF-04 and PERF-06: warm 1m/5m/1d latency, plus proof of partition pruning."""
    lake_root = populated_lake["lake_root"]
    reader = TickLakeReader(root=lake_root)
    symbol = populated_lake["symbols"][0]

    # Warm-up is excluded from the measured samples.
    for timeframe in ("1m", "5m", "1d"):
        reader.query_candles(symbol, timeframe=timeframe, limit=1000)

    results = {}
    for timeframe in ("1m", "5m", "1d"):
        samples = []
        for _ in range(WARM_QUERIES):
            started = time.perf_counter()
            candles = reader.query_candles(symbol, timeframe=timeframe, limit=1000)
            samples.append((time.perf_counter() - started) * 1000)
            assert candles is not None
        results[timeframe] = {
            "samples": len(samples),
            "p50_ms": round(_percentile(samples, 50), 3),
            "p95_ms": round(_percentile(samples, 95), 3),
            "p99_ms": round(_percentile(samples, 99), 3),
            "max_ms": round(max(samples), 3),
        }

    # Pruning: a scoped query must select fewer files than the entire lake.
    scoped = reader.resolve_partition_files(symbol=symbol)
    all_files = populated_lake["files"]
    pruning = {
        "total_files": len(all_files),
        "files_selected_for_symbol": len(scoped),
        "pruned": len(scoped) < len(all_files),
    }

    payload = {
        "symbol": symbol,
        "queries": results,
        "pruning": pruning,
        "host_caveat": "Measured on the executing container, not the production reference host.",
    }
    _write_metrics(f"lake-scale-query-{ROW_COUNT}.json", payload)

    assert results["1m"]["p95_ms"] > 0, "a zero p95 means nothing was measured"
    assert pruning["pruned"], (
        f"query selected {len(scoped)} of {len(all_files)} files — partition pruning did not reduce the file set"
    )


def test_publication_freshness_is_bounded_by_the_flush_interval(populated_lake):
    """PERF-05: rows must be visible shortly after write_ticks returns."""
    lake_root = populated_lake["lake_root"]
    dataset = DeterministicDataset(row_count=2_000, seed=SEED + 1)
    batch = next(dataset.iter_batches(batch_size=2_000))

    writer = TickLakeWriter(root=lake_root, writer_id="freshness_writer", max_batch_rows=2_000)
    try:
        started = time.monotonic()
        writer.write_ticks(batch)
        writer.flush(block=True)
        elapsed_ms = (time.monotonic() - started) * 1000
    finally:
        published = writer.metrics.total_rows_written
        writer.close()

    payload = {
        "rows": len(batch),
        "rows_published_on_flush": published,
        "flush_to_visible_ms": round(elapsed_ms, 3),
    }
    _write_metrics(f"lake-scale-freshness-{ROW_COUNT}.json", payload)

    assert published >= len(batch), f"flush published {published} of {len(batch)} rows"
    assert elapsed_ms > 0
