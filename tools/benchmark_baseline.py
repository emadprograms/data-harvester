#!/usr/bin/env python3
"""
Baseline DuckDB Characterization & Performance Benchmark Tool.
Milestone v4.0 (Partitioned Parquet Tick Lake) - Phase 15.

Establishes pre-migration performance baseline for legacy DuckDB storage:
1. Safety guard: strictly refuses execution against production Micron volume or repo data.
2. Synthetic dataset generator: reproducible ticks matching the 19-symbol production distribution.
3. Writer benchmark: measures CPU seconds per 1M ticks, throughput (ticks/sec), and peak RSS memory.
4. Event-loop lag benchmark: measures asyncio scheduling lag caused by synchronous DB writes with a concurrent heartbeat.
5. Query latency benchmark: measures percentiles (p50, p90, p95, p99) for tape query, 1m OHLCV, 5m OHLCV, and daily candle.
6. Generates reports/baseline_duckdb_characterization.json.
"""

import argparse
import asyncio
import json
import os
import platform
import random
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import duckdb
import psutil

# Ensure repo root is in pythonpath
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.analytics import MONITORED_19_SYMBOLS, get_stream_tape, get_streaming_candles
from src.database.operations import save_ticks_to_storage
from src.database.schema import init_streaming_db
import src.database.connection as conn_mod
import src.dashboard.analytics as ana_mod


from src.utils.write_guard import (
    assert_safe_write_path,
    get_run_artifacts_dir,
    is_production_path,
    ProductionAccessBlockedError,
)


# ============================================================================
# SAFETY GUARDS
# ============================================================================

def verify_safety_guards(target_dir: str):
    """
    Strictly verifies that target_dir is NOT pointing to production storage
    (/Volumes/Micron-E... or repo data symlink).
    """
    try:
        assert_safe_write_path(target_dir, operation="benchmark database access")
    except ProductionAccessBlockedError as err:
        print(f"❌ SAFETY REFUSAL: {err}", file=sys.stderr)
        print("   Benchmarks must use isolated temporary storage. Aborting.", file=sys.stderr)
        sys.exit(1)


# ============================================================================
# SYNTHETIC 19-SYMBOL DATASET GENERATOR
# ============================================================================

# Production volume distribution weights for the 19 monitored equities
SYMBOL_DISTRIBUTION_WEIGHTS = {
    "NVDA": 0.25,
    "TSLA": 0.15,
    "AAPL": 0.10,
    "MSFT": 0.08,
    "AMD": 0.08,
    "AMZN": 0.06,
    "META": 0.06,
    "GOOGL": 0.05,
    "AVGO": 0.04,
    "MU": 0.03,
    "BABA": 0.03,
    "TSM": 0.03,
    "QCOM": 0.02,
    "ORCL": 0.02,
    "SHOP": 0.01,
    "ADBE": 0.01,
    "PANW": 0.01,
    "APP": 0.01,
    "NDAQ": 0.01,
}

BASE_PRICES = {
    "NVDA": 120.50,
    "TSLA": 252.10,
    "AAPL": 151.25,
    "MSFT": 421.80,
    "AMD": 162.40,
    "AMZN": 186.30,
    "META": 582.00,
    "GOOGL": 166.75,
    "AVGO": 174.50,
    "MU": 109.80,
    "BABA": 104.20,
    "TSM": 176.10,
    "QCOM": 161.90,
    "ORCL": 169.40,
    "SHOP": 74.80,
    "ADBE": 512.60,
    "PANW": 381.10,
    "APP": 86.40,
    "NDAQ": 71.30,
}


def generate_synthetic_ticks(
    total_ticks: int = 100_000,
    base_date_str: str = "2026-10-02",
    seed: int = 42,
) -> Tuple[List[Tuple], Dict[str, int]]:
    """
    Generates reproducible synthetic quote ticks adhering strictly to the 19-symbol distribution.
    Spans a regular NYSE trading session (09:30:00 to 16:00:00 ET = 13:30:00 to 20:00:00 UTC).
    """
    rng = random.Random(seed)
    symbols = list(SYMBOL_DISTRIBUTION_WEIGHTS.keys())
    weights = [SYMBOL_DISTRIBUTION_WEIGHTS[s] for s in symbols]

    # Session window: 6.5 hours = 23,400 seconds = 23,400,000,000 microseconds
    start_utc = datetime.strptime(f"{base_date_str} 13:30:00.000000", "%Y-%m-%d %H:%M:%S.%f")
    total_session_micros = 23_400 * 1_000_000
    micro_step = max(1, total_session_micros // max(total_ticks, 1))

    current_prices = dict(BASE_PRICES)
    ticks: List[Tuple] = []
    symbol_counts: Dict[str, int] = {s: 0 for s in symbols}

    for i in range(total_ticks):
        ts = start_utc + timedelta(microseconds=i * micro_step)
        sym = rng.choices(symbols, weights=weights, k=1)[0]
        symbol_counts[sym] += 1

        # Small random walk with mean reversion
        drift = rng.uniform(-0.04, 0.04)
        current_prices[sym] = round(max(1.0, current_prices[sym] + drift), 4)
        p = current_prices[sym]
        spread = round(rng.uniform(0.01, 0.03), 4)

        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        bid = round(p - (spread / 2.0), 4)
        ask = round(p + (spread / 2.0), 4)
        vol = 1.0  # Capital observation semantics

        ticks.append((ts_str, sym, p, vol, bid, ask, "CAPITAL", "REG"))

    return ticks, symbol_counts


# ============================================================================
# PERCENTILE CALCULATOR
# ============================================================================

def compute_percentiles(values: List[float]) -> Dict[str, float]:
    """Calculates min, mean, p50, p90, p95, p99, and max from a numeric list."""
    if not values:
        return {"min": 0.0, "mean": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    s = sorted(values)
    n = len(s)

    def pct(p: float) -> float:
        idx = int(round((p / 100.0) * (n - 1)))
        return round(s[idx], 3)

    return {
        "min": round(min(s), 3),
        "mean": round(sum(s) / n, 3),
        "p50": pct(50.0),
        "p90": pct(90.0),
        "p95": pct(95.0),
        "p99": pct(99.0),
        "max": round(max(s), 3),
    }


# ============================================================================
# WRITER BENCHMARK
# ============================================================================

def benchmark_writer(
    db_path: str,
    ticks: List[Tuple],
    batch_size: int = 100,
) -> Dict[str, Any]:
    """
    Measures DuckDB SQL insert throughput, CPU seconds, and process RSS memory.
    """
    total_ticks = len(ticks)
    process = psutil.Process()
    rss_before = process.memory_info().rss

    # Open connection to target database
    client = conn_mod.get_streaming_db_connection(db_path=db_path)

    wall_start = time.perf_counter()
    cpu_start = time.process_time()

    peak_rss = rss_before
    for i in range(0, total_ticks, batch_size):
        batch = ticks[i : i + batch_size]
        save_ticks_to_storage(client, batch)
        current_rss = process.memory_info().rss
        if current_rss > peak_rss:
            peak_rss = current_rss

    cpu_elapsed = time.process_time() - cpu_start
    wall_elapsed = time.perf_counter() - wall_start

    client.close()

    cpu_sec_per_1m = (cpu_elapsed / total_ticks) * 1_000_000.0 if total_ticks else 0.0
    throughput = total_ticks / wall_elapsed if wall_elapsed > 0 else 0.0
    mb = 1024 * 1024

    return {
        "total_ticks": total_ticks,
        "batch_size": batch_size,
        "batches_committed": (total_ticks + batch_size - 1) // batch_size,
        "wall_time_seconds": round(wall_elapsed, 4),
        "cpu_time_seconds": round(cpu_elapsed, 4),
        "cpu_seconds_per_1m_ticks": round(cpu_sec_per_1m, 3),
        "throughput_ticks_per_sec": round(throughput, 1),
        "rss_before_mb": round(rss_before / mb, 2),
        "peak_rss_mb": round(peak_rss / mb, 2),
        "rss_growth_mb": round(max(0, peak_rss - rss_before) / mb, 2),
    }


# ============================================================================
# EVENT LOOP SCHEDULING LAG BENCHMARK
# ============================================================================

async def benchmark_event_loop_lag(
    db_path: str,
    ticks: List[Tuple],
    batch_size: int = 100,
    heartbeat_interval_ms: float = 5.0,
) -> Dict[str, Any]:
    """
    Measures event loop scheduling delay caused by synchronous DuckDB writes.
    A background heartbeat task sleeps for heartbeat_interval_ms; any overshoot
    is recorded as scheduling lag.
    """
    interval_s = heartbeat_interval_ms / 1000.0
    client = conn_mod.get_streaming_db_connection(db_path=db_path)
    lags_ms: List[float] = []
    stop_event = asyncio.Event()

    async def heartbeat():
        while not stop_event.is_set():
            t0 = time.perf_counter()
            await asyncio.sleep(interval_s)
            t1 = time.perf_counter()
            actual_delay = t1 - t0
            lag = max(0.0, actual_delay - interval_s)
            lags_ms.append(lag * 1000.0)

    async def writer():
        total_ticks = len(ticks)
        for i in range(0, total_ticks, batch_size):
            batch = ticks[i : i + batch_size]
            # Synchronous DuckDB write directly on event-loop thread (legacy runner behavior)
            save_ticks_to_storage(client, batch)
            # Brief cooperative yield simulating websocket receive interval
            await asyncio.sleep(0.001)
        stop_event.set()

    hb_task = asyncio.create_task(heartbeat())
    await writer()
    await hb_task

    client.close()

    percentiles = compute_percentiles(lags_ms)
    return {
        "heartbeat_interval_ms": heartbeat_interval_ms,
        "sample_count": len(lags_ms),
        **percentiles,
    }


# ============================================================================
# ANALYTICAL QUERY LATENCY BENCHMARK
# ============================================================================

def benchmark_analytical_queries(
    db_path: str,
    date_str: str = "2026-10-02",
    symbol: str = "NVDA",
    iterations: int = 30,
) -> Dict[str, Any]:
    """
    Measures latency percentiles for critical dashboard queries against the DuckDB database.
    """
    # Ensure analytical module points to benchmark database
    conn_mod.DEFAULT_STREAMING_DB_PATH = db_path
    ana_mod.DEFAULT_STREAMING_DB_PATH = db_path

    query_definitions = [
        ("tape_query", lambda: get_stream_tape(symbol=symbol, limit=50)),
        ("ohlcv_1m", lambda: get_streaming_candles(symbol=symbol, timeframe="1m", date=date_str)),
        ("ohlcv_5m", lambda: get_streaming_candles(symbol=symbol, timeframe="5m", date=date_str)),
        ("ohlcv_1d", lambda: get_streaming_candles(symbol=symbol, timeframe="1d", date=date_str)),
    ]

    results: Dict[str, Any] = {}

    for query_name, query_func in query_definitions:
        latencies_ms: List[float] = []
        # Warmup
        try:
            query_func()
        except Exception as e:
            print(f"⚠️ Warmup query '{query_name}' error: {e}", file=sys.stderr)

        for _ in range(iterations):
            t0 = time.perf_counter()
            query_func()
            t1 = time.perf_counter()
            latencies_ms.append((t1 - t0) * 1000.0)

        percentiles = compute_percentiles(latencies_ms)
        results[query_name] = {
            "iterations": iterations,
            **percentiles,
        }

    return results


# ============================================================================
# MAIN ORCHESTRATOR & CLI
# ============================================================================

def run_benchmark(
    ticks_count: int = 100_000,
    lag_ticks_count: int = 5_000,
    batch_size: int = 100,
    query_iterations: int = 30,
    output_path: str = "reports/baseline_duckdb_characterization.json",
    custom_db_dir: str = None,
    keep_db: bool = False,
) -> Dict[str, Any]:
    """Runs complete baseline characterization suite."""
    print("=" * 72)
    print("📊 DUCKDB STORAGE BASELINE CHARACTERIZATION BENCHMARK (PHASE 15)")
    print("=" * 72)

    # 0. Safety verification of output destination
    try:
        assert_safe_write_path(output_path, operation="benchmark report write")
    except ProductionAccessBlockedError as err:
        print(f"❌ SAFETY REFUSAL: {err}", file=sys.stderr)
        sys.exit(1)

    # 1. Setup isolated database directory
    if custom_db_dir:
        verify_safety_guards(custom_db_dir)
        db_dir = os.path.abspath(custom_db_dir)
        os.makedirs(db_dir, exist_ok=True)
        is_temp = False
    else:
        db_dir = tempfile.mkdtemp(prefix="benchmark_duckdb_baseline_")
        verify_safety_guards(db_dir)
        is_temp = True

    streaming_db_path = os.path.join(db_dir, "streaming.duckdb")
    print(f"📁 Benchmark Database: {streaming_db_path}")

    # Set environment and module defaults to benchmark path
    os.environ["DATA_DIR"] = db_dir
    conn_mod.DEFAULT_DATA_DIR = db_dir
    conn_mod.DEFAULT_STREAMING_DB_PATH = streaming_db_path
    ana_mod.DEFAULT_STREAMING_DB_PATH = streaming_db_path

    try:
        # 2. Initialize schema
        print("🔧 Initializing DuckDB streaming schema...")
        init_streaming_db(db_path=streaming_db_path)

        # 3. Generate synthetic ticks
        date_str = "2026-10-02"
        print(f"🎲 Generating {ticks_count:,} reproducible ticks (19-symbol distribution)...")
        t0_gen = time.perf_counter()
        ticks, sym_distribution = generate_synthetic_ticks(
            total_ticks=ticks_count,
            base_date_str=date_str,
            seed=42,
        )
        gen_time = time.perf_counter() - t0_gen
        print(f"   Generated {len(ticks):,} ticks across 19 symbols in {gen_time:.3f}s.")

        # 4. Benchmark writer performance
        print(f"💾 Benchmarking legacy DuckDB writer ({ticks_count:,} ticks, batch_size={batch_size})...")
        writer_metrics = benchmark_writer(
            db_path=streaming_db_path,
            ticks=ticks,
            batch_size=batch_size,
        )
        print(f"   Throughput: {writer_metrics['throughput_ticks_per_sec']:,.1f} ticks/sec")
        print(f"   CPU Seconds / 1M Ticks: {writer_metrics['cpu_seconds_per_1m_ticks']:.3f}s")
        print(f"   Peak RSS Memory: {writer_metrics['peak_rss_mb']:.1f} MB (Growth: +{writer_metrics['rss_growth_mb']:.1f} MB)")

        # 5. Benchmark event loop scheduling lag
        print(f"⏱️  Benchmarking event loop scheduling lag ({lag_ticks_count:,} ticks, 5ms heartbeat)...")
        lag_ticks, _ = generate_synthetic_ticks(
            total_ticks=lag_ticks_count,
            base_date_str=date_str,
            seed=99,
        )
        lag_metrics = asyncio.run(
            benchmark_event_loop_lag(
                db_path=streaming_db_path,
                ticks=lag_ticks,
                batch_size=batch_size,
                heartbeat_interval_ms=5.0,
            )
        )
        print(f"   Lag p50: {lag_metrics['p50']:.2f}ms | p95: {lag_metrics['p95']:.2f}ms | p99: {lag_metrics['p99']:.2f}ms | Max: {lag_metrics['max']:.2f}ms")

        # 6. Benchmark analytical query latencies
        print(f"📈 Benchmarking analytical queries ({query_iterations} iterations each)...")
        query_metrics = benchmark_analytical_queries(
            db_path=streaming_db_path,
            date_str=date_str,
            symbol="NVDA",
            iterations=query_iterations,
        )
        for qname, qdata in query_metrics.items():
            print(f"   Query [{qname:<11}]: p50={qdata['p50']:6.2f}ms | p95={qdata['p95']:6.2f}ms | p99={qdata['p99']:6.2f}ms | mean={qdata['mean']:6.2f}ms")

        # 7. Build full report
        report = {
            "benchmark_version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "duckdb_version": duckdb.__version__,
                "cpu_count": psutil.cpu_count(logical=True),
                "total_ram_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            },
            "dataset": {
                "total_ticks": ticks_count,
                "symbol_count": len(MONITORED_19_SYMBOLS),
                "symbols": MONITORED_19_SYMBOLS,
                "distribution_counts": sym_distribution,
            },
            "writer_metrics": writer_metrics,
            "event_loop_lag_ms": lag_metrics,
            "query_latencies_ms": query_metrics,
            "performance_gates_evaluation": {
                "cpu_seconds_per_1m_ticks": writer_metrics["cpu_seconds_per_1m_ticks"],
                "target_cpu_reduction_gate": "Phase 17 Parquet writer target is >=50% reduction below this baseline",
                "event_loop_lag_p99_ms": lag_metrics["p99"],
                "target_event_loop_lag_gate": "Phase 17 PyArrow off-loop worker target is <20.0ms p99",
                "query_1m_p95_ms": query_metrics["ohlcv_1m"]["p95"],
                "query_5m_p95_ms": query_metrics["ohlcv_5m"]["p95"],
                "target_query_latency_gate": "Phase 19 in-memory DuckDB reader target is <100.0ms p95",
            },
        }

        # 8. Save report JSON
        try:
            abs_output = str(assert_safe_write_path(output_path, operation="benchmark report write"))
        except ProductionAccessBlockedError as err:
            print(f"❌ SAFETY REFUSAL: {err}", file=sys.stderr)
            sys.exit(1)

        os.makedirs(os.path.dirname(abs_output), exist_ok=True)
        with open(abs_output, "w") as f:
            json.dump(report, f, indent=2)

        print("-" * 72)
        print(f"✅ Baseline Characterization Report written to: {abs_output}")
        print("=" * 72)

        return report

    finally:
        if is_temp and not keep_db:
            shutil.rmtree(db_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description="DuckDB Baseline Characterization Benchmark Tool (Phase 15)"
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=100_000,
        help="Number of synthetic ticks to benchmark (default: 100,000)",
    )
    parser.add_argument(
        "--lag-ticks",
        type=int,
        default=5_000,
        help="Number of ticks for event loop lag benchmark (default: 5,000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Writer batch size (default: 100)",
    )
    parser.add_argument(
        "--query-iterations",
        type=int,
        default=30,
        help="Number of query latency benchmark iterations (default: 30)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="reports/baseline_duckdb_characterization.json",
        help="Output report JSON file path",
    )
    parser.add_argument(
        "--db-dir",
        type=str,
        default=None,
        help="Custom database directory (must NOT point to production Micron volume or repo data)",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep temporary benchmark database on disk after run",
    )

    args = parser.parse_args()

    out_path = args.output
    if args.output == "reports/baseline_duckdb_characterization.json" and os.environ.get("DATA_HARVESTER_RUN_DIR"):
        out_path = str(get_run_artifacts_dir() / "baseline_duckdb_characterization.json")

    run_benchmark(
        ticks_count=args.ticks,
        lag_ticks_count=args.lag_ticks,
        batch_size=args.batch_size,
        query_iterations=args.query_iterations,
        output_path=out_path,
        custom_db_dir=args.db_dir,
        keep_db=args.keep_db,
    )


if __name__ == "__main__":
    main()
