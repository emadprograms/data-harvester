"""
Multi-Process Long-Running Soak, Chaos Monkey & Self-Healing Integration Test Suite.
Milestone v4.1 - Phase 27.

Requirements Fulfilled:
- TEST-P27-01: Multi-process soak testing under continuous ingestion and continuous analytical reading
  (tools/service_supervisor.py, tools/validate_concurrency.py).
- TEST-P27-02: Chaos monkey process termination (streamer, dashboard, supervisor) and automatic self-healing
  (tools/service_supervisor.py).

Test Suite Inventory (10 Tests):
Suite 1: Multi-Process Sustained Soak Tests (TEST-P27-01):
1. test_multi_process_sustained_soak_10k_ticks
2. test_soak_multi_wave_dashboard_load_with_writer
3. test_soak_memory_sampling_and_fd_leak_invariants
4. test_supervised_soak_service_lifecycle

Suite 2: Chaos Monkey & Self-Healing Tests (TEST-P27-02):
5. test_chaos_streamer_kill9_during_active_flush_auto_healing
6. test_chaos_dashboard_kill9_port_recovery_and_restoration
7. test_chaos_rapid_flapping_and_exponential_backoff
8. test_chaos_torn_intent_recovery_on_restarted_writer
9. test_chaos_supervisor_sigterm_graceful_drain_deadline
10. test_chaos_port_conflict_backoff_and_recovery
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import List, Optional, Tuple

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.schema import ticks_to_table
from tests.support.process_harness import wait_until
from tools.service_supervisor import ProcessSupervisor
from tools.validate_concurrency import print_summary_table, run_concurrency_validation

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _seed_mock_streamer_registry(lake_root: Path) -> None:
    """Seed one explicit instrument for mock-streamer lifecycle tests (F08)."""
    from src.storage.registry import SymbolRegistry, init_registry

    init_tick_lake(lake_root)
    init_registry(lake_root)
    SymbolRegistry(lake_root).add_symbol("AAPL", capital_ticker="AAPL")


# ============================================================================
# SUITE 1: Multi-Process Sustained Soak Tests (TEST-P27-01)
# ============================================================================

@pytest.mark.performance
@pytest.mark.integration
def test_multi_process_sustained_soak_10k_ticks(tmp_path):
    """
    4-process topology sustained soak test with 10,000 ticks across 5 symbols,
    250 dashboard requests across 4 endpoints, and 80 Repo B reader iterations.
    Asserts all 6 concurrency gates pass with zero zombies and memory metrics recorded.
    """
    lake_root = tmp_path / "soak_10k_lake"

    summary = run_concurrency_validation(
        lake_root=lake_root,
        total_ticks=10000,
        dashboard_requests=250,
        repo_b_iterations=80,
        cleanup=False,
    )

    print_summary_table(summary)
    m = summary.metrics

    # Gate 1: 0 DuckDB file lock errors
    assert m.duckdb_io_exceptions_total == 0, (
        f"DuckDB IOException file lock collisions encountered: {m.duckdb_io_exceptions_total}"
    )

    # Gate 2: 0 Parquet footer / row tearing corruptions
    assert m.repo_b_corrupted_footers == 0, (
        f"Corrupted Parquet footers encountered: {m.repo_b_corrupted_footers}"
    )

    # Gate 3: Writer event-loop lag p99 < 20ms
    assert m.writer_p99_lag_ms < 20.0, (
        f"Writer event-loop lag p99 was {m.writer_p99_lag_ms:.2f}ms (must be < 20.0ms)"
    )

    # Gate 4: Dashboard query latency p95 < 100ms with 0 HTTP errors
    assert m.dashboard_error_count == 0, (
        f"Dashboard had {m.dashboard_error_count} HTTP errors during soak"
    )
    assert m.dashboard_p95_latency_ms < 100.0, (
        f"Dashboard query latency p95 was {m.dashboard_p95_latency_ms:.2f}ms (must be < 100.0ms)"
    )

    # Gate 5: Repo B latency p95 < 100ms
    assert m.repo_b_p95_latency_ms < 100.0, (
        f"Repo B latency p95 was {m.repo_b_p95_latency_ms:.2f}ms (must be < 100.0ms)"
    )

    # Gate 6: Data parity 100% exact match
    assert m.total_ticks_published == 10000, (
        f"Expected 10,000 published ticks, got {m.total_ticks_published}"
    )
    assert m.repo_b_query_rows == 10000, (
        f"Expected Repo B to query 10,000 rows, got {m.repo_b_query_rows}"
    )
    assert m.data_parity_match is True, "Data parity mismatch between writer and Repo B reader"

    # Overall Gate & Process Telemetry
    assert summary.overall_passed is True, "One or more concurrency validation gates failed"
    assert m.writer_peak_rss_mb > 0.0, "Writer peak RSS was not recorded"
    assert m.open_fds_count > 0, "Open FDs count was not recorded"


@pytest.mark.performance
@pytest.mark.integration
def test_soak_multi_wave_dashboard_load_with_writer(tmp_path):
    """
    Executes 3 sequential high-volume request waves against the live dashboard server:
    Wave 1: 100 /api/candles
    Wave 2: 100 /api/stream/tape
    Wave 3: 50 /api/streaming/continuity
    Under concurrent active writing, asserting 100% HTTP 200 responses and p95 < 100ms.
    """
    lake_root = tmp_path / "lake_waves"
    init_tick_lake(lake_root)

    # Seed writer status
    control_dir = lake_root / "_control"
    control_dir.mkdir(parents=True, exist_ok=True)
    with open(control_dir / "writer_status.json", "w", encoding="utf-8") as f:
        json.dump({
            "status": "RUNNING",
            "writer_id": "writer_waves",
            "pid": os.getpid(),
            "total_rows_written": 0,
            "batches_published": 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)

    # Ephemeral port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    server_env = os.environ.copy()
    server_env["TICK_LAKE_ROOT"] = str(lake_root)
    server_env["DATA_DIR"] = str(lake_root)
    server_env["PYTHONPATH"] = str(REPO_ROOT)
    server_env["PYTHONUNBUFFERED"] = "1"

    server_proc = subprocess.Popen(
        [sys.executable, "-m", "src.dashboard.server", "--port", str(port), "--host", "127.0.0.1"],
        cwd=str(REPO_ROOT),
        env=server_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    streamer_proc = None
    try:
        # Await server readiness
        base_url = f"http://127.0.0.1:{port}"
        server_ready = False
        for _ in range(50):
            time.sleep(0.1)
            try:
                r = requests.get(f"{base_url}/api/status", timeout=1)
                if r.status_code == 200:
                    server_ready = True
                    break
            except Exception:
                pass
        assert server_ready, "Dashboard server failed to start on ephemeral port"

        # Start continuous synthetic streamer
        streamer_proc = subprocess.Popen(
            [
                sys.executable,
                "-m", "tools.synthetic_streamer",
                "--lake-root", str(lake_root),
                "--writer-id", "writer_waves",
                "--batch-size", "25",
                "--rate", "250",
            ],
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]

        def _fire(url: str) -> Tuple[int, float]:
            t0 = time.perf_counter()
            r = requests.get(url, timeout=5)
            lat = (time.perf_counter() - t0) * 1000.0
            return r.status_code, lat

        def _run_wave(urls: List[str]) -> Tuple[int, int, float]:
            lats = []
            errs = 0
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(_fire, u) for u in urls]
                for f in as_completed(futures):
                    try:
                        code, lat = f.result()
                        if code == 200:
                            lats.append(lat)
                        else:
                            errs += 1
                    except Exception:
                        errs += 1
            lats.sort()
            p95 = lats[int(len(lats) * 0.95)] if lats else 0.0
            return len(lats), errs, p95

        # Wave 1: 100 /api/candles
        wave1_urls = [
            f"{base_url}/api/candles?symbol={symbols[i % len(symbols)]}&source=streaming"
            for i in range(100)
        ]
        w1_success, w1_err, w1_p95 = _run_wave(wave1_urls)
        print(f"Dashboard candles wave: {w1_success} responses, {w1_err} errors, p95={w1_p95:.3f} ms")
        assert w1_err == 0 and w1_success == 100, f"Wave 1 had {w1_err} errors out of 100"
        assert w1_p95 < 100.0, f"Wave 1 p95 latency was {w1_p95:.2f}ms (threshold: 100ms)"

        # Wave 2: 100 /api/stream/tape
        wave2_urls = [
            f"{base_url}/api/stream/tape?symbol={symbols[i % len(symbols)]}&limit=50"
            for i in range(100)
        ]
        w2_success, w2_err, w2_p95 = _run_wave(wave2_urls)
        print(f"Dashboard tape wave: {w2_success} responses, {w2_err} errors, p95={w2_p95:.3f} ms")
        assert w2_err == 0 and w2_success == 100, f"Wave 2 had {w2_err} errors out of 100"
        assert w2_p95 < 100.0, f"Wave 2 p95 latency was {w2_p95:.2f}ms (threshold: 100ms)"

        # Wave 3: 50 /api/streaming/continuity
        wave3_urls = [
            f"{base_url}/api/streaming/continuity?symbol={symbols[i % len(symbols)]}&days=5"
            for i in range(50)
        ]
        w3_success, w3_err, w3_p95 = _run_wave(wave3_urls)
        print(f"Dashboard continuity wave: {w3_success} responses, {w3_err} errors, p95={w3_p95:.3f} ms")
        assert w3_err == 0 and w3_success == 50, f"Wave 3 had {w3_err} errors out of 50"
        assert w3_p95 < 100.0, f"Wave 3 p95 latency was {w3_p95:.2f}ms (threshold: 100ms)"

    finally:
        for p in [streamer_proc, server_proc]:
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()


@pytest.mark.integration
def test_soak_memory_sampling_and_fd_leak_invariants(tmp_path):
    """
    Executes 500 iterative DuckDB Parquet queries against live partitions.
    Asserts memory growth between cycle 50 and cycle 500 is < 50MB,
    and open file descriptors are non-leaking (before == after).
    """
    lake_root = tmp_path / "lake_mem_soak"
    init_tick_lake(lake_root)

    # Seed 2,500 rows across 5 symbols
    writer = TickLakeWriter(root=lake_root, writer_id="writer_mem", max_batch_rows=1000)
    symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]
    t_base = datetime(2026, 10, 3, 14, 30, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    ticks = []
    for i in range(2500):
        sym = symbols[i % len(symbols)]
        ticks.append({
            "timestamp": t_base + timedelta(milliseconds=i * 10),
            "symbol": sym,
            "price": 100.0 + (i % 50),
            "volume": 1.0,
            "bid": 99.9,
            "ask": 100.1,
            "source": "MEM_TEST",
            "session": "REG",
        })
    asyncio.run(writer.publish_batch_async(ticks))
    asyncio.run(writer.close_async())

    all_parquet_files = sorted([str(f) for f in lake_root.glob("ticks/symbol=*/date=*/*.parquet")])
    assert len(all_parquet_files) > 0

    # Warm up DuckDB
    con_warm = duckdb.connect(":memory:")
    con_warm.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [all_parquet_files]).fetchall()
    con_warm.close()
    gc.collect()

    proc = psutil.Process()
    open_fds_before = proc.num_fds() if hasattr(proc, "num_fds") else 0

    rss_50 = 0.0
    rss_500 = 0.0

    q_candles = """
        SELECT
            time_bucket(INTERVAL '1 minute', timestamp) AS bucket_time,
            symbol,
            arg_min(price, (timestamp, ingest_id)) AS open,
            max(price) AS high,
            min(price) AS low,
            arg_max(price, (timestamp, ingest_id)) AS close,
            sum(coalesce(volume, 1.0)) AS volume,
            count(*) AS tick_count
        FROM read_parquet(?, hive_partitioning=false)
        GROUP BY bucket_time, symbol
        ORDER BY bucket_time ASC, symbol ASC
    """

    for i in range(500):
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone = 'UTC'")
            _ = con.execute(q_candles, [all_parquet_files]).fetchall()
        finally:
            con.close()

        if i == 50:
            rss_50 = proc.memory_info().rss / (1024 * 1024)
        elif i == 499:
            rss_500 = proc.memory_info().rss / (1024 * 1024)

    gc.collect()
    open_fds_after = proc.num_fds() if hasattr(proc, "num_fds") else 0
    growth = abs(rss_500 - rss_50)

    # Invariants
    assert growth < 50.0, f"Memory grew by {growth:.2f}MB between cycle 50 and 500 (threshold: 50MB)"
    assert abs(open_fds_after - open_fds_before) <= 1, (
        f"File descriptor leak detected: before={open_fds_before}, after={open_fds_after}"
    )


@pytest.mark.integration
def test_supervised_soak_service_lifecycle(tmp_path):
    """
    Runs dashboard server under ProcessSupervisor in background.
    Dispatches 200 HTTP requests, stops supervisor cleanly.
    Asserts all requests succeed and child process terminates without orphans.
    """
    lake_root = tmp_path / "lake_dash_lifecycle"
    init_tick_lake(lake_root)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    supervisor = ProcessSupervisor(
        name="dash_lifecycle",
        module="src.dashboard.server",
        module_args=["--port", str(port), "--host", "127.0.0.1"],
        poll_interval=0.2,
        log_dir=tmp_path / "logs",
        handle_signals=False,
        extra_env={
            "TICK_LAKE_ROOT": str(lake_root),
            "DATA_DIR": str(lake_root),
        },
    )

    t = threading.Thread(target=supervisor.run, daemon=True)
    t.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        ready = False
        for _ in range(50):
            time.sleep(0.1)
            try:
                r = requests.get(f"{base_url}/api/status", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        assert ready, "Supervised dashboard failed to respond on ephemeral port"

        child_pid = supervisor.child_pid
        assert child_pid is not None
        assert supervisor.is_running is True

        # Dispatch 200 requests concurrently
        def _get_status():
            r = requests.get(f"{base_url}/api/status", timeout=2)
            return r.status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: _get_status(), range(200)))

        assert all(c == 200 for c in results), f"Non-200 responses encountered"
    finally:
        supervisor.stop(timeout=5.0)
        t.join(timeout=5.0)

    assert supervisor.is_running is False
    assert supervisor.process is None


# ============================================================================
# SUITE 2: Chaos Monkey & Self-Healing Tests (TEST-P27-02)
# ============================================================================

@pytest.mark.integration
def test_chaos_streamer_kill9_during_active_flush_auto_healing(tmp_path):
    """
    Supervises a continuous tick streamer. Sends SIGKILL mid-ingestion.
    Verifies kernel frees lock, supervisor detects crash (-9), auto-restarts within SLA (<5s),
    new child re-acquires lock without LakeOwnershipError, and valid Parquet rows continue being written.
    """
    lake_root = tmp_path / "lake_streamer_chaos"
    _seed_mock_streamer_registry(lake_root)

    supervisor = ProcessSupervisor(
        name="streamer_chaos",
        module="src.stream.runner",
        module_args=[
            "--lake-root", str(lake_root),
            "--writer-id", "streamer_chaos_1",
            "--flush-interval", "0.2",
            "--mock",
            "--ticks-per-sec", "200",
        ],
        poll_interval=0.2,
        backoff_factor=0.3,
        max_backoff=2.0,
        stability_threshold=5.0,
        log_dir=tmp_path / "logs",
        handle_signals=False,
    )

    t = threading.Thread(target=supervisor.run, daemon=True)
    t.start()

    try:
        # Await child start and initial writes
        pid1 = None
        for _ in range(50):
            time.sleep(0.1)
            pid = supervisor.child_pid
            parquet_files = list(lake_root.glob("ticks/symbol=*/date=*/*.parquet"))
            if pid is not None and parquet_files:
                pid1 = pid
                break

        assert pid1 is not None, "Child streamer did not start or write initial ticks"

        # Mid-ingestion sudden kill -9
        t_kill = time.time()
        os.kill(pid1, signal.SIGKILL)

        # Await supervisor auto-healing restart within SLA (<5s)
        recovered = False
        pid2 = None
        for _ in range(50):
            time.sleep(0.1)
            new_pid = supervisor.child_pid
            if new_pid is not None and new_pid != pid1 and supervisor.is_running:
                pid2 = new_pid
                recovered = True
                break

        recovery_elapsed = time.time() - t_kill
        assert recovered is True, "Supervisor failed to restart child after kill -9 within SLA"
        assert recovery_elapsed < 5.0, f"Recovery took {recovery_elapsed:.2f}s (SLA < 5.0s)"

        # Let the new child run and write additional batches
        time.sleep(1.0)
    finally:
        supervisor.stop(timeout=5.0)
        t.join(timeout=5.0)

    # Verify Parquet files can be read via DuckDB with 0 corrupted footers
    all_files = [str(f) for f in lake_root.glob("ticks/symbol=*/date=*/*.parquet")]
    assert len(all_files) > 0
    con = duckdb.connect(":memory:")
    try:
        res = con.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [all_files]).fetchone()
        row_count = res[0] if res else 0
        assert row_count > 0, "No rows found in parquet lake after recovery"
    finally:
        con.close()


@pytest.mark.integration
def test_chaos_dashboard_kill9_port_recovery_and_restoration(tmp_path):
    """
    Supervises dashboard REST server. Delivers SIGKILL mid-operation.
    Supervisor restarts child, child re-binds port via SO_REUSEADDR,
    and /api/status returns HTTP 200 within SLA (<5s).
    """
    lake_root = tmp_path / "lake_dash_chaos"
    init_tick_lake(lake_root)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    supervisor = ProcessSupervisor(
        name="dash_chaos",
        module="src.dashboard.server",
        module_args=["--port", str(port), "--host", "127.0.0.1"],
        poll_interval=0.2,
        backoff_factor=0.3,
        max_backoff=2.0,
        log_dir=tmp_path / "logs",
        handle_signals=False,
        extra_env={
            "TICK_LAKE_ROOT": str(lake_root),
            "DATA_DIR": str(lake_root),
        },
    )

    t = threading.Thread(target=supervisor.run, daemon=True)
    t.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        ready = False
        for _ in range(50):
            time.sleep(0.1)
            try:
                r = requests.get(f"{base_url}/api/status", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        assert ready, "Dashboard failed to become ready initially"

        pid1 = supervisor.child_pid
        assert pid1 is not None

        # Deliver SIGKILL to dashboard child
        t_kill = time.time()
        os.kill(pid1, signal.SIGKILL)

        # Poll until /api/status responds 200 again
        restored = False
        for _ in range(50):
            time.sleep(0.1)
            try:
                r = requests.get(f"{base_url}/api/status", timeout=1)
                if r.status_code == 200 and supervisor.child_pid != pid1:
                    restored = True
                    break
            except Exception:
                pass

        restored_elapsed = time.time() - t_kill
        assert restored is True, "Dashboard failed to restore on port within SLA"
        assert restored_elapsed < 5.0, f"Restoration took {restored_elapsed:.2f}s (SLA < 5.0s)"
    finally:
        supervisor.stop(timeout=5.0)
        t.join(timeout=5.0)


@pytest.mark.integration
def test_chaos_supervisor_flapping_backoff_and_exit_cleanly(tmp_path):
    """
    Supervises a crashing child (fails immediately with code 1).
    Verifies consecutive_crashes increments, backoff escalates,
    and supervisor cleanly exits when max_restarts=3 is reached.
    """
    lake_root = tmp_path / "lake_flapping"
    init_tick_lake(lake_root)

    supervisor = ProcessSupervisor(
        name="flapping_test",
        module="src.stream.runner",
        module_args=["--lake-root", str(lake_root), "--fail-immediately"],
        poll_interval=0.1,
        backoff_factor=0.2,
        max_backoff=2.0,
        max_restarts=3,
        log_dir=tmp_path / "logs",
        handle_signals=False,
    )

    t0 = time.time()
    t = threading.Thread(target=supervisor.run)
    t.start()
    t.join(timeout=10.0)

    elapsed = time.time() - t0
    assert not t.is_alive(), "Supervisor thread did not terminate after max restarts"
    assert supervisor.consecutive_crashes == 3, f"Expected 3 crashes, got {supervisor.consecutive_crashes}"
    assert supervisor.running is False
    assert elapsed >= 0.5, f"Supervisor exited too quickly ({elapsed:.2f}s), backoff bypassed"


# Backward-compatible alias
test_chaos_rapid_flapping_and_exponential_backoff = test_chaos_supervisor_flapping_backoff_and_exit_cleanly


@pytest.mark.integration
def test_chaos_torn_intent_recovery_on_restarted_writer(tmp_path):
    """
    Injects a torn intent (_control/intent/torn_batch_001.json) with state STAGED
    and an unpromoted .parquet.tmp file in _staging/.
    Initializes a new TickLakeWriter: asserts intent recovery promotes staged file to target partition,
    writes receipt to _control/receipts/, deletes intent, and DuckDB reads 100% of rows.
    """
    lake_root = tmp_path / "lake_torn_intent"
    init_tick_lake(lake_root)

    # 1. Create a staged parquet file
    staging_dir = lake_root / "_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    staged_file = staging_dir / "tmp_torn_batch_001.parquet.tmp"

    t_base = datetime(2026, 10, 3, 14, 30, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    ticks = []
    for i in range(100):
        ticks.append({
            "timestamp": t_base + timedelta(milliseconds=i * 10),
            "symbol": "AAPL",
            "price": 175.0 + i * 0.05,
            "volume": 5.0,
            "bid": 174.9,
            "ask": 175.1,
            "source": "TORN_TEST",
            "session": "REG",
            "ingest_id": f"ingest_{i:04d}",
        })

    tbl = ticks_to_table(ticks, validate=True)
    pq.write_table(tbl, staged_file, compression="snappy")

    fsize = staged_file.stat().st_size
    with open(staged_file, "rb") as f:
        file_sha = hashlib.sha256(f.read()).hexdigest()

    # 2. Inject torn intent
    intent_dir = lake_root / "_control" / "intent"
    intent_dir.mkdir(parents=True, exist_ok=True)
    intent_file = intent_dir / "torn_batch_001.json"

    rel_dest = "ticks/symbol=AAPL/date=2026-10-03/batch_writer_recovered_000001.parquet"
    intent_payload = {
        "batch_id": "torn_batch_001",
        "writer_id": "writer_recovered",
        "sequence": 1,
        "expected_row_count": 100,
        "state": "STAGED",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "targets": [
            {
                "relative_path": rel_dest,
                "staging_path": f"_staging/{staged_file.name}",
                "symbol": "AAPL",
                "date": "2026-10-03",
                "row_count": 100,
                "file_size_bytes": fsize,
                "sha256": file_sha,
            }
        ],
    }
    with open(intent_file, "w", encoding="utf-8") as f:
        json.dump(intent_payload, f, indent=2)

    # Verify preconditions
    assert intent_file.is_file()
    assert staged_file.is_file()
    assert not (lake_root / rel_dest).exists()

    # 3. Start writer - automatically invokes recover_pending_publications
    writer = TickLakeWriter(root=lake_root, writer_id="writer_recovered")

    # 4. Verify postconditions
    dest_path = lake_root / rel_dest
    assert dest_path.is_file(), "Staged file was not promoted to target partition"
    assert not intent_file.exists(), "Intent file was not deleted upon recovery"

    receipt_file = lake_root / "_control" / "receipts" / "torn_batch_001.json"
    assert receipt_file.is_file(), "Receipt was not created upon intent promotion"

    # 5. Verify row count via DuckDB
    con = duckdb.connect(":memory:")
    try:
        res = con.execute("SELECT count(*) FROM read_parquet(?)", [str(dest_path)]).fetchone()
        row_count = res[0] if res else 0
        assert row_count == 100, f"Expected 100 rows, got {row_count}"
    finally:
        con.close()

    writer.close()


@pytest.mark.integration
def test_chaos_supervisor_sigterm_graceful_drain_deadline(tmp_path):
    """
    Starts service supervisor as a subprocess running synthetic streamer with drain delay.
    Delivers SIGTERM to supervisor. Supervisor catches signal, allows child up to 15s to drain,
    child exits 0, supervisor exits 0, with zero zombies remaining.
    """
    lake_root = tmp_path / "lake_drain"
    _seed_mock_streamer_registry(lake_root)

    sup_proc = subprocess.Popen(
        [
            sys.executable,
            str(REPO_ROOT / "tools" / "service_supervisor.py"),
            "--name", "drain_test",
            "--module", "src.stream.runner",
            "--poll-interval", "0.2",
            "--log-dir", str(tmp_path / "logs"),
            "--args", "--lake-root", str(lake_root), "--writer-id", "writer_drain", "--mock", "--drain-delay", "1.0",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait 1.5s for child to start and stream ticks
        time.sleep(1.5)
        assert sup_proc.poll() is None, "Supervisor exited prematurely"

        # Deliver SIGTERM to supervisor
        t0 = time.time()
        sup_proc.send_signal(signal.SIGTERM)

        # Supervisor must wait for child to drain and exit 0
        sup_proc.wait(timeout=15.0)
        elapsed = time.time() - t0

        assert sup_proc.returncode == 0, f"Supervisor exited with non-zero code {sup_proc.returncode}"
        assert elapsed >= 0.8, f"Supervisor did not wait for child drain ({elapsed:.2f}s < 0.8s)"

        # Verify zero zombies
        for child in psutil.process_iter(["pid", "name"]):
            try:
                if child.ppid() == sup_proc.pid:
                    assert child.status() != psutil.STATUS_ZOMBIE
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    finally:
        if sup_proc.poll() is None:
            sup_proc.terminate()
            sup_proc.wait(timeout=2)


@pytest.mark.integration
def test_chaos_port_conflict_backoff_and_recovery(tmp_path):
    """
    Binds an external socket to a port. Starts supervised dashboard server on same port.
    Dashboard child fails with EADDRINUSE, supervisor increments crash count and enters backoff.
    Releases external socket after ~0.8s. Supervisor auto-restarts child, port is bound,
    and /api/status returns HTTP 200.
    """
    lake_root = tmp_path / "lake_port_conflict"
    init_tick_lake(lake_root)

    # 1. Bind external socket to port
    ext_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ext_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ext_sock.bind(("127.0.0.1", 0))
    port = ext_sock.getsockname()[1]
    ext_sock.listen(1)

    supervisor = ProcessSupervisor(
        name="port_conflict_service",
        module="src.dashboard.server",
        module_args=["--port", str(port), "--host", "127.0.0.1"],
        poll_interval=0.2,
        backoff_factor=0.3,
        max_backoff=2.0,
        log_dir=tmp_path / "logs",
        handle_signals=False,
        extra_env={
            "TICK_LAKE_ROOT": str(lake_root),
            "DATA_DIR": str(lake_root),
        },
    )

    t = threading.Thread(target=supervisor.run, daemon=True)
    t.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        # Deterministic barrier (v4.2 Q02/Q04). The child needs ~0.6s to import
        # and fail with EADDRINUSE on this host, so a fixed 0.8s sleep raced the
        # supervisor's crash detection and failed on slower/loaded machines.
        # Poll for the observable condition instead; the assertion is unchanged.
        assert wait_until(
            lambda: supervisor.consecutive_crashes >= 1, timeout=15.0, interval=0.05
        ), "Supervisor did not detect port conflict crash within 15s"

        # Release external socket so port is free
        ext_sock.close()

        # Wait for supervisor to restart child and /api/status to respond 200 within SLA (<10s)
        def status_ok() -> bool:
            try:
                return requests.get(f"{base_url}/api/status", timeout=1).status_code == 200
            except Exception:
                return False

        recovered = wait_until(status_ok, timeout=10.0, interval=0.2)

        assert recovered is True, "Dashboard failed to bind port after external conflict was released"
    finally:
        supervisor.stop(timeout=5.0)
        t.join(timeout=5.0)
        try:
            ext_sock.close()
        except Exception:
            pass
