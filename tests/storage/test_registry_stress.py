"""
Comprehensive Stress, Race-Condition, Flapping, and Signal Debouncing Test Suite
for Versioned Symbol Registry & Dynamic Runner Reloading.
Milestone v4.1 - Phase 24.

Covers:
- TEST-P24-01: Cross-process concurrent symbol CRUD lock serialization and monotonic versioning integrity.
- TEST-P24-02: Rapid symbol toggle/delete flapping and PENDING_PURGE generation fences.
- TEST-P24-03: File signal debouncing and dynamic reload latency under heavy polling.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import fcntl
import json
import multiprocessing as mp
import os
from pathlib import Path
import socket
import threading
import time
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, MagicMock

import duckdb
import pytest
import requests

from src.dashboard.server import create_dashboard_server
from src.storage.registry import (
    DEFAULT_SIGNAL_FILENAME,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    RegistryCorruptedError,
    RegistryError,
    RegistryLockError,
    RegistrySnapshot,
    SymbolAlreadyExistsError,
    SymbolEntry,
    SymbolNotFoundError,
    SymbolPendingPurgeError,
    SymbolRegistry,
    cleanup_orphaned_registry_staging_files,
    init_registry,
    touch_stream_reload_signal,
)
from src.stream.runner import StreamingEngine


# ============================================================================
# Top-level Multiprocessing Worker Functions (Required for macOS 'spawn')
# ============================================================================

def _mp_add_worker(root_str: str, worker_id: int, count: int, result_queue: mp.Queue):
    """Worker process: sequentially adds `count` unique symbols to the registry."""
    try:
        reg = SymbolRegistry(root=root_str, lock_timeout=30.0)
        for i in range(count):
            sym = f"P{worker_id}_SYM{i}"
            reg.add_symbol(sym, display_name=f"Symbol {sym}", capital_ticker=sym)
        result_queue.put((worker_id, "OK", count, []))
    except Exception as e:
        result_queue.put((worker_id, "ERROR", 0, [f"{type(e).__name__}: {e}"]))


def _mp_collision_worker(root_str: str, worker_id: int, symbol: str, barrier: Any, result_queue: mp.Queue):
    """Worker process: synchronizes at barrier and races to add the exact same symbol."""
    try:
        reg = SymbolRegistry(root=root_str, lock_timeout=30.0)
        # Synchronize all processes to fire simultaneously
        barrier.wait(timeout=15.0)
        reg.add_symbol(symbol, display_name=f"Collision {symbol}", capital_ticker=symbol)
        result_queue.put((worker_id, "SUCCESS"))
    except SymbolAlreadyExistsError:
        result_queue.put((worker_id, "EXISTS"))
    except Exception as e:
        result_queue.put((worker_id, f"ERROR: {type(e).__name__}: {e}"))


def _mp_mixed_crud_worker(root_str: str, worker_id: int, duration_sec: float, result_queue: mp.Queue):
    """
    Worker process: performs high-velocity mix of add, toggle, remove, and load operations
    for `duration_sec` seconds. Tracks successful mutating transactions.
    """
    reg = SymbolRegistry(root=root_str, lock_timeout=30.0)
    successful_adds = 0
    successful_toggles = 0
    successful_removes = 0
    successful_loads = 0
    errors = []

    start = time.monotonic()
    op_counter = 0

    while (time.monotonic() - start) < duration_sec:
        op_counter += 1
        sym_key = f"M{worker_id}_S{op_counter % 5}"
        op = op_counter % 4

        try:
            if op == 0:
                # Add or complete_purge + re-add
                try:
                    reg.add_symbol(sym_key, capital_ticker=sym_key)
                    successful_adds += 1
                except SymbolAlreadyExistsError:
                    pass
                except SymbolPendingPurgeError:
                    # Clear pending purge so we can re-add
                    reg.complete_purge(sym_key)
                    successful_removes += 1  # complete_purge is a mutating version bump
                    reg.add_symbol(sym_key, capital_ticker=sym_key)
                    successful_adds += 1
            elif op == 1:
                # Toggle
                try:
                    reg.toggle_symbol(sym_key)
                    successful_toggles += 1
                except (SymbolNotFoundError, SymbolPendingPurgeError):
                    pass
            elif op == 2:
                # Remove
                try:
                    reg.remove_symbol(sym_key)
                    successful_removes += 1
                except (SymbolNotFoundError, SymbolPendingPurgeError):
                    pass
            elif op == 3:
                # Concurrent read without lock: tests os.replace atomicity
                snap = reg.load()
                assert isinstance(snap, RegistrySnapshot)
                successful_loads += 1
        except Exception as e:
            errors.append(f"Worker {worker_id} op {op} on {sym_key}: {type(e).__name__}: {e}")
            break

    result_queue.put((worker_id, successful_adds, successful_toggles, successful_removes, successful_loads, errors))


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def ephemeral_server(tmp_path, monkeypatch):
    """Spawns an ephemeral multi-threaded dashboard server with isolated TICK_LAKE_ROOT."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))
    monkeypatch.setenv("DATA_DIR", str(lake_root))

    # Find free ephemeral port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url, lake_root
    finally:
        server.shutdown()
        server.server_close()


# ============================================================================
# Group 1 (TEST-P24-01): Concurrency, Lock Serialization & Monotonic Versioning
# ============================================================================

def test_multiprocess_concurrent_symbol_additions_monotonic_versioning(tmp_path):
    """
    10 independent Python processes simultaneously add 10 unique symbols each (P{pid}_SYM{i}).
    Asserts:
    - 100 total symbols successfully inserted
    - Final registry version is strictly 101 (1 initial + 100 increments)
    - Zero torn writes, partial records, or JSON corruption across all reads
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    assert reg.version == 1

    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()

    num_processes = 10
    symbols_per_proc = 10
    processes = []

    for pid in range(num_processes):
        p = ctx.Process(
            target=_mp_add_worker,
            args=(str(lake_root), pid, symbols_per_proc, result_queue),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join(timeout=30.0)
        assert not p.is_alive(), f"Process {p.pid} timed out"

    # Collect results
    total_added = 0
    all_errors = []
    for _ in range(num_processes):
        worker_id, status, count, errors = result_queue.get(timeout=5.0)
        assert status == "OK", f"Worker {worker_id} failed: {errors}"
        total_added += count
        all_errors.extend(errors)

    assert not all_errors, f"Multiprocess errors encountered: {all_errors}"
    assert total_added == 100

    # Verify snapshot integrity on disk
    snapshot = reg.load()
    assert len(snapshot.symbols) == 100, f"Expected 100 symbols, found {len(snapshot.symbols)}"
    assert snapshot.version == 101, f"Expected version 101, got {snapshot.version}"

    # Verify each symbol is present and valid
    for pid in range(num_processes):
        for i in range(symbols_per_proc):
            sym = f"P{pid}_SYM{i}"
            assert sym in snapshot.symbols, f"Symbol {sym} missing from snapshot"
            entry = snapshot.symbols[sym]
            assert entry.status == STATUS_ACTIVE
            assert entry.active is True
            assert entry.generation == 1


def test_multiprocess_same_symbol_add_collision_race(tmp_path):
    """
    10 concurrent processes simultaneously race to add the identical symbol ('COLLISION_TICKER').
    Asserts:
    - Exactly 1 process succeeds (status='SUCCESS')
    - Exactly 9 processes receive SymbolAlreadyExistsError (status='EXISTS')
    - Final registry version is strictly 2 (1 initial + 1 addition)
    - Zero deadlocks and zero corruption
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    assert reg.version == 1

    ctx = mp.get_context("spawn")
    num_processes = 10
    barrier = ctx.Barrier(num_processes)
    result_queue = ctx.Queue()

    target_symbol = "COLLISION_TICKER"
    processes = []

    for pid in range(num_processes):
        p = ctx.Process(
            target=_mp_collision_worker,
            args=(str(lake_root), pid, target_symbol, barrier, result_queue),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join(timeout=30.0)
        assert not p.is_alive(), f"Process {p.pid} timed out"

    # Collect outcomes
    results = []
    for _ in range(num_processes):
        results.append(result_queue.get(timeout=5.0))

    successes = [r for r in results if r[1] == "SUCCESS"]
    exists = [r for r in results if r[1] == "EXISTS"]
    errors = [r for r in results if r[1] not in ("SUCCESS", "EXISTS")]

    assert not errors, f"Unexpected errors in collision test: {errors}"
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(exists) == 9, f"Expected exactly 9 SymbolAlreadyExistsError, got {len(exists)}"

    # Final snapshot inspection
    snapshot = reg.load()
    assert snapshot.version == 2, f"Expected version 2, got {snapshot.version}"
    assert target_symbol in snapshot.symbols
    assert len(snapshot.symbols) == 1


def test_multiprocess_mixed_crud_stress(tmp_path):
    """
    8 processes performing high-velocity mix of add_symbol, toggle_symbol, remove_symbol,
    and continuous load() calls for 2.5 seconds.
    Asserts:
    - Zero deadlocks across all processes
    - Version strictly matches (1 + total successful mutating transactions)
    - Zero JSON decode errors or corrupted snapshot reads during concurrent os.replace operations
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    assert reg.version == 1

    ctx = mp.get_context("spawn")
    num_processes = 8
    duration_sec = 2.5
    result_queue = ctx.Queue()
    processes = []

    for pid in range(num_processes):
        p = ctx.Process(
            target=_mp_mixed_crud_worker,
            args=(str(lake_root), pid, duration_sec, result_queue),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join(timeout=20.0)
        assert not p.is_alive(), f"Process {p.pid} deadlocked or timed out"

    total_mutations = 0
    total_loads = 0
    all_errors = []

    for _ in range(num_processes):
        worker_id, adds, toggles, removes, loads, errs = result_queue.get(timeout=5.0)
        total_mutations += (adds + toggles + removes)
        total_loads += loads
        all_errors.extend(errs)

    assert not all_errors, f"Errors encountered during mixed CRUD stress: {all_errors}"
    assert total_mutations > 0, "No mutations were performed"
    assert total_loads > 0, "No concurrent loads were performed"

    # Assert version strictly matches initial version (1) + total successful mutations
    snapshot = reg.load()
    expected_version = 1 + total_mutations
    assert snapshot.version == expected_version, (
        f"Version mismatch: expected {expected_version}, got {snapshot.version}"
    )


def test_registry_lock_timeout_rejection(tmp_path):
    """
    A background thread holds fcntl.flock on registry.lock.
    SymbolRegistry(root, lock_timeout=0.1).add_symbol(...) is attempted.
    Asserts:
    - Raises RegistryLockError cleanly
    - Does NOT throw RuntimeError: cannot release un-acquired lock (verifies double-release fix)
    - After lock is released by background thread, subsequent acquisition succeeds immediately
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    lock_file = lake_root / "_control" / "registry.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)

    lock_acquired_ev = threading.Event()
    release_lock_ev = threading.Event()

    def _lock_holder():
        fd = os.open(lock_file, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            lock_acquired_ev.set()
            release_lock_ev.wait(timeout=5.0)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    t = threading.Thread(target=_lock_holder, daemon=True)
    t.start()
    assert lock_acquired_ev.wait(timeout=3.0), "Background thread failed to acquire lock"

    reg = SymbolRegistry(lake_root, lock_timeout=0.1)

    # Attempt mutation while lock is held -> must raise RegistryLockError without RuntimeError
    with pytest.raises(RegistryLockError) as exc_info:
        reg.add_symbol("TIMEOUT_SYM")

    err_str = str(exc_info.value)
    assert "cannot release un-acquired lock" not in err_str
    assert "Timed out acquiring" in err_str

    # Release lock from background thread
    release_lock_ev.set()
    t.join(timeout=3.0)
    assert not t.is_alive()

    # Subsequent acquisition on released lock must succeed immediately
    entry = reg.add_symbol("TIMEOUT_SYM", capital_ticker="TIMEOUT_SYM")
    assert entry.symbol == "TIMEOUT_SYM"
    assert reg.get_symbol("TIMEOUT_SYM") is not None
    assert reg.version == 2


def test_crashed_staging_temp_file_cleanup(tmp_path):
    """
    Creates synthetic orphaned tmp_registry_*.json files in _control/.
    cleanup_orphaned_staging_files(root, max_age_seconds=0.0) is invoked.
    Asserts:
    - All matching orphaned staging files are removed
    - Legitimate registry.json is completely untouched
    - Returned deleted count accurately reflects files removed
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    control_dir = lake_root / "_control"
    reg_file = control_dir / "registry.json"
    assert reg_file.is_file()
    initial_content = reg_file.read_text(encoding="utf-8")

    # Create synthetic orphaned tmp_registry_ files
    now = time.time()
    stale_tmp1 = control_dir / f"tmp_registry_{os.getpid()}_stale1.json"
    stale_tmp2 = control_dir / f"tmp_registry_{os.getpid()}_stale2.json"
    fresh_tmp = control_dir / f"tmp_registry_{os.getpid()}_fresh.json"

    stale_tmp1.write_text('{"partial": 1}', encoding="utf-8")
    stale_tmp2.write_text('{"partial": 2}', encoding="utf-8")
    fresh_tmp.write_text('{"partial": 3}', encoding="utf-8")

    # Set mtime for stale files to 120s in the past
    os.utime(stale_tmp1, (now - 120, now - 120))
    os.utime(stale_tmp2, (now - 120, now - 120))
    # fresh_tmp has current mtime

    # 1. Cleanup with max_age_seconds=60.0 -> deletes only stale_tmp1 and stale_tmp2
    deleted = cleanup_orphaned_registry_staging_files(lake_root, max_age_seconds=60.0)
    assert deleted == 2
    assert not stale_tmp1.exists()
    assert not stale_tmp2.exists()
    assert fresh_tmp.exists()
    assert reg_file.exists()

    # 2. Cleanup with max_age_seconds=0.0 -> deletes remaining fresh_tmp
    deleted_remaining = cleanup_orphaned_registry_staging_files(lake_root, max_age_seconds=0.0)
    assert deleted_remaining == 1
    assert not fresh_tmp.exists()

    # Legitimate registry file must be untouched
    assert reg_file.is_file()
    assert reg_file.read_text(encoding="utf-8") == initial_content


# ============================================================================
# Group 2 (TEST-P24-02): Rapid Flapping, Generations & Subscription Fencing
# ============================================================================

def test_rapid_symbol_flapping_100_cycles_strict_generation(tmp_path):
    """
    100 consecutive cycles of:
      add -> remove -> verify refusal (SymbolPendingPurgeError on add/toggle) -> complete_purge -> re-add
    Asserts:
    - Symbol generation monotonically increments and strictly reaches 100
    - Registry version strictly reaches 1 + (100 * 3) = 301
    - While in PENDING_PURGE, add_symbol and toggle_symbol raise SymbolPendingPurgeError
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    assert reg.version == 1

    sym = "FLAP"
    for cycle in range(100):
        expected_gen = cycle + 1

        # 1. Add (or re-add)
        entry = reg.add_symbol(sym, display_name=f"Flap {sym}", capital_ticker=sym)
        assert entry.generation == expected_gen
        assert entry.status == STATUS_ACTIVE
        assert entry.active is True

        # 2. Remove -> moves to PENDING_PURGE
        entry_purged = reg.remove_symbol(sym)
        assert entry_purged.status == STATUS_PENDING_PURGE
        assert entry_purged.active is False
        assert entry_purged.generation == expected_gen

        # 3. Verify refusal while in PENDING_PURGE
        with pytest.raises(SymbolPendingPurgeError):
            reg.add_symbol(sym)
        with pytest.raises(SymbolPendingPurgeError):
            reg.toggle_symbol(sym, active=True)

        # 4. Complete purge -> archives generation and removes from symbols dict
        reg.complete_purge(sym)
        snap = reg.load()
        assert sym not in snap.symbols
        assert snap.generations[sym] == expected_gen

    final_snap = reg.load()
    assert final_snap.generations[sym] == 100
    assert final_snap.version == 1 + (100 * 3)  # 301


def test_multi_symbol_interleaved_flapping_concurrency(tmp_path):
    """
    5 threads flapping 5 separate symbols simultaneously through 20 cycles each.
    Asserts:
    - Each symbol's generation reaches 20 independently
    - Registry version reaches strictly 1 + (5 * 20 * 3) = 301
    - Zero lock deadlocks or race condition data corruption
    """
    lake_root = tmp_path / "lake"
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root, lock_timeout=20.0)
    assert reg.version == 1

    symbols = [f"SYM_{i}" for i in range(5)]
    errors = []

    def _flap_worker(sym: str):
        try:
            for _ in range(20):
                reg.add_symbol(sym, capital_ticker=sym)
                reg.remove_symbol(sym)
                reg.complete_purge(sym)
        except Exception as e:
            errors.append(f"{sym}: {type(e).__name__}: {e}")

    threads = [threading.Thread(target=_flap_worker, args=(s,)) for s in symbols]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=25.0)
        assert not t.is_alive(), f"Flapping thread for {t} timed out"

    assert not errors, f"Errors in multi-symbol flapping: {errors}"

    final_snap = reg.load()
    assert final_snap.version == 1 + (5 * 20 * 3)  # 301
    for s in symbols:
        assert s not in final_snap.symbols
        assert final_snap.generations[s] == 20


def test_empty_registry_lifecycle_no_fallback_tickers(tmp_path):
    """
    Add 3 symbols, remove all 3, complete purge all 3.
    Run reload_symbols() in StreamingEngine.
    Asserts:
    - Subscriptions list is empty ([])
    - active_streaming_symbols is empty
    - None of the legacy 19 default tickers (AAPL, NVDA, TSLA, ...) are reseeded
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)

        # 1. Add 3 symbols
        for sym in ["SYM1", "SYM2", "SYM3"]:
            reg.add_symbol(sym, capital_ticker=sym)
        assert len(reg.load().symbols) == 3

        # 2. Remove all 3
        for sym in ["SYM1", "SYM2", "SYM3"]:
            reg.remove_symbol(sym)

        # 3. Complete purge all 3
        for sym in ["SYM1", "SYM2", "SYM3"]:
            reg.complete_purge(sym)

        snap = reg.load()
        assert snap.symbols == {}

        # 4. Run reload_symbols in StreamingEngine
        engine = StreamingEngine(lake_root=lake_root)
        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer

        await engine.reload_symbols()

        assert mock_streamer.update_subscriptions.called
        args, _ = mock_streamer.update_subscriptions.call_args
        assert args[0] == [], f"Expected empty subscription list [], got {args[0]}"
        assert engine.active_streaming_symbols == set()

        # Legacy 19 default tickers must NOT be reseeded
        legacy_19 = {
            "AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT", "GOOGL", "META", "NFLX", "INTC",
            "AVGO", "BABA", "DIS", "JNJ", "JPM", "V", "PG", "UNH", "HD", "SPY", "QQQ"
        }
        assert not (engine.active_streaming_symbols & legacy_19)

    asyncio.run(_run())


def test_subscription_fence_generation_transitions(tmp_path):
    """
    Verifies subscription fencing across symbol lifecycle generation transitions:
    - Add SYM (Gen 1) -> tick accepted into write queue
    - Remove SYM (PENDING_PURGE) -> tick dropped outside write queue
    - Complete purge -> tick dropped outside write queue
    - Re-add SYM (Gen 2) -> tick accepted into write queue again
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)

        engine = StreamingEngine(lake_root=lake_root)
        now = datetime.now(timezone.utc)

        # 1. Add SYM (Gen 1) -> active
        entry1 = reg.add_symbol("SYM", capital_ticker="SYM")
        assert entry1.generation == 1
        await engine.reload_symbols()
        assert "SYM" in engine.active_streaming_symbols

        await engine._handle_capital_tick({
            "epic": "SYM", "price": 100.0, "timestamp": now, "bid": 99.9, "ask": 100.1
        })
        assert engine.write_queue.qsize() == 1
        tick1 = engine.write_queue.get_nowait()
        assert tick1[1] == "SYM"
        assert tick1[2] == 100.0

        # 2. Remove SYM -> PENDING_PURGE -> tick dropped outside write queue
        reg.remove_symbol("SYM")
        assert reg.load().symbols["SYM"].status == STATUS_PENDING_PURGE
        await engine.reload_symbols()
        assert "SYM" not in engine.active_streaming_symbols

        await engine._handle_capital_tick({
            "epic": "SYM", "price": 101.0, "timestamp": now, "bid": 100.9, "ask": 101.1
        })
        assert engine.write_queue.qsize() == 0, "Tick for PENDING_PURGE symbol was not dropped"

        # 3. Complete purge -> symbol purged -> tick dropped outside write queue
        reg.complete_purge("SYM")
        await engine.reload_symbols()
        assert "SYM" not in engine.active_streaming_symbols

        await engine._handle_capital_tick({
            "epic": "SYM", "price": 102.0, "timestamp": now, "bid": 101.9, "ask": 102.1
        })
        assert engine.write_queue.qsize() == 0, "Tick for purged symbol was not dropped"

        # 4. Re-add SYM -> Gen 2 -> tick accepted again
        entry2 = reg.add_symbol("SYM", capital_ticker="SYM")
        assert entry2.generation == 2
        await engine.reload_symbols()
        assert "SYM" in engine.active_streaming_symbols

        await engine._handle_capital_tick({
            "epic": "SYM", "price": 103.0, "timestamp": now, "bid": 102.9, "ask": 103.1
        })
        assert engine.write_queue.qsize() == 1, "Tick for re-added Gen 2 symbol was not accepted"
        tick2 = engine.write_queue.get_nowait()
        assert tick2[1] == "SYM"
        assert tick2[2] == 103.0

    asyncio.run(_run())


# ============================================================================
# Group 3 (TEST-P24-03): Signal Debouncing, Dashboard REST & Corruption Recovery
# ============================================================================

def test_signal_storm_debouncing_coalesces_500_touches(tmp_path):
    """
    StreamingEngine with registry_poll_interval=10.0 and registry_debounce_interval=0.05.
    Touches signal file 500 times in rapid succession (~100ms).
    Asserts:
    - update_subscriptions is called <= 3 times, completely debouncing the storm
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)
        reg.add_symbol("NVDA", capital_ticker="NVDA")

        engine = StreamingEngine(
            lake_root=lake_root,
            registry_poll_interval=10.0,
            registry_debounce_interval=0.05,
        )
        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer
        engine.running = True

        watcher_task = asyncio.create_task(engine._symbol_watcher_worker())

        try:
            # Burst 500 signal touches across exactly ~100ms in a background thread
            def _touch_storm():
                start = time.monotonic()
                for i in range(500):
                    touch_stream_reload_signal(root=lake_root)
                    target_time = start + (i / 500.0) * 0.10
                    rem = target_time - time.monotonic()
                    if rem > 0.002:
                        time.sleep(rem)

            storm_thread = threading.Thread(target=_touch_storm)
            storm_thread.start()

            while storm_thread.is_alive():
                await asyncio.sleep(0.01)
            storm_thread.join()

            # Wait for trailing debounce window and worker processing
            await asyncio.sleep(0.15)

            call_count = mock_streamer.update_subscriptions.call_count
            assert 1 <= call_count <= 3, (
                f"Expected between 1 and 3 reloads for 500 touches across 100ms, got {call_count}"
            )
        finally:
            engine.running = False
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

    asyncio.run(_run())


def test_dashboard_rest_50_concurrent_requests_stress(ephemeral_server, monkeypatch):
    """
    50 concurrent REST requests across 10 threads (20 POST, 15 PATCH, 15 DELETE).
    Asserts:
    - Zero HTTP 500 Internal Server Errors
    - Conflicting additions return HTTP 409 Conflict
    - Zero DuckDB connections opened (duckdb.connect call count == 0)
    - Final registry version strictly matches 1 + successful mutations
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)
    reg = SymbolRegistry(lake_root)
    assert reg.version == 1

    # Seed 5 baseline symbols: BASE0..BASE4
    initial_symbols = [f"BASE{i}" for i in range(5)]
    for s in initial_symbols:
        reg.add_symbol(s, capital_ticker=s)
    baseline_version = reg.version  # 1 + 5 = 6

    # Spy on duckdb.connect to enforce zero connection invariant
    connect_calls = []
    orig_connect = duckdb.connect

    def spy_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return orig_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy_connect)

    # Prepare 50 requests
    requests_to_run = []

    # 1. 20 POST requests: 10 new unique symbols (should succeed 201), 10 duplicates (should return 409)
    for i in range(10):
        requests_to_run.append(("POST", "/api/streaming/symbols", {"symbol": f"NEW{i}", "capital_ticker": f"NEW{i}"}))
    for i in range(5):
        requests_to_run.append(("POST", "/api/streaming/symbols", {"symbol": f"BASE{i}", "capital_ticker": f"BASE{i}"}))
    for i in range(5):
        requests_to_run.append(("POST", "/api/streaming/symbols", {"symbol": f"NEW{i}", "capital_ticker": f"NEW{i}"}))

    # 2. 15 PATCH requests on existing or newly added symbols
    for i in range(5):
        requests_to_run.append(("PATCH", f"/api/streaming/symbols/BASE{i}", {"active": False}))
    for i in range(5):
        requests_to_run.append(("PATCH", f"/api/streaming/symbols/BASE{i}", {"active": True}))
    for i in range(5):
        requests_to_run.append(("PATCH", f"/api/streaming/symbols/NEW{i}", {"active": False}))

    # 3. 15 DELETE requests on baseline and new symbols
    for i in range(5):
        requests_to_run.append(("DELETE", f"/api/streaming/symbols/BASE{i}", None))
    for i in range(5):
        requests_to_run.append(("DELETE", f"/api/streaming/symbols/NEW{i}", None))
    for i in range(5):
        requests_to_run.append(("DELETE", f"/api/streaming/symbols/NONEXISTENT{i}", None))

    assert len(requests_to_run) == 50

    results = []

    def _execute_req(req):
        method, path, body = req
        url = f"{base_url}{path}"
        try:
            if method == "POST":
                resp = requests.post(url, json=body, timeout=5.0)
            elif method == "PATCH":
                resp = requests.patch(url, json=body, timeout=5.0)
            elif method == "DELETE":
                resp = requests.delete(url, timeout=5.0)
            return (method, path, resp.status_code, resp.json() if resp.content else {})
        except Exception as e:
            return (method, path, 999, {"error": str(e)})

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(_execute_req, requests_to_run))

    status_codes = [r[2] for r in results]

    # Zero 500 errors
    assert 500 not in status_codes, f"Encountered HTTP 500 errors: {[r for r in results if r[2] == 500]}"
    assert 999 not in status_codes, f"Client connection errors: {[r for r in results if r[2] == 999]}"

    # Conflicting additions returned 409
    conflicts = [r for r in results if r[2] == 409]
    assert len(conflicts) > 0, "Expected conflicting POSTs to return HTTP 409"

    # ZERO DuckDB connections opened during all 50 admin operations
    assert len(connect_calls) == 0, f"DuckDB connections were opened: {connect_calls}"

    # Successful mutations strictly incremented version
    # 201 Created from POST, 200 OK from PATCH and DELETE
    successful_mutations = len([r for r in results if r[2] in (200, 201)])
    final_snap = reg.load()
    expected_version = baseline_version + successful_mutations
    assert final_snap.version == expected_version, (
        f"Version mismatch: baseline {baseline_version} + {successful_mutations} mutations "
        f"= {expected_version}, but registry is at {final_snap.version}"
    )


def test_dashboard_409_truthful_conflict_on_pending_purge(ephemeral_server):
    """
    DELETE symbol via REST -> immediately POST same symbol via REST.
    Asserts:
    - POST returns HTTP 409 Conflict
    - Response body explicitly and truthfully explains symbol is in PENDING_PURGE
    """
    base_url, lake_root = ephemeral_server
    init_registry(lake_root)

    # 1. Add symbol via REST
    resp_add = requests.post(
        f"{base_url}/api/streaming/symbols",
        json={"symbol": "PURGE_TEST", "capital_ticker": "PURGE_TEST"},
        timeout=3.0,
    )
    assert resp_add.status_code in (200, 201)

    # 2. DELETE symbol via REST -> marks PENDING_PURGE
    resp_del = requests.delete(
        f"{base_url}/api/streaming/symbols/PURGE_TEST",
        timeout=3.0,
    )
    assert resp_del.status_code == 200
    del_data = resp_del.json()
    assert del_data.get("status") == STATUS_PENDING_PURGE

    # 3. Immediately POST same symbol via REST
    resp_conflict = requests.post(
        f"{base_url}/api/streaming/symbols",
        json={"symbol": "PURGE_TEST", "capital_ticker": "PURGE_TEST"},
        timeout=3.0,
    )
    assert resp_conflict.status_code == 409, f"Expected HTTP 409 Conflict, got {resp_conflict.status_code}"
    body = resp_conflict.json()
    err_text = (body.get("error", "") + " " + body.get("message", "")).lower()
    assert "pending purge" in err_text or "purge" in err_text


def test_corrupted_registry_resilience_and_graceful_recovery(tmp_path):
    """
    Corrupts registry.json with 0-byte file, truncated JSON, array JSON, and non-dict primitives.
    Asserts:
    - reg.load() raises RegistryCorruptedError across all corrupted cases
    - StreamingEngine symbol watcher worker logs debug and survives without crashing
    - Overwriting corrupted file with valid registry restores normal operation and triggers reload
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)
        reg.add_symbol("AAPL", capital_ticker="AAPL")
        reg_file = reg.path

        # 1. 0-byte file
        reg_file.write_bytes(b"")
        with pytest.raises(RegistryCorruptedError):
            reg.load()

        # 2. Truncated JSON
        reg_file.write_bytes(b'{"version": 5, "symbols": {"AAP')
        with pytest.raises(RegistryCorruptedError):
            reg.load()

        # 3. Array JSON root
        reg_file.write_bytes(b'["not", "a", "dictionary"]')
        with pytest.raises(RegistryCorruptedError):
            reg.load()

        # 4. Non-dict primitive root
        reg_file.write_bytes(b'"just a string"')
        with pytest.raises(RegistryCorruptedError):
            reg.load()

        # 5. Runner watcher resilience: watcher survives polling corrupted file
        engine = StreamingEngine(
            lake_root=lake_root,
            registry_poll_interval=0.05,
            registry_debounce_interval=0.01,
        )
        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer
        engine.running = True

        watcher_task = asyncio.create_task(engine._symbol_watcher_worker())

        try:
            # Let watcher poll corrupted file multiple times
            await asyncio.sleep(0.15)
            assert engine.running, "StreamingEngine watcher crashed when reading corrupted registry"
            assert not mock_streamer.update_subscriptions.called

            # 6. Recovery: overwrite with valid registry snapshot (version=10, symbol MSFT)
            valid_snap = RegistrySnapshot(
                version=10,
                updated_at=datetime.now(timezone.utc).isoformat(),
                symbols={"MSFT": SymbolEntry(symbol="MSFT", display_name="Microsoft", capital_ticker="MSFT")},
                generations={},
            )
            reg._save_snapshot(valid_snap)
            touch_stream_reload_signal(root=lake_root)

            # Wait for watcher to recover and reload subscriptions
            reloaded = False
            for _ in range(30):
                if mock_streamer.update_subscriptions.called:
                    reloaded = True
                    break
                await asyncio.sleep(0.01)

            assert reloaded, "Watcher failed to recover after registry was restored"
            args, _ = mock_streamer.update_subscriptions.call_args
            assert "MSFT" in args[0]
        finally:
            engine.running = False
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

    asyncio.run(_run())
