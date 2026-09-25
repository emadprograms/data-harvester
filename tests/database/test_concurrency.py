"""
Tests for DuckDB Connection Concurrency Robustness & Adaptive Configuration Fallback.
Validates:
1. In-process adaptive configuration matching (ro -> rw and rw -> ro without configuration mismatch errors).
2. Multi-threaded mixed configuration stress testing.
3. Dashboard server concurrent endpoint stress without DuckDB connection errors.
4. Intermittent flush locking lifecycle ensuring readers never experience permanent lockouts.
"""
import os
import time
import shutil
import tempfile
import threading
import concurrent.futures
import urllib.request
import pytest

from src.database.connection import DuckDBClient, get_duckdb_connection
from src.database.schema import init_historical_db, init_streaming_db
from src.database.operations import save_ticks_to_storage, query_ticks
from src.dashboard.server import create_dashboard_server


@pytest.fixture
def temp_test_db_dir():
    temp_dir = tempfile.mkdtemp()
    hist_path = os.path.join(temp_dir, "historical.duckdb")
    stream_path = os.path.join(temp_dir, "streaming.duckdb")
    
    # Initialize schemas (init_historical_db seeds default symbols)
    h_client = DuckDBClient(hist_path, read_only=False)
    init_historical_db(h_client)
    h_client.close()

    s_client = DuckDBClient(stream_path, read_only=False)
    init_streaming_db(s_client)
    s_client.close()

    yield hist_path, stream_path

    shutil.rmtree(temp_dir, ignore_errors=True)


def test_adaptive_config_fallback_ro_then_rw(temp_test_db_dir):
    """
    DuckDBClient must adaptively connect when read_only=True is open and another connection
    requests read_only=False in the same process.
    """
    hist_path, _ = temp_test_db_dir

    # First connection: explicitly read_only=True
    c1 = DuckDBClient(hist_path, read_only=True)
    assert c1 is not None

    # Second connection: requested as read_only=False in same process
    # Previously this crashed with: Connection Error: Can't open a connection to same database file with a different configuration
    c2 = DuckDBClient(hist_path, read_only=False)
    assert c2 is not None

    # Both must be capable of executing queries
    res1 = c1.execute("SELECT COUNT(*) FROM symbol_map").fetchone()
    res2 = c2.execute("SELECT COUNT(*) FROM symbol_map").fetchone()

    assert res1[0] > 0
    assert res2[0] == res1[0]

    c1.close()
    c2.close()


def test_adaptive_config_fallback_rw_then_ro(temp_test_db_dir):
    """
    DuckDBClient must adaptively connect when read_only=False is open and another connection
    requests read_only=True in the same process.
    """
    hist_path, _ = temp_test_db_dir

    # First connection: read_only=False
    c1 = DuckDBClient(hist_path, read_only=False)
    assert c1 is not None

    # Second connection: requested as read_only=True
    c2 = DuckDBClient(hist_path, read_only=True)
    assert c2 is not None

    res1 = c1.execute("SELECT COUNT(*) FROM symbol_map").fetchone()
    res2 = c2.execute("SELECT COUNT(*) FROM symbol_map").fetchone()

    assert res1[0] > 0
    assert res2[0] == res1[0]

    c1.close()
    c2.close()


def test_multi_threaded_mixed_config_concurrency_stress(temp_test_db_dir):
    """
    High-concurrency stress test: 30 worker threads concurrently opening connections
    with alternating read_only=True / read_only=False modes on the same file.
    All threads must succeed without configuration mismatch errors.
    """
    hist_path, _ = temp_test_db_dir

    def worker_thread(index):
        # Alternate configurations across threads
        mode = (index % 2 == 0)
        client = get_duckdb_connection(hist_path, read_only=mode)
        if client is None:
            return False, f"Thread {index} failed to obtain connection"

        try:
            res = client.execute("SELECT COUNT(*) FROM symbol_map").fetchone()
            count = res[0]
            return True, count
        except Exception as e:
            return False, str(e)
        finally:
            client.close()

    thread_count = 30
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker_thread, i) for i in range(thread_count)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    failures = [r for r in results if not r[0]]
    assert len(failures) == 0, f"Thread failures encountered: {failures}"
    assert all(r[1] > 0 for r in results)


def test_dashboard_concurrent_api_requests_no_duckdb_conflict(temp_test_db_dir, monkeypatch):
    """
    Simulates rapid concurrent requests to the dashboard HTTP server.
    Ensures /api/status, /api/symbols, and /api/integrity can all run simultaneously
    without any thread encountering a DuckDB configuration or lock conflict.
    """
    hist_path, stream_path = temp_test_db_dir
    monkeypatch.setattr("src.database.connection.DEFAULT_HISTORICAL_DB_PATH", hist_path)
    monkeypatch.setattr("src.database.connection.DEFAULT_STREAMING_DB_PATH", stream_path)
    monkeypatch.setattr("src.database.connection.DEFAULT_DB_PATH", hist_path)
    monkeypatch.setattr("src.utils.integrity.DEFAULT_HISTORICAL_DB_PATH", hist_path)
    monkeypatch.setattr("src.utils.integrity.DEFAULT_STREAMING_DB_PATH", stream_path)

    # Use a random high port
    port = 8993
    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.3)

    urls = [
        f"http://127.0.0.1:{port}/api/status",
        f"http://127.0.0.1:{port}/api/symbols",
        f"http://127.0.0.1:{port}/api/status",
        f"http://127.0.0.1:{port}/api/symbols",
        f"http://127.0.0.1:{port}/api/integrity?symbol=AAPL",
        f"http://127.0.0.1:{port}/api/status",
        f"http://127.0.0.1:{port}/api/symbols",
    ] * 2  # 14 concurrent requests

    def request_worker(url):
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                status = response.status
                body = response.read()
                return status, len(body)
        except Exception as e:
            return 500, str(e)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(request_worker, urls))

        for status, length in responses:
            assert status == 200
            assert length > 0
    finally:
        server.server_close()


def test_intermittent_flush_allows_concurrent_queries(temp_test_db_dir, monkeypatch):
    """
    Validates that save_ticks_to_storage(None, ticks) connects, flushes, and immediately
    releases the file lock, allowing concurrent read queries without lock contention.
    """
    _, stream_path = temp_test_db_dir
    monkeypatch.setattr("src.database.connection.DEFAULT_STREAMING_DB_PATH", stream_path)

    # Ingest 10 batches of ticks with intermittent flushes
    for i in range(10):
        ticks = [
            (f"2026-09-25 18:00:{i:02d}.000", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        ]
        ok = save_ticks_to_storage(None, ticks, label="TestStream")
        assert ok is True

        # Immediately query ticks on the same path
        df = query_ticks("AAPL", client=DuckDBClient(stream_path, read_only=False))
        assert len(df) == i + 1
