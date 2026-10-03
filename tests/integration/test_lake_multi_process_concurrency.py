"""
Multi-Process Concurrency & Performance Gate Integration Test.
Milestone v4.0 - Phase 21 (P8: Production Cutover, Concurrency Validation & Handoff).

Orchestrates 4 concurrent processes under sustained load:
1. Process 1 (Writer): Async TickLakeWriter ingesting 6,000 synthetic ticks across 5 symbols
   (AAPL, MSFT, NVDA, TSLA, AMZN) with max_batch_rows=500 and flush_interval_seconds=1.0.
   Monitors a 5ms heartbeat measuring event-loop scheduling lag.
2. Process 2 (Dashboard Server): Subprocess running python -m src.dashboard.server on an
   ephemeral port with TICK_LAKE_ROOT=<lake_root>.
   Receives 150+ concurrent HTTP requests across /api/candles, /api/stream/tape,
   /api/stream/status, /api/streaming/continuity.
3. Process 3 (Repo B Reader): Standalone Python subprocess with ZERO data-harvester imports
   (sanitized sys.path), querying Parquet files via in-memory DuckDB (duckdb.connect(":memory:"))
   using contracts from docs/contracts/repo_b_tick_lake_contract.md over 50+ iterations during active writes.
4. Process 4 (Lock Isolation Guard): Background subprocess holding an exclusive write lock on
   a dummy streaming.duckdb, proving live Parquet lake operations require zero database file locks.

Acceptance Gates Asserted:
- Exactly 0 DuckDB file lock errors (IOException).
- Exactly 0 row tearing or corrupted Parquet footer errors.
- Writer p99 event-loop lag < 20.0ms.
- Dashboard p95 query latency < 100.0ms.
- Repo B p95 query latency < 100.0ms.
- Data parity: 100% match between published ticks (6,000) and Repo B queried rows (6,000).
"""
import json
from pathlib import Path
import subprocess
import sys
import pytest

from tools.validate_concurrency import run_concurrency_validation, print_summary_table

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.mark.integration
def test_lake_multi_process_concurrency(tmp_path):
    """
    Executes the full 4-process concurrency benchmark with 6,000 synthetic ticks,
    160 concurrent Dashboard HTTP requests, 60 Repo B reader iterations, and
    an active exclusive DuckDB lock guard on legacy storage.
    """
    lake_root = tmp_path / "tick_lake"

    summary = run_concurrency_validation(
        lake_root=lake_root,
        total_ticks=6000,
        dashboard_requests=160,
        repo_b_iterations=60,
        cleanup=False,
    )

    # Print human-readable summary table into pytest stdout capture
    print_summary_table(summary)

    m = summary.metrics

    # Gate 1: Exactly 0 DuckDB file lock errors (duckdb.IOException)
    assert m.duckdb_io_exceptions_total == 0, (
        f"Encountered {m.duckdb_io_exceptions_total} DuckDB IOException lock collisions!"
    )

    # Gate 2: Exactly 0 row tearing or corrupted Parquet footer errors
    assert m.repo_b_corrupted_footers == 0, (
        f"Encountered {m.repo_b_corrupted_footers} corrupted Parquet footer errors!"
    )

    # Gate 3: Writer event-loop scheduling lag p99 < 20.0ms
    assert m.writer_p99_lag_ms < 20.0, (
        f"Writer event-loop p99 lag was {m.writer_p99_lag_ms:.2f}ms (must be < 20.0ms)"
    )

    # Gate 4: Dashboard p95 query latency < 100.0ms with 0 HTTP errors
    assert m.dashboard_error_count == 0, (
        f"Dashboard encountered {m.dashboard_error_count} HTTP errors during concurrent load"
    )
    assert m.dashboard_p95_latency_ms < 100.0, (
        f"Dashboard p95 query latency was {m.dashboard_p95_latency_ms:.2f}ms (must be < 100.0ms)"
    )

    # Gate 5: Repo B p95 query latency < 100.0ms
    assert m.repo_b_p95_latency_ms < 100.0, (
        f"Repo B p95 query latency was {m.repo_b_p95_latency_ms:.2f}ms (must be < 100.0ms)"
    )

    # Gate 6: Data parity (100% of published rows queried by Repo B)
    assert m.total_ticks_published == 6000, (
        f"Expected 6,000 published ticks, but got {m.total_ticks_published}"
    )
    assert m.repo_b_query_rows == 6000, (
        f"Expected Repo B to query 6,000 rows, but got {m.repo_b_query_rows}"
    )
    assert m.data_parity_match is True, "Data parity gate failed between writer and reader"

    # Overall Summary Pass Check
    assert summary.overall_passed is True, "One or more concurrency validation gates failed"


@pytest.mark.integration
def test_validate_concurrency_cli_execution(tmp_path):
    """
    Verifies that the standalone tools/validate_concurrency.py CLI script
    executes successfully and produces a valid structured JSON report.
    """
    lake_root = tmp_path / "cli_lake"
    json_out = tmp_path / "cli_summary.json"

    cmd = [
        sys.executable,
        str(REPO_ROOT / "tools" / "validate_concurrency.py"),
        "--ticks", "1000",
        "--lake-root", str(lake_root),
        "--dashboard-requests", "40",
        "--repo-b-iterations", "20",
        "--output-json", str(json_out),
    ]

    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, f"CLI validation failed: {result.stderr}\nOutput:\n{result.stdout}"
    assert json_out.is_file(), "JSON summary file was not generated by CLI"

    with open(json_out, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["overall_passed"] is True
    assert data["metrics"]["total_ticks_published"] == 1000
    assert data["metrics"]["repo_b_query_rows"] == 1000
    assert data["metrics"]["duckdb_io_exceptions_total"] == 0


@pytest.mark.integration
def test_repo_b_reader_sys_path_isolation():
    """
    Asserts that the isolation guard used in Process 3 strictly prevents
    importing data-harvester modules (src and tools) while preserving duckdb.
    """
    code = f"""
import sys, os
repo_root = r'{REPO_ROOT}'
sys.path = [
    p for p in sys.path
    if p and os.path.abspath(p) != os.path.abspath(repo_root) and ("data-harvester" not in os.path.abspath(p) or "site-packages" in p)
]

src_blocked = False
try:
    import src
except (ImportError, ModuleNotFoundError):
    src_blocked = True

tools_blocked = False
try:
    import tools
except (ImportError, ModuleNotFoundError):
    tools_blocked = True

import duckdb
assert src_blocked, "src should NOT be importable in Repo B!"
assert tools_blocked, "tools should NOT be importable in Repo B!"
assert hasattr(duckdb, "connect"), "duckdb should be importable!"
print("ISOLATION_VERIFIED", flush=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(tmp_path_fallback := Path("/tmp")),
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, f"Isolation guard check failed: {result.stderr}"
    assert "ISOLATION_VERIFIED" in result.stdout
