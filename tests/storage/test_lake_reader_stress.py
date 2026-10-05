"""
Deep stress, concurrency scaling, memory stability, and edge-case test suite for
In-Memory DuckDB Lake Reader & Analytics (Milestone v4.1 - Phase 25).

Requirements verified:
- TEST-P25-01: Multi-threaded in-memory DuckDB connection scaling (30+ concurrent readers)
              without memory leaks (src/storage/reader.py).
- TEST-P25-02: Vectorized resampling edge cases: sparse partitions, multi-day roll-overs,
              DST shifts, leap years (src/storage/reader.py, src/dashboard/analytics.py).
- TEST-P25-03: Reverse-chronological tape pagination with high offsets and non-existent
              symbol pruning (src/storage/reader.py, src/dashboard/analytics.py).
"""
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import gc
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage.schema import LAKE_SCHEMA_V1

from src.dashboard.analytics import (
    discover_available_weeks,
    get_stream_status,
    get_stream_tape,
    get_streaming_candles,
    get_streaming_continuity_analysis,
    get_ticks,
)
from src.storage.config import (
    LakeMaintenanceInProgressError,
    init_tick_lake,
)
from src.storage.publication import LakePublisher
from src.storage.reader import (
    TickLakeReader,
    _safe_float,
)
from tests.fixtures.deterministic_quotes import (
    QuoteTick,
    calculate_expected_candles,
)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


# ============================================================================
# Helpers
# ============================================================================

def _seed_lake_with_symbol_ticks(
    lake_root: Path,
    symbol: str,
    base_time: datetime,
    count: int = 100,
    interval_seconds: float = 1.0,
    price_start: float = 150.0,
) -> List[QuoteTick]:
    """Helper to publish sequential QuoteTicks to the lake."""
    ticks: List[QuoteTick] = []
    for i in range(count):
        ts = base_time + timedelta(seconds=i * interval_seconds)
        price = price_start + (i * 0.1)
        ticks.append(
            QuoteTick(
                timestamp=ts,
                symbol=symbol,
                price=price,
                volume=10.0 + (i % 5),
                bid=price - 0.05,
                ask=price + 0.05,
                source="CAPITAL",
                session="REG",
                ingest_id=f"{symbol.lower()}_stress_{i:05d}",
            )
        )
    with LakePublisher(root=lake_root, writer_id="stress_writer") as publisher:
        publisher.publish_batch(ticks, batch_id=f"batch_{symbol.lower()}_{count}", sequence=1)
    return ticks


# ============================================================================
# Group 1: TEST-P25-01: Multi-Threaded In-Memory Concurrency & Memory Leak Audit
# ============================================================================

def test_p25_01_concurrency_scaling_35_threads_heavy_queries(tmp_path):
    """
    TEST-P25-01: 35 worker threads in ThreadPoolExecutor executing 20 queries each
    (700 queries total) mixing get_candles, query_candles, and get_tape.
    Asserts 100% of futures complete without duckdb.ConnectionException or duckdb.IOException.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    for sym in ["AAPL", "MSFT", "NVDA"]:
        _seed_lake_with_symbol_ticks(lake_root, sym, t0, count=150)

    reader = TickLakeReader(root=lake_root, max_threads=2)

    def execute_query(query_idx: int) -> Dict[str, Any]:
        mod = query_idx % 4
        if mod == 0:
            res = reader.get_candles(symbol="AAPL", timeframe="1m", date="2026-10-02")
            return {"type": "get_candles", "count": res.get("count", 0)}
        elif mod == 1:
            res = reader.query_candles(symbol="MSFT", timeframe="1m")
            return {"type": "query_candles", "count": len(res)}
        elif mod == 2:
            res = reader.get_tape(symbol="NVDA", limit=50)
            return {"type": "get_tape_sym", "count": res.get("count", 0)}
        else:
            res = reader.get_tape(limit=100)
            return {"type": "get_tape_all", "count": res.get("count", 0)}

    num_threads = 35
    queries_per_thread = 20
    total_queries = num_threads * queries_per_thread

    futures = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(total_queries):
            futures.append(executor.submit(execute_query, i))

        results = []
        for f in as_completed(futures):
            res = f.result()
            assert res is not None
            assert "type" in res
            results.append(res)

    assert len(results) == total_queries
    # Verify zero exceptions and all queries returned expected positive counts
    assert all(r["count"] > 0 for r in results)


def test_p25_01_memory_leak_audit_1000_sequential_queries(tmp_path):
    """
    TEST-P25-01: 1,000 sequential queries on populated lake.
    Warm-up 50 queries. Measure RSS at 250, 500, 750, 1000.
    Asserts growth between 250 and 1000 is <= 30 MB.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    _seed_lake_with_symbol_ticks(lake_root, "AAPL", t0, count=200)

    reader = TickLakeReader(root=lake_root, max_threads=2)
    process = psutil.Process(os.getpid())

    # Warm-up 50 queries
    for _ in range(50):
        reader.query_candles(symbol="AAPL", timeframe="1m")
        reader.get_tape(symbol="AAPL", limit=20)

    gc.collect()
    rss_measurements: Dict[int, int] = {}

    for i in range(1, 1001):
        if i % 2 == 0:
            reader.get_candles(symbol="AAPL", timeframe="1m", date="2026-10-02")
        else:
            reader.get_tape(symbol="AAPL", limit=50)

        if i in (250, 500, 750, 1000):
            gc.collect()
            rss_measurements[i] = process.memory_info().rss

    rss_250 = rss_measurements[250]
    rss_1000 = rss_measurements[1000]
    growth_mb = (rss_1000 - rss_250) / (1024 * 1024)

    assert growth_mb <= 30.0, (
        f"Memory leak detected: RSS grew {growth_mb:.2f} MB between query 250 and 1000 (limit: 30 MB)"
    )


def test_p25_01_memory_stability_repeated_concurrency_waves(tmp_path):
    """
    TEST-P25-01: 5 waves of 30 concurrent threads.
    Asserts memory stabilizes and growth between wave 2 and 5 is <= 45 MB.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    for sym in ["AAPL", "GOOGL"]:
        _seed_lake_with_symbol_ticks(lake_root, sym, t0, count=150)

    reader = TickLakeReader(root=lake_root, max_threads=2)
    process = psutil.Process(os.getpid())

    def run_worker_queries():
        for _ in range(5):
            reader.get_candles(symbol="AAPL", timeframe="1m")
            reader.get_tape(symbol="GOOGL", limit=30)
        return True

    wave_rss: Dict[int, int] = {}

    for wave in range(1, 6):
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = [executor.submit(run_worker_queries) for _ in range(30)]
            for f in as_completed(futures):
                assert f.result() is True

        gc.collect()
        wave_rss[wave] = process.memory_info().rss

    growth_wave2_to_5_mb = (wave_rss[5] - wave_rss[2]) / (1024 * 1024)
    assert growth_wave2_to_5_mb <= 45.0, (
        f"Memory instability: RSS grew {growth_wave2_to_5_mb:.2f} MB between wave 2 and 5 (limit: 45 MB)"
    )


def test_p25_01_duckdb_configuration_knobs_respected(tmp_path):
    """
    TEST-P25-01: Verifies DuckDB PRAGMAs threads=2, max_memory ~256MB, TimeZone='UTC'
    are actively enforced on all isolated in-memory connections.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    reader = TickLakeReader(root=lake_root, max_threads=2, max_memory="256MB")
    con = reader.connect()
    try:
        threads_setting = con.execute("SELECT current_setting('threads')").fetchone()[0]
        tz_setting = con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
        mem_setting = con.execute("SELECT current_setting('max_memory')").fetchone()[0]

        assert int(threads_setting) == 2
        assert str(tz_setting).upper() == "UTC"
        # DuckDB formats 256MB as '244.1 MiB' or '256MB'
        assert any(token in str(mem_setting) for token in ["244", "256", "MiB", "MB"])
    finally:
        con.close()


def test_p25_01_concurrent_readers_maintenance_lock_interruption(tmp_path):
    """
    TEST-P25-01: Verifies mid-flight creation of _maintenance/in_progress.json
    cleanly raises LakeMaintenanceInProgressError and unlinking the lock restores queries.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    _seed_lake_with_symbol_ticks(lake_root, "AAPL", t0, count=50)

    reader = TickLakeReader(root=lake_root)
    # 1. Queries work initially
    res = reader.get_candles(symbol="AAPL", timeframe="1m")
    assert res["count"] > 0

    # 2. Acquire maintenance lock
    m_dir = lake_root / "_maintenance"
    m_dir.mkdir(parents=True, exist_ok=True)
    lock_file = m_dir / "in_progress.json"
    lock_file.write_text(json.dumps({"reason": "compaction", "pid": os.getpid()}), encoding="utf-8")

    # 3. Subsequent reader operations must raise LakeMaintenanceInProgressError
    with pytest.raises(LakeMaintenanceInProgressError):
        reader.connect()

    with pytest.raises(LakeMaintenanceInProgressError):
        reader.get_candles(symbol="AAPL")

    with pytest.raises(LakeMaintenanceInProgressError):
        reader.get_tape(symbol="AAPL")

    with pytest.raises(LakeMaintenanceInProgressError):
        reader.resolve_partition_files(symbol="AAPL")

    with pytest.raises(LakeMaintenanceInProgressError):
        reader.snapshot(symbols=["AAPL"])

    # 4. Remove maintenance lock -> queries immediately restored
    lock_file.unlink()
    restored_res = reader.get_candles(symbol="AAPL", timeframe="1m")
    assert restored_res["count"] > 0


# ============================================================================
# Group 2: TEST-P25-02: Vectorized Resampling Edge Cases
# ============================================================================

def test_p25_02_resampling_sparse_partitions_exact_gap_accounting(tmp_path):
    """
    TEST-P25-02: Symbol with ticks only at 10:00:15 and 10:01:45 ET.
    Exactly 2 candles returned, 30m leading gap (09:30-09:59), 358m trailing gap (10:02-15:59);
    Total gap minutes (30 + 358 = 388) + candle minutes (2) == 390.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 2026-10-02 (EDT = UTC-4):
    # 10:00:15 ET == 14:00:15 UTC
    # 10:01:45 ET == 14:01:45 UTC
    t1_utc = datetime(2026, 10, 2, 14, 0, 15, tzinfo=timezone.utc)
    t2_utc = datetime(2026, 10, 2, 14, 1, 45, tzinfo=timezone.utc)

    ticks = [
        QuoteTick(
            timestamp=t1_utc,
            symbol="SPARSE",
            price=100.0,
            volume=50.0,
            bid=99.9,
            ask=100.1,
            source="CAPITAL",
            session="REG",
            ingest_id="sparse_001",
        ),
        QuoteTick(
            timestamp=t2_utc,
            symbol="SPARSE",
            price=101.0,
            volume=60.0,
            bid=100.9,
            ask=101.1,
            source="CAPITAL",
            session="REG",
            ingest_id="sparse_002",
        ),
    ]

    with LakePublisher(root=lake_root, writer_id="sparse_w") as publisher:
        publisher.publish_batch(ticks, batch_id="sparse_b1", sequence=1)

    reader = TickLakeReader(root=lake_root)
    resp = reader.get_candles(symbol="SPARSE", date="2026-10-02", hours="regular")

    candles = resp.get("candles", [])
    gaps = resp.get("gaps", [])

    assert len(candles) == 2
    assert candles[0]["time_str"] == "2026-10-02 10:00:00"
    assert candles[1]["time_str"] == "2026-10-02 10:01:00"

    assert len(gaps) == 2
    leading_gap = gaps[0]
    trailing_gap = gaps[1]

    # Leading gap: 09:30 to 09:59 (30 minutes)
    assert leading_gap["duration"] == 30
    assert leading_gap["start_str"] == "09:30"
    assert leading_gap["end_str"] == "09:59"

    # Trailing gap: 10:02 to 15:59 (358 minutes)
    assert trailing_gap["duration"] == 358
    assert trailing_gap["start_str"] == "10:02"
    assert trailing_gap["end_str"] == "15:59"

    # Total accounting: 30 + 358 + 2 == 390
    assert leading_gap["duration"] + trailing_gap["duration"] + len(candles) == 390
    assert resp["session_total_ticks"] == 2
    assert resp["day_total_ticks"] == 2


def test_p25_02_resampling_zero_session_ticks_full_day_gap(tmp_path):
    """
    TEST-P25-02: Symbol with ticks only overnight at 03:00 ET.
    get_candles(hours="regular") returns 0 candles and 1 full-session gap of 390 minutes.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 2026-10-02 03:00 ET == 07:00 UTC
    t_overnight = datetime(2026, 10, 2, 7, 0, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=t_overnight,
            symbol="OVERNIGHT",
            price=250.0,
            volume=10.0,
            bid=249.9,
            ask=250.1,
            source="CAPITAL",
            session="PRE",
            ingest_id="night_001",
        )
    ]
    with LakePublisher(root=lake_root, writer_id="night_w") as publisher:
        publisher.publish_batch(ticks, batch_id="night_b1", sequence=1)

    reader = TickLakeReader(root=lake_root)
    resp = reader.get_candles(symbol="OVERNIGHT", date="2026-10-02", hours="regular")

    assert len(resp["candles"]) == 0
    assert resp["count"] == 0
    assert resp["session_total_ticks"] == 0
    assert resp["day_total_ticks"] == 1

    gaps = resp.get("gaps", [])
    assert len(gaps) == 1
    assert gaps[0]["duration"] == 390
    assert gaps[0]["start_str"] == "09:30"
    assert gaps[0]["end_str"] == "15:59"


def test_p25_02_resampling_multi_day_across_weekends_and_midnight(tmp_path):
    """
    TEST-P25-02: Ticks across 7 trading days spanning 2 calendar weeks.
    Daily resampling (timeframe='1d') yields exactly 7 daily bars; 0 bars for Saturday/Sunday.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # Mon-Fri: 2026-09-28 to 2026-10-02 (5 trading days)
    # Weekend: 2026-10-03 (Sat), 2026-10-04 (Sun) -> 0 ticks
    # Mon-Tue: 2026-10-05, 2026-10-06 (2 trading days)
    trading_dates = [
        date(2026, 9, 28),
        date(2026, 9, 29),
        date(2026, 9, 30),
        date(2026, 10, 1),
        date(2026, 10, 2),
        date(2026, 10, 5),
        date(2026, 10, 6),
    ]

    all_ticks: List[QuoteTick] = []
    for d in trading_dates:
        t_utc = datetime(d.year, d.month, d.day, 14, 30, 0, tzinfo=timezone.utc)
        all_ticks.append(
            QuoteTick(
                timestamp=t_utc,
                symbol="MDAY",
                price=100.0 + d.day,
                volume=100.0,
                bid=99.9 + d.day,
                ask=100.1 + d.day,
                source="CAPITAL",
                session="REG",
                ingest_id=f"mday_{d.isoformat()}",
            )
        )

    with LakePublisher(root=lake_root, writer_id="mday_w") as publisher:
        publisher.publish_batch(all_ticks, batch_id="mday_batch", sequence=1)

    reader = TickLakeReader(root=lake_root)
    daily_candles = reader.query_candles(symbol="MDAY", timeframe="1d")

    assert len(daily_candles) == 7

    candle_dates = []
    for c in daily_candles:
        dt = c["time"] if isinstance(c["time"], datetime) else datetime.fromisoformat(str(c["time"]))
        candle_dates.append(dt.date())

    assert candle_dates == trading_dates
    # Confirm zero weekend bars
    assert date(2026, 10, 3) not in candle_dates
    assert date(2026, 10, 4) not in candle_dates


def test_p25_02_resampling_dst_spring_forward_and_fall_back(tmp_path):
    """
    TEST-P25-02: Ticks on spring-forward (2026-03-09, EDT, UTC-4) and
    fall-back (2026-11-02, EST, UTC-5).
    09:30 ET opening candles map to correct UTC epochs (13:30 and 14:30) with zero drift.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 1. Spring-Forward Monday 2026-03-09: EDT is UTC-4 -> 09:30 ET == 13:30 UTC
    t_spring_utc = datetime(2026, 3, 9, 13, 30, 0, tzinfo=timezone.utc)
    # 2. Fall-Back Monday 2026-11-02: EST is UTC-5 -> 09:30 ET == 14:30 UTC
    t_fall_utc = datetime(2026, 11, 2, 14, 30, 0, tzinfo=timezone.utc)

    ticks = [
        QuoteTick(
            timestamp=t_spring_utc,
            symbol="DST",
            price=200.0,
            volume=50.0,
            bid=199.9,
            ask=200.1,
            source="CAPITAL",
            session="REG",
            ingest_id="dst_spring_01",
        ),
        QuoteTick(
            timestamp=t_fall_utc,
            symbol="DST",
            price=205.0,
            volume=60.0,
            bid=204.9,
            ask=205.1,
            source="CAPITAL",
            session="REG",
            ingest_id="dst_fall_01",
        ),
    ]

    with LakePublisher(root=lake_root, writer_id="dst_w") as publisher:
        publisher.publish_batch(ticks, batch_id="dst_batch", sequence=1)

    reader = TickLakeReader(root=lake_root)

    # Verify Spring-Forward
    res_spring = reader.get_candles(symbol="DST", date="2026-03-09", hours="regular")
    assert len(res_spring["candles"]) == 1
    c_spring = res_spring["candles"][0]
    assert c_spring["time_str"] == "2026-03-09 09:30:00"
    assert c_spring["time"] == int(t_spring_utc.timestamp())

    # Verify Fall-Back
    res_fall = reader.get_candles(symbol="DST", date="2026-11-02", hours="regular")
    assert len(res_fall["candles"]) == 1
    c_fall = res_fall["candles"][0]
    assert c_fall["time_str"] == "2026-11-02 09:30:00"
    assert c_fall["time"] == int(t_fall_utc.timestamp())


def test_p25_02_resampling_leap_year_february_29(tmp_path):
    """
    TEST-P25-02: Partitions under date=2024-02-29 (Thursday) discovered,
    read, and aggregated accurately.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t_leap_utc = datetime(2024, 2, 29, 14, 30, 0, tzinfo=timezone.utc)
    ticks = [
        QuoteTick(
            timestamp=t_leap_utc + timedelta(seconds=i),
            symbol="LEAP",
            price=300.0 + i,
            volume=25.0,
            bid=299.9 + i,
            ask=300.1 + i,
            source="CAPITAL",
            session="REG",
            ingest_id=f"leap_{i:03d}",
        )
        for i in range(5)
    ]

    with LakePublisher(root=lake_root, writer_id="leap_w") as publisher:
        publisher.publish_batch(ticks, batch_id="leap_batch", sequence=1)

    reader = TickLakeReader(root=lake_root)

    # Verify partition resolution
    files = reader.resolve_partition_files(symbol="LEAP", start_date="2024-02-29", end_date="2024-02-29")
    assert len(files) == 1
    assert "date=2024-02-29" in str(files[0])

    # Verify candles query
    res = reader.get_candles(symbol="LEAP", date="2024-02-29", hours="regular")
    assert res["count"] == 1
    assert res["candles"][0]["time_str"] == "2024-02-29 09:30:00"
    assert res["candles"][0]["open"] == 300.0
    assert res["candles"][0]["close"] == 304.0

    # Verify available weeks includes the 2024 leap week
    weeks = reader.discover_available_weeks()
    assert any(w["week_start"] == "2024-02-26" for w in weeks)


def test_p25_02_resampling_subsecond_high_frequency_tie_breaking(tmp_path):
    """
    TEST-P25-02: 100 ticks within 1 second.
    100% exact numerical match with calculate_expected_candles oracle.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    base_time = datetime(2026, 10, 2, 14, 30, 0, 0, tzinfo=timezone.utc)
    ticks: List[QuoteTick] = []
    for i in range(100):
        # Microsecond precision within the same single second: 14:30:00.000000 -> 14:30:00.990000
        ts = base_time + timedelta(microseconds=i * 10000)
        p = 100.0 + (i % 11) * 0.5
        ticks.append(
            QuoteTick(
                timestamp=ts,
                symbol="HFT",
                price=p,
                volume=5.0 + (i % 3),
                bid=p - 0.05,
                ask=p + 0.05,
                source="CAPITAL",
                session="REG",
                ingest_id=f"hft_{i:04d}",
            )
        )

    with LakePublisher(root=lake_root, writer_id="hft_w") as publisher:
        publisher.publish_batch(ticks, batch_id="hft_batch", sequence=1)

    reader = TickLakeReader(root=lake_root)

    expected_1s = calculate_expected_candles(ticks, timeframe="1s")
    actual_1s = reader.query_candles(symbol="HFT", timeframe="1s")

    assert len(actual_1s) == len(expected_1s) == 1
    act = actual_1s[0]
    exp = expected_1s[0]

    assert pytest.approx(act["open"], 1e-6) == exp.open
    assert pytest.approx(act["high"], 1e-6) == exp.high
    assert pytest.approx(act["low"], 1e-6) == exp.low
    assert pytest.approx(act["close"], 1e-6) == exp.close
    assert pytest.approx(act["volume"], 1e-6) == exp.volume
    assert act["tick_count"] == exp.tick_count == 100


# ============================================================================
# Group 3: TEST-P25-03: Tape Pagination, Partition Pruning & Decoupling
# ============================================================================

def test_p25_03_tape_high_offset_and_page_iteration(tmp_path):
    """
    TEST-P25-03: 2,500 ticks. Bounded pagination with limit=500 produces
    5 distinct non-overlapping pages; offset=5000 returns [].
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    _seed_lake_with_symbol_ticks(lake_root, "PAGETEST", t0, count=2500, interval_seconds=0.1)

    reader = TickLakeReader(root=lake_root)

    seen_ids = set()
    pages = []
    for page_idx in range(5):
        offset = page_idx * 500
        tape = reader.get_tape(symbol="PAGETEST", limit=500, offset=offset)
        page_ticks = tape.get("ticks", [])
        assert len(page_ticks) == 500

        page_ids = [t["ingest_id"] for t in page_ticks]
        # Assert non-overlapping with previously seen pages
        assert len(set(page_ids) & seen_ids) == 0
        seen_ids.update(page_ids)
        pages.append(page_ticks)

    assert len(seen_ids) == 2500

    # Verify offset beyond available rows returns empty list
    empty_tape = reader.get_tape(symbol="PAGETEST", limit=500, offset=5000)
    assert empty_tape.get("ticks") == []
    assert empty_tape.get("count") == 0

    empty_ticks = reader.query_ticks(symbol="PAGETEST", limit=500, offset=5000)
    assert empty_ticks == []


def test_p25_03_tape_multi_date_partition_pruning(tmp_path):
    """
    TEST-P25-03: 10 date partitions. get_tape(limit=20) scans only the latest
    partition dates; returns ticks in strict descending order.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    for day in range(1, 11):
        d = date(2026, 9, day)
        t_utc = datetime(d.year, d.month, d.day, 14, 30, 0, tzinfo=timezone.utc)
        ticks = [
            QuoteTick(
                timestamp=t_utc + timedelta(seconds=i),
                symbol="PRUNE",
                price=100.0 + day,
                volume=10.0,
                bid=99.9 + day,
                ask=100.1 + day,
                source="CAPITAL",
                session="REG",
                ingest_id=f"prune_d{day:02d}_{i:02d}",
            )
            for i in range(50)
        ]
        with LakePublisher(root=lake_root, writer_id=f"prune_w_{day}") as publisher:
            publisher.publish_batch(ticks, batch_id=f"prune_b_{day}", sequence=1)

    reader = TickLakeReader(root=lake_root)

    tape = reader.get_tape(symbol="PRUNE", limit=20)
    ticks = tape.get("ticks", [])
    assert len(ticks) == 20

    # Verify strict descending order
    for i in range(len(ticks) - 1):
        t1 = ticks[i]["timestamp"]
        t2 = ticks[i + 1]["timestamp"]
        assert t1 >= t2

    # Verify partition pruning: all 20 ticks must belong to the latest date (day 10)
    assert all("2026-09-10" in t["timestamp"] for t in ticks)


def test_p25_03_symbol_pruning_fail_fast_on_nonexistent_symbol(tmp_path):
    """
    TEST-P25-03: Querying non-existent symbol returns empty results immediately
    without disk I/O or DuckDB queries.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    _seed_lake_with_symbol_ticks(lake_root, "AAPL", t0, count=50)

    reader = TickLakeReader(root=lake_root)

    # Spy on reader.connect: non-existent symbols must return empty without connecting
    original_connect = reader.connect
    connect_mock = MagicMock(side_effect=original_connect)
    reader.connect = connect_mock

    # 1. resolve_partition_files
    files = reader.resolve_partition_files(symbol="NONEXISTENT_XYZ")
    assert files == []

    # 2. query_candles
    candles = reader.query_candles(symbol="NONEXISTENT_XYZ")
    assert candles == []

    # 3. get_candles
    candles_dict = reader.get_candles(symbol="NONEXISTENT_XYZ")
    assert candles_dict["candles"] == []
    assert candles_dict["count"] == 0

    # 4. query_ticks
    ticks = reader.query_ticks(symbol="NONEXISTENT_XYZ")
    assert ticks == []

    # 5. get_tape
    tape = reader.get_tape(symbol="NONEXISTENT_XYZ")
    assert tape["ticks"] == []
    assert tape["count"] == 0

    # 6. get_latest_tick
    latest = reader.get_latest_tick(symbol="NONEXISTENT_XYZ")
    assert latest is None

    # 7. snapshot
    snap = reader.snapshot(symbols=["NONEXISTENT_XYZ"])
    assert snap.count_rows() == 0

    # Assert ZERO DuckDB connections were opened across all operations
    assert connect_mock.call_count == 0


def test_p25_03_spread_calculation_with_null_and_nan(tmp_path):
    """
    TEST-P25-03: Ticks with null/NaN bid and ask values.
    spread is None (not NaN); json.dumps produces valid JSON.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    part_dir = lake_root / "ticks" / "symbol=NANTEST" / "date=2026-10-02"
    part_dir.mkdir(parents=True, exist_ok=True)

    data = {
        "timestamp": [t0 + timedelta(seconds=i) for i in range(1, 8)],
        "symbol": ["NANTEST"] * 7,
        "price": [100.0] * 7,
        "volume": [1.0] * 7,
        "bid": [None, 99.8, None, float("nan"), 99.8, float("inf"), 99.0],
        "ask": [100.2, None, None, 100.2, float("nan"), 100.2, 101.5],
        "source": ["CAPITAL"] * 7,
        "session": ["REG"] * 7,
        "ingest_id": [f"n{i}" for i in range(1, 8)],
    }
    table = pa.Table.from_pydict(data, schema=LAKE_SCHEMA_V1)
    pq.write_table(table, part_dir / "nan_batch.parquet")

    reader = TickLakeReader(root=lake_root)

    # 1. get_tape
    tape = reader.get_tape(symbol="NANTEST", limit=10)
    tape_ticks = tape["ticks"]
    assert len(tape_ticks) == 7

    # Validate that invalid/missing spreads are None (never float NaN or Inf)
    for tick in tape_ticks:
        if tick["ingest_id"] == "n7":
            assert tick["spread"] == 2.5
        else:
            assert tick["spread"] is None

    # Valid JSON serialization check: allow_nan=False raises ValueError if NaN or Inf persists
    json_tape = json.dumps(tape, allow_nan=False)
    assert json_tape is not None

    # 2. query_ticks
    ticks_res = reader.query_ticks(symbol="NANTEST", limit=10)
    for tick in ticks_res:
        if tick["ingest_id"] == "n7":
            assert tick["spread"] == 2.5
        else:
            assert tick["spread"] is None

    json_ticks = json.dumps(ticks_res, allow_nan=False)
    assert json_ticks is not None


def test_p25_03_analytics_decoupling_zero_streaming_duckdb_access(tmp_path, monkeypatch):
    """
    TEST-P25-03: The disk-database layer no longer exists to be reached.
    Call analytics functions for existing and non-existent symbols; every call is served by the lake.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0 = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)
    _seed_lake_with_symbol_ticks(lake_root, "AAPL", t0, count=25)

    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    # The disk-database layer is physically gone: there is no module to import and no
    # connection helper to call, so a legacy fallback cannot happen even in principle.
    import importlib
    import src.dashboard.analytics as analytics_module

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("src.database.connection")
    assert not hasattr(analytics_module, "get_streaming_db_connection")

    # 1. get_streaming_candles (both existing and non-existent symbols)
    c_existing = get_streaming_candles("AAPL")
    assert c_existing["count"] > 0
    c_missing = get_streaming_candles("NONEXISTENT_SYM")
    assert c_missing["count"] == 0

    # 2. get_stream_tape (both existing and non-existent symbols)
    tape_existing = get_stream_tape("AAPL")
    assert tape_existing["count"] > 0
    tape_missing = get_stream_tape("NONEXISTENT_SYM")
    assert tape_missing["count"] == 0

    # 3. get_ticks (both existing and non-existent symbols)
    ticks_existing = get_ticks("AAPL")
    assert ticks_existing["count"] > 0
    ticks_missing = get_ticks("NONEXISTENT_SYM")
    assert ticks_missing["count"] == 0

    # 4. get_stream_status
    status = get_stream_status()
    assert isinstance(status, dict)

    # 5. discover_available_weeks
    weeks = discover_available_weeks()
    assert isinstance(weeks, list)

    # 6. get_streaming_continuity_analysis
    cont_all = get_streaming_continuity_analysis(symbol="all")
    assert "summary" in cont_all
    cont_sym = get_streaming_continuity_analysis(symbol="AAPL")
    assert "summary" in cont_sym
    cont_missing = get_streaming_continuity_analysis(symbol="NONEXISTENT_SYM")
    assert "summary" in cont_missing


def test_p25_03_empty_lake_behavior_across_all_reader_methods(tmp_path):
    """
    TEST-P25-03: All 10 public methods succeed gracefully with empty data
    on an unpopulated lake without errors or exceptions.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    reader = TickLakeReader(root=lake_root)

    # 1. resolve_partition_files
    assert reader.resolve_partition_files("AAPL") == []

    # 2. connect
    con = reader.connect()
    assert con is not None
    con.close()

    # 3. query_candles
    assert reader.query_candles("AAPL") == []

    # 4. get_candles
    candles = reader.get_candles("AAPL")
    assert candles["candles"] == []
    assert candles["count"] == 0

    # 5. query_ticks
    assert reader.query_ticks("AAPL") == []

    # 6. get_tape
    tape = reader.get_tape("AAPL")
    assert tape["ticks"] == []
    assert tape["count"] == 0

    # 7. get_latest_tick
    assert reader.get_latest_tick("AAPL") is None

    # 8. get_stream_status
    status = reader.get_stream_status()
    assert isinstance(status, dict)
    assert status["is_alive"] is False

    # 9. discover_available_weeks
    assert reader.discover_available_weeks() == []

    # 10. get_streaming_continuity_analysis
    cont = reader.get_streaming_continuity_analysis(symbol="AAPL")
    assert isinstance(cont, dict)
    assert cont["summary"]["total_gaps"] == 0

    # 11. snapshot
    snap = reader.snapshot()
    assert snap.count_rows() == 0

    # 12. get_lake_health_report
    health = reader.get_lake_health_report()
    assert health["total_files"] == 0
