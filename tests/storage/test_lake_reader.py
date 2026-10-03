"""
Comprehensive test suite for TickLakeReader (Milestone v4.0 - Phase 19).

Validates:
- Empty lake graceful queries (zero crashes / no duckdb.IOException)
- Hive partition pruning at filesystem glob level
- 100% deterministic OHLCV oracle parity against calculate_expected_candles
- All 5 core edge-case scenarios (exact duplicates, subsecond bursts, late arrivals, nulls/Capital, UTC midnight)
- Exchange-aligned get_candles response format with ET time_str and session metrics
- Reverse-chronological get_tape with spread calculation
- Point lookup get_latest_tick
- Stream status reading _control/writer_status.json
- Discovery of available trading weeks from lake partition dates
"""
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import pytest

from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from tests.fixtures.deterministic_quotes import (
    QuoteTick,
    calculate_expected_candles,
    generate_exact_duplicates,
    generate_subsecond_burst,
    generate_out_of_order_late_arrivals,
    generate_null_and_capital_quotes,
    generate_utc_midnight_rollover,
    generate_multisymbol_distribution,
)


@pytest.mark.parametrize(
    ("trading_day", "tick_utc"),
    [
        (date(2026, 3, 9), datetime(2026, 3, 9, 13, 30)),
        (date(2026, 11, 2), datetime(2026, 11, 2, 14, 30)),
    ],
)
@pytest.mark.parametrize(("extended", "expected_count"), [(False, 390), (True, 960)])
def test_continuity_cached_session_preserves_dst_and_response_isolation(
    tmp_path, trading_day, tick_utc, extended, expected_count,
):
    """Cached minute templates must preserve exchange epochs and return fresh response data."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    with LakePublisher(root=lake_root, writer_id="continuity") as publisher:
        publisher.publish_batch(
            [QuoteTick(
                timestamp=tick_utc,
                symbol="AAPL",
                price=100.0,
                volume=1.0,
                ingest_id="test_tick",
            )],
            batch_id="continuity_batch",
            sequence=1,
        )

    reader = TickLakeReader(root=lake_root)
    params = dict(symbol="AAPL", target_date=trading_day.isoformat(), include_extended=extended)
    first = reader.get_streaming_continuity_analysis(**params)
    second = reader.get_streaming_continuity_analysis(**params)
    assert second == first
    buckets = second["days"][0]["buckets"]
    assert len(buckets) == expected_count
    opening = next(bucket for bucket in buckets if bucket["time"] == "09:30")
    expected_open = int(datetime(
        trading_day.year, trading_day.month, trading_day.day,
        9, 30, tzinfo=ZoneInfo("America/New_York"),
    ).timestamp())
    assert opening["start_epoch"] == expected_open
    assert opening["end_epoch"] == expected_open + 60
    assert opening["status"] == "healthy"

    first["days"][0]["buckets"][0]["status"] = "mutated"
    assert reader.get_streaming_continuity_analysis(**params) == second


@pytest.fixture
def empty_lake(tmp_path):
    """Initializes an empty tick lake with valid directory structure and metadata."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    return lake_root


@pytest.fixture
def populated_lake(tmp_path):
    """Initializes a tick lake populated with multi-symbol ticks across dates."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t0_day1 = datetime(2026, 10, 2, 14, 30, 0, 0)
    t0_day2 = datetime(2026, 10, 3, 14, 30, 0, 0)

    ticks_aapl_d1 = [
        QuoteTick(timestamp=t0_day1 + timedelta(seconds=i), symbol="AAPL", price=150.0 + i * 0.1, volume=10.0, ingest_id=f"aapl_d1_{i}")
        for i in range(10)
    ]
    ticks_aapl_d2 = [
        QuoteTick(timestamp=t0_day2 + timedelta(seconds=i), symbol="AAPL", price=151.0 + i * 0.1, volume=10.0, ingest_id=f"aapl_d2_{i}")
        for i in range(10)
    ]
    ticks_nvda_d1 = [
        QuoteTick(timestamp=t0_day1 + timedelta(seconds=i), symbol="NVDA", price=120.0 + i * 0.2, volume=20.0, ingest_id=f"nvda_d1_{i}")
        for i in range(10)
    ]
    ticks_msft_d1 = [
        QuoteTick(timestamp=t0_day1 + timedelta(seconds=i), symbol="MSFT", price=420.0 + i * 0.5, volume=30.0, ingest_id=f"msft_d1_{i}")
        for i in range(10)
    ]

    all_ticks = ticks_aapl_d1 + ticks_aapl_d2 + ticks_nvda_d1 + ticks_msft_d1
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(all_ticks, batch_id="batch_populated_01", sequence=1)

    return lake_root


# ----------------------------------------------------------------------------
# 1. Empty Lake Queries Return Empty Gracefully
# ----------------------------------------------------------------------------

def test_empty_lake_queries_return_empty(empty_lake):
    """
    Querying candles, tape, ticks, or latest tick on an empty lake (zero parquet files)
    must return empty structures gracefully without raising duckdb.IOException or crashing.
    """
    reader = TickLakeReader(root=empty_lake)

    # 1. low-level query_candles
    candles = reader.query_candles(symbol="AAPL", timeframe="1m")
    assert candles == []

    # 2. exchange-aligned get_candles
    res = reader.get_candles(symbol="AAPL", date="2026-10-02")
    assert isinstance(res, dict)
    assert res.get("symbol") == "AAPL"
    assert res.get("candles") == []
    assert res.get("count") == 0
    assert res.get("day_total_ticks") == 0

    # 3. get_tape
    tape = reader.get_tape(symbol="AAPL", limit=20)
    assert isinstance(tape, dict)
    assert tape.get("ticks") == []
    assert tape.get("count") == 0

    # 4. query_ticks
    ticks = reader.query_ticks(symbol="AAPL")
    assert ticks == []

    # 5. get_latest_tick
    latest = reader.get_latest_tick(symbol="AAPL")
    assert latest is None

    # 6. resolve_partition_files
    files = reader.resolve_partition_files(symbol="AAPL")
    assert files == []


# ----------------------------------------------------------------------------
# 2. Hive Partition Pruning at Filesystem Glob Level
# ----------------------------------------------------------------------------

def test_hive_partition_pruning(populated_lake):
    """
    Populate multiple symbols (AAPL, NVDA, MSFT) across multiple dates.
    Verify resolve_partition_files(symbol='AAPL') returns ONLY files under symbol=AAPL/
    and date filtering strictly prunes non-matching date partitions.
    """
    reader = TickLakeReader(root=populated_lake)

    # All AAPL files (across 2026-10-02 and 2026-10-03)
    aapl_files = reader.resolve_partition_files(symbol="AAPL")
    assert len(aapl_files) >= 2
    for f in aapl_files:
        p_str = str(f)
        assert "symbol=AAPL" in p_str
        assert "symbol=NVDA" not in p_str
        assert "symbol=MSFT" not in p_str

    # Date-specific pruning for AAPL (only 2026-10-03)
    aapl_d2_files = reader.resolve_partition_files(
        symbol="AAPL",
        start_date="2026-10-03",
        end_date="2026-10-03",
    )
    assert len(aapl_d2_files) == 1
    assert "date=2026-10-03" in str(aapl_d2_files[0])
    assert "date=2026-10-02" not in str(aapl_d2_files[0])

    # NVDA files
    nvda_files = reader.resolve_partition_files(symbol="NVDA")
    assert len(nvda_files) == 1
    assert "symbol=NVDA" in str(nvda_files[0])


# ----------------------------------------------------------------------------
# 3. Deterministic OHLCV Oracle Parity
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_oracle_parity(tmp_path):
    """
    Generate synthetic quotes using tests/fixtures/deterministic_quotes.py across
    multi-symbol distribution, publish them with LakePublisher, query with reader,
    and assert 100% exact numerical match with calculate_expected_candles for
    open, high, low, close, volume, and tick_count at both 1m and 5m timeframes.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_multisymbol_distribution(total_ticks=600, base_time=datetime(2026, 10, 2, 14, 30, 0))
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_multi_parity", sequence=1)

    reader = TickLakeReader(root=lake_root)

    # Validate for NVDA
    nvda_ticks = [t for t in ticks if t.symbol == "NVDA"]

    for tf in ["1m", "5m"]:
        expected_candles = calculate_expected_candles(nvda_ticks, timeframe=tf)
        actual_candles = reader.query_candles(symbol="NVDA", timeframe=tf)

        assert len(actual_candles) == len(expected_candles), (
            f"Candle count mismatch for {tf}: actual={len(actual_candles)}, expected={len(expected_candles)}"
        )

        for act, exp in zip(actual_candles, expected_candles):
            # Time check
            act_time = act["time"] if isinstance(act["time"], datetime) else datetime.fromisoformat(str(act["time"]))
            assert act_time == exp.time, f"Time mismatch: {act_time} != {exp.time}"

            # Exact OHLCV numerical parity
            assert pytest.approx(act["open"], 1e-6) == exp.open, f"Open mismatch at {exp.time}: {act['open']} != {exp.open}"
            assert pytest.approx(act["high"], 1e-6) == exp.high, f"High mismatch at {exp.time}: {act['high']} != {exp.high}"
            assert pytest.approx(act["low"], 1e-6) == exp.low, f"Low mismatch at {exp.time}: {act['low']} != {exp.low}"
            assert pytest.approx(act["close"], 1e-6) == exp.close, f"Close mismatch at {exp.time}: {act['close']} != {exp.close}"
            assert pytest.approx(act["volume"], 1e-6) == exp.volume, f"Volume mismatch at {exp.time}: {act['volume']} != {exp.volume}"
            assert act["tick_count"] == exp.tick_count, f"Tick count mismatch at {exp.time}: {act['tick_count']} != {exp.tick_count}"


# ----------------------------------------------------------------------------
# 4. Scenario 1: Exact Duplicates Tie-Breaking
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_exact_duplicates(tmp_path):
    """
    Scenario 1: Verifies that multiple ticks with identical timestamps break ties
    deterministically by (timestamp, ingest_id) for open (arg_min) and close (arg_max).
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_exact_duplicates(symbol="AAPL", base_time=datetime(2026, 10, 2, 14, 30, 0))
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_exact_dups", sequence=1)

    reader = TickLakeReader(root=lake_root)
    expected_candles = calculate_expected_candles(ticks, timeframe="1m")
    actual_candles = reader.query_candles(symbol="AAPL", timeframe="1m")

    assert len(actual_candles) == len(expected_candles)
    for act, exp in zip(actual_candles, expected_candles):
        assert pytest.approx(act["open"], 1e-6) == exp.open
        assert pytest.approx(act["close"], 1e-6) == exp.close
        assert pytest.approx(act["high"], 1e-6) == exp.high
        assert pytest.approx(act["low"], 1e-6) == exp.low
        assert act["tick_count"] == exp.tick_count


# ----------------------------------------------------------------------------
# 5. Scenario 2: Sub-second Microsecond Burst
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_subsecond_burst(tmp_path):
    """
    Scenario 2: Verifies microsecond interval precision and ordering within a single second.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_subsecond_burst(symbol="NVDA", base_time=datetime(2026, 10, 2, 14, 30, 0), count=50)
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_subsecond", sequence=1)

    reader = TickLakeReader(root=lake_root)
    expected_candles = calculate_expected_candles(ticks, timeframe="1m")
    actual_candles = reader.query_candles(symbol="NVDA", timeframe="1m")

    assert len(actual_candles) == len(expected_candles)
    for act, exp in zip(actual_candles, expected_candles):
        assert pytest.approx(act["open"], 1e-6) == exp.open
        assert pytest.approx(act["close"], 1e-6) == exp.close
        assert act["tick_count"] == 50


# ----------------------------------------------------------------------------
# 6. Scenario 3: Out-of-Order / Late Arrivals
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_out_of_order(tmp_path):
    """
    Scenario 3: Verifies that ticks arriving out of order are correctly placed into
    chronological buckets according to their event timestamp, matching oracle open/close.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_out_of_order_late_arrivals(symbol="MSFT", base_time=datetime(2026, 10, 2, 14, 30, 0))
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_out_of_order", sequence=1)

    reader = TickLakeReader(root=lake_root)
    expected_candles = calculate_expected_candles(ticks, timeframe="1m")
    actual_candles = reader.query_candles(symbol="MSFT", timeframe="1m")

    assert len(actual_candles) == len(expected_candles)
    for act, exp in zip(actual_candles, expected_candles):
        assert pytest.approx(act["open"], 1e-6) == exp.open
        assert pytest.approx(act["close"], 1e-6) == exp.close
        assert pytest.approx(act["high"], 1e-6) == exp.high
        assert pytest.approx(act["low"], 1e-6) == exp.low
        assert act["tick_count"] == exp.tick_count


# ----------------------------------------------------------------------------
# 7. Scenario 4: Nulls & Capital Observation Semantics
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_nulls_and_capital(tmp_path):
    """
    Scenario 4: Verifies null volume coalesces to 1.0 (Capital quote observation semantics)
    and null spreads/sources/sessions do not cause query failure.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_null_and_capital_quotes(symbol="AAPL", base_time=datetime(2026, 10, 2, 14, 30, 0))
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_nulls", sequence=1)

    reader = TickLakeReader(root=lake_root)
    expected_candles = calculate_expected_candles(ticks, timeframe="1m")
    actual_candles = reader.query_candles(symbol="AAPL", timeframe="1m")

    assert len(actual_candles) == len(expected_candles)
    for act, exp in zip(actual_candles, expected_candles):
        assert pytest.approx(act["volume"], 1e-6) == exp.volume
        assert act["tick_count"] == exp.tick_count


# ----------------------------------------------------------------------------
# 8. Scenario 5: UTC Midnight Rollover
# ----------------------------------------------------------------------------

def test_deterministic_ohlcv_utc_midnight_rollover(tmp_path):
    """
    Scenario 5: Verifies ticks spanning 23:59:59.999999 -> 00:00:00.000000 across UTC day
    boundary partition into separate directories and aggregate cleanly.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = generate_utc_midnight_rollover(symbol="NVDA", base_date=date(2026, 10, 2))
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_midnight", sequence=1)

    reader = TickLakeReader(root=lake_root)
    expected_candles = calculate_expected_candles(ticks, timeframe="1m")
    actual_candles = reader.query_candles(symbol="NVDA", timeframe="1m")

    assert len(actual_candles) == len(expected_candles)
    for act, exp in zip(actual_candles, expected_candles):
        assert pytest.approx(act["open"], 1e-6) == exp.open
        assert pytest.approx(act["close"], 1e-6) == exp.close
        assert act["tick_count"] == exp.tick_count


# ----------------------------------------------------------------------------
# 9. Exchange-Aligned get_candles Response Format
# ----------------------------------------------------------------------------

def test_get_candles_exchange_aligned_response_format(tmp_path):
    """
    Verifies get_candles(symbol='SPY', date='2026-10-02') returns the complete dictionary
    matching the dashboard contract:
    - time (UTC epoch int)
    - time_str (formatted in America/New_York)
    - day_total_ticks, session_total_ticks, session_start_epoch, session_end_epoch
    - gaps list
    - chronologically ascending candle list
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 14:30 UTC is 10:30 EDT (regular session)
    t_start = datetime(2026, 10, 2, 14, 30, 0)
    ticks = [
        QuoteTick(
            timestamp=t_start + timedelta(minutes=i, seconds=15),
            symbol="SPY",
            price=570.0 + i * 0.25,
            volume=100.0,
            bid=569.95 + i * 0.25,
            ask=570.05 + i * 0.25,
            source="CAPITAL",
            session="REG",
            ingest_id=f"spy_reg_{i:03d}",
        )
        for i in range(5)
    ]
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_spy_candles", sequence=1)

    reader = TickLakeReader(root=lake_root)
    res = reader.get_candles(symbol="SPY", date="2026-10-02", timeframe="1m", hours="regular")

    assert isinstance(res, dict)
    assert res.get("symbol") == "SPY"
    assert res.get("database") in ("streaming", "lake")
    assert res.get("timezone") == "America/New_York"
    assert res.get("date") == "2026-10-02"
    assert res.get("timeframe") == "1m"
    assert isinstance(res.get("day_total_ticks"), int)
    assert res["day_total_ticks"] >= 5
    assert isinstance(res.get("session_total_ticks"), int)
    assert res["session_total_ticks"] >= 5
    assert isinstance(res.get("session_start_epoch"), int)
    assert isinstance(res.get("session_end_epoch"), int)
    assert "gaps" in res
    assert isinstance(res["gaps"], list)

    candles = res.get("candles")
    assert isinstance(candles, list)
    assert len(candles) == 5

    # Ascending sort check and field validation
    for i, c in enumerate(candles):
        assert "time" in c and isinstance(c["time"], int)
        assert "time_str" in c and isinstance(c["time_str"], str)
        assert "open" in c
        assert "high" in c
        assert "low" in c
        assert "close" in c
        assert "volume" in c
        assert "tick_count" in c
        if i > 0:
            assert candles[i]["time"] > candles[i - 1]["time"]


# ----------------------------------------------------------------------------
# 10. Tape Reverse-Chronological Ordering with Spread
# ----------------------------------------------------------------------------

def test_tape_reverse_chronological(tmp_path):
    """
    Verifies get_tape(symbol='AAPL', limit=20) returns latest ticks sorted descending
    by (timestamp DESC, ingest_id DESC) with millisecond timestamps and calculated spread.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t_base = datetime(2026, 10, 2, 14, 30, 0)
    ticks = [
        QuoteTick(
            timestamp=t_base + timedelta(seconds=i),
            symbol="AAPL",
            price=150.0 + i * 0.05,
            volume=50.0,
            bid=149.95 + i * 0.05,
            ask=150.05 + i * 0.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"aapl_tape_{i:02d}",
        )
        for i in range(10)
    ]
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_tape_test", sequence=1)

    reader = TickLakeReader(root=lake_root)
    tape = reader.get_tape(symbol="AAPL", limit=5)

    assert isinstance(tape, dict)
    tick_list = tape.get("ticks")
    assert isinstance(tick_list, list)
    assert len(tick_list) == 5

    # Verify reverse chronological ordering
    for i in range(len(tick_list) - 1):
        curr_ts = tick_list[i]["timestamp"]
        next_ts = tick_list[i + 1]["timestamp"]
        assert curr_ts >= next_ts

    # Verify calculated spread
    first_tick = tick_list[0]
    assert "spread" in first_tick
    assert pytest.approx(first_tick["spread"], 1e-4) == 0.10
    assert "time_str" in first_tick


# ----------------------------------------------------------------------------
# 11. Point Lookup: get_latest_tick
# ----------------------------------------------------------------------------

def test_get_latest_tick(tmp_path):
    """
    Verifies get_latest_tick(symbol='AAPL') returns the most recent tick for that symbol.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    t_base = datetime(2026, 10, 2, 14, 30, 0)
    ticks = [
        QuoteTick(timestamp=t_base, symbol="AAPL", price=150.0, ingest_id="t1"),
        QuoteTick(timestamp=t_base + timedelta(seconds=10), symbol="AAPL", price=150.75, ingest_id="t2"),
        QuoteTick(timestamp=t_base + timedelta(seconds=5), symbol="AAPL", price=150.35, ingest_id="t3"),
    ]
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_latest_tick", sequence=1)

    reader = TickLakeReader(root=lake_root)
    latest = reader.get_latest_tick(symbol="AAPL")

    assert latest is not None
    assert latest.get("symbol") == "AAPL"
    assert pytest.approx(latest.get("price"), 1e-4) == 150.75
    assert latest.get("ingest_id") == "t2"


# ----------------------------------------------------------------------------
# 12. Stream Status Reads _control/writer_status.json
# ----------------------------------------------------------------------------

def test_stream_status_reads_writer_status_file(tmp_path):
    """
    Verifies get_stream_status() reads _control/writer_status.json and reports
    writer liveness, total written rows, batches published, and healthy status.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    control_dir = lake_root / "_control"
    control_dir.mkdir(parents=True, exist_ok=True)
    status_file = control_dir / "writer_status.json"

    payload = {
        "status": "RUNNING",
        "writer_id": "writer_worker_01",
        "pid": 98765,
        "total_rows_written": 12500,
        "batches_published": 45,
        "total_quarantined": 0,
        "total_retrying": 0,
        "last_publish_time": datetime.now(timezone.utc).timestamp(),
        "last_batch_id": "batch_last_99",
        "last_batch_rows": 100,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    reader = TickLakeReader(root=lake_root)
    st = reader.get_stream_status()

    assert isinstance(st, dict)
    assert st.get("is_alive") is True
    assert st.get("writer_id") == "writer_worker_01"
    assert st.get("ticks_total") == 12500
    assert st.get("status") in ("healthy", "RUNNING")

    # If status file removed, reports offline gracefully
    status_file.unlink()
    st_offline = reader.get_stream_status()
    assert st_offline.get("is_alive") is False
    assert st_offline.get("ticks_total") == 0


# ----------------------------------------------------------------------------
# 13. Discover Available Trading Weeks from Lake
# ----------------------------------------------------------------------------

def test_discover_available_weeks_from_lake(tmp_path):
    """
    Verifies trading dates present in the lake partitions are discovered and
    grouped Monday-to-Friday into trading weeks sorted descending by week_start.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # Week 1: 2026-09-28 (Mon), 2026-09-30 (Wed), 2026-10-02 (Fri)
    # Week 2: 2026-10-05 (Mon), 2026-10-06 (Tue)
    dates = [
        datetime(2026, 9, 28, 14, 30, 0),
        datetime(2026, 9, 30, 14, 30, 0),
        datetime(2026, 10, 2, 14, 30, 0),
        datetime(2026, 10, 5, 14, 30, 0),
        datetime(2026, 10, 6, 14, 30, 0),
    ]
    ticks = [
        QuoteTick(timestamp=d, symbol="AAPL", price=150.0, ingest_id=f"w_tick_{i}")
        for i, d in enumerate(dates)
    ]
    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        publisher.publish_batch(ticks, batch_id="batch_weeks_test", sequence=1)

    reader = TickLakeReader(root=lake_root)
    weeks = reader.discover_available_weeks()

    assert isinstance(weeks, list)
    assert len(weeks) == 2

    # Sorted descending by week_start
    assert weeks[0]["week_start"] == "2026-10-05"
    assert weeks[0]["week_end"] == "2026-10-09"
    assert weeks[0]["trading_days_count"] == 2

    assert weeks[1]["week_start"] == "2026-09-28"
    assert weeks[1]["week_end"] == "2026-10-02"
    assert weeks[1]["trading_days_count"] == 3
