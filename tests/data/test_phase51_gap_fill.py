"""Phase 51: request only all-symbol silence, and do not rewrite the lake."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from src.data.gap_fill import (
    COVERAGE_FILENAME,
    GapFillBudgetExceeded,
    GapFillRefused,
    early_close_et,
    fill_named_day,
    is_full_nyse_holiday,
    main as gap_fill_main,
    silence_stretches,
)
from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisherLock
from src.storage.schema import ticks_to_table
from tests.fixtures.deterministic_quotes import QuoteTick


NY = ZoneInfo("America/New_York")
SYMBOLS = ["AAPL", "NVDA"]
NORMAL_DAY = date(2026, 10, 2)  # Friday, not a holiday


def _et(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=NY)


def _occupy_window(day: date, start: datetime, end: datetime) -> set[datetime]:
    occupied = set()
    cursor = start
    while cursor < end:
        occupied.add(cursor)
        cursor += timedelta(minutes=1)
    return occupied


def _except(occupied: set[datetime], start: datetime, end: datetime) -> set[datetime]:
    cursor = start
    while cursor < end:
        occupied.discard(cursor)
        cursor += timedelta(minutes=1)
    return occupied


def test_qfill_01_requests_only_all_symbol_silence():
    """A minute with one symbol ticking is not requested. A 5-minute silence is."""
    start = _et(NORMAL_DAY, 4, 0)
    end = _et(NORMAL_DAY, 20, 0)
    occupied = _occupy_window(NORMAL_DAY, start, end)
    _except(occupied, _et(NORMAL_DAY, 10, 1), _et(NORMAL_DAY, 10, 6))

    stretches = silence_stretches(NORMAL_DAY, occupied, SYMBOLS)
    assert stretches == [(_et(NORMAL_DAY, 10, 1), _et(NORMAL_DAY, 10, 6))]


def test_qfill_02_thresholds_and_session_splits():
    """15 minutes pre/post, 2 minutes regular, and a split at each session boundary."""
    start = _et(NORMAL_DAY, 4, 0)
    end = _et(NORMAL_DAY, 20, 0)
    occupied = _occupy_window(NORMAL_DAY, start, end)
    _except(occupied, _et(NORMAL_DAY, 4, 0), _et(NORMAL_DAY, 4, 10))  # 10 min pre: no
    _except(occupied, _et(NORMAL_DAY, 4, 30), _et(NORMAL_DAY, 4, 44))  # 14 min pre: no
    _except(occupied, _et(NORMAL_DAY, 5, 0), _et(NORMAL_DAY, 5, 15))  # 15 min pre: yes
    _except(occupied, _et(NORMAL_DAY, 10, 30), _et(NORMAL_DAY, 10, 31))  # 1 min regular: no
    _except(occupied, _et(NORMAL_DAY, 11, 0), _et(NORMAL_DAY, 11, 2))  # 2 min regular: yes
    _except(occupied, _et(NORMAL_DAY, 9, 20), _et(NORMAL_DAY, 9, 40))  # crosses 09:30
    _except(occupied, _et(NORMAL_DAY, 15, 50), _et(NORMAL_DAY, 16, 20))  # crosses 16:00
    _except(occupied, _et(NORMAL_DAY, 18, 0), _et(NORMAL_DAY, 18, 10))  # 10 min post: no

    stretches = silence_stretches(NORMAL_DAY, occupied, SYMBOLS)
    assert (_et(NORMAL_DAY, 4, 0), _et(NORMAL_DAY, 4, 10)) not in stretches
    assert (_et(NORMAL_DAY, 4, 30), _et(NORMAL_DAY, 4, 44)) not in stretches
    assert (_et(NORMAL_DAY, 5, 0), _et(NORMAL_DAY, 5, 15)) in stretches
    assert (_et(NORMAL_DAY, 10, 30), _et(NORMAL_DAY, 10, 31)) not in stretches
    assert (_et(NORMAL_DAY, 11, 0), _et(NORMAL_DAY, 11, 2)) in stretches
    assert (_et(NORMAL_DAY, 9, 20), _et(NORMAL_DAY, 9, 40)) not in stretches
    assert (_et(NORMAL_DAY, 9, 20), _et(NORMAL_DAY, 9, 30)) not in stretches
    assert (_et(NORMAL_DAY, 9, 30), _et(NORMAL_DAY, 9, 40)) in stretches
    assert (_et(NORMAL_DAY, 15, 50), _et(NORMAL_DAY, 16, 0)) in stretches
    assert (_et(NORMAL_DAY, 16, 0), _et(NORMAL_DAY, 16, 20)) in stretches
    assert (_et(NORMAL_DAY, 15, 50), _et(NORMAL_DAY, 16, 20)) not in stretches
    assert (_et(NORMAL_DAY, 18, 0), _et(NORMAL_DAY, 18, 10)) not in stretches


def test_qfill_02_qualifying_pieces_stay_split():
    """A silence that qualifies on both sides of 09:30 is two requests, not one."""
    occupied = _occupy_window(NORMAL_DAY, _et(NORMAL_DAY, 4, 0), _et(NORMAL_DAY, 20, 0))
    _except(occupied, _et(NORMAL_DAY, 9, 0), _et(NORMAL_DAY, 9, 45))
    stretches = silence_stretches(NORMAL_DAY, occupied, SYMBOLS)
    assert (_et(NORMAL_DAY, 9, 0), _et(NORMAL_DAY, 9, 30)) in stretches
    assert (_et(NORMAL_DAY, 9, 30), _et(NORMAL_DAY, 9, 45)) in stretches
    assert (_et(NORMAL_DAY, 9, 0), _et(NORMAL_DAY, 9, 45)) not in stretches


def test_qfill_03_skips_weekend_holiday_and_time_after_early_close():
    """Weekends, full NYSE holidays, and time after 13:00 ET on an early close are skipped."""
    assert silence_stretches(date(2026, 10, 3), set(), SYMBOLS) == []
    assert is_full_nyse_holiday(date(2026, 4, 3)) is True
    assert silence_stretches(date(2026, 4, 3), set(), SYMBOLS) == []
    assert is_full_nyse_holiday(date(2026, 7, 3)) is True
    assert silence_stretches(date(2026, 7, 3), set(), SYMBOLS) == []

    early = date(2026, 12, 24)
    assert early_close_et(early) == _et(early, 13, 0)
    occupied = _occupy_window(early, _et(early, 4, 0), _et(early, 13, 0))
    stretches = silence_stretches(early, occupied, SYMBOLS)
    assert stretches == []
    assert all(end <= _et(early, 13, 0) for _start, end in silence_stretches(early, set(), SYMBOLS))
    assert all(start < _et(early, 13, 0) for start, _end in silence_stretches(early, set(), SYMBOLS))


def test_nyse_calendar_matches_published_2024_through_2026():
    """The selector uses the NYSE Group holiday and early-close dates, not a federal subset."""
    published_holidays = {
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 12, 25),
        date(2025, 1, 1), date(2025, 1, 9), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 12, 25),
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    }
    published_early = {
        date(2024, 7, 3), date(2024, 11, 29), date(2024, 12, 24),
        date(2025, 7, 3), date(2025, 11, 28), date(2025, 12, 24),
        date(2026, 11, 27), date(2026, 12, 24),
    }
    for year in (2024, 2025, 2026):
        cursor = date(year, 1, 1)
        while cursor.year == year:
            assert is_full_nyse_holiday(cursor) is (cursor in published_holidays)
            if cursor in published_early:
                assert early_close_et(cursor) == _et(cursor, 13, 0)
            else:
                assert early_close_et(cursor) is None
            cursor += timedelta(days=1)


class _Range:
    def __init__(self, frame):
        self._frame = frame

    def to_df(self):
        return self._frame


class _Timeseries:
    def __init__(self, frame):
        self.calls = []
        self._frame = frame

    def get_range(self, **kwargs):
        self.calls.append(kwargs)
        return _Range(self._frame)


class _Client:
    def __init__(self, frame):
        self.timeseries = _Timeseries(frame)


def _write_occupied_day(lake: Path, day: date, gap_start: datetime, gap_end: datetime) -> Path:
    rows = []
    cursor = _et(day, 4, 0)
    index = 0
    while cursor < _et(day, 20, 0):
        if not (gap_start <= cursor < gap_end):
            utc_naive = cursor.astimezone(timezone.utc).replace(tzinfo=None)
            rows.append(
                QuoteTick(
                    timestamp=utc_naive,
                    symbol="AAPL",
                    price=100.25,
                    volume=1.0,
                    bid=100.00,
                    ask=100.04,
                    source="CAPITAL",
                    session="REG",
                    ingest_id=f"occupied_{index:04d}",
                )
            )
            index += 1
        cursor += timedelta(minutes=1)
    table = ticks_to_table(rows, validate=True)
    dest = lake / "ticks" / "symbol=AAPL" / f"date={day.isoformat()}"
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "batch_existing.parquet"
    pq.write_table(table, path)
    return path


def test_qfill_04_requests_tbbo_stretches_and_stores_bid_and_ask(tmp_path):
    """Only the silent stretch is requested. Trade price and size are not stored."""
    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    existing = _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)
    before = existing.read_bytes()

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:01:00+00:00"]),
            "symbol": ["NVDA"],
            "price": [100.50],
            "size": [12],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _Client(raw)
    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.timeseries.calls) == 1
    call = client.timeseries.calls[0]
    assert call["schema"] == "tbbo"
    assert call["symbols"] == SYMBOLS
    assert call["start"] == "2026-10-02T14:01:00"
    assert call["end"] == "2026-10-02T14:06:00"
    assert existing.read_bytes() == before

    written = list((lake / "ticks").glob("symbol=*/date=*/*.parquet"))
    new_files = [path for path in written if path != existing]
    assert new_files
    table = pq.read_table(new_files[0])
    assert "bid_price" in table.column_names
    assert "ask_price" in table.column_names
    assert "price" not in table.column_names
    assert "volume" not in table.column_names
    assert table["bid_price"][0].as_py() == pytest.approx(100.00)
    assert table["ask_price"][0].as_py() == pytest.approx(100.04)
    assert result["requests"] == 1
    assert result["follow_up_requests"] == 0


def test_qfill_03_command_makes_no_request_on_closed_sessions(tmp_path):
    """A named closed day does not call Databento."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    client = _Client(None)
    for closed in (date(2026, 10, 3), date(2026, 4, 3), date(2026, 7, 3)):
        result = fill_named_day(closed, client=client, lake_root=lake, symbols=SYMBOLS)
        assert result["requests"] == 0
    assert client.timeseries.calls == []


def test_qfill_03_command_stops_at_the_early_close(tmp_path):
    """An empty early-close day is requested only through 13:00 ET."""
    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    client = _Client(pd.DataFrame())
    early = date(2026, 12, 24)
    fill_named_day(early, client=client, lake_root=lake, symbols=SYMBOLS, occupied_minutes=set())

    assert client.timeseries.calls
    assert [call["end"] for call in client.timeseries.calls] == [
        "2026-12-24T14:30:00",
        "2026-12-24T18:00:00",
    ]
    assert all(call["schema"] == "tbbo" for call in client.timeseries.calls)


def test_zero_cost_estimate_still_fills_silence(tmp_path):
    """A $0 estimate for the old window does not skip the silence fill."""
    from unittest.mock import MagicMock, patch

    from src.data.databento_backfill import run_databento_backfill

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    client = MagicMock()
    with patch("src.data.databento_backfill.get_target_stock_symbols", return_value=SYMBOLS), patch(
        "src.data.databento_backfill.estimate_day_cost", return_value=0.0
    ), patch(
        "src.data.gap_fill.fill_named_day",
        return_value={"requests": 1, "follow_up_requests": 0, "rows": 1},
    ) as mocked:
        run_databento_backfill(
            max_budget=25.0,
            max_days=1,
            start_date=NORMAL_DAY,
            client=client,
            lake_root=lake,
        )
    assert mocked.call_count == 1


def test_qfill_05_operator_loop_does_not_call_databento_while_locked(tmp_path):
    """The multi-day command refuses before a cost estimate or a data request."""
    from unittest.mock import MagicMock, patch

    from src.data.databento_backfill import run_databento_backfill

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    lock = LakePublisherLock(root=lake, writer_id="live_writer")
    lock.acquire(blocking=False)
    client = MagicMock()
    try:
        with patch("src.data.databento_backfill.get_target_stock_symbols", return_value=SYMBOLS):
            with pytest.raises(GapFillRefused):
                run_databento_backfill(
                    max_budget=25.0,
                    max_days=1,
                    start_date=NORMAL_DAY,
                    client=client,
                    lake_root=lake,
                )
    finally:
        lock.release()
    client.metadata.get_cost.assert_not_called()
    client.timeseries.get_range.assert_not_called()


def test_qfill_05_refuses_when_live_writer_holds_the_lock(tmp_path):
    """No Databento call is made while the publisher lock is held."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    lock = LakePublisherLock(root=lake, writer_id="live_writer")
    lock.acquire(blocking=False)
    client = _Client(None)
    try:
        with pytest.raises(GapFillRefused):
            fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    finally:
        lock.release()
    assert client.timeseries.calls == []


def test_qfill_05_holds_lock_through_download_and_publish(tmp_path):
    """Gap fill keeps the publisher lock across get_range so a live writer cannot sneak in."""
    import pandas as pd

    from src.storage.publication import LakeMaintenanceInProgressError, LakeOwnershipError

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)

    class _LockedRange:
        def __init__(self, frame):
            self._frame = frame

        def to_df(self):
            return self._frame

    class _LockedTimeseries:
        def __init__(self, frame, lake_root: Path):
            self.calls = []
            self._frame = frame
            self._lake = lake_root

        def get_range(self, **kwargs):
            self.calls.append(kwargs)
            intruder = LakePublisherLock(root=self._lake, writer_id="live_writer")
            with pytest.raises((LakeOwnershipError, LakeMaintenanceInProgressError)):
                intruder.acquire(blocking=False)
            return _LockedRange(self._frame)

    class _LockedClient:
        def __init__(self, frame, lake_root: Path):
            self.timeseries = _LockedTimeseries(frame, lake_root)

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:01:00+00:00"]),
            "symbol": ["NVDA"],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _LockedClient(raw, lake)
    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert result["requests"] == 1
    assert result["rows"] == 1
    # Lock is released after the fill so a later writer can acquire it.
    later = LakePublisherLock(root=lake, writer_id="live_writer")
    later.acquire(blocking=False)
    later.release()


def test_qfill_03_excludes_january_9_2025_day_of_mourning(tmp_path):
    """QFILL-03: 2025-01-09 is a full NYSE closure and must not be requested."""
    closed = date(2025, 1, 9)
    assert is_full_nyse_holiday(closed) is True
    assert silence_stretches(closed, set(), SYMBOLS) == []
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    client = _Client(None)
    result = fill_named_day(closed, client=client, lake_root=lake, symbols=SYMBOLS)
    assert result["requests"] == 0
    assert client.timeseries.calls == []


def test_qfill_04_sparse_success_is_not_requested_again(tmp_path):
    """QFILL-04: a sparse successful tbbo fill is coverage, not a remaining hole."""
    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)
    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:01:00+00:00"]),
            "symbol": ["NVDA"],
            "price": [100.50],
            "size": [12],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _Client(raw)
    first = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert first["requests"] == 1
    assert len(client.timeseries.calls) == 1

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert second["requests"] == 0
    assert second["follow_up_requests"] == 0
    assert len(client.timeseries.calls) == 1


def test_qfill_04_crash_between_publish_and_coverage_does_not_duplicate(tmp_path):
    """A durable publish without a ledger commit replays the same batch identity."""
    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    existing = _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)
    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:01:00+00:00"]),
            "symbol": ["NVDA"],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _Client(raw)
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    coverage = lake / "_control" / COVERAGE_FILENAME
    assert coverage.is_file()
    coverage.unlink()

    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    v2_files = [
        path
        for path in (lake / "ticks").glob("symbol=*/date=*/*.parquet")
        if path != existing and "bid_price" in pq.read_schema(path).names
    ]
    rows = sum(pq.read_table(path).num_rows for path in v2_files)
    assert rows == 1
    ingest_ids = [
        row["ingest_id"]
        for path in v2_files
        for row in pq.read_table(path).to_pylist()
    ]
    assert len(ingest_ids) == 1


def test_f4_estimates_selected_intervals_and_refuses_over_budget(tmp_path):
    """F4: estimate the stretches that will be requested; refuse before download."""
    from unittest.mock import MagicMock

    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)
    client = _Client(pd.DataFrame())
    client.metadata = MagicMock()
    client.metadata.get_cost.return_value = 9.0

    with pytest.raises(GapFillBudgetExceeded):
        fill_named_day(
            NORMAL_DAY,
            client=client,
            lake_root=lake,
            symbols=SYMBOLS,
            remaining_budget=1.0,
        )
    assert client.timeseries.calls == []
    cost_call = client.metadata.get_cost.call_args.kwargs
    assert cost_call["start"] == "2026-10-02T14:01:00"
    assert cost_call["end"] == "2026-10-02T14:06:00"
    assert cost_call["schema"] == "tbbo"
    assert cost_call["symbols"] == SYMBOLS


def test_qfill_01_named_day_cli_processes_exactly_one_date(tmp_path):
    """QFILL-01: the executable entry takes --date and does not walk other days."""
    from unittest.mock import patch

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    client = _Client(None)
    with patch("src.data.databento_backfill.get_databento_client", return_value=client), patch(
        "src.data.databento_backfill.get_target_stock_symbols", return_value=SYMBOLS
    ):
        gap_fill_main(["--date", "2026-10-03", "--lake-root", str(lake)])
    assert client.timeseries.calls == []


def test_walker_uses_overridden_lake_registry(tmp_path):
    """Symbols come from the selected lake_root registry, not the process default."""
    from unittest.mock import MagicMock, patch

    from src.data.databento_backfill import run_databento_backfill
    from src.storage.registry import SymbolRegistry, init_registry

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    init_registry(lake)
    SymbolRegistry(root=lake).add_symbol("NVDA", capital_ticker="NVDA", display_name="NVDA")

    captured = {}

    def fake_fill(trading_date, client, lake_root, symbols, remaining_budget=None, **kwargs):
        captured["symbols"] = list(symbols)
        return {"requests": 0, "follow_up_requests": 0, "rows": 0, "estimated_cost": 0.0}

    with patch("src.data.gap_fill.fill_named_day", side_effect=fake_fill):
        run_databento_backfill(
            max_budget=25.0,
            max_days=1,
            start_date=NORMAL_DAY,
            client=MagicMock(),
            lake_root=lake,
        )
    assert captured["symbols"] == ["NVDA"]
