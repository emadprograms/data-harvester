"""Phase 51: request only all-symbol silence, and do not rewrite the lake."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from src.data.gap_fill import (
    COVERAGE_FILENAME,
    GapFillBudgetExceeded,
    GapFillRecoveryError,
    GapFillRefused,
    early_close_et,
    fill_named_day,
    is_full_nyse_holiday,
    main as gap_fill_main,
    occupied_minutes_in_lake,
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


def test_qfill_04_empty_success_is_not_requested_again(tmp_path):
    """A successful empty tbbo response is coverage, not a remaining hole."""
    import pandas as pd

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    gap_start = _et(NORMAL_DAY, 10, 1)
    gap_end = _et(NORMAL_DAY, 10, 6)
    _write_occupied_day(lake, NORMAL_DAY, gap_start, gap_end)
    client = _Client(pd.DataFrame())
    first = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert first["requests"] == 1
    assert first["rows"] == 0
    assert len(client.timeseries.calls) == 1
    assert (lake / "_control" / COVERAGE_FILENAME).is_file()

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert second["requests"] == 0
    assert second["follow_up_requests"] == 0
    assert len(client.timeseries.calls) == 1


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
    """A lost ledger is rebuilt from the receipt: no re-download, no duplicate rows.

    This keeps the original deleted-ledger reproduction and strengthens it: row count
    alone used to pass while the restart still issued a second paid request, so the
    request count, the rebuilt bounds, and the byte-identical v1 file are asserted too.
    """
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
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _Client(raw)
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert len(client.timeseries.calls) == 1
    coverage = lake / "_control" / COVERAGE_FILENAME
    assert coverage.is_file()
    coverage.unlink()

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.timeseries.calls) == 1, "a deleted ledger must not buy the interval again"
    assert second["requests"] == 0
    assert json.loads(coverage.read_text(encoding="utf-8"))["intervals"][0]["start"].startswith(
        "2026-10-02T10:01:00"
    )
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
    assert existing.read_bytes() == before


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


def test_qfill_01_named_day_cli_fills_the_requested_open_day_only(tmp_path):
    """The named-day command requests the gap on that date and then exits."""
    from unittest.mock import patch

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
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    client = _Client(raw)
    with patch("src.data.databento_backfill.get_databento_client", return_value=client), patch(
        "src.data.databento_backfill.get_target_stock_symbols", return_value=SYMBOLS
    ):
        gap_fill_main(["--date", "2026-10-02", "--lake-root", str(lake)])
        gap_fill_main(["--date", "2026-10-02", "--lake-root", str(lake)])
    assert len(client.timeseries.calls) == 1
    call = client.timeseries.calls[0]
    assert call["start"] == "2026-10-02T14:01:00"
    assert call["end"] == "2026-10-02T14:06:00"
    assert all("2026-10-01" not in str(value) for value in call.values())


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


# ---------------------------------------------------------------------------
# v6.0 gap-fill remediation (quick task 261006-hfu)
#
# P1: two nonempty intervals for the same symbol and UTC day shared one
#     physical filename and the second publication raised BatchCollisionError.
# P2: a publication that succeeded while its coverage write failed caused a
#     second paid request on restart, with shortened bounds.
# ---------------------------------------------------------------------------

import json

import pandas as pd

import src.data.gap_fill as gap_fill_module
from src.storage.barriers import clear_barrier_hooks
from tests.support.faults import PersistenceFaultInjector

FIRST_GAP = (_et(NORMAL_DAY, 10, 1), _et(NORMAL_DAY, 10, 6))
SECOND_GAP = (_et(NORMAL_DAY, 10, 11), _et(NORMAL_DAY, 10, 16))


def _utc(day: date, hour: int, minute: int) -> datetime:
    """Naive UTC timestamp, matching what a historical client returns."""
    return _et(day, hour, minute).astimezone(timezone.utc).replace(tzinfo=None)


def _write_occupied_day_gaps(lake: Path, day: date, gaps) -> Path:
    """Occupy 04:00-20:00 ET except for every half-open gap in `gaps`."""
    rows = []
    cursor = _et(day, 4, 0)
    index = 0
    windows = [(start, end) for start, end in gaps]
    while cursor < _et(day, 20, 0):
        if not any(start <= cursor < end for start, end in windows):
            rows.append(
                QuoteTick(
                    timestamp=cursor.astimezone(timezone.utc).replace(tzinfo=None),
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


class _RecordingClient:
    """Records requests and filters a fixed quote set by symbols and [start, end).

    An interval with no matching quotes returns an empty frame, so a sparse or
    empty response is reproducible without a paid API.
    """

    def __init__(self, quotes=(), costs=0.0):
        self.quotes = [
            (timestamp, str(symbol).upper()) for timestamp, symbol in quotes
        ]
        self.calls = []
        self.cost_calls = []
        self.costs = costs
        self.failures_remaining = {"get_range": 0}
        self.timeseries = self
        self.metadata = self

    def get_range(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self.failures_remaining["get_range"] > 0:
            self.failures_remaining["get_range"] -= 1
            raise OSError("injected download failure")
        start = datetime.fromisoformat(kwargs["start"])
        end = datetime.fromisoformat(kwargs["end"])
        wanted = {str(symbol).upper() for symbol in kwargs.get("symbols", [])}
        matched = [
            (timestamp, symbol)
            for timestamp, symbol in self.quotes
            if start <= timestamp < end and symbol in wanted
        ]
        frame = pd.DataFrame(
            {
                "ts_event": pd.to_datetime([item[0] for item in matched], utc=True),
                "symbol": [item[1] for item in matched],
                "price": [100.50] * len(matched),
                "size": [12] * len(matched),
                "bid_px_00": [100.00] * len(matched),
                "ask_px_00": [100.04] * len(matched),
            }
        )
        return _Range(frame)

    def get_cost(self, **kwargs):
        self.cost_calls.append(dict(kwargs))
        return self.costs


def _coverage_file(lake: Path) -> Path:
    return lake / "_control" / COVERAGE_FILENAME


def _coverage_state(lake: Path) -> dict:
    return json.loads(_coverage_file(lake).read_text(encoding="utf-8"))


def _v2_files(lake: Path, existing: Path) -> list:
    return [
        path
        for path in (lake / "ticks").glob("symbol=*/date=*/*.parquet")
        if path != existing and "bid_price" in pq.read_schema(path).names
    ]


def _gfill_receipts(lake: Path) -> list:
    return sorted((lake / "_control" / "receipts").glob("gfill_*.json"))


def _request_windows(client: _RecordingClient) -> list:
    return [(call["start"], call["end"]) for call in client.calls]


def test_qfill_04_two_intervals_publish_distinct_files(tmp_path):
    """P1: two nonempty intervals on one symbol/day own two physical files."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP, SECOND_GAP])
    before = existing.read_bytes()
    client = _RecordingClient(
        [
            (_utc(NORMAL_DAY, 10, 1), "NVDA"),
            (_utc(NORMAL_DAY, 10, 11), "NVDA"),
        ]
    )

    first = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert _request_windows(client) == [
        ("2026-10-02T14:01:00", "2026-10-02T14:06:00"),
        ("2026-10-02T14:11:00", "2026-10-02T14:16:00"),
    ]
    assert first["requests"] == 2
    files = _v2_files(lake, existing)
    assert len(files) == 2, f"expected two physical files, got {[path.name for path in files]}"
    assert len({path.name for path in files}) == 2
    assert sum(pq.read_table(path).num_rows for path in files) == 2

    receipts = _gfill_receipts(lake)
    assert len(receipts) == 2
    stored = [json.loads(path.read_text(encoding="utf-8")) for path in receipts]
    assert len({record["batch_id"] for record in stored}) == 2
    assert all(record["row_count"] == 1 for record in stored)

    assert existing.read_bytes() == before

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert second["requests"] == 0
    assert len(client.calls) == 2


class _CoverageCommitFault:
    """Fails the coverage commit after publication, exactly once."""

    def __init__(self, real_write):
        self.count = 0
        self._real = real_write

    def __call__(self, path, payload):
        if (
            Path(path).name == COVERAGE_FILENAME
            and payload.get("intervals")
            and self.count == 0
        ):
            self.count += 1
            raise OSError("injected coverage-write failure")
        return self._real(path, payload)


def _interrupt_coverage_commit(monkeypatch, lake: Path) -> _CoverageCommitFault:
    """Fail the coverage commit after publication, exactly once."""
    real_write = gap_fill_module._atomic_write_json
    fault = _CoverageCommitFault(real_write)
    monkeypatch.setattr(gap_fill_module, "_atomic_write_json", fault)
    fault.restore = lambda: monkeypatch.setattr(
        gap_fill_module, "_atomic_write_json", real_write
    )
    return fault


def test_qfill_04_receipts_carry_the_original_request_scope(tmp_path):
    """A receipt names the scope and bounds that produced it, not just a hash."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP, SECOND_GAP])
    client = _RecordingClient(
        [
            (_utc(NORMAL_DAY, 10, 1), "NVDA"),
            (_utc(NORMAL_DAY, 10, 11), "NVDA"),
        ]
    )

    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    stored = [json.loads(path.read_text(encoding="utf-8")) for path in _gfill_receipts(lake)]
    assert len(stored) == 2
    for record in stored:
        assert record["request"]["batch_id"] == record["batch_id"]
        assert record["request"]["date"] == NORMAL_DAY.isoformat()
        assert record["request"]["dataset"] == "DBEQ.BASIC"
        assert record["request"]["schema"] == "tbbo"
        assert record["request"]["symbols"] == sorted(SYMBOLS)
    assert {record["request"]["start"][11:16] for record in stored} == {"10:01", "10:11"}
    assert {record["request"]["end"][11:16] for record in stored} == {"10:06", "10:16"}


def test_qfill_04_sparse_receipt_recovery_avoids_second_download(tmp_path, monkeypatch):
    """P2: a durable publish plus a failed coverage write must not re-download."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    before = existing.read_bytes()
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    injected = _interrupt_coverage_commit(monkeypatch, lake)
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert injected.count == 1
    injected.restore()

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"a second paid request happened: {_request_windows(client)}"
    assert second["requests"] == 0
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]
    recovered = _coverage_state(lake)
    assert recovered["pending"] == []
    assert len(recovered["intervals"]) == 1
    assert recovered["intervals"][0]["start"].startswith("2026-10-02T10:01:00")
    assert recovered["intervals"][0]["end"].startswith("2026-10-02T10:06:00")
    assert sum(pq.read_table(path).num_rows for path in _v2_files(lake, existing)) == 1
    assert existing.read_bytes() == before


def test_qfill_04_deleted_ledger_is_recovered_from_receipt_request_scope(tmp_path):
    """A deleted ledger is rebuilt from the receipt, with no second purchase."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    first = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert first["requests"] == 1
    published = _gfill_receipts(lake)
    assert len(published) == 1
    batch_id = json.loads(published[0].read_text(encoding="utf-8"))["batch_id"]

    _coverage_file(lake).unlink()

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"ledger loss caused a paid request: {_request_windows(client)}"
    assert second["requests"] == 0
    state = _coverage_state(lake)
    assert state["intervals"] and state["intervals"][0]["batch_id"] == batch_id
    assert state["intervals"][0]["start"].startswith("2026-10-02T10:01:00")
    assert sum(pq.read_table(path).num_rows for path in _v2_files(lake, existing)) == 1


def test_qfill_04_empty_success_publishes_recoverable_zero_row_batch(tmp_path):
    """An empty named response gets a receipt that survives a lost ledger."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([])

    before = existing.read_bytes()
    first = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert first["requests"] == 1
    assert first["rows"] == 0
    assert existing.read_bytes() == before

    receipts = _gfill_receipts(lake)
    assert len(receipts) == 1
    record = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert record["row_count"] == 0
    assert record["file_paths"] == []
    assert _v2_files(lake, existing) == []

    _coverage_file(lake).unlink()
    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"an empty publication was re-downloaded: {_request_windows(client)}"
    assert second["requests"] == 0
    assert _v2_files(lake, existing) == []
    assert _coverage_state(lake)["intervals"]


def test_qfill_04_pending_publication_recovers_after_receipt_durability_interrupt(tmp_path):
    """A crash between promotion and receipt recovers without another download."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    injector = PersistenceFaultInjector()
    injector.inject_barrier(
        "receipt_durability",
        failures=1,
        error=OSError("injected receipt_durability interrupt"),
    )
    try:
        with pytest.raises(Exception):
            fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
        injector.assert_triggered("receipt_durability")
    finally:
        injector.close()
        clear_barrier_hooks()

    assert list((lake / "_control" / "intent").glob("gfill_*.json")), "intent must be retained"
    assert _gfill_receipts(lake) == []

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"recovery re-downloaded: {_request_windows(client)}"
    assert second["requests"] == 0
    assert list((lake / "_control" / "intent").glob("gfill_*.json")) == []
    assert len(_gfill_receipts(lake)) == 1
    assert sum(pq.read_table(path).num_rows for path in _v2_files(lake, existing)) == 1
    assert _coverage_state(lake)["intervals"]


def test_qfill_04_failed_request_stays_retryable_on_original_bounds(tmp_path):
    """A request that never published is retried on its original bounds."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    client.failures_remaining["get_range"] = 1

    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert _request_windows(client) == [
        ("2026-10-02T14:01:00", "2026-10-02T14:06:00"),
        ("2026-10-02T14:01:00", "2026-10-02T14:06:00"),
    ], "the retry must use the original bounds, never a shortened interval"
    assert second["requests"] == 1
    completed = _coverage_state(lake)
    assert completed["pending"] == []
    assert completed["intervals"][0]["start"].startswith("2026-10-02T10:01:00")

    third = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert third["requests"] == 0
    assert len(client.calls) == 2


def test_qfill_04_estimate_covers_retries_and_fresh_gaps(tmp_path):
    """The cost estimate covers the complete queue: a retry and a fresh gap."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP, SECOND_GAP])
    client = _RecordingClient(
        [(_utc(NORMAL_DAY, 10, 1), "NVDA"), (_utc(NORMAL_DAY, 10, 11), "NVDA")],
        costs=0.6,
    )

    client.failures_remaining["get_range"] = 1
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]

    with pytest.raises(GapFillBudgetExceeded):
        fill_named_day(
            NORMAL_DAY,
            client=client,
            lake_root=lake,
            symbols=SYMBOLS,
            remaining_budget=1.0,
        )

    assert [call["start"] for call in client.cost_calls] == [
        "2026-10-02T14:01:00",
        "2026-10-02T14:11:00",
    ], "the retry must be estimated alongside the freshly selected gap"
    assert len(client.calls) == 1, "refusal must happen before any download"

    client.failures_remaining["get_range"] = 0
    filled = fill_named_day(
        NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=2.0
    )
    assert filled["requests"] == 2
    assert _request_windows(client)[1:] == [
        ("2026-10-02T14:01:00", "2026-10-02T14:06:00"),
        ("2026-10-02T14:11:00", "2026-10-02T14:16:00"),
    ]
    assert _coverage_state(lake)["pending"] == []


def test_qfill_04_invalid_recovery_evidence_refuses_before_cost_or_download(tmp_path, monkeypatch):
    """Corrupt evidence stops the fill before estimation and before any request."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    injected = _interrupt_coverage_commit(monkeypatch, lake)
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    injected.restore()
    assert len(client.calls) == 1

    receipt = _gfill_receipts(lake)[0]
    record = json.loads(receipt.read_text(encoding="utf-8"))
    record["file_details"][0]["sha256"] = "0" * 64
    receipt.write_text(json.dumps(record), encoding="utf-8")
    before_state = _coverage_file(lake).read_bytes()

    with pytest.raises(GapFillRecoveryError):
        fill_named_day(
            NORMAL_DAY,
            client=client,
            lake_root=lake,
            symbols=SYMBOLS,
            remaining_budget=1.0,
        )

    assert client.cost_calls == [], "refusal must happen before cost estimation"
    assert len(client.calls) == 1, "refusal must happen before any download"
    assert _coverage_file(lake).read_bytes() == before_state


def _pending_record(
    *,
    symbols=None,
    day: date = NORMAL_DAY,
    dataset: str = "DBEQ.BASIC",
    schema: str = "tbbo",
    start: datetime = None,
    end: datetime = None,
    batch_id: str = "gfill_foreign",
    writer_id: str = "gap_fill",
    sequence: int = 0,
) -> dict:
    # Default bounds mirror FIRST_GAP on the record's own day, so a shifted `day`
    # still produces a self-consistent record.
    if start is None:
        start = _et(day, 10, 1)
    if end is None:
        end = _et(day, 10, 6)
    return {
        "date": day.isoformat(),
        "dataset": dataset,
        "schema": schema,
        "symbols": list(SYMBOLS if symbols is None else symbols),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch_id": batch_id,
        "writer_id": writer_id,
        "sequence": sequence,
    }


def _write_coverage_payload(lake: Path, payload) -> None:
    _coverage_file(lake).parent.mkdir(parents=True, exist_ok=True)
    _coverage_file(lake).write_text(json.dumps(payload), encoding="utf-8")


def _coverage_record(symbols, *, day: date = NORMAL_DAY, dataset: str = "DBEQ.BASIC",
                     schema: str = "tbbo") -> dict:
    return {
        "date": day.isoformat(),
        "dataset": dataset,
        "schema": schema,
        "symbols": list(symbols),
        "start": FIRST_GAP[0].isoformat(),
        "end": FIRST_GAP[1].isoformat(),
        "batch_id": "gfill_legacy_record",
    }


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param([_coverage_record(["NVDA", "AAPL"])], id="legacy-bare-list-reordered-symbols"),
        pytest.param({"intervals": [_coverage_record(["AAPL", "NVDA"])]}, id="legacy-dict-no-version"),
    ],
)
def test_qfill_04_legacy_coverage_forms_still_cover(tmp_path, payload):
    """Legacy ledgers, including reordered symbol sets, keep suppressing requests."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    _write_coverage_payload(lake, payload)
    client = _RecordingClient([])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 0
    assert client.calls == []


@pytest.mark.parametrize(
    "record",
    [
        pytest.param(_coverage_record(SYMBOLS, day=date(2026, 10, 1)), id="other-day"),
        pytest.param(_coverage_record(SYMBOLS, dataset="DBEQ.OTHER"), id="other-dataset"),
        pytest.param(_coverage_record(SYMBOLS, schema="ohlcv"), id="other-schema"),
        pytest.param(_coverage_record(["AAPL"]), id="other-symbol-set"),
    ],
)
def test_qfill_04_coverage_is_isolated_by_day_dataset_schema_and_symbols(tmp_path, record):
    """Coverage for another scope never suppresses this day's request."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    _write_coverage_payload(lake, {"intervals": [record]})
    client = _RecordingClient([])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]


# ---------------------------------------------------------------------------
# Adversarial follow-up review (same quick task): the recovery state machine's
# failure and boundary paths that the first pass did not exercise.
# ---------------------------------------------------------------------------


def test_qfill_04_corrupt_ledger_refuses_then_rebuilds_from_receipts(tmp_path):
    """An unreadable ledger refuses; deleting it rebuilds coverage from the receipt."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert len(client.calls) == 1

    ledger = _coverage_file(lake)
    ledger.write_text("{not json", encoding="utf-8")

    with pytest.raises(GapFillRecoveryError):
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )
    assert client.cost_calls == [], "refusal must precede cost estimation"
    assert len(client.calls) == 1, "refusal must precede any download"
    assert ledger.read_text(encoding="utf-8") == "{not json", "state is retained for diagnosis"

    ledger.unlink()
    repaired = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, "the repair path must not buy the interval again"
    assert repaired["requests"] == 0
    state = _coverage_state(lake)
    assert state["intervals"][0]["start"].startswith("2026-10-02T10:01:00")
    assert state["pending"] == []
    assert sum(pq.read_table(path).num_rows for path in _v2_files(lake, existing)) == 1


@pytest.mark.parametrize(
    "record",
    [
        pytest.param("junk-entry", id="non-object-entry"),
    ],
)
def test_qfill_04_non_object_ledger_entry_refuses(tmp_path, record):
    """Coverage evidence is never silently dropped; a junk entry is a refusal."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    _write_coverage_payload(lake, {"intervals": [_coverage_record(SYMBOLS), record]})
    client = _RecordingClient([])

    with pytest.raises(GapFillRecoveryError):
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )
    assert client.cost_calls == []
    assert client.calls == []


@pytest.mark.parametrize(
    "pending",
    [
        pytest.param({"batch_id": "../../../../etc/passwd"}, id="unsafe-batch-id"),
        pytest.param({"batch_id": "gfill_bad_bounds", "start": "not-a-date"}, id="unparseable-start"),
        pytest.param(
            {"batch_id": "gfill_inverted", "start": FIRST_GAP[1].isoformat(),
             "end": FIRST_GAP[0].isoformat()},
            id="inverted-bounds",
        ),
        pytest.param({"batch_id": "gfill_no_symbols", "symbols": []}, id="empty-symbol-set"),
        pytest.param(
            {"batch_id": "gfill_wrong_day", "start": FIRST_GAP[0].replace(day=1).isoformat(),
             "end": FIRST_GAP[1].replace(day=1).isoformat()},
            id="bounds-on-a-different-day",
        ),
    ],
)
def test_qfill_04_unusable_pending_record_refuses(tmp_path, pending):
    """A hand-edited pending record that cannot be trusted refuses before any request."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    record = _pending_record()
    record.update(pending)
    _write_coverage_payload(lake, {"version": 2, "intervals": [], "pending": [record]})
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    with pytest.raises(GapFillRecoveryError):
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )
    assert client.cost_calls == []
    assert client.calls == []
    assert list((lake / "_control" / "intent").glob("*.json")) == []


def test_qfill_04_pending_for_other_scopes_is_left_alone(tmp_path):
    """Foreign pending scopes neither block the fill nor get consumed by it."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    foreign = [
        _pending_record(day=date(2026, 10, 1), batch_id="gfill_other_day"),
        _pending_record(dataset="DBEQ.OTHER", batch_id="gfill_other_dataset"),
        _pending_record(schema="ohlcv", batch_id="gfill_other_schema"),
        _pending_record(symbols=["AAPL"], batch_id="gfill_other_symbols"),
        _pending_record(day=date(2026, 10, 1), batch_id="gfill_never_published"),
    ]
    _write_coverage_payload(
        lake, {"version": 2, "intervals": [], "pending": list(foreign)}
    )
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]
    state = _coverage_state(lake)
    assert len(state["intervals"]) == 1, "only this scope's interval becomes coverage"
    assert sorted(item["batch_id"] for item in state["pending"]) == sorted(
        item["batch_id"] for item in foreign
    ), "foreign pending records must survive untouched"


def test_qfill_04_empty_success_survives_coverage_write_interruption(tmp_path, monkeypatch):
    """Audit row: an interrupted empty success restarts with zero extra downloads."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([])

    injected = _interrupt_coverage_commit(monkeypatch, lake)
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert injected.count == 1
    injected.restore()
    assert len(client.calls) == 1
    receipts = _gfill_receipts(lake)
    assert len(receipts) == 1
    assert json.loads(receipts[0].read_text(encoding="utf-8"))["row_count"] == 0

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, "the empty interval was bought twice"
    assert second["requests"] == 0
    assert _v2_files(lake, existing) == [], "no Parquet files for an empty interval"
    state = _coverage_state(lake)
    assert state["pending"] == []
    assert state["intervals"][0]["start"].startswith("2026-10-02T10:01:00")


def test_qfill_04_second_interval_publish_failure_keeps_first_covered(tmp_path, monkeypatch):
    """A publish failure on the second interval leaves the first covered and the second retryable."""
    import src.data.databento_backfill as backfill_module

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP, SECOND_GAP])
    client = _RecordingClient(
        [(_utc(NORMAL_DAY, 10, 1), "NVDA"), (_utc(NORMAL_DAY, 10, 11), "NVDA")]
    )
    real_publish = backfill_module.publish_ticks_to_lake
    calls = {"count": 0}

    def flaky_publish(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise OSError("injected publish failure on the second interval")
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(backfill_module, "publish_ticks_to_lake", flaky_publish)
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    monkeypatch.setattr(backfill_module, "publish_ticks_to_lake", real_publish)

    interrupted = _coverage_state(lake)
    assert [item["start"][11:16] for item in interrupted["intervals"]] == ["10:01"]
    assert [item["start"][11:16] for item in interrupted["pending"]] == ["10:11"]

    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert _request_windows(client)[-1] == ("2026-10-02T14:11:00", "2026-10-02T14:16:00")
    assert second["requests"] == 1, "only the unpublished interval is requested again"
    completed = _coverage_state(lake)
    assert sorted(item["start"][11:16] for item in completed["intervals"]) == ["10:01", "10:11"]
    assert completed["pending"] == []
    assert len(_v2_files(lake, existing)) == 2


def test_qfill_04_stray_receipt_file_does_not_block_the_fill(tmp_path):
    """A file that cannot be a publisher receipt is ignored, not treated as evidence."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    stray = lake / "_control" / "receipts" / "not a batch id!.json"
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_text(json.dumps({"batch_id": "whatever", "request": _pending_record()}), encoding="utf-8")
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert _coverage_state(lake)["intervals"]
    assert stray.is_file(), "the stray file is left where the operator put it"


def test_qfill_04_namespaced_publication_is_visible_to_the_occupancy_scan(tmp_path):
    """The new namespaced filename still feeds the lake reader that gap selection uses."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    files = sorted(path.name for path in (lake / "ticks").glob("symbol=NVDA/date=*/*.parquet"))
    assert files and all("db_" in name for name in files)
    occupied = occupied_minutes_in_lake(lake, SYMBOLS, NORMAL_DAY)
    assert _et(NORMAL_DAY, 10, 1) in occupied, (
        "a namespaced v2 file must be readable by the occupancy scan"
    )


def test_qfill_04_recovery_survives_compaction_lineage(tmp_path):
    """A compacted-away publication still verifies through lineage: no re-purchase.

    Compaction retires the namespaced files and maps them in `_control/lineage.json`.
    Ledger loss after compaction must therefore still rebuild coverage from receipts.
    """
    from src.storage.compaction import LakeCompactor

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP, SECOND_GAP])
    client = _RecordingClient(
        [(_utc(NORMAL_DAY, 10, 1), "NVDA"), (_utc(NORMAL_DAY, 10, 11), "NVDA")]
    )
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    originals = sorted(
        (lake / "ticks" / "symbol=NVDA" / f"date={NORMAL_DAY.isoformat()}").glob("*.parquet")
    )
    assert len(originals) == 2

    result = LakeCompactor(lake_root=lake).compact(
        symbol="NVDA", date_str=NORMAL_DAY.isoformat()
    )
    assert result.get("status") == "COMPLETED"
    assert not any(path.exists() for path in originals), "compaction retires the originals"

    _coverage_file(lake).unlink()
    second = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 2, "recovery after compaction bought the intervals again"
    assert second["requests"] == 0
    state = _coverage_state(lake)
    assert sorted(item["start"][11:16] for item in state["intervals"]) == ["10:01", "10:11"]
    assert existing.read_bytes()  # v1 partition untouched by the v2 compaction


def test_qfill_04_unrelated_publication_receipts_are_not_mistaken_for_evidence(tmp_path):
    """The receipts directory is shared; receipts without a request scope are ignored."""
    from src.storage.publication import LakePublisher
    from tests.fixtures.deterministic_quotes import QuoteTick

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])

    live_rows = [
        QuoteTick(
            timestamp=_utc(NORMAL_DAY, 9, 31),
            symbol="NVDA",
            price=100.25,
            volume=1.0,
            bid=100.00,
            ask=100.04,
            source="CAPITAL",
            session="REG",
            ingest_id="live_row_1",
        ).to_dict()
    ]
    with LakePublisher(root=lake, writer_id="streamer") as publisher:
        publisher.publish_batch(live_rows, batch_id="batch_streamer_000001", sequence=1)
    foreign_receipt = lake / "_control" / "receipts" / "batch_streamer_000001.json"
    before = foreign_receipt.read_bytes()

    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]
    assert foreign_receipt.read_bytes() == before, "another writer's receipt is left alone"
    state = _coverage_state(lake)
    assert len(state["intervals"]) == 1
    assert state["intervals"][0]["batch_id"].startswith("gfill_")


def test_qfill_04_unrecoverable_intent_refuses_and_names_the_repair(tmp_path):
    """Audit rule: an unresolved intent raises before external requests, with a repair hint."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    record = _pending_record(batch_id="gfill_stuck_intent")
    _write_coverage_payload(lake, {"version": 2, "intervals": [], "pending": [record]})
    intent = lake / "_control" / "intent" / "gfill_stuck_intent.json"
    intent.parent.mkdir(parents=True, exist_ok=True)
    intent.write_text('{"batch_id": "gfill_stuck_intent", "state": "STAGED"', encoding="utf-8")
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    with pytest.raises(GapFillRecoveryError) as excinfo:
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )

    assert "gfill_stuck_intent.json" in str(excinfo.value)
    assert client.cost_calls == []
    assert client.calls == []
    assert intent.is_file(), "the unrecoverable intent is retained for diagnosis"
    assert _coverage_state(lake)["pending"] == [record]
