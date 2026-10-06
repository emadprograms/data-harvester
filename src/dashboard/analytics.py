"""
Analytics and Query Engine for the Data Harvester Dashboard.

Every function here reads the Parquet tick lake through `TickLakeReader`; the
in-memory DuckDB engine the reader uses is an implementation detail, not a store.
The module provides:
- Candlestick data extraction with dynamic time-bucketing (1m, 5m, 15m, 1h, 1d)
- Raw tick tape and streamer daemon status
- Week discovery and continuity / gap analysis for the Bird's Eye View
- Live US market session clock and countdowns

Timestamp contract: the lake stores UTC-naive instants. Candle `time` values are true
UTC epoch seconds (`TIME_EPOCH_BASIS = "utc"`); human labels are rendered on the NYSE
clock (`America/New_York`), where the regular session opens at 09:30 and closes at 16:00.
"""
import os
import time
from datetime import datetime, date, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
from pandas.tseries.holiday import USFederalHolidayCalendar


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

EXCHANGE_TZ = "America/New_York"
TIME_EPOCH_BASIS = "utc"  # candle["time"] values are true UTC epoch seconds (not shifted wall clock)


def _get_lake_reader():
    """Select lake when configured; propagate selected-lake integrity failures."""
    from src.storage.config import StorageConfigError
    from src.storage.reader import (
        LakeReaderError,
        LakeUnavailableError,
        get_tick_lake_reader,
    )

    explicitly_selected = bool(os.environ.get("TICK_LAKE_ROOT") or os.environ.get("DATA_DIR"))
    try:
        reader = get_tick_lake_reader()
    except Exception:
        if explicitly_selected:
            raise
        return None

    if explicitly_selected:
        if reader is None or reader.root is None:
            raise LakeUnavailableError("A lake backend was explicitly selected but its root could not be resolved")
        # Configuration is not allowed to turn a missing/corrupt lake into an empty
        # legacy fallback. Validate metadata before choosing the reader.
        reader.validate_lake()
        return reader

    if reader and reader.root:
        try:
            reader.validate_lake()
            return reader
        except (LakeReaderError, StorageConfigError):
            return None
    return None


def _no_disk_database_fallback():
    """v5.0 removed the disk-database backend; never fall back to one.

    Every lake-first branch in this module used to end by opening a read-only
    connection to a disk database, so an unavailable lake would
    silently serve data from disk instead of reporting the problem.
    """
    from src.storage.reader import LakeUnavailableError

    raise LakeUnavailableError(
        "The tick lake is unavailable and v5.0 has no disk-database fallback"
    )


def get_streaming_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000, date: str = None, hours: str = "extended") -> dict:
    """
    Fetches OHLCV candles resampled on-the-fly from the raw ticks in the Parquet lake.
    Buckets are aligned to the NYSE clock (America/New_York) and follow the same timestamp
    contract: UTC epoch in `time`, exchange-local label in `time_str`.
    Supports single-day filtering via `date` ("YYYY-MM-DD") and `hours` ("extended" or "regular"),
    returning session_start_epoch and session_end_epoch.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {"error": "symbol parameter is required", "candles": [], "count": 0, "database": "streaming"}

    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    return lake_reader.get_candles(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
        date=date,
        hours=hours,
    )


def get_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000, db_source: str = None, date: str = None, hours: str = "extended") -> dict:
    """
    Unified candle entry point.

    The Parquet tick lake is the only store in v5.0, so `db_source` is accepted
    for backwards compatibility with older clients and ignored.
    """
    return get_streaming_candles(symbol, timeframe=timeframe, start=start, end=end, limit=limit, date=date, hours=hours)


def get_stream_tape(symbol: str = None, limit: int = 50, offset: int = 0) -> dict:
    """
    Returns the latest raw ticks from the lake, calculating spread and throughput.
    """
    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    return lake_reader.get_tape(symbol=symbol, limit=limit, offset=offset)


def get_ticks(symbol: str = None, start: str = None, end: str = None, limit: int = 10000, offset: int = 0, direction: str = "asc") -> dict:
    """
    Queries raw ticks from the lake with filtering by symbol, date/time range, limit, offset, and direction.
    """
    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    ticks = lake_reader.query_ticks(
                    symbol=symbol,
                    start=start,
                    end=end,
                    limit=limit,
                    offset=offset,
                    direction=direction,
                )
    return {"ticks": ticks, "count": len(ticks), "symbol": symbol or "ALL"}


def get_stream_status() -> dict:
    """
    Inspects writer status file or process table for src.stream.runner and checks recent tick throughput.
    """
    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    return lake_reader.get_stream_status()


def get_market_session_info() -> dict:
    """
    Calculates US Eastern market session phase, holiday awareness, and next session boundaries.
    """
    now_et = datetime.now(ET)
    now_utc = datetime.now(timezone.utc)
    t = now_et.time()
    weekday = now_et.weekday()

    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=now_et.date() - timedelta(days=5), end=now_et.date() + timedelta(days=10)).date

    is_holiday = now_et.date() in holidays
    is_weekend = weekday >= 5

    if is_weekend or is_holiday:
        phase = "CLOSED"
        phase_label = "Market Closed (Weekend / Holiday)"
    elif t >= dtime(9, 30) and t < dtime(16, 0):
        phase = "REGULAR"
        phase_label = "Regular Trading Hours (9:30 AM - 4:00 PM ET)"
    elif t >= dtime(4, 0) and t < dtime(9, 30):
        phase = "PRE_MARKET"
        phase_label = "Pre-Market Session (4:00 AM - 9:30 AM ET)"
    elif t >= dtime(16, 0) and t < dtime(20, 0):
        phase = "AFTER_HOURS"
        phase_label = "After-Hours Session (4:00 PM - 8:00 PM ET)"
    else:
        phase = "OVERNIGHT"
        phase_label = "Overnight Session (Market Closed)"

    # Target session determination: 8:00 PM cutoff
    cutoff_time = dtime(20, 0)
    if t > cutoff_time:
        target_date = now_et.date() + timedelta(days=1)
    else:
        target_date = now_et.date()

    while target_date.weekday() > 4 or target_date in holidays:
        target_date += timedelta(days=1)

    # Next session cutoff timestamp (8 PM ET today or target date)
    today_cutoff = datetime.combine(now_et.date(), dtime(20, 0), tzinfo=ET)
    if now_et >= today_cutoff:
        next_cutoff = datetime.combine(now_et.date() + timedelta(days=1), dtime(20, 0), tzinfo=ET)
    else:
        next_cutoff = today_cutoff

    seconds_to_cutoff = max(0, int((next_cutoff - now_et).total_seconds()))

    return {
        "time_et": now_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "time_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "phase": phase,
        "phase_label": phase_label,
        "is_regular_open": phase == "REGULAR",
        "active_session_date": target_date.strftime("%Y-%m-%d"),
        "seconds_to_session_cutoff": seconds_to_cutoff,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday
    }


# The monitored ticket is what the lake registry lists (it is the single symbol
# authority — SYMB-01). This presentation-side copy existed as a second,
# independently editable definition of "symbols" and was removed as part of the
# INCIDENT-2026-10-06 cleanup; import the one definition when a client needs it.
from src.storage.reader import MONITORED_19_SYMBOLS  # noqa: F401  (re-exported)

_AVAILABLE_WEEKS_CACHE = {"timestamp": 0.0, "weeks": []}


def discover_available_weeks() -> list[dict]:
    """
    Discovers available trading weeks from lake partitions,
    grouped Monday-to-Friday in exchange-local time (America/New_York).
    Returns list of dicts:
      [
        {
          "week_start": "YYYY-MM-DD",
          "week_end": "YYYY-MM-DD",
          "label": "Sep 28 – Oct 02, 2026 (Current)",
          "is_current": True,
          "trading_days_count": 3
        },
        ...
      ]
    sorted descending by week_start.
    """
    now = time.time()
    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    return lake_reader.discover_available_weeks()


# Backward compatibility alias
get_available_streaming_weeks = discover_available_weeks


def get_streaming_continuity_analysis(
    days: int = 5,
    symbol: str = "all",
    include_extended: bool = False,
    week_start: str = None,
    target_date: str = None,
    week_offset: int = None,
    target_week: str = None,
    end_date: str = None,
) -> dict:
    """
    Bird's Eye View Data Continuity & Integrity Visualizer Analysis Engine.
    Reads only the Parquet tick lake; there is no other store.

    Evaluates regular market session hours (09:30 to 16:00 ET, 390 min) or extended hours
    (04:00 to 20:00 ET, 960 min) across trading days (Mon-Fri, excluding holidays and weekends).
    Supports week_start filtering ("YYYY-MM-DD") and single-day target_date filtering ("YYYY-MM-DD").
    Non-market hours (overnight 20:00 to 04:00 ET and weekends) never trigger false gap alerts.

    Returns:
      - Master pulse health status: 'healthy' (green), 'partial' (amber), 'outage' (red).
      - Day-by-day minute-level buckets and detected gap incidents with exact UTC start_epoch and end_epoch.
      - 19-symbol spectrogram breakdown when symbol == 'all'.
      - Single-symbol continuity tracking when symbol != 'all'.
      - "extended_hours": bool indicating active window mode.
      - "available_weeks": list of discovered trading weeks.
      - "week_start": active week start date.
    """
    try:
        days = max(1, int(days or 5))
    except (ValueError, TypeError):
        days = 5

    symbol = (symbol or "all").strip().upper()
    is_all = (symbol == "ALL")
    view_mode = "all" if is_all else symbol
    include_extended = bool(include_extended)

    lake_reader = _get_lake_reader()
    if lake_reader is None:
        _no_disk_database_fallback()
    return lake_reader.get_streaming_continuity_analysis(
        days=days,
        symbol=symbol,
        include_extended=include_extended,
        week_start=week_start,
        target_date=target_date,
        week_offset=week_offset,
        target_week=target_week,
        end_date=end_date,
    )

