"""One-day Databento fill for stretches where every active symbol is silent.

This module does not rewrite existing files. A named day is scanned, qualifying
silence is requested, and returned quotes are appended as schema v2.
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from src.storage.publication import (
    LakeMaintenanceInProgressError,
    LakeOwnershipError,
    LakePublisherLock,
)


NY = ZoneInfo("America/New_York")
PRE_THRESHOLD_MINUTES = 15
REGULAR_THRESHOLD_MINUTES = 2
POST_THRESHOLD_MINUTES = 15
EARLY_CLOSE_HOUR = 13

Stretch = Tuple[datetime, datetime]


class GapFillRefused(RuntimeError):
    """The fill did not start because the lake writer already owns the lock."""


class GapFillBudgetExceeded(RuntimeError):
    """The summed interval estimate exceeds the remaining Databento budget."""


class GapFillEstimateError(RuntimeError):
    """Cost estimation for the actual request intervals failed."""


# Exceptional NYSE full closures in the supported historical range that are
# not produced by the recurring holiday calendar.
NYSE_EXCEPTIONAL_FULL_CLOSURES = frozenset(
    {
        date(2025, 1, 9),  # National Day of Mourning for President Carter
    }
)

COVERAGE_FILENAME = "gap_fill_coverage.json"


def _et(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=NY)


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _easter(year: int) -> date:
    """Gregorian Easter Sunday. Good Friday is two days earlier."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def nyse_holidays(year: int) -> Set[date]:
    """Full NYSE closures. Weekend observance follows the published cash-equity rule."""
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed(date(year, 6, 19)),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    next_new_year = date(year + 1, 1, 1)
    if next_new_year.weekday() == 5:
        holidays.add(date(year, 12, 31))
    holidays.update(day for day in NYSE_EXCEPTIONAL_FULL_CLOSURES if day.year == year)
    return {day for day in holidays if day.year == year}


def is_full_nyse_holiday(trading_date: date) -> bool:
    return trading_date in nyse_holidays(trading_date.year)


def early_close_et(trading_date: date) -> Optional[datetime]:
    """Official 13:00 ET early close, or None when the regular session runs to 16:00."""
    if trading_date.weekday() >= 5 or is_full_nyse_holiday(trading_date):
        return None
    close = _et(trading_date, EARLY_CLOSE_HOUR, 0)
    july4 = date(trading_date.year, 7, 4)
    if (
        trading_date == date(trading_date.year, 7, 3)
        and july4.weekday() < 5
        and is_full_nyse_holiday(july4)
    ):
        return close
    thanksgiving = _nth_weekday(trading_date.year, 11, 3, 4)
    if trading_date == thanksgiving + timedelta(days=1):
        return close
    if trading_date.month == 12 and trading_date.day == 24:
        return close
    return None


def _session_windows(trading_date: date) -> List[Tuple[datetime, datetime, int]]:
    pre_end = _et(trading_date, 9, 30)
    early = early_close_et(trading_date)
    if early is not None:
        return [
            (_et(trading_date, 4, 0), pre_end, PRE_THRESHOLD_MINUTES),
            (pre_end, early, REGULAR_THRESHOLD_MINUTES),
        ]
    return [
        (_et(trading_date, 4, 0), pre_end, PRE_THRESHOLD_MINUTES),
        (pre_end, _et(trading_date, 16, 0), REGULAR_THRESHOLD_MINUTES),
        (_et(trading_date, 16, 0), _et(trading_date, 20, 0), POST_THRESHOLD_MINUTES),
    ]


def silence_stretches(
    trading_date: date,
    occupied_minutes: Iterable[datetime],
    symbols: Sequence[str],
) -> List[Stretch]:
    """Return half-open ET stretches where every named symbol is silent.

    A minute is occupied when any symbol has a tick. Session boundaries are
    applied before the length rule, so a crossing stretch becomes two pieces.
    """
    if not symbols or trading_date.weekday() >= 5 or is_full_nyse_holiday(trading_date):
        return []
    occupied = set(occupied_minutes)
    found: List[Stretch] = []
    for start, end, threshold in _session_windows(trading_date):
        run_start: Optional[datetime] = None
        cursor = start
        while cursor < end:
            if cursor not in occupied:
                if run_start is None:
                    run_start = cursor
            elif run_start is not None:
                _append_if_long_enough(found, run_start, cursor, threshold)
                run_start = None
            cursor += timedelta(minutes=1)
        if run_start is not None:
            _append_if_long_enough(found, run_start, end, threshold)
    return found


def _append_if_long_enough(
    found: List[Stretch],
    start: datetime,
    end: datetime,
    threshold: int,
) -> None:
    minutes = int((end - start).total_seconds() // 60)
    if minutes >= threshold:
        found.append((start, end))


def session_label(ts: datetime, trading_date: date) -> str:
    """Classify PRE / REG / POST using the trading day's session boundaries."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    local = ts.astimezone(NY)
    pre_end = _et(trading_date, 9, 30)
    early = early_close_et(trading_date)
    reg_end = early if early is not None else _et(trading_date, 16, 0)
    if local < pre_end:
        return "PRE"
    if local < reg_end:
        return "REG"
    return "POST"


def _acquire_gap_fill_lock(lake_root: Path) -> LakePublisherLock:
    lock = LakePublisherLock(root=lake_root, writer_id="gap_fill")
    try:
        lock.acquire(blocking=False)
    except (LakeOwnershipError, LakeMaintenanceInProgressError) as exc:
        raise GapFillRefused("Gap fill refused because the live writer holds the lake lock") from exc
    return lock


def refuse_if_lake_locked(lake_root: Path) -> None:
    lock = _acquire_gap_fill_lock(lake_root)
    lock.release()


def _floor_et_minute(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    local = timestamp.astimezone(NY)
    return local.replace(second=0, microsecond=0)


def occupied_minutes_in_lake(
    lake_root: Path,
    symbols: Sequence[str],
    trading_date: date,
) -> Set[datetime]:
    """Minutes in the named ET day where any requested symbol has a tick."""
    import duckdb

    from src.storage.reader import TickLakeReader

    window_start = _et(trading_date, 4, 0).astimezone(timezone.utc).replace(tzinfo=None)
    window_end = _et(trading_date, 20, 0).astimezone(timezone.utc).replace(tzinfo=None)
    last_instant = window_end - timedelta(microseconds=1)
    reader = TickLakeReader(root=lake_root)
    files: List[Path] = []
    for symbol in symbols:
        files.extend(
            reader.resolve_partition_files(
                symbol=symbol,
                start_date=window_start.date(),
                end_date=last_instant.date(),
            )
        )
    if not files:
        return set()

    v1_paths: List[str] = []
    v2_paths: List[str] = []
    for path in files:
        names = pq.read_schema(path).names
        (v2_paths if "bid_price" in names else v1_paths).append(str(path))
    parts = []
    params: List[Any] = []
    if v1_paths:
        parts.append("SELECT timestamp, symbol FROM read_parquet(?, hive_partitioning=false)")
        params.append(v1_paths)
    if v2_paths:
        parts.append("SELECT timestamp, symbol FROM read_parquet(?, hive_partitioning=false)")
        params.append(v2_paths)
    sql = " UNION ALL ".join(parts)
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT timestamp, symbol
            FROM ({sql})
            WHERE symbol IN (SELECT UNNEST(?))
              AND timestamp >= ?
              AND timestamp < ?
            """,
            [*params, list(symbols), window_start, window_end],
        ).fetchall()
    finally:
        con.close()
    return {_floor_et_minute(row[0]) for row in rows}


def _coverage_path(root: Path) -> Path:
    return root / "_control" / COVERAGE_FILENAME


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load_coverage(root: Path) -> List[dict]:
    path = _coverage_path(root)
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    intervals = data.get("intervals") if isinstance(data, dict) else data
    if not isinstance(intervals, list):
        return []
    return [item for item in intervals if isinstance(item, dict)]


def _symbol_key(symbols: Sequence[str]) -> List[str]:
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


def _parse_coverage_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(NY)


def _coverage_minutes(
    root: Path,
    trading_date: date,
    symbols: Sequence[str],
    dataset: str,
    schema: str,
) -> Set[datetime]:
    wanted = _symbol_key(symbols)
    minutes: Set[datetime] = set()
    for record in _load_coverage(root):
        if record.get("date") != trading_date.isoformat():
            continue
        if record.get("dataset") != dataset or record.get("schema") != schema:
            continue
        if _symbol_key(record.get("symbols") or []) != wanted:
            continue
        cursor = _parse_coverage_time(str(record["start"]))
        end = _parse_coverage_time(str(record["end"]))
        while cursor < end:
            minutes.add(cursor)
            cursor += timedelta(minutes=1)
    return minutes


def _interval_identity(
    trading_date: date,
    symbols: Sequence[str],
    dataset: str,
    schema: str,
    start: datetime,
    end: datetime,
) -> str:
    payload = "|".join(
        [
            trading_date.isoformat(),
            dataset,
            schema,
            ",".join(_symbol_key(symbols)),
            start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:40]
    return f"gfill_{digest}"


def _record_coverage(
    root: Path,
    trading_date: date,
    symbols: Sequence[str],
    dataset: str,
    schema: str,
    start: datetime,
    end: datetime,
    batch_id: str,
) -> None:
    record = {
        "date": trading_date.isoformat(),
        "dataset": dataset,
        "schema": schema,
        "symbols": _symbol_key(symbols),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch_id": batch_id,
    }
    intervals = [
        item
        for item in _load_coverage(root)
        if item.get("batch_id") != batch_id
    ]
    intervals.append(record)
    _atomic_write_json(_coverage_path(root), {"intervals": intervals})


def fill_named_day(
    trading_date: date,
    client: Any,
    lake_root: Any,
    symbols: Sequence[str],
    occupied_minutes: Optional[Iterable[datetime]] = None,
    dataset: str = "DBEQ.BASIC",
    remaining_budget: Optional[float] = None,
    schema: str = "tbbo",
) -> dict:
    """Request tbbo only for all-symbol silence on one named day, then append v2 rows.

    Existing files are not rewritten. A smaller Databento result is not a reason
    to ask again. Successful interval coverage is recorded after a durable
    publish or a successful empty response.
    """
    if lake_root is None:
        from src.storage.config import resolve_tick_lake_root

        root = resolve_tick_lake_root()
    else:
        root = Path(lake_root)
    lock = _acquire_gap_fill_lock(root)
    empty = {
        "requests": 0,
        "follow_up_requests": 0,
        "rows": 0,
        "stretches": [],
        "estimated_cost": 0.0,
    }
    try:
        if not symbols or trading_date.weekday() >= 5 or is_full_nyse_holiday(trading_date):
            return empty

        from src.data.databento_backfill import (
            estimate_interval_cost,
            normalize_tbbo_frame,
            publish_ticks_to_lake,
        )

        if occupied_minutes is None:
            occupied = occupied_minutes_in_lake(root, symbols, trading_date)
        else:
            occupied = set(occupied_minutes)
        occupied |= _coverage_minutes(root, trading_date, symbols, dataset, schema)
        stretches = silence_stretches(trading_date, occupied, symbols)
        if not stretches:
            return empty

        estimated_cost = 0.0
        if remaining_budget is not None:
            try:
                estimated_cost = sum(
                    estimate_interval_cost(
                        client,
                        list(symbols),
                        start,
                        end,
                        schema=schema,
                        dataset=dataset,
                    )
                    for start, end in stretches
                )
            except Exception as exc:
                raise GapFillEstimateError(
                    f"Cost estimate failed for {trading_date.isoformat()}: {exc}"
                ) from exc
            if estimated_cost > remaining_budget:
                raise GapFillBudgetExceeded(
                    f"Estimated ${estimated_cost:.4f} for {len(stretches)} interval(s) "
                    f"exceeds remaining budget ${remaining_budget:.4f}"
                )

        written = 0
        for start, end in stretches:
            batch_id = _interval_identity(trading_date, symbols, dataset, schema, start, end)
            data = client.timeseries.get_range(
                dataset=dataset,
                symbols=list(symbols),
                schema=schema,
                start=start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                end=end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            )
            frame = normalize_tbbo_frame(data.to_df(), trading_date)
            if frame.empty:
                _record_coverage(root, trading_date, symbols, dataset, schema, start, end, batch_id)
                continue
            written += publish_ticks_to_lake(
                frame,
                lake_root=root,
                batch_id=batch_id,
                writer_id="gap_fill",
                sequence=0,
                ownership_lock=lock,
            )
            _record_coverage(root, trading_date, symbols, dataset, schema, start, end, batch_id)
        return {
            "requests": len(stretches),
            "follow_up_requests": 0,
            "rows": written,
            "stretches": [(start.isoformat(), end.isoformat()) for start, end in stretches],
            "estimated_cost": estimated_cost,
        }
    finally:
        lock.release()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill all-symbol silence on exactly one named day.",
    )
    parser.add_argument("--date", required=True, help="Trading date YYYY-MM-DD")
    parser.add_argument("--lake-root", default=None, help="Lake root. Defaults to the resolved tick lake.")
    parser.add_argument("--max-budget", type=float, default=None, help="Refuse if interval estimates exceed this USD amount.")
    parser.add_argument("--dataset", default="DBEQ.BASIC")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    trading_date = date.fromisoformat(args.date)
    from src.data.databento_backfill import get_databento_client, get_target_stock_symbols
    from src.storage.config import resolve_tick_lake_root

    root = Path(args.lake_root) if args.lake_root else resolve_tick_lake_root()
    symbols = get_target_stock_symbols(root)
    client = get_databento_client()
    result = fill_named_day(
        trading_date,
        client=client,
        lake_root=root,
        symbols=symbols,
        remaining_budget=args.max_budget,
        dataset=args.dataset,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
