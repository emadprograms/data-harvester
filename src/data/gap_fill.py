"""One-day Databento fill for stretches where every active symbol is silent.

This module does not rewrite existing files. A named day is scanned, qualifying
silence is requested, and returned quotes are appended as schema v2.
"""

import argparse
import errno
import hashlib
import json
import os
import re
import sys
import time
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


class GapFillRecoveryError(RuntimeError):
    """Recovery evidence exists but cannot be verified. Refuse before spending."""


# Exceptional NYSE full closures in the supported historical range that are
# not produced by the recurring holiday calendar.
NYSE_EXCEPTIONAL_FULL_CLOSURES = frozenset(
    {
        date(2025, 1, 9),  # National Day of Mourning for President Carter
    }
)

COVERAGE_FILENAME = "gap_fill_coverage.json"
COVERAGE_VERSION = 2
# Same alphabet LakePublisher._safe_batch_id enforces. Validated here before any batch
# identity is used to build a path, so a hand-edited ledger cannot probe outside the lake.
BATCH_ID_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,180}")
# Every gap-fill batch identity starts with this prefix, so a receipt or intent
# carrying it is recoverable evidence for this module rather than shared-directory junk.
GAP_FILL_BATCH_PREFIX = "gfill_"
PENDING_FIELDS = ("date", "dataset", "schema", "symbols", "start", "end", "batch_id")
GAP_FILL_WRITER_ID = "gap_fill"


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


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry after an atomic replace.

    The directory barrier is part of the recovery contract: the pending interval and
    the completed coverage record are what let a crash rebuild state instead of buying
    the same data twice. A filesystem that explicitly refused the write (EIO, ENOSPC)
    must therefore surface, not be reported as success. Refusals that only mean
    "directory fsync is unavailable here" stay tolerated, matching LakePublisher's
    policy for the same barrier.
    """
    if sys.platform == "win32":
        return
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError as err:
        if err.errno in (errno.ENOSPC, errno.EIO):
            raise
        return
    try:
        os.fsync(descriptor)
    except OSError as err:
        if err.errno in (errno.ENOSPC, errno.EIO):
            raise
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Flush, fsync, atomically replace, then fsync the parent directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_directory(path.parent)


def _empty_state() -> dict:
    return {"version": COVERAGE_VERSION, "intervals": [], "pending": []}


def _load_state(root: Path) -> dict:
    """Strict read of the coverage ledger, pending intervals included.

    Legacy shapes (a bare interval list, or a dict without a version) stay readable as
    completed coverage. An unreadable or malformed ledger raises instead of being read
    as "no coverage": a silent reset would buy the same intervals twice.
    """
    path = _coverage_path(root)
    if not path.is_file():
        return _empty_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GapFillRecoveryError(
            f"Cannot read gap-fill coverage ledger {path}: {exc}"
        ) from exc
    if isinstance(data, list):
        raw_intervals, raw_pending = data, []
    elif isinstance(data, dict):
        raw_intervals = data.get("intervals", [])
        raw_pending = data.get("pending") or []
    else:
        raise GapFillRecoveryError("Coverage ledger has an unsupported shape")
    if not isinstance(raw_intervals, list) or not isinstance(raw_pending, list):
        raise GapFillRecoveryError("Coverage ledger intervals/pending must be lists")
    # A non-object entry cannot be read as coverage, and dropping it silently would make the
    # interval look like a hole again — the exact re-purchase this ledger exists to prevent.
    if any(not isinstance(item, dict) for item in raw_intervals):
        raise GapFillRecoveryError(
            f"Coverage ledger {path} has a non-object interval entry; inspect it, or delete "
            "the ledger to rebuild coverage from verified publication receipts"
        )
    outstanding: List[dict] = []
    for item in raw_pending:
        if not isinstance(item, dict):
            raise GapFillRecoveryError("Pending intervals must be objects")
        missing = [field for field in PENDING_FIELDS if item.get(field) in (None, "", [])]
        if missing:
            raise GapFillRecoveryError(f"Pending interval is missing {', '.join(missing)}")
        batch_id = str(item["batch_id"])
        if not BATCH_ID_PATTERN.fullmatch(batch_id) or batch_id in {".", ".."}:
            raise GapFillRecoveryError(f"Pending interval has an unusable batch id {batch_id!r}")
        try:
            start = _parse_coverage_time(str(item["start"]))
            end = _parse_coverage_time(str(item["end"]))
        except (TypeError, ValueError) as exc:
            raise GapFillRecoveryError(
                f"Pending interval {batch_id} has unreadable bounds: {exc}"
            ) from exc
        if end <= start:
            raise GapFillRecoveryError(
                f"Pending interval {batch_id} has non-increasing bounds {start} -> {end}"
            )
        if start.date().isoformat() != str(item["date"]):
            raise GapFillRecoveryError(
                f"Pending interval {batch_id} is not on its own date {item['date']}"
            )
        outstanding.append(item)
    return {"version": COVERAGE_VERSION, "intervals": list(raw_intervals), "pending": outstanding}


def _save_state(root: Path, intervals: List[dict], pending: List[dict]) -> None:
    _atomic_write_json(
        _coverage_path(root),
        {"version": COVERAGE_VERSION, "intervals": intervals, "pending": pending},
    )


def _upsert(records: List[dict], record: dict) -> List[dict]:
    batch_id = str(record.get("batch_id"))
    return [item for item in records if str(item.get("batch_id")) != batch_id] + [record]


def _drop_batch(records: List[dict], batch_id: str) -> List[dict]:
    return [item for item in records if str(item.get("batch_id")) != batch_id]


def _load_coverage(root: Path) -> List[dict]:
    """Completed intervals only, read leniently for occupancy scans.

    `_load_state` is the strict path used before any request; this one must not break
    an occupancy scan over a malformed ledger, and pending intervals are never coverage.
    """
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


def _scope_key(record: Any) -> Tuple[str, str, str, Tuple[str, ...]]:
    """Identity of a request scope: day, dataset, schema, and the symbol set."""
    if not isinstance(record, dict):
        record = {}
    return (
        str(record.get("date", "")),
        str(record.get("dataset", "")),
        str(record.get("schema", "")),
        tuple(_symbol_key(record.get("symbols") or [])),
    )


def _request_scope(
    trading_date: date,
    symbols: Sequence[str],
    dataset: str,
    schema: str,
    start: datetime,
    end: datetime,
    batch_id: str,
) -> dict:
    return {
        "date": trading_date.isoformat(),
        "dataset": dataset,
        "schema": schema,
        "symbols": _symbol_key(symbols),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch_id": batch_id,
    }


def _interval_from_scope(scope: dict) -> dict:
    return {
        "date": scope["date"],
        "dataset": scope["dataset"],
        "schema": scope["schema"],
        "symbols": list(scope.get("symbols") or []),
        "start": scope["start"],
        "end": scope["end"],
        "batch_id": scope["batch_id"],
    }


def _minute_set(start: datetime, end: datetime) -> Set[datetime]:
    minutes: Set[datetime] = set()
    cursor = start
    while cursor < end:
        minutes.add(cursor)
        cursor += timedelta(minutes=1)
    return minutes


def _is_gap_fill_evidence(stem: str) -> bool:
    """True when a file name could only have been written by this module's fill."""
    return stem.startswith(GAP_FILL_BATCH_PREFIX) and bool(BATCH_ID_PATTERN.fullmatch(stem))


def _scope_bounds_present(scope: Any) -> bool:
    """True when a request scope can be rebuilt into a completed coverage interval."""
    if not isinstance(scope, dict):
        return False
    if not all(
        key in scope for key in ("date", "dataset", "schema", "symbols", "start", "end")
    ):
        return False
    return isinstance(scope.get("symbols"), (list, tuple))


def _unattributable_evidence(path: Path, why: str) -> GapFillRecoveryError:
    """Refusal for gap-fill evidence whose interval can no longer be established."""
    return GapFillRecoveryError(
        f"Gap-fill evidence at {path} cannot be verified ({why}), so the interval it "
        "covers cannot be established and requesting again could pay twice. Inspect it: "
        "if the batch never reached the lake, remove the file so its interval can be "
        "requested again."
    )


def _verified_receipt(root: Path, batch_id: str) -> Optional[Any]:
    """Verified receipt for one batch, or None when nothing was published."""
    from src.storage.publication import PublishError, verify_published_receipt

    try:
        return verify_published_receipt(root, batch_id)
    except PublishError as exc:
        raise GapFillRecoveryError(
            f"Publication receipt for {batch_id} failed verification: {exc}"
        ) from exc


def _scope_receipts(root: Path, scope_key: Tuple, known: Set[str]) -> List[Any]:
    """Verified receipts in this scope whose coverage entry is missing.

    Names that only the gap-fill path can write (`gfill_…`) are treated as evidence:
    if one is unreadable or carries no usable request scope it must refuse, because
    ignoring it is indistinguishable from having no evidence at all and the interval
    would be bought again. Files that could not be gap-fill evidence stay skippable,
    so junk in the shared receipts directory never blocks a fill.
    """
    receipts_dir = root / "_control" / "receipts"
    if not receipts_dir.is_dir():
        return []
    found: List[Any] = []
    for path in sorted(receipts_dir.glob("*.json")):
        if path.stem in known:
            continue
        # Receipts are only ever written under a safe batch id. Skip a stray file that could
        # not be one, so junk in the receipts directory cannot block the fill.
        if not BATCH_ID_PATTERN.fullmatch(path.stem) or path.stem in {".", ".."}:
            continue
        gap_fill_evidence = _is_gap_fill_evidence(path.stem)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            if gap_fill_evidence:
                raise _unattributable_evidence(path, f"unreadable: {exc}") from exc
            continue
        scope = data.get("request") if isinstance(data, dict) else None
        if not _scope_bounds_present(scope):
            if gap_fill_evidence:
                raise _unattributable_evidence(path, "no readable request scope")
            continue
        if _scope_key(scope) != scope_key:
            continue
        receipt = _verified_receipt(root, path.stem)
        if receipt is not None:
            found.append(receipt)
    return found


def _orphan_intents(root: Path, scope_key: Tuple, known: Set[str]) -> List[dict]:
    """Gap-fill intents in this scope that the ledger no longer describes.

    Reconciliation used to walk only the ledger's `pending` entries, so losing the
    ledger hid a surviving intent completely: the promoted quote shortened the next
    selection and the original bounds were requested again. Intents are therefore
    discovered from the intent directory itself, and one that cannot be read or
    attributed refuses before any spend rather than being silently dropped.
    """
    intent_dir = root / "_control" / "intent"
    if not intent_dir.is_dir():
        return []
    found: List[dict] = []
    for path in sorted(intent_dir.glob("*.json")):
        stem = path.stem
        if stem in known or not _is_gap_fill_evidence(stem):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise _unattributable_evidence(path, f"unreadable: {exc}") from exc
        if not isinstance(data, dict) or str(data.get("batch_id")) != stem:
            raise _unattributable_evidence(path, "identity mismatch")
        if str(data.get("writer_id", "")) != GAP_FILL_WRITER_ID:
            continue  # another writer's in-flight batch is not this fill's evidence
        try:
            sequence = int(data.get("sequence", 0))
        except (TypeError, ValueError) as exc:
            raise _unattributable_evidence(path, "unreadable sequence") from exc
        if sequence != 0:
            raise _unattributable_evidence(path, "unexpected sequence")
        scope = data.get("request")
        if not _scope_bounds_present(scope):
            raise _unattributable_evidence(path, "no readable request scope")
        if _scope_key(scope) != scope_key:
            continue
        found.append(dict(scope))
    return found


def _assert_receipt_matches(record: dict, receipt: Any) -> None:
    """A receipt is evidence only for the pending interval it names."""
    if str(record.get("writer_id", GAP_FILL_WRITER_ID)) != str(receipt.writer_id):
        raise GapFillRecoveryError(
            f"Receipt writer {receipt.writer_id!r} does not match interval {receipt.batch_id}"
        )
    if int(record.get("sequence", 0)) != int(receipt.sequence):
        raise GapFillRecoveryError(
            f"Receipt sequence {receipt.sequence} does not match interval {receipt.batch_id}"
        )
    scope = receipt.request_scope
    if not scope:
        return
    if _scope_key(scope) != _scope_key(record):
        raise GapFillRecoveryError(f"Receipt scope does not match interval {receipt.batch_id}")
    if (
        str(scope.get("start")) != str(record.get("start"))
        or str(scope.get("end")) != str(record.get("end"))
    ):
        raise GapFillRecoveryError(f"Receipt bounds do not match interval {receipt.batch_id}")


def _reconcile_recovery_state(
    root: Path,
    lock: LakePublisherLock,
    trading_date: date,
    symbols: Sequence[str],
    dataset: str,
    schema: str,
) -> Tuple[List[dict], int]:
    """Complete matching pending intervals from verified publication evidence.

    Runs under the fill lock and strictly before occupancy scanning, cost estimation, or
    any download. Returns the intervals that must still be retried on their original
    bounds, and how many intervals were recovered without spending anything.
    """
    from src.storage.publication import recover_pending_publications

    state = _load_state(root)
    scope_key = _scope_key(
        _request_scope(trading_date, symbols, dataset, schema,
                       _et(trading_date, 0, 0), _et(trading_date, 0, 0), "scope")
    )
    intervals = state["intervals"]
    known = {str(item.get("batch_id")) for item in intervals}
    remaining: List[dict] = []
    retries: List[dict] = []
    recovered = 0
    changed = False

    for record in state["pending"]:
        if _scope_key(record) != scope_key:
            remaining.append(record)
            continue
        batch_id = str(record["batch_id"])
        intent_path = root / "_control" / "intent" / f"{batch_id}.json"
        receipt = _verified_receipt(root, batch_id)
        if receipt is None and intent_path.is_file():
            recover_pending_publications(root, batch_ids={batch_id}, ownership_lock=lock)
            receipt = _verified_receipt(root, batch_id)
            if receipt is None:
                raise GapFillRecoveryError(
                    f"Publication intent {intent_path} is not recoverable, so interval "
                    f"{batch_id} cannot be completed or safely retried. Inspect it: if the "
                    "batch never reached the lake, remove the intent so the interval can be "
                    "requested again."
                )
        if receipt is None:
            retries.append(record)
            remaining.append(record)
            continue
        _assert_receipt_matches(record, receipt)
        intervals = _upsert(intervals, _interval_from_scope(record))
        known.add(batch_id)
        recovered += 1
        changed = True

    for scope in _orphan_intents(root, scope_key, known):
        batch_id = str(scope["batch_id"])
        record = dict(scope)
        record["writer_id"] = GAP_FILL_WRITER_ID
        record["sequence"] = 0
        receipt = _verified_receipt(root, batch_id)
        if receipt is None:
            recover_pending_publications(root, batch_ids={batch_id}, ownership_lock=lock)
            receipt = _verified_receipt(root, batch_id)
        if receipt is None:
            raise _unattributable_evidence(
                root / "_control" / "intent" / f"{batch_id}.json",
                "the named publication is not recoverable",
            )
        _assert_receipt_matches(record, receipt)
        intervals = _upsert(intervals, _interval_from_scope(record))
        known.add(batch_id)
        recovered += 1
        changed = True

    for receipt in _scope_receipts(root, scope_key, known):
        if not receipt.request_scope:
            continue
        intervals = _upsert(intervals, _interval_from_scope(receipt.request_scope))
        known.add(receipt.batch_id)
        recovered += 1
        changed = True

    if changed:
        _save_state(root, intervals, remaining)
    return retries, recovered


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


def _record_pending(root: Path, scope: dict) -> None:
    """Make one interval durable before its download, on its original bounds."""
    state = _load_state(root)
    record = dict(scope)
    record["writer_id"] = GAP_FILL_WRITER_ID
    record["sequence"] = 0
    _save_state(root, state["intervals"], _upsert(state["pending"], record))


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
    """Move one interval from pending to completed coverage in a single write."""
    state = _load_state(root)
    record = {
        "date": trading_date.isoformat(),
        "dataset": dataset,
        "schema": schema,
        "symbols": _symbol_key(symbols),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch_id": batch_id,
    }
    _save_state(
        root,
        _upsert(state["intervals"], record),
        _drop_batch(state["pending"], batch_id),
    )


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

    Existing files are not rewritten. A smaller Databento result is not a reason to ask
    again. Before anything is requested, pending intervals are reconciled against
    verified publication receipts, so a crash between publication and the coverage
    write never buys the same data twice. Each interval is persisted as pending on its
    original bounds before its download, and completed coverage is committed after a
    durable publish (or a recoverable empty publication).
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

        # Recovery first: complete or retry interrupted intervals before scanning, so a
        # shortened interval can never be selected in place of the original request.
        retries, _recovered = _reconcile_recovery_state(
            root, lock, trading_date, symbols, dataset, schema
        )

        if occupied_minutes is None:
            occupied = occupied_minutes_in_lake(root, symbols, trading_date)
        else:
            occupied = set(occupied_minutes)
        occupied |= _coverage_minutes(root, trading_date, symbols, dataset, schema)
        for record in retries:
            occupied |= _minute_set(
                _parse_coverage_time(str(record["start"])),
                _parse_coverage_time(str(record["end"])),
            )
        stretches = silence_stretches(trading_date, occupied, symbols)

        queue: List[Tuple[datetime, datetime, str, bool]] = [
            (
                _parse_coverage_time(str(record["start"])),
                _parse_coverage_time(str(record["end"])),
                str(record["batch_id"]),
                True,
            )
            for record in retries
        ]
        queue += [
            (
                start,
                end,
                _interval_identity(trading_date, symbols, dataset, schema, start, end),
                False,
            )
            for start, end in stretches
        ]
        if not queue:
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
                    for start, end, _batch_id, _retry in queue
                )
            except Exception as exc:
                raise GapFillEstimateError(
                    f"Cost estimate failed for {trading_date.isoformat()}: {exc}"
                ) from exc
            if estimated_cost > remaining_budget:
                raise GapFillBudgetExceeded(
                    f"Estimated ${estimated_cost:.4f} for {len(queue)} interval(s) "
                    f"exceeds remaining budget ${remaining_budget:.4f}"
                )

        written = 0
        requested: List[Tuple[str, str]] = []
        for start, end, batch_id, _retry in queue:
            request_scope = _request_scope(
                trading_date, symbols, dataset, schema, start, end, batch_id
            )
            _record_pending(root, request_scope)
            data = None
            last_get_range_exc = None
            for attempt in range(3):
                try:
                    data = client.timeseries.get_range(
                        dataset=dataset,
                        symbols=list(symbols),
                        schema=schema,
                        start=start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                        end=end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                    )
                    break
                except Exception as exc:
                    status = getattr(exc, "http_status", None) or getattr(exc, "status_code", None)
                    is_transient = (
                        (isinstance(status, int) and status >= 500)
                        or "BentoServerError" in type(exc).__name__
                        or "504" in str(exc)
                        or "Gateway" in str(exc)
                    )
                    if not is_transient:
                        raise
                    last_get_range_exc = exc
                    if attempt < 2:
                        time.sleep(2.0 * (attempt + 1))
            if data is None:
                raise last_get_range_exc
            requested.append((start.isoformat(), end.isoformat()))
            frame = normalize_tbbo_frame(data.to_df(), trading_date)
            written += publish_ticks_to_lake(
                frame,
                lake_root=root,
                batch_id=batch_id,
                writer_id=GAP_FILL_WRITER_ID,
                sequence=0,
                ownership_lock=lock,
                request_scope=request_scope,
            )
            _record_coverage(root, trading_date, symbols, dataset, schema, start, end, batch_id)
        return {
            "requests": len(requested),
            "follow_up_requests": 0,
            "rows": written,
            "stretches": requested,
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
