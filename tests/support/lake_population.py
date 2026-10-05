"""Test support — build a tick lake the way the product does.

These helpers replace the retired `DuckDBClient(":memory:")` scaffolding that the
streaming-dashboard suites used before v5.0. Publishing through `LakePublisher`
means those suites now exercise the production path instead of a legacy schema
that no longer exists anywhere in `src/`.
"""
from __future__ import annotations

import itertools
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple
from zoneinfo import ZoneInfo

from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from src.storage.registry import SymbolRegistry, init_registry
from tests.fixtures.deterministic_quotes import QuoteTick

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

DEFAULT_SYMBOLS = ("NVDA", "AAPL", "SPY")

_SYNTHETIC_SEQUENCE = itertools.count(1)


def _sequence_for(sequence):
    """Each publish needs a distinct (writer, sequence) pair: batch filenames embed it."""
    return next(_SYNTHETIC_SEQUENCE) if sequence is None else sequence


def et_to_utc_naive(session_date: date, hour: int, minute: int, second: int = 0) -> datetime:
    """(date, ET wall clock) -> UTC-naive timestamp, the lake's storage form."""
    aware = datetime(session_date.year, session_date.month, session_date.day, hour, minute, second, tzinfo=ET)
    return aware.astimezone(UTC).replace(tzinfo=None)


def session_label(hour: int, minute: int) -> str:
    """PRE before 09:30 ET, REG 09:30-16:00, POST after."""
    if (hour, minute) < (9, 30):
        return "PRE"
    if (hour, minute) < (16, 0):
        return "REG"
    return "POST"


def create_lake(root: Path, symbols: Sequence[str] = DEFAULT_SYMBOLS) -> Path:
    """Initialise a lake with a registry listing `symbols`."""
    root = Path(root)
    init_tick_lake(root)
    init_registry(root)
    registry = SymbolRegistry(root=root)
    for symbol in symbols:
        registry.add_symbol(symbol=symbol, display_name=symbol, capital_ticker=symbol)
    return root


def publish_minutes(
    root: Path,
    session_date: date,
    symbol: str,
    minutes_list: Iterable[Tuple[int, int]] = None,
    base_price: float = 100.0,
    volume: float = 10.0,
    source: str = "TEST_SOURCE",
    writer_id: str = "test_population",
    sequence: int | None = None,
) -> int:
    """Publishes one tick per (hour, minute) ET entry, or a full 09:30-16:00 session when
    `minutes_list` is omitted. Returns the tick count."""
    if minutes_list is None:
        minutes_list = [(9 + (30 + i) // 60, (30 + i) % 60) for i in range(391)]
    ticks: List[QuoteTick] = []
    for index, entry in enumerate(minutes_list):
        hour, minute = entry[0], entry[1]
        second = entry[2] if len(entry) > 2 else 0
        if second > 59:
            raise ValueError(
                f"({hour}, {minute}, {second}) looks like a tick count, not a second; "
                "use publish_minute_counts() for N ticks inside one minute"
            )
        price = base_price + index * 0.01
        ticks.append(
            QuoteTick(
                timestamp=et_to_utc_naive(session_date, hour, minute, second),
                symbol=symbol,
                price=price,
                volume=volume,
                bid=round(price - 0.05, 4),
                ask=round(price + 0.05, 4),
                source=source,
                session=session_label(hour, minute),
                ingest_id=f"{writer_id}_{symbol}_{index:06d}",
            )
        )

    if not ticks:
        return 0

    sequence = _sequence_for(sequence)
    with LakePublisher(root=Path(root), writer_id=writer_id) as publisher:
        publisher.publish_batch(ticks, batch_id=f"{writer_id}_batch_{sequence}", sequence=sequence)
    return len(ticks)


def publish_minute_counts(
    root: Path,
    session_date: date,
    symbol: str,
    specs: Iterable[Tuple[int, int, int]],
    base_price: float = 100.0,
    volume: float = 1.0,
    source: str = "TEST_SOURCE",
    writer_id: str = "test_population",
) -> int:
    """Publishes `count` ticks inside each (hour, minute, count) ET bucket.

    Ticks within a bucket share a timestamp but keep distinct ingest ids, so
    day/session tick-count assertions see exactly the requested volume.
    """
    ticks: List[QuoteTick] = []
    for hour, minute, count in specs:
        for index in range(count):
            price = base_price + index * 0.01
            ticks.append(
                QuoteTick(
                    timestamp=et_to_utc_naive(session_date, hour, minute, min(index, 59)),
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    bid=round(price - 0.05, 4),
                    ask=round(price + 0.05, 4),
                    source=source,
                    session=session_label(hour, minute),
                    ingest_id=f"{writer_id}_{symbol}_{hour:02d}{minute:02d}_{index:04d}",
                )
            )
    if not ticks:
        return 0
    sequence = _sequence_for(None)
    with LakePublisher(root=Path(root), writer_id=writer_id) as publisher:
        publisher.publish_batch(ticks, batch_id=f"{writer_id}_{symbol}_minute_counts_{sequence}", sequence=sequence)
    return len(ticks)


def populate_trading_session(
    root: Path,
    session_date: date,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    start_hour: int = 9,
    start_min: int = 30,
    end_hour: int = 16,
    end_min: int = 0,
    interval_minutes: int = 1,
    blackout_window: Tuple = None,
    symbol_blackouts: dict = None,
    base_price: float = 100.0,
    volume: float = 10.0,
    source: str = "TEST",
    writer_id: str = "test_population",
    sequence: int | None = None,
) -> int:
    """Publishes a whole trading session (default 09:30-16:00 ET), one tick per symbol per interval.

    `blackout_window` (start_time, end_time) skips every symbol; `symbol_blackouts`
    maps a symbol to its own (start_time, end_time) skip window.
    """
    start_et = datetime(session_date.year, session_date.month, session_date.day, start_hour, start_min, tzinfo=ET)
    end_et = datetime(session_date.year, session_date.month, session_date.day, end_hour, end_min, tzinfo=ET)

    ticks: List[QuoteTick] = []
    current = start_et
    while current <= end_et:
        current_time = current.time()
        in_global_blackout = False
        if blackout_window:
            window_start, window_end = blackout_window
            in_global_blackout = window_start <= current_time < window_end
        if not in_global_blackout:
            timestamp = current.astimezone(UTC).replace(tzinfo=None)
            for symbol in symbols:
                if symbol_blackouts and symbol in symbol_blackouts:
                    window_start, window_end = symbol_blackouts[symbol]
                    if window_start <= current_time < window_end:
                        continue
                ticks.append(
                    QuoteTick(
                        timestamp=timestamp,
                        symbol=symbol,
                        price=base_price,
                        volume=volume,
                        bid=round(base_price - 0.05, 4),
                        ask=round(base_price + 0.05, 4),
                        source=source,
                        session="REG",
                        ingest_id=f"{writer_id}_{symbol}_{current:%Y%m%d%H%M}",
                    )
                )
        current += timedelta(minutes=interval_minutes)

    if not ticks:
        return 0
    sequence = _sequence_for(sequence)
    with LakePublisher(root=Path(root), writer_id=writer_id) as publisher:
        publisher.publish_batch(ticks, batch_id=f"{writer_id}_session_{session_date}", sequence=sequence)
    return len(ticks)


def populate_week(
    root: Path,
    week_start: date,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    gap_date: date = None,
    **session_kwargs,
) -> int:
    """Seeds every weekday (Mon-Fri) of `week_start`'s week.

    The continuity engine evaluates a whole trading week, so a weekday with no
    ticks at all reads as an outage — tests that want exactly one gap seed the
    rest of the week healthy. `session_kwargs` (blackout_window / symbol_blackouts)
    apply to `gap_date` when given, otherwise to every day.
    """
    monday = week_start - timedelta(days=week_start.weekday())
    total = 0
    for offset in range(5):
        day = monday + timedelta(days=offset)
        kwargs = session_kwargs if (gap_date is None or day == gap_date) else {}
        total += populate_trading_session(root, session_date=day, symbols=symbols, **kwargs)
    return total


def publish_series(
    root: Path,
    symbol: str,
    start: datetime,
    count: int,
    step: timedelta = timedelta(minutes=1),
    base_price: float = 100.0,
    volume: float = 10.0,
    source: str = "TEST_SOURCE",
    writer_id: str = "test_population",
    session: str = "REG",
    sequence: int | None = None,
) -> int:
    """Publishes `count` ticks at a fixed step from `start` (UTC-naive)."""
    ticks = [
        QuoteTick(
            timestamp=start + step * i,
            symbol=symbol,
            price=base_price + i * 0.01,
            volume=volume,
            bid=round(base_price + i * 0.01 - 0.05, 4),
            ask=round(base_price + i * 0.01 + 0.05, 4),
            source=source,
            session=session,
            ingest_id=f"{writer_id}_{symbol}_{i:08d}",
        )
        for i in range(count)
    ]
    sequence = _sequence_for(sequence)
    with LakePublisher(root=Path(root), writer_id=writer_id) as publisher:
        publisher.publish_batch(ticks, batch_id=f"{writer_id}_{symbol}_series", sequence=sequence)
    return len(ticks)


def build_rows(rows: Iterable[Sequence], writer_id: str = "test_population") -> List[QuoteTick]:
    """Builds QuoteTicks from raw 8-tuples shaped like the retired `tick_data` insert.

    Tuple shape: (utc_timestamp_string, symbol, price, volume, bid, ask, source, session).
    """
    # Accept either a list of rows or a single row.
    if rows and isinstance(rows[0], (str, datetime)):
        rows = [rows]

    return [
        QuoteTick(
            timestamp=datetime.fromisoformat(str(row[0])),
            symbol=row[1],
            price=float(row[2]),
            volume=float(row[3]) if row[3] is not None else None,
            bid=float(row[4]) if row[4] is not None else None,
            ask=float(row[5]) if row[5] is not None else None,
            source=row[6],
            session=row[7],
            ingest_id=f"{writer_id}_row_{index:08d}",
        )
        for index, row in enumerate(rows)
    ]


def publish_rows(
    root: Path,
    rows: Iterable[Sequence],
    writer_id: str = "test_population",
    sequence: int | None = None,
) -> int:
    """Publishes raw 8-tuples shaped like the retired `tick_data` insert."""
    ticks = build_rows(rows, writer_id=writer_id)
    if not ticks:
        return 0
    sequence = _sequence_for(sequence)
    with LakePublisher(root=Path(root), writer_id=writer_id) as publisher:
        publisher.publish_batch(ticks, batch_id=f"{writer_id}_rows_{sequence}", sequence=sequence)
    return len(ticks)
