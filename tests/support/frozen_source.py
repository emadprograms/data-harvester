"""
Large frozen source fixture for migration rehearsal (Phase 34 / MIGR-01, MIGR-02).

The source is built by construction rather than sampled from a seed, so every
edge case named in the requirement is present at a known position and a failure
points at a specific guarantee:

- **inactive symbols** that are not part of the live streaming set
- **exact duplicates** that must keep their multiplicity
- **ties** on (timestamp, symbol) that must get deterministic ingest ids
- **nulls** across every nullable column
- **float edge values**: denormal minimum, near-maximum, non-representable
  decimals, integer-valued doubles
- **late events** inserted out of chronological order
- **symbols needing percent-encoding** (BRK.B)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence

from tests.support.migration_factory import create_source_db

ACTIVE_SYMBOLS = ("AAPL", "MSFT")
INACTIVE_SYMBOLS = ("DELISTED", "OLDCO")
ENCODED_SYMBOLS = ("BRK.B",)
ALL_SYMBOLS = ACTIVE_SYMBOLS + INACTIVE_SYMBOLS + ENCODED_SYMBOLS

DATES = ("2026-07-10", "2026-07-11", "2026-07-12")

# Float edges: denormal minimum, tiny, non-representable decimal, recurring
# binary fraction, integer-valued double, near the top of the range.
FLOAT_EDGE_PRICES = (
    5e-324,
    1e-300,
    0.1,
    1.0 / 3.0,
    123456.789,
    1e6,
    1.7976931348623157e308,
)
FLOAT_EDGE_VOLUMES = (0.0, 5e-324, 1e-300, 0.1, 1.0 / 3.0, 1e6)


def _base_rows(symbol: str, day: str, count: int, offset: int) -> List[tuple]:
    """Regular rows at one-second intervals from 13:30 UTC."""
    day_start = datetime.fromisoformat(f"{day}T13:30:00")
    rows = []
    for index in range(count):
        stamp = day_start + timedelta(seconds=index)
        row_index = offset + index
        rows.append(
            (
                stamp,
                symbol,
                100.0 + (row_index % 500) * 0.25,
                float(100 + (row_index % 97)),
                100.0 + (row_index % 500) * 0.25 - 0.05,
                100.0 + (row_index % 500) * 0.25 + 0.05,
                "CAPITAL",
                "REG",
            )
        )
    return rows


def build_frozen_source_rows(per_symbol_date: int = 400) -> List[tuple]:
    """Every symbol/date block, plus the named edge cases appended last."""
    rows: List[tuple] = []
    offset = 0
    for symbol in ALL_SYMBOLS:
        for day in DATES:
            rows.extend(_base_rows(symbol, day, per_symbol_date, offset))
            offset += per_symbol_date

    # --- exact duplicates: same values, must survive with multiplicity 3 -----
    dup_stamp = datetime.fromisoformat("2026-07-10T14:00:00")
    for _ in range(3):
        rows.append((dup_stamp, "AAPL", 111.11, 222.0, 111.10, 111.12, "CAPITAL", "REG"))

    # --- ties: identical (timestamp, symbol), different prices ---------------
    tie_stamp = datetime.fromisoformat("2026-07-11T15:00:00")
    for index, price in enumerate((50.0, 60.0, 40.0)):
        rows.append(
            (tie_stamp, "MSFT", price, float(10 + index), 49.9, 60.1, "CAPITAL", "REG")
        )

    # --- nulls across every nullable column ----------------------------------
    null_stamp = datetime.fromisoformat("2026-07-12T16:00:00")
    rows.append((null_stamp, "AAPL", 90.0, None, None, None, None, None))
    rows.append((null_stamp, "DELISTED", 12.5, None, 12.4, 12.6, None, "PRE"))

    # --- float edge values ---------------------------------------------------
    for index, price in enumerate(FLOAT_EDGE_PRICES):
        stamp = datetime.fromisoformat("2026-07-10T17:00:00") + timedelta(seconds=index)
        rows.append((stamp, "OLDCO", price, FLOAT_EDGE_VOLUMES[index % len(FLOAT_EDGE_VOLUMES)],
                     None, None, "CAPITAL", "POST"))

    # --- late events: older timestamps inserted after newer rows -------------
    late_stamp = datetime.fromisoformat("2026-07-10T13:00:00")
    rows.append((late_stamp, "AAPL", 77.77, 7.0, 77.7, 77.8, "CAPITAL", "PRE"))
    rows.append((late_stamp, "BRK.B", 300.25, 1.0, 300.2, 300.3, "CAPITAL", "REG"))

    return rows


def create_frozen_source(path: Path, per_symbol_date: int = 400) -> List[tuple]:
    """Write the frozen source DB and return the exact rows it contains."""
    rows = build_frozen_source_rows(per_symbol_date=per_symbol_date)
    create_source_db(Path(path), rows)
    return rows


def rows_to_records(rows: Iterable[Sequence]) -> List[dict]:
    return [
        {
            "timestamp": r[0],
            "symbol": r[1],
            "price": r[2],
            "volume": r[3],
            "bid": r[4],
            "ask": r[5],
            "source": r[6],
            "session": r[7],
        }
        for r in rows
    ]
