"""
Deterministic lake fixture for the Repo B contract tests (Phase 33).

Every tick below corresponds to a documented edge case, so a failure points at a
specific guarantee rather than at a blob of generated data. The fixture is built
by the product writer and then verified against the independent Parquet oracle,
so the population the contract tests compare against is known to be exactly what
was asked for.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.storage.parquet_writer import TickLakeWriter
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.lake_assertions import assert_row_multiset, read_final_rows

SYMBOL = "AAPL"


def _tick(
    timestamp: str,
    ingest_id: str,
    price: float,
    volume=None,
    symbol: str = SYMBOL,
    session: str = "REG",
) -> QuoteTick:
    return QuoteTick(
        timestamp=datetime.fromisoformat(timestamp),
        symbol=symbol,
        price=price,
        volume=volume,
        bid=price - 0.05,
        ask=price + 0.05,
        source="CAPITAL",
        session=session,
        ingest_id=ingest_id,
    )


CONTRACT_TICKS = [
    # --- UTC midnight rollover -------------------------------------------
    _tick("2026-10-02T23:59:59.500000", "utc_midnight_before", 100.0, 1.0),
    _tick("2026-10-03T00:00:00.500000", "utc_midnight_after", 101.0, 1.0),
    # 02:00 UTC on 10-02 is 22:00 ET on 10-01: the UTC event date must win.
    _tick("2026-10-02T02:00:00.000000", "exchange_boundary", 99.0, 1.0, session="POST"),
    # --- US DST transition (2026-11-01, 06:00 UTC) ------------------------
    _tick("2026-11-01T05:59:30.000000", "dst_before", 200.0, 5.0),
    _tick("2026-11-01T06:00:30.000000", "dst_after", 201.0, 5.0),
    # --- Deterministic tie-break ----------------------------------------
    # The two halves of the tie are deliberately written in SEPARATE batch
    # files. Within one file the writer sorts by (timestamp, ingest_id), so a
    # co-resident tie would be resolved by physical row order and the documented
    # (timestamp, ingest_id) rule would be untestable. Across files, "first row
    # encountered" and "MIN ingest_id" disagree, which makes the rule observable.
    _tick("2026-10-02T12:00:00.000000", "tie_z", 160.0, 1.0),
    # --- Exact duplicate row: multiplicity must survive --------------------
    _tick("2026-10-02T12:01:00.000000", "duplicate_row", 152.0, 10.0),
    _tick("2026-10-02T12:01:00.000000", "duplicate_row", 152.0, 10.0),
    # --- Volume semantics: null coalesces to 1.0, explicit zero stays 0.0 --
    _tick("2026-10-02T12:02:00.000000", "null_volume", 153.0, None),
    _tick("2026-10-02T12:03:00.000000", "zero_volume", 154.0, 0.0),
    # --- Encoded symbols --------------------------------------------------
    _tick("2026-10-02T12:00:00.000000", "brk_1", 300.0, 1.0, symbol="BRK.B"),
    _tick("2026-10-02T12:00:00.000000", "eur_1", 1.1, 1.0, symbol="EUR/USD"),
    # --- Late arrival: written last, must land in its own event date ------
    _tick("2026-10-02T11:00:00.000000", "late_arrival", 155.0, 1.0),
]

# Written in a second batch file, so the tie spans files rather than rows.
SECOND_BATCH_TICKS = [
    _tick("2026-10-02T12:00:00.000000", "tie_a", 150.0, 1.0),
]

ALL_TICKS = CONTRACT_TICKS + SECOND_BATCH_TICKS

EDGE_CASE_TICKS = {
    "utc_midnight_before": "tick immediately before a UTC date rollover",
    "utc_midnight_after": "tick immediately after a UTC date rollover",
    "exchange_boundary": "UTC date differs from the exchange-local date",
    "dst_before": "tick before a daylight-saving clock change",
    "dst_after": "tick after a daylight-saving clock change",
    "tie_a": "one half of a timestamp tie, lower ingest_id, later file",
    "tie_z": "one half of a timestamp tie, higher ingest_id, earlier file",
    "duplicate_row": "exact duplicate row, present twice",
    "null_volume": "null volume, coalesces to 1.0",
    "zero_volume": "explicit zero volume, stays 0.0",
    "brk_1": "symbol containing a period, percent-encoded on disk",
    "eur_1": "symbol containing a slash, percent-encoded on disk",
    "late_arrival": "older timestamp written after newer rows",
}


def build_contract_lake(root: Path) -> list:
    """Write the contract edge-case ticks and verify what actually landed.

    Published in two batches so the timestamp tie spans two files; see
    SECOND_BATCH_TICKS.
    """
    writer = TickLakeWriter(root=Path(root), writer_id="contract_lake", max_batch_rows=10**9)
    writer.write_ticks(CONTRACT_TICKS)
    writer.flush(block=True)
    writer.write_ticks(SECOND_BATCH_TICKS)
    writer.flush(block=True)
    writer.close()

    rows = read_final_rows(Path(root))
    assert len(rows) == len(ALL_TICKS), (
        f"expected {len(ALL_TICKS)} rows in the fixture lake, found {len(rows)}"
    )
    assert_row_multiset(rows, [t.__dict__ for t in ALL_TICKS])
    return rows
