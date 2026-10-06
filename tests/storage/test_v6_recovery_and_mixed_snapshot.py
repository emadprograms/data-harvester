"""v6.0 audit F1/F2: schema-v2 crash recovery and mixed Arrow snapshots."""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.storage.barriers import clear_barrier_hooks
from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import recover_pending_publications
from src.storage.reader import TickLakeReader
from src.storage.schema import LAKE_SCHEMA_MIXED, ticks_to_table, ticks_to_table_v2
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.faults import PersistenceFaultInjector


PERSISTENT_FAILURES = 12
DAY = datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc)


def _new_writer(root: Path, writer_id: str) -> TickLakeWriter:
    return TickLakeWriter(
        root=root,
        writer_id=writer_id,
        max_batch_rows=10**9,
        retry_backoff_base=0.005,
    )


def _v1_nvda() -> dict:
    return QuoteTick(
        timestamp=DAY.replace(tzinfo=None),
        symbol="NVDA",
        price=50.25,
        volume=1.0,
        bid=50.00,
        ask=50.50,
        source="CAPITAL",
        session="REG",
        ingest_id="v1_nvda_row",
    ).to_dict()


def _v2_nvda() -> dict:
    return {
        "timestamp": DAY,
        "symbol": "NVDA",
        "bid_price": 100.00,
        "ask_price": 100.04,
        "source": "CAPITAL",
        "session": "REG",
        "ingest_id": "v2_nvda_row",
    }


def _interrupt_at_receipt(lake_root: Path, rows: list[dict], writer_id: str) -> None:
    writer = _new_writer(lake_root, writer_id)
    injector = PersistenceFaultInjector()
    injector.inject_barrier(
        "receipt_durability",
        failures=PERSISTENT_FAILURES,
        error=OSError("injected receipt_durability interrupt"),
    )
    try:
        writer.write_ticks(rows)
        with pytest.raises(Exception):
            writer.flush(block=True)
        with contextlib.suppress(Exception):
            writer.close()
        injector.assert_triggered("receipt_durability")
    finally:
        injector.close()
        clear_barrier_hooks()
        with contextlib.suppress(Exception):
            writer.close()


def _write_named_parquet(lake: Path, filename: str, table) -> Path:
    dest = lake / "ticks" / "symbol=NVDA" / "date=2026-10-02"
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / filename
    pq.write_table(table, path)
    return path


def test_f1_v1_receipt_durability_recovery_still_commits(tmp_path):
    """Control: a v1 batch interrupted after promotion recovers one receipt."""
    lake = tmp_path / "lake_v1"
    _interrupt_at_receipt(lake, [_v1_nvda()], "f1_v1")

    assert list((lake / "_control" / "receipts").glob("*.json")) == []
    intents = list((lake / "_control" / "intent").glob("*.json"))
    assert len(intents) == 1

    recovered = recover_pending_publications(lake)
    assert len(recovered) == 1
    assert recovered[0].row_count == 1
    assert recovered[0].status == "PUBLISHED"
    assert list((lake / "_control" / "intent").glob("*.json")) == []
    assert len(list((lake / "_control" / "receipts").glob("*.json"))) == 1


def test_f1_v2_receipt_durability_recovery_commits_valid_batch(tmp_path):
    """F1: a v2 batch interrupted after promotion must recover like v1."""
    lake = tmp_path / "lake_v2"
    _interrupt_at_receipt(lake, [_v2_nvda()], "f1_v2")

    assert list((lake / "_control" / "receipts").glob("*.json")) == []
    intents = list((lake / "_control" / "intent").glob("*.json"))
    assert len(intents) == 1

    recovered = recover_pending_publications(lake)
    assert len(recovered) == 1, (
        f"v2 recovery returned {len(recovered)} receipts; pending v2 batches must not be skipped"
    )
    assert recovered[0].row_count == 1
    assert recovered[0].status == "PUBLISHED"
    assert list((lake / "_control" / "intent").glob("*.json")) == []
    assert len(list((lake / "_control" / "receipts").glob("*.json"))) == 1

    files = list((lake / "ticks").glob("symbol=*/date=*/*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert "bid_price" in table.schema.names
    assert table["bid_price"][0].as_py() == pytest.approx(100.00)
    assert table["ask_price"][0].as_py() == pytest.approx(100.04)
    assert "price" not in table.schema.names


@pytest.mark.parametrize(
    "v1_name,v2_name",
    [
        ("batch_a_v1.parquet", "batch_z_v2.parquet"),
        ("batch_z_v1.parquet", "batch_a_v2.parquet"),
    ],
)
def test_f2_mixed_snapshot_preserves_both_quote_column_sets(tmp_path, v1_name, v2_name):
    """F2: mixed snapshot uses a union schema and never invents bid_price from price."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_named_parquet(lake, v1_name, ticks_to_table([_v1_nvda()], validate=True))
    _write_named_parquet(lake, v2_name, ticks_to_table_v2([_v2_nvda()], validate=True))

    reader = TickLakeReader(root=lake)
    table = reader.snapshot(symbols=["NVDA"]).to_table()

    assert table.schema == LAKE_SCHEMA_MIXED
    assert table.num_rows == 2
    names = set(table.schema.names)
    assert {"price", "volume", "bid", "ask", "bid_price", "ask_price"} <= names

    rows = {row["ingest_id"]: row for row in table.to_pylist()}
    v1 = rows["v1_nvda_row"]
    v2 = rows["v2_nvda_row"]

    assert v1["price"] == pytest.approx(50.25)
    assert v1["bid"] == pytest.approx(50.00)
    assert v1["ask"] == pytest.approx(50.50)
    assert v1["bid_price"] is None
    assert v1["ask_price"] is None

    assert v2["bid_price"] == pytest.approx(100.00)
    assert v2["ask_price"] == pytest.approx(100.04)
    assert v2["price"] is None
    assert v2["bid"] is None
    assert v2["ask"] is None
    assert v2["volume"] is None
    assert v2["bid_price"] != pytest.approx(50.25)
