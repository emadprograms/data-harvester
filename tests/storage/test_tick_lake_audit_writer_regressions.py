"""T1 regression specifications for restart identities and partial publication retry."""
from datetime import datetime
import json

import pytest

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import (
    BatchCollisionError,
    LakePublisher,
    PublishError,
)
from src.storage.schema import QuoteTick
from tests.support.faults import inject_failure
from tests.support.lake_assertions import (
    assert_ingest_ids_unique,
    assert_row_multiset,
    finalized_parquet_files,
    read_final_rows,
)
import src.storage.publication as publication_module


TICK_TS = datetime(2026, 10, 2, 14, 30, 0, 123456)


def _row(price: float, ingest_id: str):
    return {
        "timestamp": TICK_TS,
        "symbol": "AAPL",
        "price": price,
        "volume": 1.0,
        "bid": 99.9,
        "ask": 100.1,
        "source": "CAPITAL",
        "session": "REG",
        "ingest_id": ingest_id,
    }


def _writer_tick(price: float):
    return {
        "timestamp": TICK_TS,
        "symbol": "AAPL",
        "price": price,
        "volume": 1.0,
        "bid": 99.9,
        "ask": 100.1,
        "source": "CAPITAL",
        "session": "REG",
    }


def test_default_writer_restart_publishes_new_observation_once(tmp_path):
    """Operator defaults must not reuse receipt or ingest identities across restarts."""
    lake_root = tmp_path / "lake"
    first = TickLakeWriter(root=lake_root)
    first.write_tick(_writer_tick(100.0))
    first.flush()
    first.close()

    original_rows = read_final_rows(lake_root)
    assert len(original_rows) == 1
    original_receipts = {
        path.name: path.read_bytes()
        for path in (lake_root / "_control" / "receipts").glob("*.json")
    }
    original_files = {path: path.read_bytes() for path in finalized_parquet_files(lake_root)}

    second = TickLakeWriter(root=lake_root)
    try:
        second.write_tick(_writer_tick(200.0))
        second.flush()
    finally:
        second.close()

    rows = read_final_rows(lake_root)
    second_ingest_id = next(row["ingest_id"] for row in rows if row["price"] == 200.0)
    assert_row_multiset(rows, [_row(100.0, original_rows[0]["ingest_id"]),
                               _row(200.0, second_ingest_id)])
    assert len(rows) == 2
    assert_ingest_ids_unique(rows)
    assert {path.name: path.read_bytes() for path in finalized_parquet_files(lake_root)
            if path in original_files} == {path.name: payload for path, payload in original_files.items()}
    assert {path.name: path.read_bytes() for path in (lake_root / "_control" / "receipts").glob("*.json")
            if path.name in original_receipts} == original_receipts


def test_receipt_identity_replay_rejects_changed_payload_and_corrupt_target(tmp_path):
    """An idempotency receipt authorizes only its original payload and intact files."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    original = QuoteTick(TICK_TS, "AAPL", 100.0, 1.0, 99.9, 100.1, "CAPITAL", "REG", "fixed-id")

    with LakePublisher(lake_root, writer_id="publisher") as publisher:
        first = publisher.publish_batch([original], batch_id="stable-batch", sequence=1)
    original_rows = read_final_rows(lake_root)
    assert first.row_count == 1

    changed = QuoteTick(TICK_TS, "AAPL", 200.0, 1.0, 99.9, 100.1, "CAPITAL", "REG", "fixed-id")
    with LakePublisher(lake_root, writer_id="publisher") as publisher:
        with pytest.raises(BatchCollisionError):
            publisher.publish_batch([changed], batch_id="stable-batch", sequence=1)
    assert_row_multiset(read_final_rows(lake_root), original_rows)

    final_file = lake_root / first.file_paths[0]
    final_file.write_bytes(b"corrupt final parquet")
    with LakePublisher(lake_root, writer_id="publisher") as publisher:
        with pytest.raises((BatchCollisionError, PublishError)):
            publisher.publish_batch([original], batch_id="stable-batch", sequence=1)


def test_receipt_failure_retry_reuses_prepared_batch_without_duplicate_rows(tmp_path, monkeypatch):
    """Failure after final rename must retry the same batch identity, not republish ticks."""
    lake_root = tmp_path / "lake"
    writer = TickLakeWriter(
        root=lake_root,
        retry_attempts=1,
        flush_interval_seconds=3600.0,
        max_batch_rows=100,
    )
    receipt_path = writer.publisher.receipts_dir / f"batch_{writer._run_id}_000001.json"
    counter = inject_failure(
        monkeypatch,
        publication_module.os,
        "replace",
        predicate=lambda src, dst, *a, **kw: str(dst) == str(receipt_path),
        failures=1,
        error=OSError("injected receipt fsync/replace failure"),
    )

    writer.write_tick(_writer_tick(101.0))
    with pytest.raises(OSError, match="receipt"):
        writer.flush()

    # Same accepted in-memory item remains available for an explicit retry.
    with writer._buffer_lock:
        assert len(writer._buffer) == 1
    receipt = writer.flush()
    writer.close()

    rows = read_final_rows(lake_root)
    assert counter.injected == 1
    assert receipt.row_count == 1
    assert len(rows) == 1
    assert rows[0]["price"] == 101.0
    assert_ingest_ids_unique(rows)
    receipt_json = json.loads((writer.publisher.receipts_dir / f"{receipt.batch_id}.json").read_text())
    assert receipt_json["row_count"] == 1
