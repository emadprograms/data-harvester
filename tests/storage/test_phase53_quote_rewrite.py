"""Phase 53: rewrite a fixture schema v1 lake. Never open the production lake."""

from datetime import datetime
from pathlib import Path
import json
import math
import inspect

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage.config import init_tick_lake, load_lake_metadata
from src.storage.publication import (
    LakePublisher,
    LakePublisherLock,
    _payload_fingerprint,
    _sha256_file,
    _verify_receipt_files,
)
from src.storage.schema import ticks_to_table
from src.storage.quote_rewrite import QuoteRewriteError, build_parser, rewrite_quote_lake
from src.storage import quote_rewrite as quote_rewrite_mod


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 2, hour, minute, 0)


def _v1_row(symbol: str, hour: int, bid, ask, price, ingest: str):
    return {
        "timestamp": _ts(hour),
        "symbol": symbol,
        "price": price,
        "volume": 1.0,
        "bid": bid,
        "ask": ask,
        "source": "CAPITAL",
        "session": "REG",
        "ingest_id": ingest,
    }


def _publish_v1(lake: Path, rows: list, batch_id: str, sequence: int = 1) -> None:
    publisher = LakePublisher(root=lake, writer_id="fixture_v1")
    publisher.lock.acquire()
    try:
        table = ticks_to_table(rows, validate=True)
        publisher.publish_batch(table, batch_id=batch_id, sequence=sequence)
    finally:
        publisher.lock.release()


def _write_v1_parquet_with_nulls(lake: Path, rows: list, relative: str) -> Path:
    table = ticks_to_table(rows, validate=False)
    path = lake / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    return path


def _write_receipt(lake: Path, relative: str, path: Path, rows: list, batch_id: str) -> None:
    table = pq.ParquetFile(path).read()
    payload = {
        "batch_id": batch_id,
        "writer_id": "fixture_v1",
        "sequence": 1,
        "row_count": table.num_rows,
        "file_paths": [relative],
        "file_details": [
            {
                "relative_path": relative,
                "symbol": rows[0]["symbol"],
                "date": "2026-10-02",
                "row_count": table.num_rows,
                "file_size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        ],
        "payload_sha256": _payload_fingerprint(table),
        "status": "PUBLISHED",
        "published_at": "2026-10-02T00:00:00+00:00",
    }
    dest = lake / "_control" / "receipts" / f"{batch_id}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parquet_files(root: Path) -> list[Path]:
    return sorted((root / "ticks").glob("symbol=*/date=*/*.parquet"))


def test_rewrite_source_does_not_resolve_production_lake():
    source = inspect.getsource(quote_rewrite_mod)
    assert "resolve_tick_lake_root()" not in source.replace(" ", "")
    assert "resolve_tick_lake_root()" not in inspect.getsource(rewrite_quote_lake)


def test_rewrite_01_kept_row_uses_bid_and_ask_not_price(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(
        lake,
        [
            _v1_row("NVDA", 14, bid=100.00, ask=100.04, price=100.50, ingest="i1"),
            _v1_row("NVDA", 15, bid=101.00, ask=101.06, price=50.00, ingest="i2"),
        ],
        batch_id="batch_keep",
    )

    report = rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    files = _parquet_files(lake)
    assert len(files) == 1
    table = pq.ParquetFile(files[0]).read()
    assert "bid_price" in table.schema.names
    assert "price" not in table.schema.names
    assert table["bid_price"].to_pylist() == pytest.approx([100.00, 101.00])
    assert table["ask_price"].to_pylist() == pytest.approx([100.04, 101.06])
    assert 100.50 not in table["bid_price"].to_pylist()
    assert 50.00 not in table["bid_price"].to_pylist()
    assert report["kept_rows"] == 2
    meta = load_lake_metadata(lake, check_maintenance=False)
    assert meta.schema_version == 2
    assert 2 in meta.compatible_versions


def test_rewrite_03_quarantines_missing_bid_and_does_not_use_price(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    relative = "ticks/symbol=NVDA/date=2026-10-02/batch_nulls.parquet"
    rows = [
        _v1_row("NVDA", 14, bid=100.00, ask=100.04, price=100.50, ingest="ok"),
        _v1_row("NVDA", 15, bid=None, ask=100.10, price=999.99, ingest="bad_bid"),
        _v1_row("NVDA", 16, bid=100.20, ask=float("nan"), price=888.88, ingest="bad_ask"),
    ]
    path = _write_v1_parquet_with_nulls(lake, rows, relative)
    _write_receipt(lake, relative, path, rows, "batch_nulls")

    report = rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    table = pq.ParquetFile(lake / relative).read()
    assert table.num_rows == 1
    assert table["bid_price"][0].as_py() == pytest.approx(100.00)
    assert 999.99 not in table["bid_price"].to_pylist()
    assert 888.88 not in table["bid_price"].to_pylist()
    assert report["quarantined_rows"] == 2
    prices = {item["price"] for item in report["quarantine"]}
    assert 999.99 in prices
    assert 888.88 in prices
    assert all(item["ingest_id"] != "ok" for item in report["quarantine"])


def test_rewrite_04_refuses_when_copy_will_not_fit(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(lake, [_v1_row("NVDA", 14, 100.0, 100.04, 100.5, "i1")], "batch_space")
    with pytest.raises(QuoteRewriteError, match="will not fit"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=1)
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 1
    assert list((lake / "ticks").rglob("*.parquet"))


def test_rewrite_04_refuses_when_copy_fails(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(lake, [_v1_row("NVDA", 14, 100.0, 100.04, 100.5, "i1")], "batch_copy")

    def boom(*_args, **_kwargs):
        raise OSError("disk died")

    with pytest.raises(QuoteRewriteError, match="copy"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12, copy_impl=boom)
    table = pq.ParquetFile(_parquet_files(lake)[0]).read()
    assert "price" in table.schema.names


def test_rewrite_04_refuses_when_publisher_lock_held(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(lake, [_v1_row("NVDA", 14, 100.0, 100.04, 100.5, "i1")], "batch_lock")
    holder = LakePublisherLock(lake, writer_id="live_writer")
    holder.acquire()
    try:
        with pytest.raises(QuoteRewriteError, match="lock"):
            rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)
    finally:
        holder.release()
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 1


def test_rewrite_04_refuses_foreign_maintenance_guard(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(lake, [_v1_row("NVDA", 14, 100.0, 100.04, 100.5, "i1")], "batch_guard")
    guard = lake / "_maintenance" / "in_progress.json"
    guard.parent.mkdir(parents=True, exist_ok=True)
    guard.write_text(json.dumps({"operation": "compaction", "owner": "other"}), encoding="utf-8")
    with pytest.raises(QuoteRewriteError, match="maintenance"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)
    assert not (backup / "BACKUP_COMPLETE").exists()


def test_rewrite_04_resume_skips_finished_files(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    _publish_v1(lake, [_v1_row("AAPL", 14, 10.0, 10.04, 99.0, "a1")], "batch_a", sequence=1)
    publisher = LakePublisher(root=lake, writer_id="fixture_v1")
    publisher.lock.acquire()
    try:
        publisher.publish_batch(
            ticks_to_table([_v1_row("NVDA", 14, 20.0, 20.04, 88.0, "n1")], validate=True),
            batch_id="batch_n",
            sequence=2,
        )
    finally:
        publisher.lock.release()

    seen: list[str] = []

    def stop_after_first(relative: str) -> None:
        seen.append(relative)
        if len(seen) == 1:
            raise RuntimeError("stopped")

    with pytest.raises(RuntimeError, match="stopped"):
        rewrite_quote_lake(
            lake_root=lake,
            backup_root=backup,
            free_bytes=10**12,
            after_file=stop_after_first,
        )
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 1

    report = rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)
    assert report["files_rewritten"] + report["files_skipped"] == 2
    assert report["files_skipped"] >= 1
    for path in _parquet_files(lake):
        names = pq.ParquetFile(path).schema_arrow.names
        assert "bid_price" in names
        assert "price" not in names
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 2


def test_rewrite_05_receipts_match_and_retired_deleted(tmp_path):
    lake = tmp_path / "lake"
    backup = tmp_path / "backup"
    init_tick_lake(lake)
    rows_a = [_v1_row("AAPL", 14, 10.0, 10.04, 50.0, "a1")]
    rows_b = [_v1_row("NVDA", 14, 20.0, 20.04, 60.0, "n1")]
    publisher = LakePublisher(root=lake, writer_id="fixture_v1")
    publisher.lock.acquire()
    try:
        publisher.publish_batch(
            ticks_to_table(rows_a + rows_b, validate=True),
            batch_id="multi",
            sequence=1,
        )
    finally:
        publisher.lock.release()

    rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    receipt_path = lake / "_control" / "receipts" / "multi.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    _verify_receipt_files(lake, receipt)
    assert receipt["row_count"] == 2
    retired = list((lake / "_maintenance" / "quote_rewrite_retired").rglob("*.parquet"))
    assert retired == []
    assert (backup / "BACKUP_COMPLETE").is_file()
    meta = json.loads((lake / "lake.json").read_text(encoding="utf-8"))
    assert meta["schema_version"] == 2
    assert 2 in meta["compatible_versions"]


def test_rewrite_requires_explicit_lake_root():
    with pytest.raises(TypeError):
        rewrite_quote_lake()


def test_cli_requires_lake_root_and_backup_root():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args(["--lake-root", "/tmp/lake"])
    parsed = parser.parse_args(["--lake-root", "/tmp/fixture-lake", "--backup-root", "/tmp/backup"])
    assert parsed.lake_root == "/tmp/fixture-lake"
    assert parsed.backup_root == "/tmp/backup"
