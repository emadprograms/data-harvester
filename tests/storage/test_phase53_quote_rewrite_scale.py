"""Phase 53 at production scale: checkpoints, receipts too large to fingerprint, lineage.

The fixture lakes in test_phase53_quote_rewrite.py are tiny. The real lake has 2.2M
quarantined rows, a 105.9M-row migration receipt and compacted originals, so these tests
pin the behaviour that only matters there. Never open the production lake.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.storage import quote_rewrite as quote_rewrite_mod
from src.storage.config import init_tick_lake, load_lake_metadata
from src.storage.publication import _sha256_file
from src.storage.quote_rewrite import rewrite_quote_lake
from src.storage.schema import ticks_to_table_v2
from tests.storage.test_phase53_quote_rewrite import (
    _parquet_files,
    _v1_row,
    _write_receipt,
    _write_v1_parquet_with_nulls,
)

FIRST = "ticks/symbol=NVDA/date=2026-10-02/batch_first.parquet"
SECOND = "ticks/symbol=AAPL/date=2026-10-02/batch_second.parquet"


def _two_file_lake(tmp_path):
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    first_rows = [
        _v1_row("NVDA", 14, bid=100.00, ask=100.04, price=100.50, ingest="ok"),
        _v1_row("NVDA", 15, bid=None, ask=100.10, price=999.99, ingest="bad_bid"),
    ]
    second_rows = [_v1_row("AAPL", 14, bid=10.00, ask=10.04, price=50.00, ingest="a1")]
    first = _write_v1_parquet_with_nulls(lake, first_rows, FIRST)
    second = _write_v1_parquet_with_nulls(lake, second_rows, SECOND)
    _write_receipt(lake, FIRST, first, first_rows, "batch_first")
    _write_receipt(lake, SECOND, second, second_rows, "batch_second")
    return lake, tmp_path / "backup"


def _stop_after_first(relative):
    # Files go in sorted order (AAPL, then NVDA). Stop once NVDA, the file that has
    # quarantined rows, has been swapped.
    if relative == FIRST:
        raise RuntimeError("stopped")


def _progress(lake):
    return json.loads((lake / "_maintenance" / "quote_rewrite_progress.json").read_text(encoding="utf-8"))


def test_checkpoint_holds_counts_not_quarantined_rows(tmp_path):
    lake, backup = _two_file_lake(tmp_path)
    with pytest.raises(RuntimeError, match="stopped"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12, after_file=_stop_after_first)

    for info in _progress(lake)["files"].values():
        assert "quarantine" not in info
        assert set(info) == {"kept_rows", "quarantined_rows"}
    parts = list((lake / "_maintenance" / quote_rewrite_mod.QUARANTINE_PARTS_DIRNAME).glob("*.json"))
    assert len(parts) == 1, "the quarantined rows are durable in a per-file part"


def test_old_progress_format_with_embedded_rows_is_migrated_on_resume(tmp_path):
    """A run started on the earlier format must resume without losing quarantine."""
    lake, backup = _two_file_lake(tmp_path)
    with pytest.raises(RuntimeError, match="stopped"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12, after_file=_stop_after_first)

    # Rebuild the earlier on-disk shape: rows embedded in the progress file, no parts.
    progress = _progress(lake)
    parts_dir = lake / "_maintenance" / quote_rewrite_mod.QUARANTINE_PARTS_DIRNAME
    for relative, info in progress["files"].items():
        if info["quarantined_rows"]:
            part = quote_rewrite_mod._quarantine_part_path(lake, relative)
            info["quarantine"] = json.loads(part.read_text(encoding="utf-8"))["quarantine"]
        del info["quarantined_rows"]
    for part in parts_dir.glob("*.json"):
        part.unlink()
    (lake / "_maintenance" / "quote_rewrite_progress.json").write_text(json.dumps(progress), encoding="utf-8")

    report = rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    assert report["quarantined_rows"] == 1
    assert report["kept_rows"] == 2
    assert any(item["ingest_id"] == "bad_bid" for item in report["quarantine"])
    assert not any("quarantine" in info for info in _progress(lake)["files"].values())
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 2


def test_missing_quarantine_part_refuses_to_report(tmp_path):
    lake, backup = _two_file_lake(tmp_path)
    with pytest.raises(RuntimeError, match="stopped"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12, after_file=_stop_after_first)
    for part in (lake / "_maintenance" / quote_rewrite_mod.QUARANTINE_PARTS_DIRNAME).glob("*.json"):
        part.unlink()

    with pytest.raises(quote_rewrite_mod.QuoteRewriteError, match="quarantine part"):
        rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)
    assert load_lake_metadata(lake, check_maintenance=False).schema_version == 1


def test_small_receipt_is_refingerprinted_and_matches_new_bytes(tmp_path):
    lake, backup = _two_file_lake(tmp_path)
    rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    receipt = json.loads((lake / "_control" / "receipts" / "batch_first.json").read_text(encoding="utf-8"))
    detail = receipt["file_details"][0]
    live = lake / FIRST
    assert detail["sha256"] == _sha256_file(live)
    assert detail["file_size_bytes"] == live.stat().st_size
    assert detail["row_count"] == pq.ParquetFile(live).metadata.num_rows == receipt["row_count"]
    assert receipt["payload_sha256"]
    assert "payload_sha256_invalidated" not in receipt


def test_large_receipt_is_invalidated_not_loaded_into_memory(tmp_path, monkeypatch):
    lake, backup = _two_file_lake(tmp_path)
    old = json.loads((lake / "_control" / "receipts" / "batch_first.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(quote_rewrite_mod, "LARGE_RECEIPT_ROWS", 0)

    def boom(*_args, **_kwargs):
        raise AssertionError("a large receipt must not be fingerprinted from in-memory rows")

    monkeypatch.setattr(quote_rewrite_mod, "_payload_fingerprint", boom)
    monkeypatch.setattr(quote_rewrite_mod, "_verify_receipt_files", boom)

    rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    receipt = json.loads((lake / "_control" / "receipts" / "batch_first.json").read_text(encoding="utf-8"))
    live = lake / FIRST
    assert receipt["payload_sha256"] == ""
    note = receipt["payload_sha256_invalidated"]
    assert note["operation"] == "quote_rewrite"
    assert note["previous_payload_sha256"] == old["payload_sha256"]
    # The per-file evidence is exact even though the logical hash is invalidated.
    assert receipt["file_details"][0]["sha256"] == _sha256_file(live)
    assert receipt["file_details"][0]["row_count"] == pq.ParquetFile(live).metadata.num_rows
    assert receipt["row_count"] == pq.ParquetFile(live).metadata.num_rows == 1


def test_receipt_of_files_this_run_did_not_touch_is_left_alone(tmp_path):
    lake, backup = _two_file_lake(tmp_path)
    v2_rel = "ticks/symbol=MSFT/date=2026-10-02/batch_live_v2.parquet"
    v2_path = lake / v2_rel
    v2_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "timestamp": _v1_row("MSFT", 14, 1, 1, 1, "x")["timestamp"],
            "symbol": "MSFT",
            "bid_price": 1.0,
            "ask_price": 1.1,
            "source": "CAPITAL",
            "session": "REG",
            "ingest_id": "live1",
        }
    ]
    pq.write_table(ticks_to_table_v2(rows, validate=True), v2_path)
    receipt_path = lake / "_control" / "receipts" / "batch_live_v2.json"
    receipt_path.write_text(
        json.dumps(
            {
                "batch_id": "batch_live_v2",
                "row_count": 1,
                "file_paths": [v2_rel],
                "file_details": [
                    {
                        "relative_path": v2_rel,
                        "symbol": "MSFT",
                        "date": "2026-10-02",
                        "row_count": 1,
                        "file_size_bytes": v2_path.stat().st_size,
                        "sha256": _sha256_file(v2_path),
                    }
                ],
                "payload_sha256": "kept-as-is",
            }
        ),
        encoding="utf-8",
    )
    before = receipt_path.read_text(encoding="utf-8")

    rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    assert receipt_path.read_text(encoding="utf-8") == before


def test_lineage_compacted_original_keeps_its_entry(tmp_path):
    """A receipt naming an original that compaction retired is updated, not skipped."""
    lake, backup = _two_file_lake(tmp_path)
    original_rel = "ticks/symbol=NVDA/date=2026-10-02/batch_retired_original.parquet"
    (lake / "_control" / "lineage.json").write_text(
        json.dumps(
            {
                "version": 1,
                "file_lineage": {original_rel: {"compacted_file": FIRST}},
                "receipt_lineage": {},
                "compactions": [],
            }
        ),
        encoding="utf-8",
    )
    receipt_path = lake / "_control" / "receipts" / "batch_lineage.json"
    retired_detail = {
        "relative_path": original_rel,
        "symbol": "NVDA",
        "date": "2026-10-02",
        "row_count": 7,
        "file_size_bytes": 1234,
        "sha256": "f" * 64,
    }
    second_detail = json.loads(
        (lake / "_control" / "receipts" / "batch_second.json").read_text(encoding="utf-8")
    )["file_details"][0]
    receipt_path.write_text(
        json.dumps(
            {
                "batch_id": "batch_lineage",
                "row_count": 8,
                "file_paths": [original_rel, SECOND],
                "file_details": [retired_detail, second_detail],
                "payload_sha256": "stale",
            }
        ),
        encoding="utf-8",
    )

    rewrite_quote_lake(lake_root=lake, backup_root=backup, free_bytes=10**12)

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    by_path = {item["relative_path"]: item for item in receipt["file_details"]}
    assert by_path[original_rel] == retired_detail, "the retired original's recorded entry is kept"
    live = lake / SECOND
    assert by_path[SECOND]["sha256"] == _sha256_file(live)
    assert receipt["payload_sha256"] == ""
    assert receipt["payload_sha256_invalidated"]["previous_payload_sha256"] == "stale"
    assert all(path.stat().st_size for path in _parquet_files(lake))
