"""
Unit and integration tests for crash recovery, duplicate idempotency,
destination collision detection, and orphaned staging file cleanup (Milestone v4.0 - Phase 16).
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
import pytest
import pyarrow.parquet as pq

from src.storage.config import init_tick_lake
from src.storage.publication import (
    BatchCollisionError,
    LakePublisher,
    PublishIntent,
    PublishReceipt,
    recover_pending_publications,
    cleanup_orphaned_staging_files,
)
from src.storage.schema import ticks_to_table
from tests.fixtures.deterministic_quotes import QuoteTick


def test_idempotent_duplicate_publish(tmp_path):
    """
    Publishing a batch with an identical batch_id that was already published
    must return a receipt with status='ALREADY_PUBLISHED' and not modify or
    re-write any partition files on disk.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=150.25,
            ingest_id="idemp_01",
        )
    ]

    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt1 = publisher.publish_batch(ticks, batch_id="dup_batch_001", sequence=1)
        assert receipt1.status == "PUBLISHED"

        target_file = lake_root / receipt1.file_paths[0]
        assert target_file.is_file()
        mtime_before = target_file.stat().st_mtime_ns
        with open(target_file, "rb") as f:
            sha_before = hashlib.sha256(f.read()).hexdigest()

        # Second publish call with same batch_id
        receipt2 = publisher.publish_batch(ticks, batch_id="dup_batch_001", sequence=1)
        assert receipt2.status == "ALREADY_PUBLISHED"
        assert receipt2.batch_id == "dup_batch_001"
        assert receipt2.row_count == receipt1.row_count
        assert receipt2.file_paths == receipt1.file_paths

        # Verify the target file was untouched
        assert target_file.stat().st_mtime_ns == mtime_before
        with open(target_file, "rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == sha_before


def test_collision_detection(tmp_path):
    """
    If a destination file already exists with a different SHA256 checksum,
    publishing another batch targeting the same file name must raise BatchCollisionError
    and preserve the existing file.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ticks_v1 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=150.0,
            ingest_id="v1_01",
        )
    ]
    ticks_v2 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0),
            symbol="AAPL",
            price=999.99,  # Distinct price produces different checksum
            ingest_id="v2_01",
        )
    ]

    with LakePublisher(root=lake_root, writer_id="w1") as publisher:
        receipt1 = publisher.publish_batch(ticks_v1, batch_id="batch_initial", sequence=1)
        target_file = lake_root / receipt1.file_paths[0]
        with open(target_file, "rb") as f:
            original_sha = hashlib.sha256(f.read()).hexdigest()

        # Attempt to publish ticks_v2 with sequence=1 under a new batch_id -> collision on filename
        with pytest.raises(BatchCollisionError):
            publisher.publish_batch(ticks_v2, batch_id="batch_colliding", sequence=1)

        # Confirm original file remains intact
        with open(target_file, "rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == original_sha


def test_recovery_from_intent_post_rename(tmp_path):
    """
    Simulate crash after partition file was renamed to ticks/ but before receipt
    was written to _control/receipts/ and intent removed.
    Recovery should verify the file, create the receipt, and delete the intent.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    batch_id = "crash_batch_post_rename"
    rel_path = "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000001.parquet"
    target_file = lake_root / rel_path
    target_file.parent.mkdir(parents=True, exist_ok=True)

    # Write valid parquet file to target destination
    ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0, i * 1000),
            symbol="AAPL",
            price=150.0 + i,
            ingest_id=f"tick_{i}",
        )
        for i in range(5)
    ]
    table = ticks_to_table(ticks, validate=True)
    pq.write_table(table, target_file, compression="snappy")

    with open(target_file, "rb") as f:
        file_sha = hashlib.sha256(f.read()).hexdigest()
    file_size = target_file.stat().st_size

    # Create intent record simulating state="STAGED"
    intent_dir = lake_root / "_control" / "intent"
    intent_dir.mkdir(parents=True, exist_ok=True)
    intent_file = intent_dir / f"{batch_id}.json"

    intent_payload = {
        "batch_id": batch_id,
        "writer_id": "w1",
        "sequence": 1,
        "expected_row_count": 5,
        "state": "STAGED",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "targets": [
            {
                "relative_path": rel_path,
                "symbol": "AAPL",
                "date": "2026-10-02",
                "row_count": 5,
                "file_size_bytes": file_size,
                "sha256": file_sha,
            }
        ],
    }
    intent_file.write_text(json.dumps(intent_payload), encoding="utf-8")

    receipt_file = lake_root / "_control" / "receipts" / f"{batch_id}.json"
    assert not receipt_file.exists(), "Receipt must not exist before recovery"

    # Run recovery
    recovered_receipts = recover_pending_publications(lake_root)

    assert len(recovered_receipts) == 1
    rec = recovered_receipts[0]
    assert rec.batch_id == batch_id
    assert rec.status == "PUBLISHED"
    assert rec.row_count == 5

    # Receipt must now exist on disk
    assert receipt_file.is_file()
    with open(receipt_file, "r", encoding="utf-8") as f:
        receipt_data = json.load(f)
    assert receipt_data["batch_id"] == batch_id
    assert receipt_data["file_details"][0]["sha256"] == file_sha

    # Intent must have been deleted
    assert not intent_file.exists(), "Intent file should be deleted after successful recovery"


def test_recovery_from_partial_batch(tmp_path):
    """
    Simulate crash mid-batch for a multi-partition batch where Partition 1
    was already renamed, but Partition 2 remained in _staging/.
    Recovery must rename Partition 2 without modifying Partition 1, then
    issue the receipt and delete the intent.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    batch_id = "crash_batch_partial"
    rel_path_1 = "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000005.parquet"
    rel_path_2 = "ticks/symbol=NVDA/date=2026-10-02/batch_w1_000005.parquet"

    target_file_1 = lake_root / rel_path_1
    target_file_1.parent.mkdir(parents=True, exist_ok=True)

    # 1. Target 1 already in place
    ticks_1 = [QuoteTick(datetime(2026, 10, 2, 14, 30, 0), "AAPL", 150.0, ingest_id="a1")]
    table_1 = ticks_to_table(ticks_1, validate=True)
    pq.write_table(table_1, target_file_1, compression="snappy")
    mtime_1 = target_file_1.stat().st_mtime_ns
    with open(target_file_1, "rb") as f:
        sha_1 = hashlib.sha256(f.read()).hexdigest()

    # 2. Target 2 still in _staging/
    staging_file_2 = lake_root / "_staging" / f"tmp_w1_000005_nvda.parquet.tmp"
    ticks_2 = [QuoteTick(datetime(2026, 10, 2, 14, 30, 0), "NVDA", 120.0, ingest_id="n1")]
    table_2 = ticks_to_table(ticks_2, validate=True)
    pq.write_table(table_2, staging_file_2, compression="snappy")
    with open(staging_file_2, "rb") as f:
        sha_2 = hashlib.sha256(f.read()).hexdigest()

    # 3. Create Intent listing both
    intent_dir = lake_root / "_control" / "intent"
    intent_dir.mkdir(parents=True, exist_ok=True)
    intent_file = intent_dir / f"{batch_id}.json"

    intent_payload = {
        "batch_id": batch_id,
        "writer_id": "w1",
        "sequence": 5,
        "expected_row_count": 2,
        "state": "STAGED",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "targets": [
            {
                "relative_path": rel_path_1,
                "symbol": "AAPL",
                "date": "2026-10-02",
                "row_count": 1,
                "file_size_bytes": target_file_1.stat().st_size,
                "sha256": sha_1,
            },
            {
                "relative_path": rel_path_2,
                "staging_path": str(staging_file_2.relative_to(lake_root)),
                "symbol": "NVDA",
                "date": "2026-10-02",
                "row_count": 1,
                "file_size_bytes": staging_file_2.stat().st_size,
                "sha256": sha_2,
            },
        ],
    }
    intent_file.write_text(json.dumps(intent_payload), encoding="utf-8")

    # Run recovery
    recovered = recover_pending_publications(lake_root)

    assert len(recovered) == 1
    rec = recovered[0]
    assert rec.batch_id == batch_id
    assert rec.row_count == 2
    assert len(rec.file_paths) == 2

    # Verify Partition 1 was not re-written
    assert target_file_1.stat().st_mtime_ns == mtime_1

    # Verify Partition 2 is now in place
    target_file_2 = lake_root / rel_path_2
    assert target_file_2.is_file()
    assert not staging_file_2.exists(), "Staging file should have been renamed/cleaned"

    # Intent removed, receipt written
    assert not intent_file.exists()
    receipt_file = lake_root / "_control" / "receipts" / f"{batch_id}.json"
    assert receipt_file.is_file()


def test_cleanup_orphaned_staging_files(tmp_path):
    """
    Verify cleanup_orphaned_staging_files removes .tmp files in _staging/ older than
    max_age_seconds, but preserves recent files and files referenced by active intents.
    """
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    staging_dir = lake_root / "_staging"

    now = time.time()
    old_time = now - 7200  # 2 hours old

    # 1. Stale orphan 1
    f_stale1 = staging_dir / "orphan_old_1.tmp"
    f_stale1.write_text("dangling data 1")
    os.utime(f_stale1, (old_time, old_time))

    # 2. Stale orphan 2
    f_stale2 = staging_dir / "orphan_old_2.parquet.tmp"
    f_stale2.write_text("dangling data 2")
    os.utime(f_stale2, (old_time, old_time))

    # 3. Active recent file (should NOT be deleted)
    f_recent = staging_dir / "active_recent.tmp"
    f_recent.write_text("active data")

    # 4. Old file referenced by an active intent (should NOT be deleted)
    f_locked = staging_dir / "intent_locked.parquet.tmp"
    f_locked.write_text("in progress batch data")
    os.utime(f_locked, (old_time, old_time))

    intent_dir = lake_root / "_control" / "intent"
    intent_dir.mkdir(parents=True, exist_ok=True)
    intent_file = intent_dir / "active_intent_001.json"
    intent_file.write_text(
        json.dumps({
            "batch_id": "active_intent_001",
            "targets": [{"staging_path": "_staging/intent_locked.parquet.tmp"}],
        }),
        encoding="utf-8",
    )

    deleted_count = cleanup_orphaned_staging_files(lake_root, max_age_seconds=3600)

    assert deleted_count == 2
    assert not f_stale1.exists(), "Stale orphan 1 was not deleted"
    assert not f_stale2.exists(), "Stale orphan 2 was not deleted"
    assert f_recent.exists(), "Recent staging file was wrongly deleted"
    assert f_locked.exists(), "Active intent staging file was wrongly deleted"
