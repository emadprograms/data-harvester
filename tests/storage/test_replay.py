"""
Unit and integration tests for Market Rewind and Bounded Replay Iterator.
Milestone 4.3 - Package F (RPLY-01, RPLY-02, RPLY-03, RPLY-04).

Covers:
- ReplaySnapshot freezing, digest calculation, validation, and late-arrival exclusion
- ReplayCursor serialization, token encode/decode, and corrupt token rejection
- Deterministic total ordering matching independent oracle sort across multiple files and symbols
- Keyset pagination with duplicate tick multiplicity preservation
- Interrupted replay and cross-process cursor resumption with zero missing and zero duplicate rows
- Batch size 1 single-row batch verification
- Empty query range and boundary tests
- Compaction retirement detection (ReplaySnapshotRetiredError)
- Fast-fail on lake maintenance in progress (LakeMaintenanceInProgressError)
- Lake root mismatch rejection
- All output formats (RecordBatch, Table, dict)
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Tuple, Union

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.compaction import LakeCompactor
from src.storage.config import (
    LakeMaintenanceInProgressError,
    init_tick_lake,
)
from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from src.storage.replay import (
    ReplayCursor,
    ReplayCursorCorruptedError,
    ReplayError,
    ReplaySnapshot,
    ReplaySnapshotInvalidError,
    ReplaySnapshotRetiredError,
    TickLakeReplayIterator,
    create_replay_snapshot,
)
from src.storage.schema import QuoteTick, LAKE_SCHEMA_V1


def _oracle_sort_key(tick: Union[QuoteTick, Dict[str, Any]]) -> Tuple:
    """Independent Python oracle sorting key strictly matching the total order contract."""
    if isinstance(tick, QuoteTick):
        d = tick.to_dict()
    else:
        d = dict(tick)
    ts = d["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

    vol = d.get("volume")
    bid = d.get("bid")
    ask = d.get("ask")
    source = d.get("source") or ""
    session = d.get("session") or ""

    # In DuckDB ASC NULLS LAST:
    return (
        ts,
        d["symbol"],
        d["ingest_id"],
        float(d["price"]),
        (vol is None, float(vol) if vol is not None else 0.0),
        (bid is None, float(bid) if bid is not None else 0.0),
        (ask is None, float(ask) if ask is not None else 0.0),
        source,
        session,
    )


def _canonical_dict(row: Union[QuoteTick, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(row, QuoteTick):
        d = row.to_dict()
    else:
        d = dict(row)
    ts = d["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    d["timestamp"] = ts
    d["price"] = float(d["price"])
    d["volume"] = float(d["volume"]) if d.get("volume") is not None else None
    d["bid"] = float(d["bid"]) if d.get("bid") is not None else None
    d["ask"] = float(d["ask"]) if d.get("ask") is not None else None
    return d


# ==============================================================================
# 1. Snapshot Freezing, Digests, and Validation (RPLY-01, RPLY-02)
# ==============================================================================

def test_replay_snapshot_creation_and_validation(tmp_path):
    """Snapshot freezes explicit inventory with SHA-256 digests and validates integrity."""
    lake_root = tmp_path / "lake_snap_test"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    t1 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 10, 0, 0, 100000, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.0,
            volume=10.0,
            ingest_id="aapl_01",
        ),
    ]
    t2 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 10, 0, 1, 200000, tzinfo=timezone.utc),
            symbol="MSFT",
            price=300.0,
            volume=20.0,
            ingest_id="msft_01",
        ),
    ]
    pub.publish_batch(t1, batch_id="b1", sequence=1)
    pub.publish_batch(t2, batch_id="b2", sequence=2)
    pub.close()

    # Freeze snapshot for AAPL only
    snap_aapl = ReplaySnapshot.create(lake_root=lake_root, symbols=["AAPL"], persist=True)
    assert len(snap_aapl.files) == 1
    assert "symbol=AAPL" in snap_aapl.files[0]
    assert len(snap_aapl.file_digests) == 1
    assert snap_aapl.files[0] in snap_aapl.file_digests

    # Validate snapshot: passes
    snap_aapl.validate()

    # Freeze whole lake snapshot
    snap_all = ReplaySnapshot.create(lake_root=lake_root, persist=True)
    assert len(snap_all.files) == 2
    snap_all.validate()

    # Tamper with one file's content
    file_to_tamper = lake_root / snap_all.files[0]
    with open(file_to_tamper, "ab") as f:
        f.write(b"corrupt")

    # Validation should now raise ReplaySnapshotInvalidError due to digest mismatch
    with pytest.raises(ReplaySnapshotInvalidError, match="digest mismatch"):
        snap_all.validate()


def test_replay_snapshot_missing_file_raises_error(tmp_path):
    """Removing a file from disk raises ReplaySnapshotInvalidError."""
    lake_root = tmp_path / "lake_missing_file"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.0,
            ingest_id="a1",
        )
    ]
    pub.publish_batch(ticks, batch_id="b1", sequence=1)
    pub.close()

    snap = ReplaySnapshot.create(lake_root=lake_root)
    assert len(snap.files) == 1

    # Remove the file
    (lake_root / snap.files[0]).unlink()

    with pytest.raises(ReplaySnapshotInvalidError, match="file missing on disk"):
        snap.validate()


def test_replay_snapshot_excludes_late_arriving_files(tmp_path):
    """Batches written after snapshot creation are invisible to the active replay iterator."""
    lake_root = tmp_path / "lake_late_arrival"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    t1 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.0,
            ingest_id="a1",
        )
    ]
    pub.publish_batch(t1, batch_id="b1", sequence=1)

    reader = TickLakeReader(root=lake_root)
    snap = reader.create_replay_snapshot(symbols=["AAPL"])
    assert len(snap.files) == 1

    # Publish late-arriving batch
    t2 = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 10, 0, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            price=150.5,
            ingest_id="a2",
        )
    ]
    pub.publish_batch(t2, batch_id="b2", sequence=2)
    pub.close()

    # Replay iterator bound to frozen snapshot only sees 1 tick
    it = reader.create_replay_iterator(snapshot=snap, batch_size=10)
    batches = list(it)
    assert len(batches) == 1
    assert batches[0].num_rows == 1
    assert batches[0].column("ingest_id")[0].as_py() == "a1"

    # A fresh snapshot sees both ticks
    snap2 = reader.create_replay_snapshot(symbols=["AAPL"])
    assert len(snap2.files) == 2
    it2 = reader.create_replay_iterator(snapshot=snap2, batch_size=10)
    batches2 = list(it2)
    total_rows = sum(b.num_rows for b in batches2)
    assert total_rows == 2


# ==============================================================================
# 2. Cursor Token Serialization and Corrupt Token Handling (RPLY-02, RPLY-03)
# ==============================================================================

def test_replay_cursor_roundtrip_and_corruption():
    """Cursor serializes to token and robustly detects corrupt tokens."""
    cursor = ReplayCursor(
        snapshot_id="snap_123456",
        symbols=["AAPL", "GOOGL"],
        start_date="2026-10-01T00:00:00",
        end_date="2026-10-02T23:59:59.999999",
        batch_size=1000,
        last_key=("2026-10-02 12:34:56.789000", "AAPL", "id_999"),
        offset_in_key=2,
        emitted_count=5432,
        lake_root="/path/to/lake",
    )

    token = cursor.to_token()
    assert isinstance(token, str)

    restored = ReplayCursor.from_token(token)
    assert restored.snapshot_id == cursor.snapshot_id
    assert restored.symbols == cursor.symbols
    assert restored.start_date == cursor.start_date
    assert restored.end_date == cursor.end_date
    assert restored.batch_size == cursor.batch_size
    assert restored.last_key == cursor.last_key
    assert restored.offset_in_key == cursor.offset_in_key
    assert restored.emitted_count == cursor.emitted_count
    assert restored.lake_root == cursor.lake_root

    # Corrupt tokens
    with pytest.raises(ReplayCursorCorruptedError):
        ReplayCursor.from_token("")
    with pytest.raises(ReplayCursorCorruptedError):
        ReplayCursor.from_token("not-a-valid-token-!!!")
    with pytest.raises(ReplayCursorCorruptedError):
        ReplayCursor.from_token("e30=")  # empty json object "{}" missing snapshot_id
    with pytest.raises(ReplayCursorCorruptedError, match="snapshot_id"):
        # JSON without snapshot_id
        ReplayCursor.from_token('{"batch_size": 100}')
    with pytest.raises(ReplayCursorCorruptedError, match="batch_size"):
        # Invalid batch size
        ReplayCursor.from_token('{"snapshot_id": "s1", "batch_size": -5}')
    with pytest.raises(ReplayCursorCorruptedError, match="emitted_count"):
        # Invalid emitted_count
        ReplayCursor.from_token('{"snapshot_id": "s1", "emitted_count": -1}')
    with pytest.raises(ReplayCursorCorruptedError, match="last_key"):
        # Malformed last_key
        ReplayCursor.from_token('{"snapshot_id": "s1", "last_key": [1, 2]}')


# ==============================================================================
# 3. Deterministic Total Order vs Independent Oracle Sort (RPLY-03, RPLY-04)
# ==============================================================================

def test_deterministic_total_order_matches_oracle_sort(tmp_path):
    """
    Replay iterator yields batches in strict deterministic total order matching
    independent Python oracle sort across multiple symbols, files, ties, and duplicates.
    """
    lake_root = tmp_path / "lake_total_order"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")

    # Generate intricate multi-symbol, ties, and duplicate observations
    dt_base = datetime(2026, 10, 2, 14, 0, 0, tzinfo=timezone.utc)

    # Batch 1: AAPL and GOOGL interleaved
    b1_ticks = [
        # AAPL at 14:00:00.100000
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 100000, tzinfo=timezone.utc), symbol="AAPL", price=150.0, volume=10.0, ingest_id="a1"),
        # GOOGL at exact same microsecond 14:00:00.100000 (multi-symbol tie)
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 100000, tzinfo=timezone.utc), symbol="GOOGL", price=180.0, volume=5.0, ingest_id="g1"),
        # Duplicate AAPL tick (exact duplicate values, same ingest_id)
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 100000, tzinfo=timezone.utc), symbol="AAPL", price=150.0, volume=10.0, ingest_id="a1"),
        # AAPL at 14:00:00.200000
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 200000, tzinfo=timezone.utc), symbol="AAPL", price=150.2, volume=None, ingest_id="a2"),
    ]

    # Batch 2: Published later with microsecond ties across files
    b2_ticks = [
        # GOOGL at 14:00:00.100000 with different ingest_id g2 (tie with b1)
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 100000, tzinfo=timezone.utc), symbol="GOOGL", price=180.5, volume=2.0, ingest_id="g2"),
        # AAPL at 14:00:00.150000 (interleaved between a1 and a2)
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 150000, tzinfo=timezone.utc), symbol="AAPL", price=150.1, volume=8.0, ingest_id="a15"),
        # GOOGL with null bid/ask
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 300000, tzinfo=timezone.utc), symbol="GOOGL", price=181.0, volume=1.0, bid=None, ask=None, ingest_id="g3"),
        # Third exact duplicate of a1
        QuoteTick(timestamp=datetime(2026, 10, 2, 14, 0, 0, 100000, tzinfo=timezone.utc), symbol="AAPL", price=150.0, volume=10.0, ingest_id="a1"),
    ]

    pub.publish_batch(b1_ticks, batch_id="b1", sequence=1)
    pub.publish_batch(b2_ticks, batch_id="b2", sequence=2)
    pub.close()

    all_raw_ticks = b1_ticks + b2_ticks
    expected_sorted = sorted(all_raw_ticks, key=_oracle_sort_key)

    reader = TickLakeReader(root=lake_root)
    iterator = reader.create_replay_iterator(batch_size=3, output_format="dict")

    emitted_rows: List[Dict[str, Any]] = []
    for batch in iterator:
        emitted_rows.extend(batch)

    assert len(emitted_rows) == len(expected_sorted)

    for i, (emitted, expected) in enumerate(zip(emitted_rows, expected_sorted)):
        c_emitted = _canonical_dict(emitted)
        c_expected = _canonical_dict(expected)
        assert c_emitted == c_expected, f"Row {i} mismatch:\nEmitted:  {c_emitted}\nExpected: {c_expected}"


# ==============================================================================
# 4. Keyset Resumption Across Fresh Python Processes (RPLY-03)
# ==============================================================================

def test_cursor_resumption_across_fresh_process(tmp_path):
    """
    Interrupts replay at arbitrary batch boundary, exports token, resumes in a
    completely fresh Python subprocess, and asserts 100% exact match with 0 duplicates and 0 lost rows.
    """
    lake_root = tmp_path / "lake_process_resume"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")

    # Generate 45 ticks across 3 batches with ties and duplicates
    all_ticks: List[QuoteTick] = []
    for b_idx in range(3):
        batch_ticks = []
        for i in range(15):
            seq = b_idx * 15 + i
            sec = seq // 10
            ts_us = (seq % 10) * 100000
            batch_ticks.append(QuoteTick(
                timestamp=datetime(2026, 10, 2, 10, 0, sec, ts_us, tzinfo=timezone.utc),
                symbol="AAPL" if (seq % 2 == 0) else "MSFT",
                price=100.0 + (seq * 0.1),
                volume=10.0 + i,
                ingest_id=f"tick_{sec}_{ts_us}_{seq}",
            ))
        pub.publish_batch(batch_ticks, batch_id=f"b_{b_idx}", sequence=b_idx + 1)
        all_ticks.extend(batch_ticks)
    pub.close()

    reader = TickLakeReader(root=lake_root)

    # 1. Run unbroken replay with batch_size=4
    unbroken_it = reader.create_replay_iterator(batch_size=4, output_format="dict")
    unbroken_rows = [r for batch in unbroken_it for r in batch]
    assert len(unbroken_rows) == 45

    # 2. Interrupted replay: stop after emitting 2 batches (8 rows)
    interrupted_it = reader.create_replay_iterator(batch_size=4, output_format="dict")
    part1_rows = []
    for _ in range(2):
        part1_rows.extend(next(interrupted_it))
    assert len(part1_rows) == 8

    # Extract cursor token
    cursor_token = interrupted_it.get_cursor_token()
    interrupted_it.close()

    # 3. Spawn a fresh Python subprocess to resume from token
    out_file = tmp_path / "subprocess_out.json"
    worker_script = """
import json
import sys
from src.storage.reader import TickLakeReader

lake_root = sys.argv[1]
token = sys.argv[2]
out_path = sys.argv[3]

reader = TickLakeReader(lake_root)
it = reader.create_replay_iterator(cursor=token, output_format="dict")

rows = []
for batch in it:
    for r in batch:
        r["timestamp"] = r["timestamp"].isoformat()
        rows.append(r)

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(rows, f)
"""
    result = subprocess.run(
        [sys.executable, "-c", worker_script, str(lake_root), cursor_token, str(out_file)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Subprocess failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"

    with open(out_file, "r", encoding="utf-8") as f:
        part2_raw = json.load(f)

    # Re-parse timestamps to datetime
    part2_rows = []
    for r in part2_raw:
        r["timestamp"] = datetime.fromisoformat(r["timestamp"])
        part2_rows.append(r)

    assert len(part2_rows) == 37  # 45 - 8 = 37

    # 4. Verify combined stream matches unbroken replay 100%
    combined_rows = part1_rows + part2_rows
    assert len(combined_rows) == len(unbroken_rows)

    for i in range(len(unbroken_rows)):
        c_comb = _canonical_dict(combined_rows[i])
        c_unbr = _canonical_dict(unbroken_rows[i])
        assert c_comb == c_unbr, f"Discrepancy at index {i}:\nCombined: {c_comb}\nUnbroken: {c_unbr}"


# ==============================================================================
# 5. Edge Cases & Verification (RPLY-04)
# ==============================================================================

def test_batch_size_one_emits_exact_single_row_batches(tmp_path):
    """Batch size 1 emits exact single-row batches and allows resumption at any single row."""
    lake_root = tmp_path / "lake_batch_one"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    ticks = [
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, 100000, tzinfo=timezone.utc), symbol="AAPL", price=150.0, ingest_id="t1"),
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, 200000, tzinfo=timezone.utc), symbol="AAPL", price=150.1, ingest_id="t2"),
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, 300000, tzinfo=timezone.utc), symbol="AAPL", price=150.2, ingest_id="t3"),
    ]
    pub.publish_batch(ticks, batch_id="b1", sequence=1)
    pub.close()

    reader = TickLakeReader(root=lake_root)
    it = reader.create_replay_iterator(batch_size=1)

    b1 = next(it)
    assert b1.num_rows == 1
    assert b1.column("ingest_id")[0].as_py() == "t1"

    token = it.get_cursor_token()
    it.close()

    # Resume from cursor token
    it_resumed = reader.create_replay_iterator(cursor=token)
    b2 = next(it_resumed)
    assert b2.num_rows == 1
    assert b2.column("ingest_id")[0].as_py() == "t2"

    b3 = next(it_resumed)
    assert b3.num_rows == 1
    assert b3.column("ingest_id")[0].as_py() == "t3"

    with pytest.raises(StopIteration):
        next(it_resumed)


def test_empty_query_range_returns_zero_batches(tmp_path):
    """Empty lake, absent symbol, or out-of-range dates return 0 batches without error."""
    lake_root = tmp_path / "lake_empty"
    init_tick_lake(lake_root)

    reader = TickLakeReader(root=lake_root)

    # 1. Empty lake
    it = reader.create_replay_iterator()
    batches = list(it)
    assert len(batches) == 0
    assert it.emitted_count == 0

    # 2. Lake with AAPL data, but query MSFT
    pub = LakePublisher(lake_root, writer_id="w1")
    pub.publish_batch([
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc), symbol="AAPL", price=150.0, ingest_id="a1")
    ], batch_id="b1", sequence=1)
    pub.close()

    it_msft = reader.create_replay_iterator(symbols=["MSFT"])
    assert len(list(it_msft)) == 0

    # 3. Out of range dates
    it_date = reader.create_replay_iterator(start_date="2026-10-05", end_date="2026-10-06")
    assert len(list(it_date)) == 0


def test_duplicate_ticks_preserve_exact_multiplicity(tmp_path):
    """Exact identical duplicate rows preserve multiplicity and resume correctly."""
    lake_root = tmp_path / "lake_exact_duplicates"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    dup_tick = QuoteTick(
        timestamp=datetime(2026, 10, 2, 12, 0, 0, 500000, tzinfo=timezone.utc),
        symbol="AAPL",
        price=150.0,
        volume=10.0,
        ingest_id="dup_id",
    )
    # 4 identical rows
    ticks = [dup_tick, dup_tick, dup_tick, dup_tick]
    pub.publish_batch(ticks, batch_id="b1", sequence=1)
    pub.close()

    reader = TickLakeReader(root=lake_root)

    # Replay with batch_size 1
    it = reader.create_replay_iterator(batch_size=1)
    b1 = next(it)
    assert b1.num_rows == 1
    tok1 = it.get_cursor_token()
    it.close()

    # Resume: should emit 3 remaining duplicate rows
    it2 = reader.create_replay_iterator(cursor=tok1)
    remaining = list(it2)
    assert len(remaining) == 3
    for b in remaining:
        assert b.num_rows == 1
        assert b.column("ingest_id")[0].as_py() == "dup_id"


def test_compaction_integration_raises_retired_error(tmp_path):
    """If compaction retires a file in a saved snapshot, attempting replay raises ReplaySnapshotRetiredError."""
    lake_root = tmp_path / "lake_compaction_retired"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    t1 = [
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc), symbol="AAPL", price=150.0, ingest_id="a1")
    ]
    t2 = [
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 1, tzinfo=timezone.utc), symbol="AAPL", price=150.5, ingest_id="a2")
    ]
    pub.publish_batch(t1, batch_id="b1", sequence=1)
    pub.publish_batch(t2, batch_id="b2", sequence=2)
    pub.close()

    reader = TickLakeReader(root=lake_root)
    # Freeze snapshot referencing the 2 original files
    snap = reader.create_replay_snapshot(symbols=["AAPL"])
    assert len(snap.files) == 2
    snap.validate()

    # Compact AAPL partition
    compactor = LakeCompactor(lake_root=lake_root)
    res = compactor.compact(symbol="AAPL", date_str="2026-10-02")
    assert res["status"] == "COMPLETED"

    # Attempting to validate or create replay iterator over retired snapshot must fail
    with pytest.raises(ReplaySnapshotRetiredError, match="retired by compaction"):
        snap.validate()

    with pytest.raises(ReplaySnapshotRetiredError, match="retired by compaction"):
        reader.create_replay_iterator(snapshot=snap)


def test_maintenance_in_progress_fails_fast(tmp_path):
    """LakeMaintenanceInProgressError is raised immediately if maintenance lock exists."""
    lake_root = tmp_path / "lake_maint_guard"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    pub.publish_batch([
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc), symbol="AAPL", price=150.0, ingest_id="a1")
    ], batch_id="b1", sequence=1)
    pub.close()

    reader = TickLakeReader(root=lake_root)

    # Place maintenance guard
    guard = lake_root / "_maintenance" / "in_progress.json"
    guard.parent.mkdir(parents=True, exist_ok=True)
    guard.write_text('{"maintenance_id": "test_maint"}')

    # Creation of snapshot or iterator must fail fast
    with pytest.raises(LakeMaintenanceInProgressError):
        reader.create_replay_snapshot()

    with pytest.raises(LakeMaintenanceInProgressError):
        reader.create_replay_iterator()

    # Remove guard
    guard.unlink()

    # Now creation succeeds
    it = reader.create_replay_iterator()
    batches = list(it)
    assert len(batches) == 1


def test_mismatched_lake_root_raises_error(tmp_path):
    """ReplaySnapshot or cursor applied to mismatched lake root raises ReplaySnapshotInvalidError."""
    lake1 = tmp_path / "lake1"
    lake2 = tmp_path / "lake2"
    init_tick_lake(lake1)
    init_tick_lake(lake2)

    reader1 = TickLakeReader(root=lake1)
    snap1 = reader1.create_replay_snapshot()

    # Attempt to validate or replay against lake2
    with pytest.raises(ReplaySnapshotInvalidError, match="Lake root mismatch"):
        snap1.validate(lake_root=lake2)

    reader2 = TickLakeReader(root=lake2)
    with pytest.raises(ReplaySnapshotInvalidError, match="Lake root mismatch"):
        reader2.create_replay_iterator(snapshot=snap1)


def test_output_formats_table_batch_and_dict(tmp_path):
    """Verifies output_format options: record_batch, table, and dict, and convenience methods."""
    lake_root = tmp_path / "lake_formats"
    init_tick_lake(lake_root)

    pub = LakePublisher(lake_root, writer_id="w1")
    ticks = [
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 0, tzinfo=timezone.utc), symbol="AAPL", price=150.0, ingest_id="a1"),
        QuoteTick(timestamp=datetime(2026, 10, 2, 10, 0, 1, tzinfo=timezone.utc), symbol="AAPL", price=150.5, ingest_id="a2"),
    ]
    pub.publish_batch(ticks, batch_id="b1", sequence=1)
    pub.close()

    reader = TickLakeReader(root=lake_root)

    # 1. RecordBatch (default)
    it_batch = reader.create_replay_iterator(output_format="record_batch")
    for b in it_batch:
        assert isinstance(b, pa.RecordBatch)
        assert b.schema.equals(LAKE_SCHEMA_V1)

    # 2. Table
    it_table = reader.create_replay_iterator(output_format="table")
    for t in it_table:
        assert isinstance(t, pa.Table)
        assert t.schema.equals(LAKE_SCHEMA_V1)

    # 3. Dict
    it_dict = reader.create_replay_iterator(output_format="dict")
    for d_list in it_dict:
        assert isinstance(d_list, list)
        assert isinstance(d_list[0], dict)
        assert d_list[0]["symbol"] == "AAPL"

    # 4. Convenience generators
    it_gen1 = reader.create_replay_iterator()
    tables = list(it_gen1.iter_tables())
    assert len(tables) == 1
    assert isinstance(tables[0], pa.Table)

    it_gen2 = reader.create_replay_iterator()
    batches = list(it_gen2.iter_batches())
    assert len(batches) == 1
    assert isinstance(batches[0], pa.RecordBatch)

    it_gen3 = reader.create_replay_iterator()
    dicts = list(it_gen3.iter_dicts())
    assert len(dicts) == 1
    assert isinstance(dicts[0], list)
