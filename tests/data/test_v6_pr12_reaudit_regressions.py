"""PR #12 re-audit regressions (CR-01..CR-04).

Each test states the behaviour the fix must provide, so every one of them fails on
the pre-fix candidate 1d51a22c and passes once the corresponding defect is closed.
The four defects are described in `.planning/milestones/v6.0-MILESTONE-AUDIT.md`
under the PR #12 re-audit record.

CR-01  a stale pending interval plus a scope change duplicated one stored quote
CR-02  a lost ledger hid a recoverable publication intent and re-bought the interval
CR-03  a malformed orphan gap-fill receipt was silently read as "no evidence"
CR-04  a directory fsync failure (EIO) was reported as a durable success
"""

import errno
import json
import os
import stat
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import src.data.gap_fill as gap_fill_module
from src.data.gap_fill import GapFillRecoveryError, fill_named_day
from src.data.databento_backfill import publish_ticks_to_lake
from src.storage.barriers import clear_barrier_hooks
from src.storage.config import init_tick_lake
from tests.data.test_phase51_gap_fill import (
    FIRST_GAP,
    NORMAL_DAY,
    SYMBOLS,
    _RecordingClient,
    _coverage_file,
    _coverage_state,
    _et,
    _gfill_receipts,
    _interrupt_coverage_commit,
    _pending_record,
    _request_windows,
    _utc,
    _v2_files,
    _write_occupied_day_gaps,
)

OTHER_SCOPE = SYMBOLS + ["MSFT"]


def _stored_rows(lake: Path, existing: Path):
    """Every schema-v2 row the fills appended, as (files, rows)."""
    files = _v2_files(lake, existing)
    rows = [row for path in files for row in pq.ParquetFile(path).read().to_pylist()]
    return files, rows


def _identities(rows):
    return {row["ingest_id"] for row in rows}


# ---------------------------------------------------------------------------
# CR-01 - identical quotes must never be appended twice under different batches
# ---------------------------------------------------------------------------


def test_cr01_stale_pending_scope_change_appends_no_duplicate(tmp_path):
    """A stale pending interval must not re-append a quote another scope stored.

    Mirrors the reviewer's reproduction: fail the download for [AAPL, NVDA], fill the
    same silence for [AAPL, NVDA, MSFT], then resume [AAPL, NVDA]. The resumed batch
    must collapse onto the already-stored identity instead of writing a second file.
    """
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    client.failures_remaining["get_range"] = 1
    with pytest.raises(OSError):
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=OTHER_SCOPE)
    resumed = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    files, rows = _stored_rows(lake, existing)
    assert len(files) == 1, f"expected one physical file, got {[path.name for path in files]}"
    assert len(rows) == 1, f"expected one stored row, got {len(rows)}"
    assert len(_identities(rows)) == 1
    assert resumed["rows"] == 0, "a fully deduplicated response appended rows"

    # A completely deduplicated response still records recoverable coverage. Each
    # symbol scope owns one coverage entry for the same wall-clock interval.
    state = _coverage_state(lake)
    assert state["pending"] == []
    assert len(state["intervals"]) == 2, "each scope records its own coverage"
    assert {item["start"][11:16] for item in state["intervals"]} == {"10:01"}
    assert {item["end"][11:16] for item in state["intervals"]} == {"10:06"}

    settled = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    assert settled["requests"] == 0, "the covered interval was requested again"
    assert len(client.calls) == 3


def test_cr01_partial_overlap_appends_only_new_identities(tmp_path):
    """Overlapping responses append only the identities the lake does not hold yet."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])

    # Another scope already paid for the 10:01 quote.
    seed = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    fill_named_day(NORMAL_DAY, client=seed, lake_root=lake, symbols=OTHER_SCOPE)

    # This scope's response repeats 10:01 and adds 10:03.
    client = _RecordingClient(
        [(_utc(NORMAL_DAY, 10, 1), "NVDA"), (_utc(NORMAL_DAY, 10, 3), "NVDA")]
    )
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    files, rows = _stored_rows(lake, existing)
    assert len(rows) == len(_identities(rows)) == 2, (
        f"expected exactly the two distinct quotes, got {len(rows)} rows / "
        f"{len(_identities(rows))} identities"
    )
    assert sorted(row["timestamp"].strftime("%H:%M") for row in rows) == ["14:01", "14:03"]
    assert len(files) == 2, "each distinct payload still owns its own file"


def test_cr01_partially_deduplicated_batch_still_replays(tmp_path):
    """A batch published with rows already dropped must still replay cleanly.

    The receipt has to accept the caller's original (pre-deduplication) rows, or the
    retry after a crash would fail a payload check instead of replaying.
    """
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    other = _quote_frame(
        [("2026-10-02 14:01:00", "dup_1001")], symbol="NVDA"
    )
    publish_ticks_to_lake(other, lake_root=lake, batch_id="gfill_other", sequence=0)

    both = _quote_frame(
        [("2026-10-02 14:01:00", "dup_1001"), ("2026-10-02 14:03:00", "new_1003")],
        symbol="NVDA",
    )
    written = publish_ticks_to_lake(both, lake_root=lake, batch_id="gfill_partial", sequence=0)
    assert written == 1, "only the new identity may be appended"

    replayed = publish_ticks_to_lake(both, lake_root=lake, batch_id="gfill_partial", sequence=0)
    assert replayed == 1, "the replay must report the published row count, not a collision"

    files = sorted((lake / "ticks" / "symbol=NVDA" / f"date={NORMAL_DAY.isoformat()}").glob("*.parquet"))
    rows = [row for path in files for row in pq.ParquetFile(path).read().to_pylist()]
    assert len(rows) == len(_identities(rows)) == 2


def _quote_frame(pairs, symbol: str = "NVDA"):
    """A v2 quote frame: the same (timestamp, ingest_id) pair is the same quote."""
    return pd.DataFrame(
        {
            "timestamp": [timestamp for timestamp, _ in pairs],
            "symbol": [symbol] * len(pairs),
            "bid_price": [100.00] * len(pairs),
            "ask_price": [100.04] * len(pairs),
            "source": ["DATABENTO"] * len(pairs),
            "session": ["REG"] * len(pairs),
            "ingest_id": [ingest_id for _, ingest_id in pairs],
        }
    )


# ---------------------------------------------------------------------------
# CR-02 - a surviving intent must be recovered without the ledger
# ---------------------------------------------------------------------------


def _interrupt_at_receipt_durability(lake: Path, client) -> None:
    """Leave one promoted quote, a valid scoped intent, and no receipt."""
    from tests.support.faults import PersistenceFaultInjector

    injector = PersistenceFaultInjector()
    injector.inject_barrier(
        "receipt_durability", failures=1, error=OSError("injected receipt_durability interrupt")
    )
    try:
        with pytest.raises(Exception):
            fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
        injector.assert_triggered("receipt_durability")
    finally:
        injector.close()
        clear_barrier_hooks()


def test_cr02_orphan_intent_recovers_without_a_ledger(tmp_path):
    """Losing the ledger must not hide the intent that names the original request."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    _interrupt_at_receipt_durability(lake, client)
    intents = list((lake / "_control" / "intent").glob("gfill_*.json"))
    assert intents and _gfill_receipts(lake) == [], "setup must leave one intent and no receipt"

    _coverage_file(lake).unlink()  # the ledger is gone

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"the orphaned intent bought data again: {_request_windows(client)}"
    assert result["requests"] == 0
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]

    state = _coverage_state(lake)
    assert state["pending"] == []
    assert state["intervals"][0]["start"].startswith("2026-10-02T10:01:00")
    assert state["intervals"][0]["end"].startswith("2026-10-02T10:06:00")
    assert list((lake / "_control" / "intent").glob("gfill_*.json")) == []
    assert len(_gfill_receipts(lake)) == 1

    _, rows = _stored_rows(lake, existing)
    assert len(rows) == len(_identities(rows)) == 1, "recovery appended a duplicate"


def test_cr02_foreign_intent_is_left_alone(tmp_path):
    """An intent for another scope is evidence for that scope, not for this fill."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    intent_dir = lake / "_control" / "intent"
    intent_dir.mkdir(parents=True, exist_ok=True)
    foreign = intent_dir / "gfill_other_scope.json"
    foreign.write_text(
        json.dumps(
            {
                "batch_id": "gfill_other_scope",
                "writer_id": "gap_fill",
                "sequence": 0,
                "state": "STAGED",
                "request": {
                    "date": "2026-10-05",
                    "dataset": "DBEQ.BASIC",
                    "schema": "tbbo",
                    "symbols": list(SYMBOLS),
                    "start": "2026-10-05T10:01:00-04:00",
                    "end": "2026-10-05T10:06:00-04:00",
                    "batch_id": "gfill_other_scope",
                },
            }
        ),
        encoding="utf-8",
    )
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1, "another day's intent must not block this fill"
    assert foreign.is_file(), "foreign intent evidence is left where it was"


# ---------------------------------------------------------------------------
# CR-03 - malformed orphan gap-fill receipts must refuse before spending
# ---------------------------------------------------------------------------


def _orphan_a_gap_fill_receipt(lake: Path, client) -> Path:
    """Fill once, then drop the ledger so only the receipt could carry the evidence."""
    fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)
    receipt = _gfill_receipts(lake)[0]
    _coverage_file(lake).unlink()
    return receipt


def test_cr03_corrupt_orphan_receipt_refuses_before_spending(tmp_path):
    """An unreadable gap-fill receipt is evidence that cannot be checked: refuse."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    receipt = _orphan_a_gap_fill_receipt(lake, client)
    receipt.write_text("{", encoding="utf-8")

    with pytest.raises(GapFillRecoveryError) as excinfo:
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )

    assert receipt.name in str(excinfo.value), "the refusal must name the file to inspect"
    assert client.cost_calls == [], "refusal must happen before cost estimation"
    assert len(client.calls) == 1, "refusal must happen before a second download"
    assert receipt.read_text(encoding="utf-8") == "{", "corrupt evidence is retained"


def test_cr03_malformed_scope_structure_refuses_before_spending(tmp_path):
    """A receipt whose request scope cannot be parsed cannot be attributed: refuse."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    receipt = _orphan_a_gap_fill_receipt(lake, client)
    receipt.write_text(json.dumps({"batch_id": receipt.stem, "request": "broken"}), encoding="utf-8")

    with pytest.raises(GapFillRecoveryError):
        fill_named_day(
            NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS, remaining_budget=1.0
        )

    assert client.cost_calls == []
    assert len(client.calls) == 1


def test_cr03_unrelated_corrupt_receipt_is_still_ignored(tmp_path):
    """Junk that cannot be gap-fill evidence must not block the fill."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    stray = lake / "_control" / "receipts" / "batch_streamer_000042.json"
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_text("{", encoding="utf-8")
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert stray.read_text(encoding="utf-8") == "{"


# ---------------------------------------------------------------------------
# CR-04 - a refused directory fsync is not a durable success
# ---------------------------------------------------------------------------


class _DirectoryFsyncFault:
    """Fail directory fsyncs with EIO; optionally only after `skip` of them."""

    def __init__(self, skip: int = 0):
        self.skip = skip
        self.directory_calls = 0
        self.failures = 0

    def __call__(self, fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            self.directory_calls += 1
            if self.directory_calls > self.skip:
                self.failures += 1
                raise OSError(errno.EIO, "injected directory fsync failure")
            return
        return self._real(fd)


def _install_directory_fsync_fault(monkeypatch, skip: int = 0) -> _DirectoryFsyncFault:
    fault = _DirectoryFsyncFault(skip=skip)
    fault._real = os.fsync
    monkeypatch.setattr(os, "fsync", fault)
    return fault


def test_cr04_pending_fsync_failure_refuses_before_download(tmp_path, monkeypatch):
    """If the pending interval cannot be made durable, no paid request may follow."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])
    fault = _install_directory_fsync_fault(monkeypatch, skip=0)

    with pytest.raises(OSError) as excinfo:
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert excinfo.value.errno == errno.EIO
    assert fault.failures >= 1, "the injected durability failure was never reached"
    assert client.calls == [], "a paid download followed a failed durability barrier"


def test_cr04_completed_coverage_fsync_failure_recovers_from_the_receipt(tmp_path, monkeypatch):
    """A refused coverage commit must raise, then recover without buying again."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    existing = _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    client = _RecordingClient([])  # empty success: two directory fsyncs, no staging writes
    fault = _install_directory_fsync_fault(monkeypatch, skip=1)

    with pytest.raises(OSError) as excinfo:
        fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert excinfo.value.errno == errno.EIO
    assert fault.failures == 1, "the first (pending) write must have been persisted"
    monkeypatch.undo()

    assert len(_gfill_receipts(lake)) == 1, "the publication receipt must survive"
    recovery = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert len(client.calls) == 1, f"recovery bought the interval again: {_request_windows(client)}"
    assert recovery["requests"] == 0
    state = _coverage_state(lake)
    assert state["pending"] == []
    assert state["intervals"][0]["start"].startswith("2026-10-02T10:01:00")
    assert _stored_rows(lake, existing)[0] == []


# ---------------------------------------------------------------------------
# CR-02/CR-03 negative controls: a pending record with no intent must still retry
# ---------------------------------------------------------------------------


def test_pr12_unresolved_pending_interval_still_retries_on_original_bounds(tmp_path):
    """The dedup and intent work must not weaken the original P2 retry contract."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    _write_occupied_day_gaps(lake, NORMAL_DAY, [FIRST_GAP])
    record = _pending_record(batch_id="gfill_missing_intent")
    gap_fill_module._save_state(  # noqa: SLF001 - the ledger is the fixture
        lake, [], [record]
    )
    client = _RecordingClient([(_utc(NORMAL_DAY, 10, 1), "NVDA")])

    result = fill_named_day(NORMAL_DAY, client=client, lake_root=lake, symbols=SYMBOLS)

    assert result["requests"] == 1
    assert _request_windows(client) == [("2026-10-02T14:01:00", "2026-10-02T14:06:00")]
    assert _coverage_state(lake)["pending"] == []
