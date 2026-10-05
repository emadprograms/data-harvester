"""
Persistence-boundary fault injection (Q05 / DURB-02).

Faults are injected at the exact points where durability is decided — file
fsync, staged-file promotion (`os.replace`), and receipt write — rather than at
the API boundary. Predicates scope each fault by path so status-file I/O is not
mistaken for a failed data publication.

Two fault budgets are used deliberately, because the writer's own retry and
drain behaviour changes the outcome:

- `failures=1` — a *transient* fault. The writer retries and publishes; the
  contract under test is that the retry publishes each row exactly once.
- `PERSISTENT_FAILURES` — a fault that outlasts `retry_attempts` *and* the
  drain performed by `close()`. Only then is the honest-failure contract
  observable: nothing claimed, nothing visible.

An earlier version of these tests expected `flush()` to raise on a single
injected fault; it does not, because the retry absorbs it. That is correct
behaviour and is now asserted as such.
"""

import contextlib
import os
from pathlib import Path

import pytest

from src.storage.barriers import PERSISTENCE_BOUNDARIES
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import recover_pending_publications
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.faults import PersistenceFaultInjector, inject_failure
from tests.support.lake_assertions import assert_row_multiset, finalized_parquet_files, read_final_rows

ROWS = 25
PERSISTENT_FAILURES = 12


def make_ticks(offset: int = 0) -> list[QuoteTick]:
    return [
        QuoteTick(
            timestamp=f"2026-10-02T14:30:{i:02d}.{i:06d}",
            symbol="AAPL",
            price=150.0 + i,
            volume=100.0 + i,
            bid=150.0 + i - 0.05,
            ask=150.0 + i + 0.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"fault_{offset}_{i:04d}",
        )
        for i in range(ROWS)
    ]


def as_rows(ticks):
    return [t.__dict__.copy() for t in ticks]


def _is_promotion(_src, dst) -> bool:
    return str(dst).endswith(".parquet") and f"{os.sep}ticks{os.sep}" in str(dst)


def _is_receipt(_src, dst) -> bool:
    return "receipts" in str(dst)


def _new_writer(root: Path) -> TickLakeWriter:
    # A very large batch threshold keeps publication under the test's control.
    return TickLakeWriter(root=root, writer_id="fault_writer", max_batch_rows=10**9, retry_backoff_base=0.005)


def test_transient_promotion_failure_is_retried_and_published_once(tmp_path, monkeypatch):
    """A single failed promotion must be absorbed by retry, with no duplicate or loss."""
    lake_root = tmp_path / "lake"
    writer = _new_writer(lake_root)
    ticks = make_ticks(1)

    counter = inject_failure(
        monkeypatch, os, "replace", predicate=_is_promotion, failures=1,
        error=OSError("injected transient promotion failure"),
    )

    writer.write_ticks(ticks)
    writer.flush(block=True)  # must not raise: the retry succeeds
    try:
        rows = read_final_rows(lake_root)
        assert counter.injected == 1, "the fault never fired — the test proves nothing"
        assert writer.metrics.total_published == ROWS
        assert len(rows) == ROWS, f"retry published {len(rows)} rows, expected {ROWS}"
        assert_row_multiset(rows, as_rows(ticks))
    finally:
        writer.close()


def test_persistent_promotion_failure_is_not_claimed(tmp_path, monkeypatch):
    """When promotion cannot succeed, nothing may be reported or become visible."""
    lake_root = tmp_path / "lake"
    writer = _new_writer(lake_root)

    counter = inject_failure(
        monkeypatch, os, "replace", predicate=_is_promotion,
        failures=PERSISTENT_FAILURES, error=OSError("injected promotion failure"),
    )

    writer.write_ticks(make_ticks(2))
    with pytest.raises(Exception):
        writer.flush(block=True)
    writer.close()

    assert counter.injected >= 1, "the fault never fired — the test proves nothing"
    assert writer.metrics.total_published == 0, (
        f"writer claimed {writer.metrics.total_published} rows despite a promotion failure"
    )
    assert finalized_parquet_files(lake_root) == [], "a file became visible despite a failed promotion"


def test_persistent_fsync_failure_is_not_claimed(tmp_path, monkeypatch):
    """A failed fsync means durability was never established, so nothing is committed."""
    lake_root = tmp_path / "lake"
    writer = _new_writer(lake_root)

    counter = inject_failure(
        monkeypatch, os, "fsync", predicate=None,
        failures=PERSISTENT_FAILURES, error=OSError("injected fsync failure"),
    )

    writer.write_ticks(make_ticks(3))
    with pytest.raises(Exception):
        writer.flush(block=True)
    writer.close()

    assert counter.injected >= 1, "the fault never fired — the test proves nothing"
    assert writer.metrics.total_published == 0, "rows were committed without a successful fsync"
    assert finalized_parquet_files(lake_root) == []


def test_persistent_receipt_failure_is_not_claimed(tmp_path, monkeypatch):
    """A batch without a durable receipt must not be counted as published."""
    lake_root = tmp_path / "lake"
    writer = _new_writer(lake_root)

    counter = inject_failure(
        monkeypatch, os, "replace", predicate=_is_receipt,
        failures=PERSISTENT_FAILURES, error=OSError("injected receipt failure"),
    )

    writer.write_ticks(make_ticks(4))
    with pytest.raises(Exception):
        writer.flush(block=True)
    writer.close()

    assert counter.injected >= 1, "the fault never fired — the test proves nothing"
    assert writer.metrics.total_published == 0, "a batch was committed without a durable receipt"


def test_persistent_fault_leaves_previous_batches_intact(tmp_path, monkeypatch):
    """A later failure must not disturb batches that were already durable."""
    lake_root = tmp_path / "lake"
    writer = _new_writer(lake_root)

    first = make_ticks(5)
    writer.write_ticks(first)
    writer.flush(block=True)
    assert len(read_final_rows(lake_root)) == ROWS

    inject_failure(
        monkeypatch, os, "replace", predicate=_is_promotion,
        failures=PERSISTENT_FAILURES, error=OSError("injected promotion failure"),
    )

    writer.write_ticks(make_ticks(6))
    with pytest.raises(Exception):
        writer.flush(block=True)
    # close() performs a final drain, which also fails while the fault persists;
    # that failure is expected and is not what this test is asserting.
    with contextlib.suppress(Exception):
        writer.close()

    rows = read_final_rows(lake_root)
    assert len(rows) == ROWS, f"the durable batch changed after a later failure ({len(rows)} rows)"
    assert_row_multiset(rows, as_rows(first))

    # Recovery also promotes staged files with os.replace, so the fault must be
    # lifted first: what we are testing is recovery's behaviour on a healthy
    # filesystem, not its behaviour under the same injected failure.
    monkeypatch.undo()
    recover_pending_publications(lake_root)

    # Recovery may legitimately FINISH a valid pending publication — that is its
    # job, and losing it would be worse. What it must never do is produce a
    # partial or duplicated one: the already-durable batch stays intact, no
    # ingest_id appears twice, and the total is either one batch or two.
    final_rows = read_final_rows(lake_root)
    ids = [row["ingest_id"] for row in final_rows]
    assert len(ids) == len(set(ids)), "recovery produced duplicate rows"
    assert {t.ingest_id for t in first} <= set(ids), "the durable batch lost rows during recovery"
    assert len(final_rows) in (ROWS, 2 * ROWS), (
        f"unexpected row count after recovery: {len(final_rows)} (expected {ROWS} or {2 * ROWS})"
    )


def make_multi_symbol_ticks() -> list[QuoteTick]:
    aapl_ticks = [
        QuoteTick(
            timestamp=f"2026-10-02T14:30:{i:02d}.{i:06d}",
            symbol="AAPL",
            price=150.0 + i,
            volume=100.0 + i,
            bid=150.0 + i - 0.05,
            ask=150.0 + i + 0.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"multi_aapl_{i:04d}",
        )
        for i in range(10)
    ]
    msft_ticks = [
        QuoteTick(
            timestamp=f"2026-10-02T14:30:{i:02d}.{i:06d}",
            symbol="MSFT",
            price=300.0 + i,
            volume=100.0 + i,
            bid=300.0 + i - 0.05,
            ask=300.0 + i + 0.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"multi_msft_{i:04d}",
        )
        for i in range(10)
    ]
    return aapl_ticks + msft_ticks


@pytest.mark.parametrize("boundary", PERSISTENCE_BOUNDARIES)
def test_named_persistence_barrier_transient_fault_is_retried_and_published_once(tmp_path, boundary):
    """
    DURB-02: A single transient fault at any named persistence boundary is absorbed
    by retry logic and committed exactly once with 100% multiset match and zero duplicates.
    """
    lake_root = tmp_path / f"lake_transient_{boundary}"
    writer = _new_writer(lake_root)
    ticks = make_ticks(100)

    with PersistenceFaultInjector() as injector:
        injector.inject_barrier(boundary, failures=1, error=OSError(f"injected transient fault at {boundary}"))

        if boundary == "admission":
            for tick in ticks:
                for attempt in range(3):
                    try:
                        writer.write_tick(tick)
                        break
                    except OSError:
                        if attempt == 2:
                            raise
        else:
            writer.write_ticks(ticks)

        writer.flush(block=True)
        injector.assert_triggered(boundary)
        rows = read_final_rows(lake_root)
        assert writer.metrics.total_published == len(ticks)
        assert len(rows) == len(ticks)
        assert_row_multiset(rows, as_rows(ticks))
        writer.close()


@pytest.mark.parametrize("boundary", PERSISTENCE_BOUNDARIES)
def test_named_persistence_barrier_persistent_fault_is_not_claimed(tmp_path, boundary):
    """
    DURB-02: A persistent fault outlasting all retries at any named persistence boundary
    results in clean rejection, without claiming rows committed or publishing partial data.
    """
    lake_root = tmp_path / f"lake_persistent_{boundary}"
    writer = _new_writer(lake_root)
    ticks = make_ticks(200)

    with PersistenceFaultInjector() as injector:
        injector.inject_barrier(
            boundary,
            failures=PERSISTENT_FAILURES,
            error=OSError(f"injected persistent fault at {boundary}"),
        )

        if boundary == "admission":
            with pytest.raises(OSError):
                for tick in ticks:
                    writer.write_tick(tick)
            with contextlib.suppress(Exception):
                writer.flush(block=True)
            with contextlib.suppress(Exception):
                writer.close()
            injector.assert_triggered("admission")
            assert writer.metrics.total_published == 0
            assert finalized_parquet_files(lake_root) == []
        elif boundary == "acknowledgment":
            writer.write_ticks(ticks)
            with pytest.raises(Exception):
                writer.flush(block=True)
            with contextlib.suppress(Exception):
                writer.close()
            injector.assert_triggered("acknowledgment")
            assert writer.metrics.total_published == 0
        elif boundary in ("directory_fsync", "receipt_durability"):
            # Files may have been promoted to partition folders, but directory fsync or
            # receipt durability persistently failed. The publication was never committed,
            # so total_published must be 0 and no receipt file must exist.
            writer.write_ticks(ticks)
            with pytest.raises(Exception):
                writer.flush(block=True)
            with contextlib.suppress(Exception):
                writer.close()
            injector.assert_triggered(boundary)
            assert writer.metrics.total_published == 0
            assert not any((lake_root / "_control" / "receipts").glob("*.json"))
        else:
            writer.write_ticks(ticks)
            with pytest.raises(Exception):
                writer.flush(block=True)
            with contextlib.suppress(Exception):
                writer.close()
            injector.assert_triggered(boundary)
            assert writer.metrics.total_published == 0
            assert finalized_parquet_files(lake_root) == []


def test_multi_partition_batch_partial_promotion_and_fresh_recovery(tmp_path):
    """
    DURB-02: Multi-symbol batch ("AAPL", "MSFT") spanning multiple partitions (total 20 rows).
    Inject persistent fault during promotion of 2nd symbol. First symbol (AAPL) is promoted,
    second symbol fails.
    In fresh recovery pass (recover_pending_publications), assert remaining staged files are
    promoted, receipt is committed, and all 20 rows are verified with 100% multiset equivalence
    and zero duplicates.
    """
    lake_root = tmp_path / "lake_multi"
    writer = _new_writer(lake_root)
    ticks = make_multi_symbol_ticks()
    assert len(ticks) == 20

    injector = PersistenceFaultInjector()
    injector.inject_barrier(
        "staged_promotion",
        failures=PERSISTENT_FAILURES,
        error=OSError("injected persistent disk error promoting MSFT partition"),
        predicate=lambda **kwargs: kwargs.get("symbol") == "MSFT",
    )

    writer.write_ticks(ticks)
    with pytest.raises(Exception):
        writer.flush(block=True)
    with contextlib.suppress(Exception):
        writer.close()

    injector.assert_triggered("staged_promotion")

    # Assert partial promotion state before fresh recovery:
    files_before = finalized_parquet_files(lake_root)
    assert len(files_before) == 1, f"Expected 1 promoted file before recovery, found {len(files_before)}"
    assert "AAPL" in str(files_before[0])
    assert "MSFT" not in str(files_before[0])

    # Receipt was NOT committed because batch publication failed
    receipts_before = list((lake_root / "_control" / "receipts").glob("*.json"))
    assert len(receipts_before) == 0, "Receipt must not be committed for partially promoted batch"

    # Intent file exists and retains staged state
    intents = list((lake_root / "_control" / "intent").glob("*.json"))
    assert len(intents) == 1, "Intent file must be retained for recovery"

    # Lift the fault for fresh recovery pass
    injector.close()

    # Fresh recovery pass
    recovered = recover_pending_publications(lake_root)
    assert len(recovered) == 1, f"Expected 1 recovered receipt, got {len(recovered)}"
    receipt = recovered[0]
    assert receipt.row_count == 20
    assert receipt.status == "PUBLISHED"

    # Verify post-recovery state:
    files_after = finalized_parquet_files(lake_root)
    assert len(files_after) == 2, f"Expected 2 finalized files (AAPL, MSFT), found {len(files_after)}"
    assert any("symbol=AAPL" in str(p) for p in files_after)
    assert any("symbol=MSFT" in str(p) for p in files_after)

    # Receipt file is committed
    receipt_file = lake_root / "_control" / "receipts" / f"{receipt.batch_id}.json"
    assert receipt_file.is_file(), f"Receipt file {receipt_file} was not written"

    # Intent file was removed
    assert not any((lake_root / "_control" / "intent").glob("*.json")), "Intent file was not removed"

    # All 20 rows are verified with 100% multiset equivalence and zero duplicates
    rows = read_final_rows(lake_root)
    assert len(rows) == 20, f"Expected 20 durable rows, found {len(rows)}"
    ids = [r["ingest_id"] for r in rows]
    assert len(ids) == len(set(ids)), "Recovery produced duplicate ingest_ids"
    assert_row_multiset(rows, as_rows(ticks))
