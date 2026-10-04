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

from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import recover_pending_publications
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.faults import inject_failure
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
    return TickLakeWriter(root=root, writer_id="fault_writer", max_batch_rows=10**9)


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
