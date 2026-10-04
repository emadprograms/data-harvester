"""
Cutover rehearsal behaviour (Phase 34 / MIGR-04).

The rehearsal must be honest: a stalled drain aborts before publication, and a
failed restart is reported as a failure even though the data was published. Both
are easy to get wrong in a runbook that reports only a final "success" flag.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import LakeOwnershipError, LakePublisherLock
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.cutover_rehearsal import CutoverRehearsal
from tests.support.frozen_source import create_frozen_source
from tests.support.lake_assertions import finalized_parquet_files, read_final_rows
from tests.support.migration_cli import run_cli

CHUNK_SIZE = 200
PER_SYMBOL_DATE = 60


class StalledDrainError(RuntimeError):
    """Raised by the injected stalled drain."""


class RestartFailureError(RuntimeError):
    """Raised by the injected failed restart."""


def _except_all(source_rows, final_rows, fields) -> tuple:
    import duckdb

    columns = ", ".join(fields)
    con = duckdb.connect(":memory:")
    try:
        types = ", ".join(
            f"{c} " + ("TIMESTAMP" if c == "timestamp" else "VARCHAR" if c in ("symbol", "source", "session") else "DOUBLE")
            for c in fields
        )
        placeholders = ", ".join("?" for _ in fields)
        con.execute(f"CREATE TABLE src ({types})")
        con.execute(f"CREATE TABLE fin ({types})")
        con.executemany(
            f"INSERT INTO src ({columns}) VALUES ({placeholders})",
            [tuple(r[f] for f in fields) for r in source_rows],
        )
        con.executemany(
            f"INSERT INTO fin ({columns}) VALUES ({placeholders})",
            [tuple(r[f] for f in fields) for r in final_rows],
        )
        missing = con.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM src EXCEPT ALL SELECT {columns} FROM fin)"
        ).fetchone()[0]
        extra = con.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM fin EXCEPT ALL SELECT {columns} FROM src)"
        ).fetchone()[0]
        return missing, extra
    finally:
        con.close()


@pytest.fixture
def rehearsal_env(tmp_path):
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    db = tmp_path / "streaming.duckdb"
    rows = create_frozen_source(db, per_symbol_date=PER_SYMBOL_DATE)

    source_records = [
        {
            "timestamp": r[0], "symbol": r[1], "price": r[2], "volume": r[3],
            "bid": r[4], "ask": r[5], "source": r[6], "session": r[7],
        }
        for r in rows
    ]
    fields = tuple(source_records[0])

    # A live writer holding buffered rows, as it would be at cutover time.
    writer = TickLakeWriter(root=lake, writer_id="pre_cutover", max_batch_rows=10**9)
    writer.write_ticks(
        [
            QuoteTick(
                timestamp="2026-10-02T14:30:00",
                symbol="AAPL",
                price=250.0,
                volume=1.0,
                source="CAPITAL",
                session="REG",
                ingest_id="pre_cutover_0001",
            )
        ]
    )

    def publish():
        result = run_cli(
            ["--source-db", str(db), "--lake-root", str(lake), "--chunk-size", str(CHUNK_SIZE), "--mode", "all"]
        )
        if not result.ok:
            raise RuntimeError(f"migration CLI failed: {result.describe()}")
        return result

    def migrated_rows():
        # The live row buffered before cutover is deliberately not part of the
        # frozen source, so it must be excluded from migration reconciliation.
        return [
            r for r in read_final_rows(lake)
            if str(r["timestamp"]).startswith("2026-07")
        ]

    def reconcile():
        return _except_all(source_records, migrated_rows(), fields)

    return {
        "lake": lake,
        "db": db,
        "writer": writer,
        "publish": publish,
        "reconcile": reconcile,
        "migrated_rows": migrated_rows,
        "source_records": source_records,
        "fields": fields,
    }


def _rehearsal(env, **overrides) -> CutoverRehearsal:
    return CutoverRehearsal(
        lake=env["lake"],
        source_db=env["db"],
        writer=env["writer"],
        publish=env["publish"],
        reconcile=env["reconcile"],
        **overrides,
    )


def test_successful_cutover_publishes_restarts_and_reconciles(rehearsal_env):
    """Every step passing yields a clean rehearsal with reconciled data."""
    result = _rehearsal(rehearsal_env).run()

    assert result.failed_step is None, f"rehearsal failed:\n{result.report()}"
    assert result.published and result.restarted
    assert [s.status for s in result.steps] == ["PASSED"] * 6

    # The live row buffered before cutover survived, and the migration landed.
    rows = read_final_rows(rehearsal_env["lake"])
    assert any(r["ingest_id"] == "pre_cutover_0001" for r in rows), (
        "the live row buffered before cutover was lost"
    )
    missing, extra = _except_all(
        rehearsal_env["source_records"], rehearsal_env["migrated_rows"](), rehearsal_env["fields"]
    )
    assert (missing, extra) == (0, 0), f"reconciliation after cutover: missing={missing} extra={extra}"


def test_stalled_drain_aborts_before_publishing(rehearsal_env):
    """A stalled drain must stop the rehearsal before any data is published."""
    def stalled(writer):
        raise StalledDrainError("drain did not complete within the timeout")

    result = _rehearsal(rehearsal_env, drain=stalled).run()

    assert result.step("stop_and_drain").status == "FAILED"
    assert result.published is False, "the rehearsal published despite a stalled drain"
    assert result.step("publish").status == "SKIPPED", (
        f"publish should have been skipped, not {result.step('publish').status}"
    )
    # Nothing from the migration reached the lake.
    migrated = [
        r for r in read_final_rows(rehearsal_env["lake"])
        if not r["ingest_id"].startswith("pre_cutover")
    ]
    assert migrated == [], "migration rows appeared despite the aborted rehearsal"


def test_failed_restart_is_reported_and_does_not_claim_success(rehearsal_env):
    """Publishing succeeded but the restart failed: the run is not a success."""
    def failing_restart(root):
        raise RestartFailureError("streamer failed to start after cutover")

    result = _rehearsal(rehearsal_env, restart=failing_restart).run()

    assert result.published is True, "the migration should have been published"
    assert result.restarted is False
    assert result.step("restart").status == "FAILED"
    assert result.failed_step is not None, "the rehearsal reported no failure step"
    assert result.failed_step.name == "restart"


def test_no_competing_writer_can_own_the_lake_during_cutover(rehearsal_env):
    """While the rehearsal holds ownership, another publisher is refused."""
    lake = rehearsal_env["lake"]
    # The fixture's live writer holds ownership; the rehearsal step is to release
    # it before taking ownership for the cutover.
    rehearsal_env["writer"].close()
    owner = LakePublisherLock(lake, writer_id="cutover_owner")
    assert owner.acquire(blocking=False), "could not take ownership after releasing it"
    try:
        # Ownership is refused loudly: acquire raises rather than returning False
        # when another publisher holds the lock.
        competitor = LakePublisherLock(lake, writer_id="competing_writer")
        with pytest.raises(LakeOwnershipError):
            competitor.acquire(blocking=False)
    finally:
        owner.release()

    # Ownership is genuinely released afterwards.
    after = LakePublisherLock(lake, writer_id="post_cutover")
    assert after.acquire(blocking=False), "ownership was not released"
    after.release()


def test_rehearsal_reports_every_step_even_when_it_fails_early(rehearsal_env):
    """A failed rehearsal still reports the full sequence, with no silent gaps."""
    def stalled(writer):
        raise StalledDrainError("drain stalled")

    result = _rehearsal(rehearsal_env, drain=stalled).run()

    assert [s.name for s in result.steps] == [
        "stop_and_drain",
        "capture_frozen_source",
        "release_ownership",
        "publish",
        "restart",
        "reconcile",
    ], result.report()
    assert result.report(), "the rehearsal produced no report"
