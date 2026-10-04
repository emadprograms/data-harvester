"""
Migration, backup and restore rehearsal (Phase 34 / MIGR-01, MIGR-02, MIGR-03, MIGR-05).

Every migration here runs through the real CLI in a **fresh process**
(`tests/support/migration_cli.py`), because "resumes after a crash" has to mean
it survives losing all in-memory state. Crash points that cannot be produced by
stopping early are injected at the syscall that decides durability via
`tests/support/migration_fault_runner.py`.

Two findings are pinned by these tests rather than fixed, because both need a
design decision that belongs to a later milestone:

- **F10** — re-running a migration with a *different* date filter after a
  completed migration duplicates every partition in that filter, and the tool's
  own `verify` does not notice.
- **F11** — `verify` reconciles the run-owned **staging** inventory against the
  source, not the **final published** inventory. It proves the export was
  lossless; it does not prove the published lake matches the source.
"""

from __future__ import annotations

import hashlib
import shutil
from collections import Counter
from pathlib import Path

import duckdb
import pytest

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from tests.fixtures.deterministic_quotes import QuoteTick
from tests.support.frozen_source import (
    ALL_SYMBOLS,
    INACTIVE_SYMBOLS,
    create_frozen_source,
)
from tests.support.lake_assertions import finalized_parquet_files, read_final_rows
from tests.support.migration_cli import run_cli, run_cli_with_fault, start_cli
from tests.support.process_harness import wait_until

CHUNK_SIZE = 200
PER_SYMBOL_DATE = 120

MAPPED_FIELDS = (
    "timestamp",
    "symbol",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
    "session",
)


# ---------------------------------------------------------------- helpers


def _final_files(lake: Path):
    return sorted(finalized_parquet_files(lake))


def _digests(lake: Path) -> dict:
    return {
        str(p.relative_to(lake)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in _final_files(lake)
    }


FLAG = "__flag__"  # marks a switch-style argument that takes no value


def _cli_args(db: Path, lake: Path, **extra) -> list:
    args = [
        "--source-db", str(db),
        "--lake-root", str(lake),
        "--chunk-size", str(CHUNK_SIZE),
    ]
    for key, value in extra.items():
        flag = f"--{key.replace('_', '-')}"
        if value is FLAG or value is True:
            args.append(flag)
        else:
            args.extend([flag, str(value)])
    return args


def _except_all_differences(source_rows, final_rows) -> tuple:
    """Bidirectional EXCEPT ALL over the mapped fields. Returns (missing, extra)."""
    con = duckdb.connect(":memory:")
    try:
        columns = ", ".join(MAPPED_FIELDS)
        placeholders = ", ".join("?" for _ in MAPPED_FIELDS)
        con.execute(
            f"CREATE TABLE source_rows ({', '.join(f'{c} {_sql_type(c)}' for c in MAPPED_FIELDS)})"
        )
        con.execute(
            f"CREATE TABLE final_rows ({', '.join(f'{c} {_sql_type(c)}' for c in MAPPED_FIELDS)})"
        )
        con.executemany(
            f"INSERT INTO source_rows ({columns}) VALUES ({placeholders})",
            [tuple(row[field] for field in MAPPED_FIELDS) for row in source_rows],
        )
        con.executemany(
            f"INSERT INTO final_rows ({columns}) VALUES ({placeholders})",
            [tuple(row[field] for field in MAPPED_FIELDS) for row in final_rows],
        )
        missing = con.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM source_rows "
            f"EXCEPT ALL SELECT {columns} FROM final_rows)"
        ).fetchone()[0]
        extra = con.execute(
            f"SELECT count(*) FROM (SELECT {columns} FROM final_rows "
            f"EXCEPT ALL SELECT {columns} FROM source_rows)"
        ).fetchone()[0]
        return missing, extra
    finally:
        con.close()


def _sql_type(field: str) -> str:
    if field == "timestamp":
        return "TIMESTAMP"
    if field in ("symbol", "source", "session"):
        return "VARCHAR"
    return "DOUBLE"


def _source_records(rows) -> list:
    return [
        {
            "timestamp": r[0], "symbol": r[1], "price": r[2], "volume": r[3],
            "bid": r[4], "ask": r[5], "source": r[6], "session": r[7],
        }
        for r in rows
    ]


@pytest.fixture(scope="module")
def frozen(tmp_path_factory):
    """A frozen source plus an empty lake, shared by the module's tests."""
    root = tmp_path_factory.mktemp("frozen")
    db = root / "streaming.duckdb"
    rows = create_frozen_source(db, per_symbol_date=PER_SYMBOL_DATE)
    return {"db": db, "rows": rows, "root": root}


@pytest.fixture
def lake(tmp_path) -> Path:
    root = tmp_path / "lake"
    init_tick_lake(root)
    return root


# ---------------------------------------------------------------- MIGR-01


def test_large_frozen_source_reconciles_through_final_publication(frozen, lake):
    """MIGR-01: a large frozen source reaches the final published inventory."""
    result = run_cli(_cli_args(frozen["db"], lake, mode="all"))
    assert result.ok, f"migration failed:\n{result.describe()}"

    final = read_final_rows(lake)
    assert len(final) == len(frozen["rows"]), (
        f"expected {len(frozen['rows'])} published rows, found {len(final)}"
    )

    # Inactive symbols are not part of the live streaming set but must migrate.
    published_symbols = {row["symbol"] for row in final}
    for symbol in INACTIVE_SYMBOLS:
        assert symbol in published_symbols, f"inactive symbol {symbol} did not migrate"
    assert published_symbols == set(ALL_SYMBOLS), published_symbols


def test_frozen_source_edge_cases_survive_publication(frozen, lake):
    """MIGR-01: duplicates, ties, nulls, float edges and late events all land."""
    result = run_cli(_cli_args(frozen["db"], lake, mode="all"))
    assert result.ok, f"migration failed:\n{result.describe()}"

    final = read_final_rows(lake)

    # Exact duplicates keep multiplicity 3.
    dupes = [
        row for row in final
        if row["symbol"] == "AAPL" and abs(row["price"] - 111.11) < 1e-9
    ]
    assert len(dupes) == 3, f"expected 3 duplicate rows, found {len(dupes)}"

    # Ties on (timestamp, symbol) all survive.
    ties = [
        row for row in final
        if row["symbol"] == "MSFT" and str(row["timestamp"]).startswith("2026-07-11 15:00:00")
    ]
    assert len(ties) == 3, f"expected 3 tied rows, found {len(ties)}"
    assert sorted(round(row["price"], 6) for row in ties) == [40.0, 50.0, 60.0], ties

    # Nulls stay null rather than being defaulted.
    null_row = [
        row for row in final
        if row["symbol"] == "AAPL" and abs(row["price"] - 90.0) < 1e-9
    ][0]
    assert null_row["volume"] is None and null_row["bid"] is None, null_row
    assert null_row["source"] is None and null_row["session"] is None, null_row

    # Float edge values round-trip bit-exactly, including the denormal minimum.
    prices = {row["price"] for row in final if row["symbol"] == "OLDCO"}
    for edge in (5e-324, 1e-300, 0.1, 1.7976931348623157e308):
        assert edge in prices, f"float edge {edge!r} did not round-trip"

    # Late events land in their own event date despite being inserted last.
    late = [
        row for row in final
        if str(row["timestamp"]).startswith("2026-07-10 13:00:00")
    ]
    assert len(late) == 2, f"expected 2 late events, found {len(late)}"


# ---------------------------------------------------------------- MIGR-02


def test_final_output_reconciles_with_the_source_by_bidirectional_except_all(frozen, lake):
    """MIGR-02: final published output reconciles with the frozen source, both ways."""
    result = run_cli(_cli_args(frozen["db"], lake, mode="all"))
    assert result.ok, f"migration failed:\n{result.describe()}"

    missing, extra = _except_all_differences(
        _source_records(frozen["rows"]), read_final_rows(lake)
    )
    assert missing == 0, f"{missing} source row(s) never reached the final inventory"
    assert extra == 0, f"{extra} published row(s) have no counterpart in the source"


def test_per_symbol_and_date_counts_match_the_source(frozen, lake):
    """MIGR-02: per-symbol/date counts agree with the source."""
    result = run_cli(_cli_args(frozen["db"], lake, mode="all"))
    assert result.ok, f"migration failed:\n{result.describe()}"

    def _key(row):
        stamp = row["timestamp"] if not isinstance(row["timestamp"], str) else row["timestamp"]
        return (str(stamp)[:10], row["symbol"])

    source_counts = Counter(_key(row) for row in _source_records(frozen["rows"]))
    final_counts = Counter(_key(row) for row in read_final_rows(lake))
    assert source_counts == final_counts, (
        "per-symbol/date counts disagree with the source: "
        f"{ {k: (source_counts[k], final_counts[k]) for k in source_counts if source_counts[k] != final_counts[k]} }"
    )


def test_reconciliation_reads_final_output_not_staging(frozen, lake):
    """MIGR-02: the reconciliation above is against ticks/, and staging is empty."""
    result = run_cli(_cli_args(frozen["db"], lake, mode="all"))
    assert result.ok, f"migration failed:\n{result.describe()}"

    staged = list((lake / "_migration").rglob("*.parquet"))
    assert staged == [], (
        f"staging still holds {len(staged)} parquet file(s); a reconciliation that "
        "read staging rather than ticks/ would be unverifiable here"
    )
    assert _final_files(lake), "no finalized files to reconcile against"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F11: MigrationOrchestrator.verify reconciles the run-owned staging inventory "
        "against the source, not the final published inventory. It therefore passes "
        "even when the published lake contains rows the source never had. Remove this "
        "marker when verify reconciles against ticks/."
    ),
)
def test_tool_verify_detects_published_rows_absent_from_the_source(frozen, lake):
    """MIGR-02 (gap): verify should catch phantom rows already in the lake."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all")).ok

    # Introduce published rows the source does not contain.
    writer = TickLakeWriter(root=lake, writer_id="phantom", max_batch_rows=10**9)
    writer.write_ticks(
        [
            QuoteTick(
                timestamp="2026-07-10T13:30:00",
                symbol="AAPL",
                price=1.0,
                volume=1.0,
                source="PHANTOM",
                session="REG",
                ingest_id="phantom_0001",
            )
        ]
    )
    writer.flush(block=True)
    writer.close()

    verify = run_cli(_cli_args(frozen["db"], lake, mode="verify", force=FLAG))
    # Desired: a non-zero exit, because the published inventory no longer matches.
    assert not verify.ok, "verify passed despite phantom published rows"


# ---------------------------------------------------------------- MIGR-03


def test_crash_during_export_resumes_in_a_fresh_process(frozen, lake):
    """MIGR-03: SIGKILL mid-export, then resume in a new process."""
    process = start_cli(_cli_args(frozen["db"], lake, mode="export"))
    try:
        appeared = wait_until(
            lambda: any((lake / "_migration").rglob("*.parquet")),
            timeout=120.0,
            interval=0.05,
        )
        assert appeared, "export produced no staged chunk to crash against"
        process.kill()
        process.wait(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=60)
        if process.stdout:
            process.stdout.close()

    # A fresh process resumes the interrupted export.
    resumed = run_cli(_cli_args(frozen["db"], lake, mode="export", resume=FLAG))
    assert resumed.ok, f"resumed export failed:\n{resumed.describe()}"

    assert run_cli(_cli_args(frozen["db"], lake, mode="verify")).ok
    assert run_cli(_cli_args(frozen["db"], lake, mode="publish")).ok

    missing, extra = _except_all_differences(
        _source_records(frozen["rows"]), read_final_rows(lake)
    )
    assert (missing, extra) == (0, 0), f"after crash and resume: missing={missing} extra={extra}"


@pytest.mark.parametrize(
    "fault,match",
    [
        # Data-file boundary: migration publishes with os.link.
        ("link", "parquet"),
        # Receipt boundary: written with os.replace.
        ("replace", "receipt"),
    ],
)
def test_crash_at_a_publish_boundary_resumes_in_a_fresh_process(frozen, lake, fault, match):
    """MIGR-03: a crash during publish leaves nothing lost or duplicated."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="export")).ok
    assert run_cli(_cli_args(frozen["db"], lake, mode="verify")).ok

    crashed = run_cli_with_fault(
        _cli_args(frozen["db"], lake, mode="publish"),
        fault=fault,
        failures=20,
        match=match,
    )
    assert not crashed.ok, f"the injected crash did not stop publish:\n{crashed.describe()}"

    retried = run_cli(_cli_args(frozen["db"], lake, mode="publish"))
    assert retried.ok, f"publish retry failed:\n{retried.describe()}"

    missing, extra = _except_all_differences(
        _source_records(frozen["rows"]), read_final_rows(lake)
    )
    assert (missing, extra) == (0, 0), (
        f"after a publish crash ({fault}/{match}): missing={missing} extra={extra}"
    )


def test_append_migration_preserves_prior_immutable_files(frozen, lake):
    """MIGR-03: appending a new date range leaves earlier files byte-identical."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all", date_start="2026-07-10", date_end="2026-07-10")).ok
    before = _digests(lake)
    assert before, "the first migration produced no files"

    assert run_cli(_cli_args(frozen["db"], lake, mode="all", date_start="2026-07-11", date_end="2026-07-11")).ok
    after = _digests(lake)

    changed = [name for name in before if name in after and before[name] != after[name]]
    assert changed == [], f"prior immutable files changed: {changed}"
    assert len(after) > len(before), "the append added no files"

    appended_rows = [
        row for row in read_final_rows(lake) if str(row["timestamp"]).startswith("2026-07-11")
    ]
    expected = len([r for r in frozen["rows"] if str(r[0]).startswith("2026-07-11")])
    assert len(appended_rows) == expected, (
        f"appended date has {len(appended_rows)} rows, source has {expected}"
    )


def test_repeating_a_migration_is_idempotent(frozen, lake):
    """MIGR-03: re-running the same migration neither duplicates nor rewrites."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all")).ok
    before = _digests(lake)
    count_before = len(read_final_rows(lake))

    repeat = run_cli(_cli_args(frozen["db"], lake, mode="all", force=FLAG))
    assert repeat.ok, f"repeat migration failed:\n{repeat.describe()}"

    after = _digests(lake)
    assert len(read_final_rows(lake)) == count_before, "repeating the migration changed the row count"
    assert set(after) == set(before), "repeating the migration published new files"
    changed = [name for name in before if before[name] != after[name]]
    assert changed == [], f"repeating the migration rewrote files: {changed}"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F10: re-running a migration with a different date filter after a completed "
        "migration treats the new plan as new work and duplicates every partition in "
        "that filter (measured: 51 -> 102 rows per symbol). The staging-based verify "
        "does not detect it. Remove this marker when migration guards on lake content "
        "rather than plan identity."
    ),
)
def test_rerunning_with_a_different_filter_does_not_duplicate(frozen, lake):
    """MIGR-03 (gap): a narrower re-run must not duplicate already-migrated data."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all")).ok
    before = len(read_final_rows(lake))

    rerun = run_cli(
        _cli_args(frozen["db"], lake, mode="all", date_start="2026-07-12", date_end="2026-07-12", force=FLAG)
    )
    assert rerun.ok, f"re-run failed:\n{rerun.describe()}"

    assert len(read_final_rows(lake)) == before, (
        "a narrower re-run duplicated already-migrated partitions"
    )


# ---------------------------------------------------------------- MIGR-05


def test_backup_restores_into_a_new_scratch_destination(frozen, lake, tmp_path):
    """MIGR-05: a retained backup restores, is queryable and reconciles."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all")).ok

    backup = tmp_path / "backup"
    shutil.copytree(lake, backup)

    restored = tmp_path / "restored"
    shutil.copytree(backup, restored)

    missing, extra = _except_all_differences(
        _source_records(frozen["rows"]), read_final_rows(restored)
    )
    assert (missing, extra) == (0, 0), f"restored copy mismatched: missing={missing} extra={extra}"

    reader = TickLakeReader(root=restored)
    candles = reader.query_candles("AAPL", "1d")
    assert candles, "the restored lake is not queryable"


def test_rollback_preserves_newly_written_live_data(frozen, lake):
    """MIGR-05: removing migrated files must not touch live data written after them."""
    assert run_cli(_cli_args(frozen["db"], lake, mode="all")).ok
    migrated_files = set(_final_files(lake))
    assert migrated_files

    # Live ingestion continues after the migration.
    writer = TickLakeWriter(root=lake, writer_id="live_after", max_batch_rows=10**9)
    writer.write_ticks(
        [
            QuoteTick(
                timestamp=f"2026-10-02T14:30:{index:02d}",
                symbol="AAPL",
                price=200.0 + index,
                volume=1.0,
                source="CAPITAL",
                session="REG",
                ingest_id=f"live_after_{index:03d}",
            )
            for index in range(5)
        ]
    )
    writer.flush(block=True)
    writer.close()

    live_files = set(_final_files(lake)) - migrated_files
    assert live_files, "the live write produced no files"

    # Rollback: remove exactly the files the migration published.
    for path in migrated_files:
        path.unlink()

    remaining = read_final_rows(lake)
    live_ids = {row["ingest_id"] for row in remaining if row["ingest_id"].startswith("live_after_")}
    assert len(live_ids) == 5, f"rollback destroyed live data: {len(live_ids)}/5 rows survived"

    migrated_rows = [row for row in remaining if not row["ingest_id"].startswith("live_after_")]
    assert migrated_rows == [], "rollback left migrated rows behind"

    reader = TickLakeReader(root=lake)
    assert reader.query_candles("AAPL", "1d"), "the lake is not queryable after rollback"
