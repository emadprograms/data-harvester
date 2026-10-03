"""T1 regression specifications for verified and immutable historical migration."""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage.config import init_tick_lake
from src.storage.publication import LakeOwnershipError, LakePublisherLock
from src.storage.registry import SymbolRegistry, init_registry
from tests.support.lake_assertions import (
    assert_ingest_ids_unique,
    finalized_parquet_files,
    read_final_rows,
    row_multiset,
)
from tests.support.migration_factory import create_source_db, quote_row
from tests.support.tree_snapshot import assert_tree_unchanged, snapshot_tree
import tools.migrate_streaming_to_parquet as migration_module
from tools.service_supervisor import ProcessSupervisor
from tools.migrate_streaming_to_parquet import MigrationConfig, MigrationOrchestrator


BASE_TS = datetime(2026, 10, 2, 14, 30, 0)


def _source(tmp_path: Path, name: str, price: float = 100.0, rows: int = 2) -> Path:
    data = [
        quote_row(
            BASE_TS + timedelta(seconds=i),
            "AAPL",
            price + i,
            volume=1.0,
            bid=price + i - 0.01,
            ask=price + i + 0.01,
        )
        for i in range(rows)
    ]
    return create_source_db(tmp_path / f"{name}.duckdb", data)


def _prepare_migration(source_db: Path, lake_root: Path, *, symbols=None):
    init_tick_lake(lake_root)
    config = MigrationConfig(
        source_db=source_db,
        lake_root=lake_root,
        chunk_size=10,
        symbols=symbols,
    )
    orchestrator = MigrationOrchestrator(config)
    orchestrator.export()
    result = orchestrator.verify()
    assert result.status == "PASSED"
    return orchestrator


@pytest.mark.parametrize("mutation", ["empty", "price", "extra_file", "schema"])
def test_publish_rejects_staging_mutated_after_successful_verification(tmp_path, mutation):
    source_db = _source(tmp_path, "source", rows=2)
    lake_root = tmp_path / "lake"
    orchestrator = _prepare_migration(source_db, lake_root)
    staged_files = sorted((lake_root / "_migration" / "staging").glob("ticks/**/*.parquet"))
    assert len(staged_files) == 1
    staged = staged_files[0]
    table = pq.read_table(staged)

    if mutation == "empty":
        pq.write_table(table.slice(0, 0), staged)
    elif mutation == "price":
        values = table.to_pydict()
        values["price"][0] += 500.0
        pq.write_table(pa.Table.from_pydict(values, schema=table.schema), staged)
    elif mutation == "extra_file":
        pq.write_table(table.slice(0, 1), staged.with_name("unverified-extra.parquet"))
    else:
        pq.write_table(pa.table({"not_the_lake_schema": [1]}), staged)

    with pytest.raises((RuntimeError, ValueError)):
        orchestrator.publish()
    assert finalized_parquet_files(lake_root) == []
    assert staged.exists() or mutation == "extra_file"


def test_publish_verification_is_bound_to_source_scope_and_migration(tmp_path):
    source_a = _source(tmp_path, "source-a", price=100.0)
    source_b = _source(tmp_path, "source-b", price=300.0)
    lake_root = tmp_path / "lake"
    _prepare_migration(source_a, lake_root, symbols=["AAPL"])

    # A passed report for source A/scope A must not authorize a different source or scope.
    changed = MigrationOrchestrator(MigrationConfig(
        source_db=source_b,
        lake_root=lake_root,
        symbols=["MSFT"],
    ))
    with pytest.raises((RuntimeError, ValueError)):
        changed.publish()
    assert finalized_parquet_files(lake_root) == []


def test_distinct_migrations_append_immutably_and_same_migration_is_idempotent(tmp_path):
    source_a = _source(tmp_path, "source-a", price=100.0, rows=3)
    source_b = _source(tmp_path, "source-b", price=300.0, rows=3)
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    first = MigrationOrchestrator(MigrationConfig(
        source_db=source_a, lake_root=lake_root, chunk_size=2
    ))
    assert first.run() == 0
    a_rows = read_final_rows(lake_root)
    a_files = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in finalized_parquet_files(lake_root)}

    second = MigrationOrchestrator(MigrationConfig(
        source_db=source_b, lake_root=lake_root, chunk_size=2
    ))
    assert second.run() == 0
    combined = read_final_rows(lake_root)
    assert len(combined) == 6
    assert sorted(row["price"] for row in combined) == [100.0, 101.0, 102.0, 300.0, 301.0, 302.0]
    assert_ingest_ids_unique(combined)
    assert row_multiset(a_rows) <= row_multiset(combined)
    for path, digest in a_files.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest

    # Repeating source A under its original automatic migration identity must not add copies.
    retry_a = MigrationOrchestrator(MigrationConfig(
        source_db=source_a, lake_root=lake_root, chunk_size=2, resume=True
    ))
    assert retry_a.run() == 0
    assert len(read_final_rows(lake_root)) == 6
    assert_ingest_ids_unique(read_final_rows(lake_root))


def test_resume_does_not_trust_corrupt_completed_chunk_checkpoint(tmp_path):
    source_db = _source(tmp_path, "source", rows=3)
    lake_root = tmp_path / "lake"
    first = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, chunk_size=1
    ))
    first.export()
    chunks = sorted((lake_root / "_migration" / "staging").glob("ticks/**/*.parquet"))
    assert len(chunks) == 3
    chunks[0].write_bytes(b"corrupt completed checkpoint")

    resumed = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, chunk_size=1, resume=True
    ))
    try:
        resumed.export()
    except (RuntimeError, ValueError):
        return
    assert resumed.verify().status == "PASSED", (
        "resume skipped a corrupt completed chunk instead of rebuilding and re-verifying it"
    )
    assert all(pq.ParquetFile(path).metadata.num_rows == 1 for path in chunks)


def test_resume_rejects_different_source_or_scope(tmp_path):
    source_a = _source(tmp_path, "source-a", price=100.0)
    source_b = _source(tmp_path, "source-b", price=200.0)
    lake_root = tmp_path / "lake"
    first = MigrationOrchestrator(MigrationConfig(source_db=source_a, lake_root=lake_root))
    first.export()

    changed = MigrationOrchestrator(MigrationConfig(
        source_db=source_b, lake_root=lake_root, resume=True
    ))
    with pytest.raises((RuntimeError, ValueError)):
        changed.export()


def test_publish_ownership_blocks_competing_live_writer(tmp_path):
    """Migration must not bypass the normal publisher ownership lock."""
    source_db = _source(tmp_path, "source")
    lake_root = tmp_path / "lake"
    orchestrator = _prepare_migration(source_db, lake_root)
    with LakePublisherLock(lake_root, writer_id="live-writer"):
        with pytest.raises(LakeOwnershipError):
            orchestrator.publish()
    assert finalized_parquet_files(lake_root) == []


@pytest.mark.parametrize("dry_mode", ["plan", "export", "verify", "publish", "all"])
def test_each_dry_run_mode_preserves_existing_tree_byte_for_byte(tmp_path, dry_mode):
    source_db = _source(tmp_path, "source", rows=3)
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)
    initial = MigrationOrchestrator(MigrationConfig(source_db=source_db, lake_root=lake_root, chunk_size=2))
    assert initial.run() == 0
    report = lake_root / "_migration" / "verification.json"
    assert report.is_file() and json.loads(report.read_text())["status"] == "PASSED"
    before = snapshot_tree(lake_root)

    dry = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, chunk_size=2, dry_run=True, mode=dry_mode
    ))
    assert dry.run() == 0
    assert_tree_unchanged(lake_root, before)


def test_dry_run_does_not_create_a_nonexistent_destination(tmp_path):
    source_db = _source(tmp_path, "source")
    lake_root = tmp_path / "must-not-exist"
    assert not lake_root.exists()
    dry = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, dry_run=True, mode="all"
    ))
    assert dry.run() == 0
    assert not lake_root.exists()


def test_writer_handoff_contract_preserves_pre_and_post_cutover_rows(tmp_path):
    """A bounded handoff releases/reacquires ownership and preserves live/history rows."""
    source_db = _source(tmp_path, "history", price=50.0, rows=2)
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    from src.storage.parquet_writer import TickLakeWriter
    live_before = TickLakeWriter(root=lake_root, writer_id="live-before")
    live_before.write_tick({
        "timestamp": BASE_TS, "symbol": "AAPL", "price": 10.0, "volume": 1.0,
        "source": "CAPITAL", "session": "REG",
    })
    live_before.flush()
    live_before.close()

    migration = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, chunk_size=1
    ))
    migration.export()
    assert migration.verify().status == "PASSED"
    migration.publish()

    live_after = TickLakeWriter(root=lake_root, writer_id="live-after")
    try:
        live_after.write_tick({
            "timestamp": BASE_TS + timedelta(seconds=10), "symbol": "AAPL", "price": 20.0,
            "volume": 1.0, "source": "CAPITAL", "session": "REG",
        })
        live_after.flush()
    finally:
        live_after.close()

    rows = read_final_rows(lake_root)
    assert sorted(row["price"] for row in rows) == [10.0, 20.0, 50.0, 51.0]
    assert_ingest_ids_unique(rows)


def _wait_for_status(status_file: Path, predicate, timeout: float = 12.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            payload = json.loads(status_file.read_text(encoding="utf-8"))
            if predicate(payload):
                return payload
        except (OSError, json.JSONDecodeError):
            pass
        time.sleep(0.02)
    raise AssertionError(f"writer status did not satisfy readiness predicate: {status_file}")


def test_real_supervised_runner_cutover_drains_publishes_and_restarts(tmp_path, isolated_subprocess_env):
    """The real runner process is stopped, history is published under exclusive ownership, then capture resumes."""
    source_db = _source(tmp_path, "history", price=50.0, rows=2)
    lake_root = tmp_path / "supervised-handoff-lake"
    init_tick_lake(lake_root)
    init_registry(lake_root)
    SymbolRegistry(root=lake_root).add_symbol("AAPL", capital_ticker="AAPL")

    env = dict(isolated_subprocess_env)
    env["TICK_LAKE_ROOT"] = str(lake_root)
    supervisor = ProcessSupervisor(
        name="audit-handoff-streamer",
        module="src.stream.runner",
        module_args=[
            "--lake-root", str(lake_root), "--writer-id", "writer_1", "--mock",
            "--ticks-per-sec", "50", "--flush-interval", "0.1", "--max-batch-rows", "25",
        ],
        extra_env=env,
        log_dir=tmp_path / "logs",
        handle_signals=False,
    )
    status_file = lake_root / "_control" / "writer_status.json"
    supervisor._start_child()
    try:
        _wait_for_status(
            status_file,
            lambda status: status.get("status") == "RUNNING" and status.get("total_rows_written", 0) > 0,
        )
        before_rows = read_final_rows(lake_root)
        assert before_rows

        migration = MigrationOrchestrator(MigrationConfig(
            source_db=source_db, lake_root=lake_root, chunk_size=1
        ))
        migration.export()
        assert migration.verify().status == "PASSED"
        with pytest.raises(LakeOwnershipError):
            migration.publish()

        coordinator_type = getattr(migration_module, "MigrationHandoffCoordinator", None)
        assert coordinator_type is not None, (
            "historical cutover must use a supervised, persisted handoff protocol; "
            "manual stop/start is not an acceptance implementation"
        )
        coordinator = coordinator_type(supervisor, migration, drain_timeout=10.0)
        coordinator.execute()

        handoff_state = lake_root / "_migration" / "handoff.json"
        assert handoff_state.is_file()
        assert json.loads(handoff_state.read_text(encoding="utf-8"))["phase"] == "COMPLETE"
        _wait_for_status(
            status_file,
            lambda status: status.get("status") == "RUNNING" and status.get("total_rows_written", 0) > 0,
        )
        supervisor._stop_child(timeout=10.0)

        all_rows = read_final_rows(lake_root)
        assert row_multiset(before_rows) <= row_multiset(all_rows)
        assert sorted(row["price"] for row in all_rows if row["price"] in {50.0, 51.0}) == [50.0, 51.0]
        assert len(all_rows) >= len(before_rows) + 3
        assert_ingest_ids_unique(all_rows)
    finally:
        supervisor._stop_child(timeout=10.0)


def test_publish_holds_migration_ownership_during_promotion(tmp_path, monkeypatch):
    """A cooperating exporter cannot mutate the verified tree inside the promotion window."""
    source_db = _source(tmp_path, "locked-source", rows=2)
    lake_root = tmp_path / "locked-migration-lake"
    migration = _prepare_migration(source_db, lake_root)
    competing = MigrationOrchestrator(MigrationConfig(
        source_db=source_db, lake_root=lake_root, chunk_size=1
    ))

    original_link = migration_module.os.link
    attempted = []

    def try_mutation_during_promotion(src, dst, *args, **kwargs):
        if not attempted:
            attempted.append(True)
            with pytest.raises(RuntimeError):
                competing.export()
        return original_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(migration_module.os, "link", try_mutation_during_promotion)
    receipts = migration.publish()
    assert len(receipts) == 1
    assert attempted == [True]
    assert len(read_final_rows(lake_root)) == 2


def test_supervised_handoff_persists_publish_failure_and_restarts_capture(
    tmp_path, isolated_subprocess_env, monkeypatch
):
    """A failed historical publish is recorded and never leaves the managed writer down."""
    source_db = _source(tmp_path, "handoff-failure", price=50.0, rows=2)
    lake_root = tmp_path / "handoff-failure-lake"
    init_tick_lake(lake_root)
    init_registry(lake_root)
    SymbolRegistry(root=lake_root).add_symbol("AAPL", capital_ticker="AAPL")

    env = dict(isolated_subprocess_env)
    env["TICK_LAKE_ROOT"] = str(lake_root)
    supervisor = ProcessSupervisor(
        name="audit-handoff-failure",
        module="src.stream.runner",
        module_args=[
            "--lake-root", str(lake_root), "--writer-id", "writer_1", "--mock",
            "--ticks-per-sec", "50", "--flush-interval", "0.1", "--max-batch-rows", "25",
        ],
        extra_env=env,
        log_dir=tmp_path / "failure-logs",
        handle_signals=False,
    )
    status_file = lake_root / "_control" / "writer_status.json"
    supervisor._start_child()
    try:
        initial = _wait_for_status(
            status_file,
            lambda status: status.get("status") == "RUNNING" and status.get("total_rows_written", 0) > 0,
        )
        old_pid = initial["pid"]
        migration = MigrationOrchestrator(MigrationConfig(
            source_db=source_db, lake_root=lake_root, chunk_size=1
        ))
        migration.export()
        assert migration.verify().status == "PASSED"

        def fail_publish():
            raise OSError("injected history promotion failure")

        monkeypatch.setattr(migration, "publish", fail_publish)
        coordinator_type = getattr(migration_module, "MigrationHandoffCoordinator", None)
        assert coordinator_type is not None, "supervised handoff coordinator is required"
        coordinator = coordinator_type(supervisor, migration, drain_timeout=10.0)
        with pytest.raises(OSError, match="injected history promotion failure"):
            coordinator.execute()

        state = json.loads((lake_root / "_migration" / "handoff.json").read_text(encoding="utf-8"))
        assert state["phase"] == "FAILED"
        assert state["failed_phase"] == "PUBLISHING"
        restarted = _wait_for_status(
            status_file,
            lambda status: (
                status.get("status") == "RUNNING"
                and status.get("pid") != old_pid
                and status.get("total_rows_written", 0) > 0
            ),
        )
        assert restarted["pid"] != old_pid
    finally:
        supervisor._stop_child(timeout=10.0)
