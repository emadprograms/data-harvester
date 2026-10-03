"""T1 regression specifications for maintenance fences and backend routing."""
from datetime import datetime
import json
from pathlib import Path

import pytest

from src.storage.config import (
    LakeMaintenanceInProgressError,
    init_tick_lake,
)
from src.storage.parquet_writer import TickLakeWriter
import src.storage.publication as publication_module
from src.storage.publication import LakeOwnershipError, LakePublisher, recover_pending_publications
from src.storage.schema import QuoteTick
from src.stream.runner import StreamingEngine
from tests.support.migration_factory import create_source_db, quote_row
from tests.support.tree_snapshot import assert_tree_unchanged, snapshot_tree
from tools.migrate_streaming_to_parquet import MigrationConfig, MigrationOrchestrator


def _write_maintenance_marker(lake_root: Path):
    marker = lake_root / "_maintenance" / "in_progress.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"operation": "offline-compaction", "owner": "test"}), encoding="utf-8")
    return marker


def test_writer_startup_and_direct_publication_are_fenced_by_maintenance(tmp_path):
    lake_root = tmp_path / "maintenance-lake"
    init_tick_lake(lake_root)
    _write_maintenance_marker(lake_root)
    before = snapshot_tree(lake_root)

    writer = None
    try:
        with pytest.raises(LakeMaintenanceInProgressError):
            writer = TickLakeWriter(root=lake_root)
    finally:
        if writer is not None:
            writer.close()
    assert_tree_unchanged(lake_root, before)

    publisher = None
    try:
        with pytest.raises(LakeMaintenanceInProgressError):
            publisher = LakePublisher(root=lake_root, writer_id="maintenance-test")
    finally:
        if publisher is not None:
            publisher.close()
    assert_tree_unchanged(lake_root, before)


def test_pending_publication_recovery_is_fenced_by_maintenance(tmp_path):
    lake_root = tmp_path / "recovery-maintenance-lake"
    init_tick_lake(lake_root)
    _write_maintenance_marker(lake_root)
    before = snapshot_tree(lake_root)

    with pytest.raises(LakeMaintenanceInProgressError):
        recover_pending_publications(lake_root)
    assert_tree_unchanged(lake_root, before)


def test_migration_publication_is_fenced_by_maintenance(tmp_path):
    source_db = tmp_path / "source.duckdb"
    create_source_db(source_db, [quote_row(datetime(2026, 10, 2, 14, 30), "AAPL", 100.0)])
    lake_root = tmp_path / "migration-maintenance-lake"
    init_tick_lake(lake_root)
    migration = MigrationOrchestrator(MigrationConfig(source_db=source_db, lake_root=lake_root))
    migration.export()
    assert migration.verify().status == "PASSED"
    _write_maintenance_marker(lake_root)
    before = snapshot_tree(lake_root)

    with pytest.raises(LakeMaintenanceInProgressError):
        migration.publish()
    assert_tree_unchanged(lake_root, before)


def test_maintenance_lock_serializes_with_publisher_and_fences_new_publishers(tmp_path):
    lake_root = tmp_path / "maintenance-owner-lake"
    init_tick_lake(lake_root)
    maintenance_type = getattr(publication_module, "LakeMaintenanceLock", None)
    assert maintenance_type is not None, (
        "maintenance requires an exclusive ownership API shared with lake publishers"
    )

    writer_lock = publication_module.LakePublisherLock(lake_root, writer_id="active-writer")
    writer_lock.acquire()
    try:
        maintenance = maintenance_type(lake_root, operation="test-maintenance")
        with pytest.raises(LakeOwnershipError):
            maintenance.acquire()
        assert not (lake_root / "_maintenance" / "in_progress.json").exists()
    finally:
        writer_lock.release()

    with maintenance_type(lake_root, operation="test-maintenance"):
        assert (lake_root / "_maintenance" / "in_progress.json").is_file()
        with pytest.raises(LakeMaintenanceInProgressError):
            LakePublisher(root=lake_root, writer_id="racing-writer")
    assert not (lake_root / "_maintenance" / "in_progress.json").exists()


def test_explicit_legacy_backend_remains_available(tmp_path):
    legacy_db = tmp_path / "legacy" / "streaming.duckdb"
    engine = StreamingEngine(db_path=str(legacy_db))
    try:
        assert engine.writer is None
        assert engine.lake_root is None
        assert engine.db_path == str(legacy_db)
    finally:
        engine.stop()


def test_environment_selected_lake_failure_never_falls_back_to_streaming_duckdb(tmp_path, monkeypatch):
    lake_root = tmp_path / "configured-lake"
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "legacy-data"))

    import src.storage.parquet_writer as writer_module
    import src.stream.runner as runner_module

    def lake_failure(*args, **kwargs):
        raise RuntimeError("configured lake unavailable")

    legacy_attempted = []
    def forbidden_legacy(*args, **kwargs):
        legacy_attempted.append((args, kwargs))
        raise AssertionError("configured lake failure must not select streaming DuckDB")

    monkeypatch.setattr(writer_module, "TickLakeWriter", lake_failure)
    monkeypatch.setattr(runner_module, "get_streaming_db_connection", forbidden_legacy)

    with pytest.raises(RuntimeError, match="configured lake unavailable"):
        StreamingEngine()
    assert legacy_attempted == []


def test_dashboard_lake_error_does_not_fall_back_to_streaming_duckdb(tmp_path, monkeypatch):
    import src.dashboard.analytics as analytics
    import src.storage.reader as reader_module

    monkeypatch.setenv("TICK_LAKE_ROOT", str(tmp_path / "configured-lake"))
    legacy_attempted = []

    def corrupt_lake():
        raise ValueError("lake metadata is corrupt")

    def forbidden_legacy(*args, **kwargs):
        legacy_attempted.append((args, kwargs))
        raise AssertionError("dashboard must surface lake failure rather than query legacy storage")

    monkeypatch.setattr(reader_module, "get_tick_lake_reader", corrupt_lake)
    monkeypatch.setattr(analytics, "get_streaming_db_connection", forbidden_legacy)

    with pytest.raises(ValueError, match="lake metadata is corrupt"):
        analytics.get_streaming_candles("AAPL")
    assert legacy_attempted == []


def test_maintenance_and_publisher_ownership_cross_process_barriers(tmp_path):
    """The shared lake lock serializes real processes, not only same-process callers."""
    from tests.support.process_harness import BarrierProcess, hold_lake_publisher_lock

    lake_root = tmp_path / "cross-process-maintenance-lake"
    init_tick_lake(lake_root)
    with BarrierProcess(hold_lake_publisher_lock, args=(str(lake_root),)) as child:
        assert child.wait_ready(), "child publisher did not acquire its owner lock"
        maintenance = publication_module.LakeMaintenanceLock(
            lake_root, operation="cross-process-test"
        )
        with pytest.raises(LakeOwnershipError):
            maintenance.acquire()
        assert not (lake_root / "_maintenance" / "in_progress.json").exists()
        child.allow()
        assert child.wait_committed(), "child publisher did not pass its release barrier"
        assert child.join() == 0

    with publication_module.LakeMaintenanceLock(lake_root, operation="cross-process-test"):
        with pytest.raises(LakeMaintenanceInProgressError):
            publication_module.LakePublisherLock(lake_root, writer_id="blocked-publisher").acquire()
