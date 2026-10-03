"""
Unit and integration tests for tick lake storage configuration, root resolution,
and metadata lifecycle (Milestone v4.0 - Phase 16).
"""
import json
import os
from pathlib import Path
import pytest

from src.storage.config import (
    StorageConfigError,
    LakeNotFoundError,
    LakeMaintenanceInProgressError,
    IncompatibleSchemaError,
    LakeMetadata,
    TICK_LAKE_ROOT_ENV,
    DATA_DIR_ENV,
    MICRON_DATA_DIR,
    DEFAULT_LAKE_SUBDIR,
    LAKE_METADATA_FILENAME,
    MAINTENANCE_GUARD_FILENAME,
    SUBDIRECTORIES,
    resolve_tick_lake_root,
    init_tick_lake,
    load_lake_metadata,
)


def test_root_resolution_precedence(tmp_path, monkeypatch):
    """
    Verify root resolution precedence:
    custom_root > TICK_LAKE_ROOT > DATA_DIR > MICRON_DATA_DIR > error.
    """
    custom_dir = tmp_path / "custom_lake"
    custom_dir.mkdir()
    env_root_dir = tmp_path / "env_tick_lake"
    env_root_dir.mkdir()
    data_dir = tmp_path / "data_parent"
    data_dir.mkdir()

    # 1. custom_root takes highest precedence even when env vars are set
    monkeypatch.setenv(TICK_LAKE_ROOT_ENV, str(env_root_dir))
    monkeypatch.setenv(DATA_DIR_ENV, str(data_dir))
    resolved = resolve_tick_lake_root(custom_root=custom_dir)
    assert resolved == custom_dir.resolve()

    # 2. TICK_LAKE_ROOT takes precedence over DATA_DIR when custom_root is None
    resolved = resolve_tick_lake_root(custom_root=None)
    assert resolved == env_root_dir.resolve()

    # 3. DATA_DIR takes precedence when custom_root and TICK_LAKE_ROOT are None
    monkeypatch.delenv(TICK_LAKE_ROOT_ENV, raising=False)
    resolved = resolve_tick_lake_root(custom_root=None)
    assert resolved == (data_dir / DEFAULT_LAKE_SUBDIR).resolve()

    # 4. Hardware volume MICRON_DATA_DIR is used when env vars are absent and volume exists
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)

    fake_micron = tmp_path / "fake_micron"
    fake_micron.mkdir()
    monkeypatch.setattr("src.storage.config.MICRON_DATA_DIR", str(fake_micron))

    resolved = resolve_tick_lake_root(custom_root=None)
    assert resolved == (fake_micron / DEFAULT_LAKE_SUBDIR).resolve()

    # 5. When no root can be resolved, raise StorageConfigError
    non_existent = tmp_path / "non_existent_micron"
    monkeypatch.setattr("src.storage.config.MICRON_DATA_DIR", str(non_existent))

    # Also ensure repo data check does not find a fallback
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)
    with pytest.raises(StorageConfigError):
        resolve_tick_lake_root(custom_root=None)


def test_root_resolution_broken_symlink(tmp_path, monkeypatch):
    """
    Verify that a broken symlink for storage raises StorageConfigError
    and refuses to silently fallback to creating an arbitrary empty lake.
    """
    monkeypatch.delenv(TICK_LAKE_ROOT_ENV, raising=False)
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)
    monkeypatch.setattr("src.storage.config.MICRON_DATA_DIR", "/nonexistent/drive/data")

    broken_link = tmp_path / "broken_data"
    target_path = tmp_path / "missing_target"
    broken_link.symlink_to(target_path)

    # Mock checking repo root data directory to hit this broken symlink
    monkeypatch.setattr("pathlib.Path.is_symlink", lambda self: True)
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)

    with pytest.raises(StorageConfigError) as exc_info:
        resolve_tick_lake_root(custom_root=None)

    err_msg = str(exc_info.value).lower()
    assert "broken" in err_msg or "mount missing" in err_msg or "missing" in err_msg


def test_init_tick_lake_creates_hierarchy(tmp_path):
    """
    Verify init_tick_lake creates all standard subdirectories and lake.json.
    """
    lake_root = tmp_path / "tick_lake"
    lake_root.mkdir()

    meta = init_tick_lake(lake_root)

    # Verify all expected subdirectories exist
    for subdir in SUBDIRECTORIES:
        expected_dir = lake_root / subdir
        assert expected_dir.is_dir(), f"Expected subdirectory {subdir} does not exist"

    # Verify lake.json exists and contains correct metadata
    lake_json_path = lake_root / LAKE_METADATA_FILENAME
    assert lake_json_path.is_file(), "lake.json file was not created"

    with open(lake_json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    assert payload["format"] == "tick_lake"
    assert payload["schema_version"] == 1
    assert "lake_id" in payload and len(payload["lake_id"]) > 0
    assert "created_at" in payload
    assert payload["partition_layout"] == "ticks/symbol={symbol}/date={date}"
    assert payload["compression"] == "snappy"
    assert payload["ordering"] == ["timestamp", "ingest_id"]
    assert 1 in payload["compatible_versions"]

    # Verify returned LakeMetadata dataclass matches JSON payload
    assert isinstance(meta, LakeMetadata)
    assert meta.lake_id == payload["lake_id"]
    assert meta.schema_version == payload["schema_version"]
    assert meta.format == payload["format"]


def test_init_tick_lake_idempotent(tmp_path):
    """
    Verify calling init_tick_lake multiple times preserves lake_id and existing metadata.
    """
    lake_root = tmp_path / "tick_lake"
    lake_root.mkdir()

    meta1 = init_tick_lake(lake_root)
    meta2 = init_tick_lake(lake_root)

    assert meta1.lake_id == meta2.lake_id
    assert meta1.created_at == meta2.created_at

    with open(lake_root / LAKE_METADATA_FILENAME, "r", encoding="utf-8") as f:
        payload = json.load(f)
    assert payload["lake_id"] == meta1.lake_id


def test_load_lake_metadata_readonly(tmp_path):
    """
    Verify load_lake_metadata is strictly read-only and causes no side effects.
    """
    lake_root = tmp_path / "tick_lake"
    lake_root.mkdir()
    created_meta = init_tick_lake(lake_root)

    # Snapshot directory state (names and mtimes)
    snapshot_before = {
        p.relative_to(lake_root): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in lake_root.rglob("*")
    }

    loaded_meta = load_lake_metadata(lake_root)

    snapshot_after = {
        p.relative_to(lake_root): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in lake_root.rglob("*")
    }

    assert loaded_meta.lake_id == created_meta.lake_id
    assert loaded_meta.schema_version == created_meta.schema_version
    assert snapshot_before == snapshot_after, "load_lake_metadata modified the filesystem"


def test_load_lake_metadata_missing_raises(tmp_path):
    """
    Verify load_lake_metadata raises LakeNotFoundError if lake.json does not exist.
    """
    empty_lake_dir = tmp_path / "empty_lake"
    empty_lake_dir.mkdir()

    with pytest.raises(LakeNotFoundError):
        load_lake_metadata(empty_lake_dir)


def test_maintenance_guard_blocks(tmp_path):
    """
    Verify presence of _maintenance/in_progress.json raises LakeMaintenanceInProgressError.
    """
    lake_root = tmp_path / "tick_lake"
    lake_root.mkdir()
    init_tick_lake(lake_root)

    maintenance_guard = lake_root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
    maintenance_guard.write_text(
        json.dumps({"operation": "compaction", "started_at": "2026-10-03T08:00:00Z"}),
        encoding="utf-8",
    )

    # With check_maintenance=True (default), it must raise LakeMaintenanceInProgressError
    with pytest.raises(LakeMaintenanceInProgressError):
        load_lake_metadata(lake_root, check_maintenance=True)

    # With check_maintenance=False, it should succeed without raising
    meta = load_lake_metadata(lake_root, check_maintenance=False)
    assert meta.format == "tick_lake"
