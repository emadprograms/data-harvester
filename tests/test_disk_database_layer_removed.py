"""Phase 46 STOR-05 — the disk-database layer is deleted, not disabled.

`src/database/{connection,schema,operations}.py` was the only way to open a
disk-backed DuckDB database. v5.0 keeps the `duckdb` *package* (in-memory query
engine) but deletes this layer, so no runtime path can recreate a `.duckdb` file.

The one permitted exception is `tools/migrate_streaming_to_parquet.py`, the
owner-run Phase 49 migration gate: it reads the legacy file before deletion.

The scanners work on the AST, not on raw text, so a test may still *name* a
deleted symbol to assert its absence (`hasattr(x, "…")`, `monkeypatch.setattr`
targets). Only real imports and real uses are reported.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SELF = "tests/test_disk_database_layer_removed.py"

DELETED_MODULES = (
    "src/database/__init__.py",
    "src/database/connection.py",
    "src/database/schema.py",
    "src/database/operations.py",
)

# The single permitted disk-database reader (migration gate, owner-run).
EXEMPT = {SELF, "tools/migrate_streaming_to_parquet.py"}

FORBIDDEN_USES = frozenset(
    {
        "DuckDBClient",
        "get_duckdb_connection",
        "get_historical_db_connection",
        "get_streaming_db_connection",
        "get_archive_db_connection",
        "get_unified_connection",
        "init_historical_db",
        "init_streaming_db",
        "init_db",
        "save_ticks_to_storage",
        "save_ticks_to_streaming_db",
        "DEFAULT_HISTORICAL_DB_PATH",
        "DEFAULT_STREAMING_DB_PATH",
        "DEFAULT_DB_PATH",
        "DEFAULT_DATA_DIR",
    }
)


def _python_files() -> list[Path]:
    files = list((REPO_ROOT / "src").rglob("*.py")) + list((REPO_ROOT / "tests").rglob("*.py"))
    return [
        path
        for path in files
        if str(path.relative_to(REPO_ROOT)).replace("\\", "/") not in EXEMPT
    ]


def test_disk_database_layer_is_deleted() -> None:
    survivors = [rel for rel in DELETED_MODULES if (REPO_ROOT / rel).exists()]
    assert not survivors, f"disk-database modules survived: {survivors}"


def test_no_module_imports_the_deleted_layer() -> None:
    offenders = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if module == "src.database" or module.startswith("src.database."):
                    offenders.append(f"{path.relative_to(REPO_ROOT)} -> {module}")
    assert not offenders, f"modules still import the deleted layer: {offenders}"


def test_no_module_uses_a_disk_database_symbol() -> None:
    offenders = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in FORBIDDEN_USES:
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: {node.id}")
            elif isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_USES:
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: .{node.attr}")
    assert not offenders, f"disk-database symbols are still used: {offenders}"


def test_duckdb_package_survives_as_the_query_engine() -> None:
    """Option A: storage is removed, the in-memory query engine is not."""
    import duckdb

    assert duckdb.__version__.startswith("1.")
    from src.storage import reader

    assert hasattr(reader, "TickLakeReader")
