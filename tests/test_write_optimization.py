"""
Tests for the Write Optimization & Sync Refactor.
Verifies all four issues are fixed:
  P0: Cache key is stable (no github.run_id)
  P1: Mirror schema init exists in sync_mirror.py
  P2: Delete-then-Insert replaced with INSERT OR IGNORE
  P3: No automatic index creation in schema.py
"""
import pytest
import os
from unittest.mock import MagicMock, patch, call
from libsql_client import create_client_sync

import src.database.schema as schema_module
import src.database.operations as ops_module


# =============================================================================
# P3: Verify index creation is removed from schema.py
# =============================================================================

class TestIndexRemoval:
    """P3: CREATE INDEX must not exist in application startup code."""

    def test_no_create_index_in_schema_source(self):
        """The schema.py source file must NOT contain CREATE INDEX."""
        with open("src/database/schema.py", "r") as f:
            content = f.read()
        assert "CREATE INDEX" not in content, (
            "CREATE INDEX found in schema.py — this causes massive write spikes "
            "on large tables and must be run manually via Turso CLI instead."
        )

    def test_init_db_does_not_execute_create_index(self):
        """init_db must not send any CREATE INDEX statement to the client."""
        mock_client = MagicMock()
        # Make PRAGMA table_info return a result that includes 'source'
        # so the migration branch is skipped
        mock_pragma = MagicMock()
        mock_pragma.rows = [
            (0, 'timestamp', 'TEXT', 1, None, 0),
            (1, 'symbol', 'TEXT', 1, None, 0),
            (2, 'open', 'REAL', 0, None, 0),
            (3, 'high', 'REAL', 0, None, 0),
            (4, 'low', 'REAL', 0, None, 0),
            (5, 'close', 'REAL', 0, None, 0),
            (6, 'volume', 'REAL', 0, None, 0),
            (7, 'session', 'TEXT', 0, None, 0),
            (8, 'source', 'TEXT', 0, None, 0),
        ]
        # SELECT count(*) returns > 0 so seeding is skipped
        mock_count = MagicMock()
        mock_count.rows = [(10,)]

        def side_effect(sql, *args, **kwargs):
            if "PRAGMA" in sql:
                return mock_pragma
            if "SELECT count" in sql:
                return mock_count
            return MagicMock()

        mock_client.execute.side_effect = side_effect

        schema_module.init_db(mock_client)

        # Check that no call contains "CREATE INDEX"
        for c in mock_client.execute.call_args_list:
            sql_arg = c[0][0] if c[0] else ""
            assert "CREATE INDEX" not in sql_arg, (
                f"init_db sent CREATE INDEX to the client: {sql_arg}"
            )

    def test_init_db_still_creates_tables(self):
        """init_db must still create the required tables."""
        mock_client = MagicMock()
        mock_count = MagicMock()
        mock_count.rows = [(10,)]
        mock_pragma = MagicMock()
        mock_pragma.rows = [(8, 'source', 'TEXT', 0, None, 0)]

        def side_effect(sql, *args, **kwargs):
            if "SELECT count" in sql:
                return mock_count
            if "PRAGMA" in sql:
                return mock_pragma
            return MagicMock()

        mock_client.execute.side_effect = side_effect
        schema_module.init_db(mock_client)

        all_sql = " ".join(
            c[0][0] for c in mock_client.execute.call_args_list if c[0]
        )
        assert "CREATE TABLE IF NOT EXISTS symbol_map" in all_sql
        assert "CREATE TABLE IF NOT EXISTS market_data" in all_sql


# =============================================================================
# P2: Verify _mirror_insert uses INSERT OR IGNORE
# =============================================================================

class TestMirrorInsert:
    """P2: _mirror_insert must use INSERT OR IGNORE, not ON CONFLICT DO UPDATE."""

    def test_mirror_insert_exists(self):
        """_mirror_insert function must exist in operations module."""
        assert hasattr(ops_module, '_mirror_insert'), (
            "_mirror_insert function not found in operations.py"
        )

    def test_mirror_insert_uses_ignore(self):
        """_mirror_insert must use INSERT OR IGNORE (0 writes for duplicates)."""
        mock_client = MagicMock()
        logger = MagicMock()

        rows = [
            ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
        ]

        ops_module._mirror_insert(mock_client, rows, logger, "TestMirror")

        # Verify the SQL uses INSERT OR IGNORE
        executed_sql = mock_client.execute.call_args_list[0][0][0]
        assert "INSERT OR IGNORE" in executed_sql, (
            f"_mirror_insert must use INSERT OR IGNORE, got: {executed_sql}"
        )
        assert "DO UPDATE" not in executed_sql, (
            "_mirror_insert must NOT use DO UPDATE — it wastes writes on existing rows"
        )

    def test_mirror_insert_returns_true_on_success(self):
        """_mirror_insert must return True on successful commit."""
        mock_client = MagicMock()
        rows = [
            ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
        ]
        result = ops_module._mirror_insert(mock_client, rows)
        assert result is True

    def test_mirror_insert_returns_false_on_empty(self):
        """_mirror_insert must return False if rows are empty."""
        mock_client = MagicMock()
        assert ops_module._mirror_insert(mock_client, []) is False
        assert ops_module._mirror_insert(None, [("row",)]) is False

    def test_mirror_insert_returns_false_on_error(self):
        """_mirror_insert must return False and not crash on DB error."""
        mock_client = MagicMock()
        mock_client.execute.side_effect = Exception("DB Error")
        rows = [
            ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
        ]
        result = ops_module._mirror_insert(mock_client, rows)
        assert result is False

    def test_mirror_insert_batching(self):
        """_mirror_insert must batch rows in groups of 100."""
        mock_client = MagicMock()
        # Create 250 rows
        rows = [
            (f"2025-01-02 {i:02d}:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE")
            for i in range(250)
        ]
        ops_module._mirror_insert(mock_client, rows)
        # 250 rows / 100 batch = 3 execute calls
        assert mock_client.execute.call_count == 3

    def test_save_to_client_still_uses_do_update(self):
        """_save_to_client must still use ON CONFLICT DO UPDATE for Archive writes."""
        mock_client = MagicMock()
        rows = [
            ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
        ]
        ops_module._save_to_client(mock_client, rows)
        executed_sql = mock_client.execute.call_args_list[0][0][0]
        assert "DO UPDATE" in executed_sql, (
            "_save_to_client must STILL use ON CONFLICT DO UPDATE for tier protection"
        )


# =============================================================================
# P2 + P1: Verify sync_mirror.py changes
# =============================================================================

class TestSyncMirrorRefactor:
    """P2: DELETE must be removed. P1: init_db must be called on mirror."""

    def test_no_delete_in_sync_mirror_source(self):
        """sync_mirror.py must NOT contain DELETE FROM market_data."""
        with open("tools/sync_mirror.py", "r") as f:
            content = f.read()
        assert "DELETE FROM market_data" not in content, (
            "DELETE FROM market_data found in sync_mirror.py — "
            "this causes massive write spikes via delete-then-insert."
        )

    def test_sync_mirror_imports_mirror_insert(self):
        """sync_mirror.py must import _mirror_insert, not _save_to_client."""
        with open("tools/sync_mirror.py", "r") as f:
            content = f.read()
        assert "_mirror_insert" in content, (
            "sync_mirror.py must import and use _mirror_insert"
        )
        assert "from src.database.operations import _save_to_client" not in content, (
            "sync_mirror.py must NOT import _save_to_client — use _mirror_insert instead"
        )

    def test_sync_mirror_calls_init_db(self):
        """sync_mirror.py must call init_db on the mirror for cold-start safety."""
        with open("tools/sync_mirror.py", "r") as f:
            content = f.read()
        assert "init_db" in content, (
            "sync_mirror.py must call init_db(mirror) to handle cold starts"
        )
        assert "from src.database.schema import init_db" in content, (
            "sync_mirror.py must import init_db from schema"
        )


# =============================================================================
# P0: Verify cache key fix
# =============================================================================

class TestCacheKeyFix:
    """P0: GitHub Actions cache must use a stable key."""

    def test_no_run_id_in_workflow(self):
        """sync_mirror.yml must NOT use github.run_id in cache key."""
        with open(".github/workflows/sync_mirror.yml", "r") as f:
            content = f.read()
        assert "github.run_id" not in content, (
            "github.run_id found in cache key — this creates a new cache entry "
            "every run, wasting ~626MB per run and breaking incremental sync."
        )

    def test_stable_cache_key_exists(self):
        """sync_mirror.yml must have a stable versioned cache key."""
        with open(".github/workflows/sync_mirror.yml", "r") as f:
            content = f.read()
        assert "turso-replicas-v1" in content, (
            "Cache key must be a stable string like 'turso-replicas-v1'"
        )


# =============================================================================
# Integration: Verify surgical sync works end-to-end with a real SQLite DB
# =============================================================================

class TestSurgicalSyncIntegration:
    """End-to-end test: INSERT OR IGNORE skips existing rows, inserts only new ones."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        self.db_path = "test_surgical_sync.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        yield
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _new_client(self):
        return create_client_sync(url=f"file:{self.db_path}")

    def test_insert_or_ignore_skips_duplicates(self):
        """INSERT OR IGNORE must insert new rows and skip existing ones."""
        client = self._new_client()

        try:
            # Create table
            schema_module.init_db(client)

            # Insert initial data
            initial_rows = [
                ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
                ("2025-01-02 11:00:00", "AAPL", 151.0, 152.0, 150.0, 151.5, 2000.0, "REG", "MASSIVE"),
            ]
            ops_module._mirror_insert(client, initial_rows)

            # Verify 2 rows
            res = client.execute("SELECT COUNT(*) FROM market_data")
            assert res.rows[0][0] == 2

            # Now insert 3 rows: 2 duplicates + 1 new
            mixed_rows = [
                ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),  # dup
                ("2025-01-02 11:00:00", "AAPL", 151.0, 152.0, 150.0, 151.5, 2000.0, "REG", "MASSIVE"),  # dup
                ("2025-01-02 12:00:00", "AAPL", 152.0, 153.0, 151.0, 152.5, 3000.0, "REG", "MASSIVE"),  # NEW
            ]
            ops_module._mirror_insert(client, mixed_rows)

            # Verify only 3 rows total (not 5!)
            res = client.execute("SELECT COUNT(*) FROM market_data")
            assert res.rows[0][0] == 3, (
                f"Expected 3 rows after INSERT OR IGNORE, got {res.rows[0][0]}. "
                "Duplicates should be silently skipped."
            )
        finally:
            client.close()

    def test_original_data_not_overwritten(self):
        """INSERT OR IGNORE must NOT overwrite existing row values."""
        client = self._new_client()

        try:
            schema_module.init_db(client)

            # Insert a row with source MASSIVE
            original = [
                ("2025-01-02 10:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE"),
            ]
            ops_module._mirror_insert(client, original)

            # Try to overwrite with different values + different source
            overwrite_attempt = [
                ("2025-01-02 10:00:00", "AAPL", 999.0, 999.0, 999.0, 999.0, 9999.0, "REG", "YAHOO"),
            ]
            ops_module._mirror_insert(client, overwrite_attempt)

            # Verify original values are preserved
            res = client.execute(
                "SELECT open, source FROM market_data WHERE symbol='AAPL' AND timestamp='2025-01-02 10:00:00'"
            )
            assert res.rows[0][0] == 150.0, "INSERT OR IGNORE should NOT overwrite existing data"
            assert res.rows[0][1] == "MASSIVE", "Source should remain MASSIVE, not YAHOO"
        finally:
            client.close()
