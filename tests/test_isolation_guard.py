"""
Tests for test isolation guards in conftest.py.
Verifies that:
1. Attempting duckdb.connect on a production path (repo data/ or /Volumes/Micron-E 0256 A/...)
   raises ProductionAccessBlockedError.
2. Attempting external socket connection without @pytest.mark.live raises network violation error.
3. The resolved tick-lake root is redirected to a safe temporary directory, never to the
   repo data/ directory or the Micron production volume.
"""
import os
import socket
import tempfile
from pathlib import Path
import pytest
import duckdb

from tests.conftest import (
    ProductionAccessBlockedError,
    NetworkBlockedError,
    is_protected_path,
)
from src.storage.config import MICRON_DATA_DIR


class TestProductionPathGuard:
    """Verifies that automated tests cannot access or mutate production lake paths."""

    @pytest.mark.parametrize(
        "blocked_path",
        [
            "data/streaming.duckdb",
            "data/historical.duckdb",
            "data/market_data.duckdb",
            "data/tick_lake",
            os.path.join(MICRON_DATA_DIR, "tick_lake"),
            os.path.abspath("data/streaming.duckdb"),
            os.path.abspath("data/historical.duckdb"),
            MICRON_DATA_DIR,
            os.path.join(MICRON_DATA_DIR, "streaming.duckdb"),
            os.path.join(MICRON_DATA_DIR, "historical.duckdb"),
            "/Volumes/Micron-E 0256 A/data-harvester/data",
            "/Volumes/Micron-E 0256 A/data-harvester/data/streaming.duckdb",
            "/Volumes/Micron-E 0256 A/data-harvester/data/historical.duckdb",
        ],
    )
    def test_duckdb_connect_to_production_path_blocked(self, blocked_path):
        """Attempting to open any production path must raise ProductionAccessBlockedError."""
        with pytest.raises(ProductionAccessBlockedError):
            duckdb.connect(blocked_path)

    def test_duckdb_connect_to_tmp_path_allowed(self, tmp_path):
        """Connections to tmp_path files must succeed without restriction."""
        safe_db = str(tmp_path / "safe_test.duckdb")
        conn = duckdb.connect(safe_db)
        try:
            conn.execute("CREATE TABLE test (id INT)")
            res = conn.execute("SELECT 1").fetchall()
            assert res[0][0] == 1
        finally:
            conn.close()

    def test_duckdb_connect_in_memory_allowed(self):
        """Connections to in-memory databases (:memory:) must succeed without restriction."""
        conn = duckdb.connect(":memory:")
        try:
            res = conn.execute("SELECT 42").fetchall()
            assert res[0][0] == 42
        finally:
            conn.close()


class TestFilesystemWriteIsolationGuard:
    """Writes and parquet output must be blocked for a fake protected root."""

    def test_registered_fake_protected_root_blocks_mutations(self, protected_temp_dir, tmp_path):
        protected_file = protected_temp_dir / "nested" / "data.bin"
        assert is_protected_path(protected_file)

        with pytest.raises(ProductionAccessBlockedError):
            protected_temp_dir.mkdir()
        with pytest.raises(ProductionAccessBlockedError):
            open(protected_file, "wb")
        with pytest.raises(ProductionAccessBlockedError):
            protected_file.write_bytes(b"must not be written")
        with pytest.raises(ProductionAccessBlockedError):
            duckdb.connect(str(protected_temp_dir / "protected.duckdb"))

        import pyarrow as pa
        import pyarrow.parquet as pq
        with pytest.raises(ProductionAccessBlockedError):
            pq.write_table(pa.table({"value": [1]}), protected_file)

        # The guard is scoped: ordinary per-test temporary files remain writable.
        safe_file = tmp_path / "safe.bin"
        safe_file.write_bytes(b"allowed")
        assert safe_file.read_bytes() == b"allowed"
        pq.write_table(pa.table({"value": [1]}), tmp_path / "safe.parquet")

    def test_test_storage_environment_is_set_before_application_configuration(self):
        from src.storage.config import resolve_tick_lake_root

        root = resolve_tick_lake_root()
        assert root == Path(os.environ["TICK_LAKE_ROOT"]).resolve()
        temp_root = Path(tempfile.gettempdir()).resolve()
        assert root.is_relative_to(temp_root)
        assert Path(os.environ["DATA_DIR"]).resolve().is_relative_to(temp_root)

    def test_symlink_alias_to_protected_root_blocks_mutations(self, protected_temp_dir, tmp_path):
        symlink_dir = tmp_path / "symlink_protected"
        symlink_dir.symlink_to(protected_temp_dir, target_is_directory=True)
        symlink_file = symlink_dir / "target.bin"

        assert is_protected_path(symlink_dir)
        assert is_protected_path(symlink_file)

        with pytest.raises(ProductionAccessBlockedError):
            open(symlink_file, "wb")
        with pytest.raises(ProductionAccessBlockedError):
            symlink_file.write_bytes(b"should fail")

    def test_session_run_dir_is_outside_tracked_artifacts(self):
        from src.utils.write_guard import get_run_artifacts_dir
        run_dir = get_run_artifacts_dir()
        repo_root = Path(__file__).resolve().parents[1]
        tracked_artifacts = (repo_root / ".planning" / "artifacts").resolve()
        assert run_dir.resolve() != tracked_artifacts
        assert not run_dir.resolve().is_relative_to(tracked_artifacts)


class TestNetworkIsolationGuard:
    """Verifies that tests without @pytest.mark.live cannot open external network sockets."""

    def test_external_socket_connection_blocked_without_live_marker(self):
        """Attempting external network connection in a normal test raises NetworkBlockedError."""
        with pytest.raises(NetworkBlockedError):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(0.5)
                # Attempt connection to public DNS
                sock.connect(("8.8.8.8", 53))
            finally:
                sock.close()

    def test_external_socket_create_connection_blocked_without_live_marker(self):
        """Attempting socket.create_connection in a normal test raises NetworkBlockedError."""
        with pytest.raises(NetworkBlockedError):
            socket.create_connection(("1.1.1.1", 53), timeout=0.5)

    def test_local_loopback_connection_allowed(self):
        """Loopback (127.0.0.1 / localhost) connections are allowed for mock servers and IPC."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_sock.connect(("127.0.0.1", port))
            conn, _ = server_sock.accept()
            conn.close()
        except NetworkBlockedError:
            pytest.fail("NetworkBlockedError was raised on a local loopback (127.0.0.1) connection")
        finally:
            client_sock.close()
            server_sock.close()

    @pytest.mark.live
    def test_external_socket_connection_allowed_with_live_marker(self):
        """Tests explicitly marked @pytest.mark.live are permitted to make external calls."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(("1.1.1.1", 53))
            sock.close()
        except NetworkBlockedError:
            pytest.fail("NetworkBlockedError raised on a test marked with @pytest.mark.live")
        except (OSError, socket.timeout):
            # Normal network timeout or offline environment is acceptable, only NetworkBlockedError is forbidden
            pass


class TestDefaultPathsRedirection:
    """Verifies the resolved tick-lake root is isolated away from production."""

    def test_resolved_lake_root_not_pointing_to_production(self):
        """resolve_tick_lake_root must not resolve to the Micron volume or the repo data directory."""
        from src.storage.config import resolve_tick_lake_root

        resolved = os.path.realpath(str(resolve_tick_lake_root()))
        assert "/Volumes/Micron-E 0256 A" not in resolved, (
            f"tick lake root resolves to the production external volume: {resolved}"
        )
        repo_data_real = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
        assert not resolved.startswith(repo_data_real + os.sep), (
            f"tick lake root points directly at the repo data directory: {resolved}"
        )
