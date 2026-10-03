import os
import sys
import shutil
import tempfile
import socket
import ipaddress
from datetime import datetime, timedelta, date, timezone
import pytest
import duckdb

from src.config import US_EASTERN, UTC


class ProductionAccessBlockedError(PermissionError):
    """Raised when test code attempts to access production database files or directories."""
    pass


class NetworkBlockedError(RuntimeError):
    """Raised when non-live test code attempts external socket connections."""
    pass


# ============================================================================
# SESSION-LEVEL PATH REDIRECTION
# ============================================================================

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO_DATA_DIR = os.path.join(REPO_ROOT, "data")
REPO_DATA_REAL = os.path.realpath(REPO_DATA_DIR)
MICRON_DATA_DIR = "/Volumes/Micron-E 0256 A/data-harvester/data"

_SESSION_TEMP_DIR = tempfile.mkdtemp(prefix="pytest_data_harvest_")
os.environ["DATA_DIR"] = _SESSION_TEMP_DIR


def _patch_all_modules():
    import src.database.connection as conn_mod
    conn_mod.DEFAULT_DATA_DIR = _SESSION_TEMP_DIR
    conn_mod.DEFAULT_HISTORICAL_DB_PATH = os.path.join(_SESSION_TEMP_DIR, "historical.duckdb")
    conn_mod.DEFAULT_STREAMING_DB_PATH = os.path.join(_SESSION_TEMP_DIR, "streaming.duckdb")
    conn_mod.LEGACY_MARKET_DATA_PATH = os.path.join(_SESSION_TEMP_DIR, "market_data.duckdb")
    conn_mod.DEFAULT_DB_PATH = conn_mod.DEFAULT_HISTORICAL_DB_PATH

    for mod_name in (
        "src.dashboard.server",
        "src.dashboard.analytics",
        "src.stream.runner",
        "src.utils.integrity",
    ):
        if mod_name in sys.modules:
            mod = sys.modules[mod_name]
            if hasattr(mod, "DEFAULT_DATA_DIR"):
                setattr(mod, "DEFAULT_DATA_DIR", _SESSION_TEMP_DIR)
            if hasattr(mod, "DEFAULT_HISTORICAL_DB_PATH"):
                setattr(mod, "DEFAULT_HISTORICAL_DB_PATH", os.path.join(_SESSION_TEMP_DIR, "historical.duckdb"))
            if hasattr(mod, "DEFAULT_STREAMING_DB_PATH"):
                setattr(mod, "DEFAULT_STREAMING_DB_PATH", os.path.join(_SESSION_TEMP_DIR, "streaming.duckdb"))
            if hasattr(mod, "RELOAD_SIGNAL_FILE"):
                setattr(mod, "RELOAD_SIGNAL_FILE", os.path.join(_SESSION_TEMP_DIR, ".stream_reload.signal"))


_patch_all_modules()

# Initialize session isolated databases with schema tables
from src.database.schema import init_historical_db, init_streaming_db
init_historical_db()
init_streaming_db()


# ============================================================================
# PRODUCTION PATH GUARD
# ============================================================================

def is_production_path(path) -> bool:
    if path is None:
        return False
    path_str = str(path).strip()
    if not path_str or path_str == ":memory:":
        return False

    # Check for direct relative paths to repo data
    norm = os.path.normpath(path_str)
    if norm == "data" or norm.startswith("data" + os.sep) or norm.startswith("data/"):
        return True
    if norm == "." + os.sep + "data" or norm.startswith("." + os.sep + "data" + os.sep):
        return True

    # Check Micron production volume path strings
    if "/Volumes/Micron-E" in path_str:
        return True

    # Check absolute path
    abs_path = os.path.abspath(path_str)
    if (
        abs_path == REPO_DATA_DIR
        or abs_path.startswith(REPO_DATA_DIR + os.sep)
        or abs_path.startswith(REPO_DATA_DIR + "/")
    ):
        return True
    if "/Volumes/Micron-E" in abs_path:
        return True

    # Check realpath (resolving symlinks)
    real_path = os.path.realpath(path_str)
    if "/Volumes/Micron-E" in real_path:
        return True
    if (
        real_path == REPO_DATA_REAL
        or real_path.startswith(REPO_DATA_REAL + os.sep)
        or real_path.startswith(REPO_DATA_REAL + "/")
    ):
        return True

    return False


_original_duckdb_connect = duckdb.connect


def guarded_duckdb_connect(*args, **kwargs):
    db_target = ":memory:"
    if args:
        db_target = args[0]
    elif "database" in kwargs:
        db_target = kwargs["database"]

    if is_production_path(db_target):
        raise ProductionAccessBlockedError(
            f"Blocked access to production database path: {db_target}"
        )
    return _original_duckdb_connect(*args, **kwargs)


duckdb.connect = guarded_duckdb_connect

from src.database.connection import DuckDBClient

_original_duckdb_client_attach = DuckDBClient.attach


def guarded_duckdb_client_attach(self, target_db_path: str, alias: str, read_only: bool = True):
    if is_production_path(target_db_path):
        raise ProductionAccessBlockedError(
            f"Blocked attach to production database path: {target_db_path}"
        )
    return _original_duckdb_client_attach(self, target_db_path, alias, read_only=read_only)


DuckDBClient.attach = guarded_duckdb_client_attach


# Ensure tests.conftest in sys.modules points to this module
sys.modules["tests.conftest"] = sys.modules[__name__]

# ============================================================================
# NETWORK SOCKET ISOLATION GUARD
# ============================================================================

def is_allowed_network_target(address) -> bool:
    if getattr(sys, "_pytest_current_test_is_live", False):
        return True
    if isinstance(address, str):
        # AF_UNIX local socket or IPC
        return True
    if isinstance(address, (tuple, list)) and len(address) > 0:
        host = str(address[0]).lower().strip()
        if host in ("127.0.0.1", "localhost", "::1", "0.0.0.0", "::", "0:0:0:0:0:0:0:1"):
            return True
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_loopback:
                return True
        except ValueError:
            pass
    return False


_original_socket_connect = socket.socket.connect
_original_socket_connect_ex = socket.socket.connect_ex


def guarded_socket_connect(self, address):
    if not is_allowed_network_target(address):
        raise NetworkBlockedError(
            f"External network connection to {address} blocked. Mark test with @pytest.mark.live to allow."
        )
    return _original_socket_connect(self, address)


def guarded_socket_connect_ex(self, address):
    if not is_allowed_network_target(address):
        raise NetworkBlockedError(
            f"External network connection to {address} blocked. Mark test with @pytest.mark.live to allow."
        )
    return _original_socket_connect_ex(self, address)


socket.socket.connect = guarded_socket_connect
socket.socket.connect_ex = guarded_socket_connect_ex


def pytest_runtest_setup(item):
    is_live = item.get_closest_marker("live") is not None
    setattr(sys, "_pytest_current_test_is_live", is_live)


def pytest_runtest_teardown(item, nextitem):
    setattr(sys, "_pytest_current_test_is_live", False)


def pytest_configure(config):
    _patch_all_modules()


def pytest_sessionfinish(session, exitstatus):
    global _SESSION_TEMP_DIR
    if _SESSION_TEMP_DIR and os.path.exists(_SESSION_TEMP_DIR):
        shutil.rmtree(_SESSION_TEMP_DIR, ignore_errors=True)


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def _network_and_path_tracker(request):
    is_live = request.node.get_closest_marker("live") is not None
    setattr(sys, "_pytest_current_test_is_live", is_live)
    _patch_all_modules()
    try:
        yield
    finally:
        setattr(sys, "_pytest_current_test_is_live", False)


@pytest.fixture(autouse=True)
def reset_binance_domain():
    """Reset the Binance module-level global between tests to prevent leakage."""
    import src.api.binance as binance_mod
    original = binance_mod.WORKING_BINANCE_DOMAIN
    yield
    binance_mod.WORKING_BINANCE_DOMAIN = original


@pytest.fixture
def safe_test_date():
    """Returns a recent trading day (not a weekend) within the last 30 days."""
    now_et = datetime.now(US_EASTERN).date()
    target = now_et - timedelta(days=2)  # 2 days back to be safe
    while target.weekday() > 4:
        target -= timedelta(days=1)
    return target


@pytest.fixture
def safe_test_date_str(safe_test_date):
    """Returns the safe test date as a YYYY-MM-DD string."""
    return safe_test_date.strftime("%Y-%m-%d")


@pytest.fixture
def safe_test_range(safe_test_date):
    """
    Returns (start_dt, end_dt) in UTC for the 8 PM ET session 
    covering the safe_test_date.
    """
    prev = safe_test_date - timedelta(days=1)
    while prev.weekday() > 4:
        prev -= timedelta(days=1)
    
    start_et = US_EASTERN.localize(datetime.combine(prev, datetime.strptime("20:00", "%H:%M").time()))
    end_et = US_EASTERN.localize(datetime.combine(safe_test_date, datetime.strptime("20:00", "%H:%M").time()))
    
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)
