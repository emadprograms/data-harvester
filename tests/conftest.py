import builtins
import io
import os
import sys
import shutil
import tempfile
import socket
import ipaddress
import threading
from datetime import datetime, timedelta, date, timezone
import pytest
import duckdb


class ProductionAccessBlockedError(PermissionError):
    """Raised when test code attempts to access protected storage paths."""
    pass


class NetworkBlockedError(RuntimeError):
    """Raised when non-live test code attempts external socket connections."""
    pass


# ============================================================================
# SESSION-LEVEL PATH REDIRECTION — MUST PRECEDE APPLICATION IMPORTS
# ============================================================================

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO_DATA_DIR = os.path.join(REPO_ROOT, "data")
REPO_DATA_REAL = os.path.realpath(REPO_DATA_DIR)
MICRON_DATA_DIR = "/Volumes/Micron-E 0256 A/data-harvester/data"

_SESSION_TEMP_DIR = tempfile.mkdtemp(prefix="pytest_data_harvest_")
_SESSION_LAKE_ROOT = os.path.join(_SESSION_TEMP_DIR, "tick_lake")
_SESSION_RUN_DIR = os.path.join(_SESSION_TEMP_DIR, "run")
os.makedirs(_SESSION_RUN_DIR, exist_ok=True)
# Explicitly override inherited values before importing any application module.
# load_dotenv() uses override=False, so a repository .env cannot replace these.
os.environ["DATA_DIR"] = _SESSION_TEMP_DIR
os.environ["TICK_LAKE_ROOT"] = _SESSION_LAKE_ROOT
if "DATA_HARVESTER_RUN_DIR" not in os.environ:
    os.environ["DATA_HARVESTER_RUN_DIR"] = _SESSION_RUN_DIR

# The fake protected roots are added only by a fixture that tests the guard.
_PROTECTED_TEST_ROOTS = set()
_PROTECTED_ROOTS_LOCK = threading.RLock()

from src.utils.write_guard import register_protected_root, unregister_protected_root

from src.config import US_EASTERN, UTC


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
from src.storage.config import init_tick_lake
init_historical_db()
init_streaming_db()
init_tick_lake(_SESSION_LAKE_ROOT)

STANDARD_HISTORICAL_SYMBOLS = [
    "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "SPY", "QQQ", "AMD",
    "INTC", "NFLX", "BABA", "DIS", "JNJ", "JPM", "V", "PG", "UNH", "HD",
    "MA", "BAC", "XOM", "PFE", "KO", "PEP", "CSCO", "COST", "ABT", "MRK",
    "TMO", "ACN", "AVGO", "NKE", "LLY", "ORCL", "CRM", "WMT", "CVX", "ADBE"
]


def _seed_isolated_session_historical_db():
    from src.database.connection import get_historical_db_connection
    client = get_historical_db_connection()
    if not client:
        return
    try:
        # Category 3: Seed 40 standard symbols into historical_database_symbols
        for sym in STANDARD_HISTORICAL_SYMBOLS:
            client.execute(
                """INSERT OR REPLACE INTO historical_database_symbols 
                   (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) 
                   VALUES (?, ?, ?, ?, ?)""",
                [sym, sym, sym, None, sym]
            )

        # Category 2: Seed synthetic 1-minute candlestick bars for all symbols into minute_data
        # Reference date: 2026-07-10 (regular NYSE trading session, 09:30 to 11:00 EDT -> 13:30 to 15:00 UTC)
        base_dt = datetime(2026, 7, 10, 13, 30, 0)
        bar_rows = []
        for sym in STANDARD_HISTORICAL_SYMBOLS:
            bar_count = 90 if sym in ("NVDA", "AAPL", "SPY") else 5
            base_price = 125.0 if sym == "NVDA" else (220.0 if sym == "AAPL" else (550.0 if sym == "SPY" else 100.0))
            for i in range(bar_count):
                ts = base_dt + timedelta(minutes=i)
                open_p = round(base_price + i * 0.05, 4)
                high_p = round(open_p + 0.5, 4)
                low_p = round(open_p - 0.5, 4)
                close_p = round(open_p + 0.2, 4)
                volume = round(1000.0 + i * 10.0, 2)
                bar_rows.append((ts, sym, open_p, high_p, low_p, close_p, volume, "REG", "MASSIVE"))

        client.executemany(
            """INSERT OR IGNORE INTO minute_data 
               (timestamp, symbol, open, high, low, close, volume, session, source) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            bar_rows
        )
    finally:
        client.close()


_seed_isolated_session_historical_db()



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


def _normalised_path_for_guard(path):
    if isinstance(path, int) or path is None:
        return None
    if hasattr(path, "name") and not isinstance(path, (str, bytes, os.PathLike)):
        path = path.name
    try:
        return os.fspath(path)
    except TypeError:
        return None


def is_protected_path(path) -> bool:
    """Return true for production paths and fixture-registered fake protected roots."""
    path_str = _normalised_path_for_guard(path)
    if not path_str:
        return False
    if isinstance(path_str, bytes):
        path_str = os.fsdecode(path_str)
    if is_production_path(path_str):
        return True

    absolute = os.path.abspath(path_str)
    resolved = os.path.realpath(absolute)
    with _PROTECTED_ROOTS_LOCK:
        roots = tuple(_PROTECTED_TEST_ROOTS)
    for root in roots:
        root_abs = os.path.abspath(root)
        root_real = os.path.realpath(root_abs)
        for candidate, protected_root in ((absolute, root_abs), (resolved, root_real)):
            try:
                if os.path.commonpath((candidate, protected_root)) == protected_root:
                    return True
            except ValueError:
                continue
    return False


def _deny_protected_write(path, operation: str) -> None:
    if is_protected_path(path):
        raise ProductionAccessBlockedError(
            f"Blocked {operation} to protected path: {path}"
        )


_original_builtin_open = builtins.open
_original_io_open = io.open


def _guarded_open(file, mode="r", *args, **kwargs):
    if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
        _deny_protected_write(file, "open for write")
    return _original_builtin_open(file, mode, *args, **kwargs)


def _guarded_io_open(file, mode="r", *args, **kwargs):
    if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
        _deny_protected_write(file, "open for write")
    return _original_io_open(file, mode, *args, **kwargs)


builtins.open = _guarded_open
io.open = _guarded_io_open

_original_os_open = os.open
_original_os_replace = os.replace
_original_os_rename = os.rename
_original_os_remove = os.remove
_original_os_unlink = os.unlink
_original_os_mkdir = os.mkdir
_original_os_makedirs = os.makedirs
_original_os_rmdir = os.rmdir


def _guarded_os_open(path, flags, *args, **kwargs):
    write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
    if flags & write_flags:
        _deny_protected_write(path, "os.open")
    return _original_os_open(path, flags, *args, **kwargs)


def _guarded_os_replace(src, dst, *args, **kwargs):
    _deny_protected_write(src, "replace source")
    _deny_protected_write(dst, "replace destination")
    return _original_os_replace(src, dst, *args, **kwargs)


def _guarded_os_rename(src, dst, *args, **kwargs):
    _deny_protected_write(src, "rename source")
    _deny_protected_write(dst, "rename destination")
    return _original_os_rename(src, dst, *args, **kwargs)


def _guarded_os_remove(path, *args, **kwargs):
    _deny_protected_write(path, "remove")
    return _original_os_remove(path, *args, **kwargs)


def _guarded_os_unlink(path, *args, **kwargs):
    _deny_protected_write(path, "unlink")
    return _original_os_unlink(path, *args, **kwargs)


def _guarded_os_mkdir(path, *args, **kwargs):
    _deny_protected_write(path, "mkdir")
    return _original_os_mkdir(path, *args, **kwargs)


def _guarded_os_makedirs(path, *args, **kwargs):
    _deny_protected_write(path, "makedirs")
    return _original_os_makedirs(path, *args, **kwargs)


def _guarded_os_rmdir(path, *args, **kwargs):
    _deny_protected_write(path, "rmdir")
    return _original_os_rmdir(path, *args, **kwargs)


os.open = _guarded_os_open
os.replace = _guarded_os_replace
os.rename = _guarded_os_rename
os.remove = _guarded_os_remove
os.unlink = _guarded_os_unlink
os.mkdir = _guarded_os_mkdir
os.makedirs = _guarded_os_makedirs
os.rmdir = _guarded_os_rmdir

# PyArrow's C++ writer does not necessarily pass through Python's open()/os.open().
import pyarrow.parquet as _guarded_pq
_original_parquet_write_table = _guarded_pq.write_table


def _guarded_parquet_write_table(table, where, *args, **kwargs):
    _deny_protected_write(where, "Parquet write")
    return _original_parquet_write_table(table, where, *args, **kwargs)


_guarded_pq.write_table = _guarded_parquet_write_table


_original_duckdb_connect = duckdb.connect


def guarded_duckdb_connect(*args, **kwargs):
    db_target = ":memory:"
    if args:
        db_target = args[0]
    elif "database" in kwargs:
        db_target = kwargs["database"]

    if is_protected_path(db_target):
        raise ProductionAccessBlockedError(
            f"Blocked access to production database path: {db_target}"
        )
    return _original_duckdb_connect(*args, **kwargs)


duckdb.connect = guarded_duckdb_connect

from src.database.connection import DuckDBClient

_original_duckdb_client_attach = DuckDBClient.attach


def guarded_duckdb_client_attach(self, target_db_path: str, alias: str, read_only: bool = True):
    if is_protected_path(target_db_path):
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

@pytest.fixture
def isolated_lake_root(tmp_path, monkeypatch):
    """A fresh lake root selected explicitly for this test."""
    root = tmp_path / "lake"
    monkeypatch.setenv("TICK_LAKE_ROOT", str(root))
    return root


@pytest.fixture
def isolated_legacy_data_dir(tmp_path, monkeypatch):
    """An explicit legacy-only DATA_DIR with no lake selection in the environment."""
    data_dir = tmp_path / "legacy-data"
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.delenv("TICK_LAKE_ROOT", raising=False)
    return data_dir


@pytest.fixture
def isolated_subprocess_env(tmp_path):
    """Environment for child processes; keeps all default storage inside tmp_path."""
    env = os.environ.copy()
    data_dir = tmp_path / "child-data"
    lake_root = tmp_path / "child-lake"
    env["DATA_DIR"] = str(data_dir)
    env["TICK_LAKE_ROOT"] = str(lake_root)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [REPO_ROOT, env.get("PYTHONPATH", "")])
    )
    return env


@pytest.fixture
def protected_temp_dir(tmp_path):
    """Register a fake protected directory to exercise guards without real storage."""
    protected_root = tmp_path / "fake-protected-root"
    with _PROTECTED_ROOTS_LOCK:
        _PROTECTED_TEST_ROOTS.add(str(protected_root))
    register_protected_root(str(protected_root))
    prev_env = os.environ.get("DATA_HARVESTER_PROTECTED_ROOTS")
    os.environ["DATA_HARVESTER_PROTECTED_ROOTS"] = (
        f"{prev_env}:{protected_root}" if prev_env else str(protected_root)
    )
    try:
        yield protected_root
    finally:
        with _PROTECTED_ROOTS_LOCK:
            _PROTECTED_TEST_ROOTS.discard(str(protected_root))
        unregister_protected_root(str(protected_root))
        if prev_env is not None:
            os.environ["DATA_HARVESTER_PROTECTED_ROOTS"] = prev_env
        else:
            os.environ.pop("DATA_HARVESTER_PROTECTED_ROOTS", None)


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
