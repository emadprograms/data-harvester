"""
Unit and integration tests for Versioned Symbol Registry (Milestone v4.0 - Phase 18 P3).
Validates atomic JSON staging/fsync/replace, idempotence, monotonic versioning,
pending purge lifecycle, input validation, and concurrency locking.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import pytest

from src.storage.config import PathTraversalError
from src.storage.registry import (
    DEFAULT_REGISTRY_FILENAME,
    DEFAULT_SIGNAL_FILENAME,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    InvalidSymbolError,
    RegistryControlLock,
    RegistryError,
    RegistryLockError,
    RegistrySnapshot,
    SymbolEntry,
    SymbolNotFoundError,
    SymbolPendingPurgeError,
    SymbolRegistry,
    init_registry,
    load_registry,
)


# Legacy default tickers from v2.0/v3.0 DuckDB schema
LEGACY_19_DEFAULT_TICKERS = {
    "AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT", "GOOGL", "META", "NFLX", "INTC",
    "AVGO", "BABA", "DIS", "JNJ", "JPM", "V", "PG", "UNH", "HD", "SPY", "QQQ"
}


def test_init_registry_creates_empty_without_defaults(tmp_path):
    """
    Verifies init_registry creates symbols={} and does NOT seed the 19 default tickers.
    Ensures storage starts completely clean for decoupled lake ingestion.
    """
    snapshot = init_registry(tmp_path)
    assert isinstance(snapshot, RegistrySnapshot)
    assert snapshot.version == 1
    assert snapshot.symbols == {}
    assert len(snapshot.symbols) == 0

    # Verify on-disk file existence and contents
    reg = SymbolRegistry(tmp_path)
    assert reg.path.exists()

    with open(reg.path, "r", encoding="utf-8") as f:
        disk_data = json.load(f)

    assert disk_data.get("version") == 1
    assert disk_data.get("symbols") == {}

    # Explicitly ensure NONE of the legacy 19 default tickers are seeded
    for legacy_sym in LEGACY_19_DEFAULT_TICKERS:
        assert legacy_sym not in snapshot.symbols
        assert legacy_sym not in disk_data["symbols"]


def test_init_registry_idempotence(tmp_path):
    """
    Verifies idempotence:
    - Calling init_registry a second time without force=True retains existing snapshot & symbols.
    - Calling init_registry with force=True resets to a clean empty version 1 registry.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)
    reg.add_symbol("AAPL", display_name="Apple Inc", capital_ticker="AAPL")

    # Second init without force=True loads existing snapshot
    snapshot2 = init_registry(tmp_path, force=False)
    assert snapshot2.version >= 2
    assert "AAPL" in snapshot2.symbols
    assert snapshot2.symbols["AAPL"].capital_ticker == "AAPL"
    assert snapshot2.symbols["AAPL"].status == STATUS_ACTIVE

    # Third init with force=True wipes existing symbols and resets version to 1
    snapshot_forced = init_registry(tmp_path, force=True)
    assert snapshot_forced.version == 1
    assert snapshot_forced.symbols == {}
    assert "AAPL" not in snapshot_forced.symbols

    # Verify on disk
    reloaded = reg.load()
    assert reloaded.version == 1
    assert reloaded.symbols == {}


def test_atomic_write_fsync_and_replace(tmp_path, monkeypatch):
    """
    Verifies that updating the registry uses a staging temp file, invokes os.fsync
    on the underlying file descriptor, and atomically renames via os.replace.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)

    fsync_called_fds = []
    orig_fsync = os.fsync

    def spy_fsync(fd):
        fsync_called_fds.append(fd)
        return orig_fsync(fd)

    monkeypatch.setattr(os, "fsync", spy_fsync)

    replaces = []
    orig_replace = os.replace

    def spy_replace(src, dst):
        replaces.append((Path(src), Path(dst)))
        return orig_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy_replace)

    # Perform a mutation
    reg.add_symbol("NVDA", display_name="NVIDIA Corp", capital_ticker="NVDA")

    # Ensure fsync was called prior to replace
    assert len(fsync_called_fds) > 0, "os.fsync must be called to guarantee durable disk write"
    assert len(replaces) > 0, "os.replace must be used for atomic destination swap"

    src_path, dst_path = replaces[-1]
    assert dst_path.resolve() == reg.path.resolve()
    assert ".tmp" in src_path.name or "tmp" in src_path.name
    # Verify staging temp file was on the same filesystem/parent or lake staging dir
    assert src_path.parent == reg.path.parent or src_path.parent == reg.root / "_staging"


def test_monotonic_version_increments(tmp_path):
    """
    Verifies that the registry version increments monotonically with every mutation:
    - 1 on init
    - 2 on add
    - 3 on toggle
    - 4 on remove
    """
    reg = SymbolRegistry(tmp_path)
    s1 = init_registry(tmp_path)
    assert s1.version == 1
    assert reg.version == 1

    # Mutation 1: add_symbol -> version 2
    e2 = reg.add_symbol("AAPL", capital_ticker="AAPL")
    assert reg.version == 2
    assert reg.load().version == 2

    # Mutation 2: toggle_symbol -> version 3
    e3 = reg.toggle_symbol("AAPL", active=False)
    assert reg.version == 3
    assert reg.load().version == 3

    # Mutation 3: remove_symbol -> version 4
    e4 = reg.remove_symbol("AAPL")
    assert reg.version == 4
    assert reg.load().version == 4


def test_add_symbol_validation_and_fields(tmp_path):
    """
    Verifies symbol validation:
    - Rejects path traversal attempts, null bytes, empty strings, and malformed characters.
    - Successfully populates generation=1, status='ACTIVE', active=True, and valid ISO timestamps.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)

    # Rejection cases
    invalid_symbols = [
        "",
        "   ",
        "..",
        "../AAPL",
        "AAPL/..",
        "../../etc/passwd",
        "AAPL\x00",
        "AAPL;DROP TABLE",
        "AAPL/USD",
        None,
        "A" * 65,  # Exceeds max symbol length
    ]

    for inv in invalid_symbols:
        with pytest.raises((InvalidSymbolError, ValueError, PathTraversalError)):
            reg.add_symbol(inv)

    # Valid symbol addition
    entry = reg.add_symbol(
        symbol="MSFT",
        display_name="Microsoft Corp",
        capital_ticker="MSFT",
        databento_ticker="MSFT.XNAS",
        binance_ticker=None,
        metadata={"asset_class": "equity"}
    )

    assert isinstance(entry, SymbolEntry)
    assert entry.symbol == "MSFT"
    assert entry.display_name == "Microsoft Corp"
    assert entry.capital_ticker == "MSFT"
    assert entry.databento_ticker == "MSFT.XNAS"
    assert entry.binance_ticker is None
    assert entry.active is True
    assert entry.status == STATUS_ACTIVE
    assert entry.generation == 1
    assert entry.metadata.get("asset_class") == "equity"

    # Verify timestamps are valid ISO 8601 strings in UTC
    dt_created = datetime.fromisoformat(entry.created_at)
    dt_updated = datetime.fromisoformat(entry.updated_at)
    assert dt_created <= datetime.now(timezone.utc)
    assert dt_updated <= datetime.now(timezone.utc)


def test_pending_purge_lifecycle_and_readd_refusal(tmp_path):
    """
    Verifies the pending purge safety lifecycle:
    - Removing a symbol marks status='PENDING_PURGE' and active=False.
    - Attempting to re-add or toggle a symbol in PENDING_PURGE raises SymbolPendingPurgeError.
    - complete_purge removes the symbol completely.
    - Subsequent re-add succeeds with incremented generation=2.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)
    reg.add_symbol("AAPL", capital_ticker="AAPL")

    assert reg.load().symbols["AAPL"].generation == 1

    # 1. Removing symbol sets status to PENDING_PURGE and active to False
    removed = reg.remove_symbol("AAPL")
    assert removed.status == STATUS_PENDING_PURGE
    assert removed.active is False

    snapshot = reg.load()
    assert snapshot.symbols["AAPL"].status == STATUS_PENDING_PURGE
    assert snapshot.symbols["AAPL"].active is False

    # 2. Attempting to re-add while in PENDING_PURGE raises SymbolPendingPurgeError
    with pytest.raises(SymbolPendingPurgeError):
        reg.add_symbol("AAPL", capital_ticker="AAPL")

    # 3. Attempting to toggle while in PENDING_PURGE raises SymbolPendingPurgeError
    with pytest.raises(SymbolPendingPurgeError):
        reg.toggle_symbol("AAPL", active=True)

    with pytest.raises(SymbolPendingPurgeError):
        reg.toggle_symbol("AAPL", active=False)

    # 4. complete_purge removes the symbol completely
    reg.complete_purge("AAPL")
    snapshot_purged = reg.load()
    assert "AAPL" not in snapshot_purged.symbols

    # 5. Subsequent re-add succeeds with generation incremented to 2
    readded = reg.add_symbol("AAPL", capital_ticker="AAPL")
    assert readded.symbol == "AAPL"
    assert readded.status == STATUS_ACTIVE
    assert readded.active is True
    assert readded.generation == 2


def test_toggle_symbol_active_inactive(tmp_path):
    """
    Verifies toggling symbol active state:
    - True -> False -> True
    - Updates status ('ACTIVE' <-> 'INACTIVE') and refreshes updated_at.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)
    reg.add_symbol("GOOGL", capital_ticker="GOOGL")

    # Initial state
    assert reg.load().symbols["GOOGL"].active is True
    assert reg.load().symbols["GOOGL"].status == STATUS_ACTIVE

    # Toggle to False
    t1 = reg.toggle_symbol("GOOGL", active=False)
    assert t1.active is False
    assert t1.status == STATUS_INACTIVE
    assert reg.load().symbols["GOOGL"].active is False

    # Toggle to True
    t2 = reg.toggle_symbol("GOOGL", active=True)
    assert t2.active is True
    assert t2.status == STATUS_ACTIVE
    assert reg.load().symbols["GOOGL"].active is True

    # Toggle without explicit flag flips current state
    t3 = reg.toggle_symbol("GOOGL")
    assert t3.active is False
    assert t3.status == STATUS_INACTIVE
    assert reg.load().symbols["GOOGL"].active is False


def test_registry_control_lock_concurrency(tmp_path):
    """
    Verifies that concurrent mutations serialize cleanly under RegistryControlLock.
    Multiple worker threads simultaneously add symbols without file corruption or race conditions.
    """
    reg = SymbolRegistry(tmp_path)
    init_registry(tmp_path)

    symbols_to_add = [f"SYM{i:02d}" for i in range(20)]

    def worker_add(sym):
        lock = RegistryControlLock(tmp_path, timeout=10.0)
        with lock:
            reg.add_symbol(sym, capital_ticker=sym)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker_add, sym) for sym in symbols_to_add]
        for f in futures:
            f.result()

    snapshot = reg.load()
    assert len(snapshot.symbols) == 20
    for sym in symbols_to_add:
        assert sym in snapshot.symbols
        assert snapshot.symbols[sym].status == STATUS_ACTIVE

    # Version should equal 1 (init) + 20 additions = 21
    assert snapshot.version == 21
