"""
Versioned JSON Symbol Registry and Single-Control-Owner Mutator for Tick Lake.
Milestone v4.0 - Phase 18 (P3).

Provides atomic mutations, JSON serialization with fsync, monotonic versioning,
generation tracking across purges, and cross-process reload signaling.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

from src.storage.config import resolve_tick_lake_root, StorageConfigError

logger = logging.getLogger("symbol_registry")

# Filename constants
DEFAULT_REGISTRY_FILENAME = "registry.json"
DEFAULT_SIGNAL_FILENAME = ".stream_reload.signal"
DEFAULT_CONTROL_LOCK_FILENAME = "registry.lock"

# Status constants
STATUS_ACTIVE = "ACTIVE"
STATUS_INACTIVE = "INACTIVE"
STATUS_PENDING_PURGE = "PENDING_PURGE"


class RegistryError(Exception):
    """Base exception for all symbol registry errors."""
    pass


class SymbolPendingPurgeError(RegistryError):
    """Raised when attempting to add or modify a symbol that is in PENDING_PURGE status."""
    pass


class SymbolNotFoundError(RegistryError):
    """Raised when a requested symbol does not exist in the registry."""
    pass


class SymbolAlreadyExistsError(RegistryError):
    """Raised when attempting to add a symbol that already exists in the registry."""
    pass


class InvalidSymbolError(RegistryError, ValueError):
    """Raised when a symbol contains invalid characters or path traversal attempts."""
    pass


class RegistryLockError(RegistryError):
    """Raised when registry control lock acquisition fails or times out."""
    pass


class RegistryCorruptedError(RegistryError):
    """Raised when the registry file on disk is invalid or corrupted."""
    pass


def validate_symbol(symbol: Any) -> str:
    """Validates symbol string against path traversal, control chars, and invalid formats."""
    if symbol is None or not isinstance(symbol, str):
        raise InvalidSymbolError("Symbol must be a non-empty string")
    sym = symbol.strip()
    if not sym:
        raise InvalidSymbolError("Symbol cannot be empty or whitespace")
    if len(sym) > 64:
        raise InvalidSymbolError(f"Symbol length ({len(sym)}) exceeds maximum allowed (64)")
    if "\x00" in sym or "\\" in sym or "/" in sym or ";" in sym:
        raise InvalidSymbolError(f"Symbol contains illegal characters: {sym}")
    if sym == "." or sym == ".." or ".." in sym:
        raise InvalidSymbolError(f"Path traversal detected in symbol: {sym}")
    if not re.match(r"^[A-Za-z0-9_.-]+$", sym):
        raise InvalidSymbolError(f"Symbol contains invalid characters: {sym}")
    return sym.upper()


@dataclass
class SymbolEntry:
    """Represents a tracked symbol entry in the versioned registry."""
    symbol: str
    display_name: str
    capital_ticker: Optional[str] = None
    databento_ticker: Optional[str] = None
    binance_ticker: Optional[str] = None
    active: bool = True
    status: str = STATUS_ACTIVE  # "ACTIVE", "INACTIVE", "PENDING_PURGE"
    generation: int = 1
    created_at: str = ""
    updated_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.display_name

    @property
    def epic(self) -> str:
        return self.capital_ticker or self.symbol

    @property
    def added_at(self) -> str:
        return self.created_at

    @property
    def asset_class(self) -> str:
        if isinstance(self.metadata, dict):
            return self.metadata.get("asset_class", "EQUITY")
        return "EQUITY"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "display_name": self.display_name,
            "capital_ticker": self.capital_ticker,
            "databento_ticker": self.databento_ticker,
            "binance_ticker": self.binance_ticker,
            "active": self.active,
            "status": self.status,
            "generation": self.generation,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": dict(self.metadata) if isinstance(self.metadata, dict) else {},
            # Compatibility fields
            "name": self.display_name,
            "epic": self.capital_ticker or self.symbol,
            "added_at": self.created_at,
            "asset_class": self.asset_class,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SymbolEntry":
        sym = data.get("symbol", "")
        disp = data.get("display_name") or data.get("name") or sym
        cap = data.get("capital_ticker") or data.get("epic")
        dbn = data.get("databento_ticker")
        binance = data.get("binance_ticker")
        active = data.get("active", True)
        status = data.get("status", STATUS_ACTIVE)
        gen = int(data.get("generation", 1))
        created = data.get("created_at") or data.get("added_at") or ""
        updated = data.get("updated_at") or ""
        meta = data.get("metadata")
        if not isinstance(meta, dict):
            meta = {}
        if "asset_class" in data and "asset_class" not in meta:
            meta = dict(meta)
            meta["asset_class"] = data["asset_class"]

        return cls(
            symbol=sym,
            display_name=disp,
            capital_ticker=cap,
            databento_ticker=dbn,
            binance_ticker=binance,
            active=active,
            status=status,
            generation=gen,
            created_at=created,
            updated_at=updated,
            metadata=meta,
        )


@dataclass
class RegistrySnapshot:
    """Represents a snapshot of the versioned symbol registry."""
    version: int = 1
    updated_at: str = ""
    lake_id: Optional[str] = None
    symbols: Dict[str, SymbolEntry] = field(default_factory=dict)
    generations: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "updated_at": self.updated_at,
            "lake_id": self.lake_id,
            "symbols": {k: v.to_dict() for k, v in self.symbols.items()},
            "generations": dict(self.generations),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RegistrySnapshot":
        symbols_raw = data.get("symbols", {})
        symbols: Dict[str, SymbolEntry] = {}
        for k, v in symbols_raw.items():
            if isinstance(v, dict):
                symbols[k] = SymbolEntry.from_dict(v)
            elif isinstance(v, SymbolEntry):
                symbols[k] = v

        generations_raw = data.get("generations", {})
        generations = {k: int(v) for k, v in generations_raw.items()}

        return cls(
            version=int(data.get("version", 1)),
            updated_at=data.get("updated_at", ""),
            lake_id=data.get("lake_id"),
            symbols=symbols,
            generations=generations,
        )


_lock_table: Dict[str, threading.RLock] = {}
_table_lock = threading.Lock()
_local_lock_state = threading.local()


class RegistryControlLock:
    """Advisory control lock ensuring single-writer serialization for symbol registry mutations."""
    def __init__(self, root_or_path: Union[str, Path], timeout: float = 5.0):
        p = Path(root_or_path).resolve()
        if p.suffix == ".lock" or (p.exists() and p.is_file()):
            self.lock_path = p
        else:
            self.lock_path = p / "_control" / DEFAULT_CONTROL_LOCK_FILENAME
        self.timeout = float(timeout)
        self._key = str(self.lock_path)
        self._fd: Optional[int] = None
        self._acquired = False
        self._is_reentrant = False

    def acquire(self) -> bool:
        start_time = time.monotonic()
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

        if not hasattr(_local_lock_state, "held_locks"):
            _local_lock_state.held_locks = {}

        # Re-entrancy check on same thread
        if self._key in _local_lock_state.held_locks:
            _local_lock_state.held_locks[self._key]["depth"] += 1
            self._acquired = True
            self._is_reentrant = True
            return True

        with _table_lock:
            if self._key not in _lock_table:
                _lock_table[self._key] = threading.RLock()
            rlock = _lock_table[self._key]

        # Acquire thread lock
        elapsed = time.monotonic() - start_time
        remaining = max(0.01, self.timeout - elapsed)
        acquired_thread = rlock.acquire(timeout=remaining)
        if not acquired_thread:
            raise RegistryLockError(f"Timed out acquiring thread lock on {self.lock_path}")

        # Acquire OS flock (looping with non-blocking until timeout)
        fd = None
        try:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._fd = fd
                    self._acquired = True
                    self._is_reentrant = False
                    _local_lock_state.held_locks[self._key] = {
                        "depth": 1,
                        "fd": fd,
                        "rlock": rlock
                    }
                    return True
                except (BlockingIOError, OSError):
                    if (time.monotonic() - start_time) >= self.timeout:
                        raise RegistryLockError(f"Timed out acquiring process lock on {self.lock_path}")
                    time.sleep(0.01)
        except Exception:
            if fd is not None and not self._acquired:
                try:
                    os.close(fd)
                except OSError:
                    pass
            rlock.release()
            raise

    def release(self) -> None:
        if not self._acquired:
            return

        if not hasattr(_local_lock_state, "held_locks") or self._key not in _local_lock_state.held_locks:
            self._acquired = False
            return

        entry = _local_lock_state.held_locks[self._key]
        entry["depth"] -= 1

        if entry["depth"] <= 0:
            fd = entry["fd"]
            rlock = entry["rlock"]
            del _local_lock_state.held_locks[self._key]
            try:
                if fd is not None:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    finally:
                        os.close(fd)
            finally:
                rlock.release()

        self._acquired = False

    def __enter__(self) -> "RegistryControlLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class SymbolRegistry:
    """Versioned symbol registry managing atomic mutations, serialization, and signal wakeups."""
    def __init__(
        self,
        root: Optional[Union[str, Path]] = None,
        registry_path: Optional[Union[str, Path]] = None,
        signal_path: Optional[Union[str, Path]] = None,
        lock_timeout: float = 5.0,
    ):
        self._lock_timeout = float(lock_timeout)
        if root is not None:
            self._root = Path(root).resolve()
        elif registry_path is not None:
            rp = Path(registry_path).resolve()
            self._root = rp.parent.parent if rp.parent.name == "_control" else rp.parent
        else:
            try:
                self._root = resolve_tick_lake_root()
            except Exception:
                self._root = Path.cwd().resolve()

        if registry_path is not None:
            self._registry_path = Path(registry_path).resolve()
        else:
            direct = self._root / DEFAULT_REGISTRY_FILENAME
            control_p = self._root / "_control" / DEFAULT_REGISTRY_FILENAME
            if direct.exists() and not control_p.exists():
                self._registry_path = direct
            else:
                self._registry_path = control_p

        if signal_path is not None:
            self._signal_path = Path(signal_path).resolve()
        else:
            self._signal_path = self._root / DEFAULT_SIGNAL_FILENAME

    @property
    def path(self) -> Path:
        return self._registry_path

    @property
    def root(self) -> Path:
        return self._root

    @property
    def signal_path(self) -> Path:
        return self._signal_path

    @property
    def lock_timeout(self) -> float:
        return self._lock_timeout

    @property
    def version(self) -> int:
        return self.load().version

    def _save_snapshot(self, snapshot: RegistrySnapshot) -> None:
        """Atomically stages, fsyncs, and replaces the registry file on disk."""
        self._registry_path.parent.mkdir(parents=True, exist_ok=True)
        staging_dir = self._registry_path.parent
        tmp_filename = f"tmp_registry_{os.getpid()}_{uuid.uuid4().hex}.json"
        tmp_path = staging_dir / tmp_filename

        data = snapshot.to_dict()
        payload = json.dumps(data, indent=2)

        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, self._registry_path)

    def load(self) -> RegistrySnapshot:
        """Loads and returns current registry snapshot from disk without mutating."""
        if not self._registry_path.exists():
            raise RegistryError(f"Registry file not found at {self._registry_path}")
        try:
            with open(self._registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise RegistryCorruptedError(
                    f"Registry root element must be a dictionary, got {type(data).__name__}"
                )
            return RegistrySnapshot.from_dict(data)
        except RegistryCorruptedError:
            raise
        except (json.JSONDecodeError, AttributeError, ValueError, TypeError, KeyError) as e:
            raise RegistryCorruptedError(f"Registry corrupted: {e}") from e

    def get_snapshot(self) -> RegistrySnapshot:
        """Alias for load()."""
        return self.load()

    def get_version(self) -> int:
        """Returns current monotonic version."""
        return self.version

    def get_symbols(self) -> List[SymbolEntry]:
        """Returns list of all symbol entries in the registry."""
        return list(self.load().symbols.values())

    def get_active_symbols(self) -> List[SymbolEntry]:
        """Returns list of active symbol entries (active=True and status=ACTIVE)."""
        return [s for s in self.load().symbols.values() if s.active and s.status == STATUS_ACTIVE]

    def get_all_symbols(self, active_only: bool = False) -> Dict[str, SymbolEntry]:
        """Returns dict of all symbols, optionally filtered to active only."""
        snapshot = self.load()
        if active_only:
            return {k: v for k, v in snapshot.symbols.items() if v.active and v.status == STATUS_ACTIVE}
        return dict(snapshot.symbols)

    def get_active_streaming_symbols(self) -> List[str]:
        """Returns list of active symbol strings."""
        return [s.capital_ticker or s.symbol for s in self.get_active_symbols()]

    def get_symbol(self, symbol: str) -> Optional[SymbolEntry]:
        """Returns entry by symbol using case-insensitive lookup."""
        if not symbol:
            return None
        s_upper = symbol.strip().upper()
        snapshot = self.load()
        if s_upper in snapshot.symbols:
            return snapshot.symbols[s_upper]
        for k, v in snapshot.symbols.items():
            if k.upper() == s_upper or (v.display_name and v.display_name.upper() == s_upper):
                return v
        return None

    def add_symbol(
        self,
        symbol: str,
        display_name: Optional[str] = None,
        capital_ticker: Optional[str] = None,
        databento_ticker: Optional[str] = None,
        binance_ticker: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        epic: Optional[str] = None,
        asset_class: Optional[str] = None,
    ) -> SymbolEntry:
        """Adds a new symbol under lock, incrementing version monotonically."""
        sym = validate_symbol(symbol)
        disp = display_name or name or sym
        cap = capital_ticker or epic
        meta = dict(metadata) if isinstance(metadata, dict) else {}
        if asset_class and "asset_class" not in meta:
            meta["asset_class"] = asset_class

        with RegistryControlLock(self._root, timeout=self._lock_timeout):
            snapshot = self.load()
            if sym in snapshot.symbols:
                existing = snapshot.symbols[sym]
                if existing.status == STATUS_PENDING_PURGE:
                    raise SymbolPendingPurgeError(
                        f"Symbol {sym} is currently pending purge and cannot be added"
                    )
                raise SymbolAlreadyExistsError(f"Symbol {sym} already exists in registry")

            gen = snapshot.generations.get(sym, 0) + 1
            now_iso = datetime.now(timezone.utc).isoformat()
            entry = SymbolEntry(
                symbol=sym,
                display_name=disp,
                capital_ticker=cap,
                databento_ticker=databento_ticker,
                binance_ticker=binance_ticker,
                active=True,
                status=STATUS_ACTIVE,
                generation=gen,
                created_at=now_iso,
                updated_at=now_iso,
                metadata=meta,
            )
            snapshot.symbols[sym] = entry
            snapshot.version += 1
            snapshot.updated_at = now_iso
            self._save_snapshot(snapshot)
            self.touch_signal()
            return entry

    def toggle_symbol(self, symbol: str, active: Optional[bool] = None) -> SymbolEntry:
        """Toggles or sets the active state of a symbol under lock."""
        sym = validate_symbol(symbol)
        with RegistryControlLock(self._root, timeout=self._lock_timeout):
            snapshot = self.load()
            if sym not in snapshot.symbols:
                raise SymbolNotFoundError(f"Symbol {sym} not found in registry")
            entry = snapshot.symbols[sym]
            if entry.status == STATUS_PENDING_PURGE:
                raise SymbolPendingPurgeError(
                    f"Symbol {sym} is pending purge and cannot be toggled"
                )
            new_active = (not entry.active) if active is None else bool(active)
            entry.active = new_active
            entry.status = STATUS_ACTIVE if new_active else STATUS_INACTIVE
            now_iso = datetime.now(timezone.utc).isoformat()
            entry.updated_at = now_iso
            snapshot.version += 1
            snapshot.updated_at = now_iso
            self._save_snapshot(snapshot)
            self.touch_signal()
            return entry

    def remove_symbol(self, symbol: str) -> SymbolEntry:
        """Marks symbol as PENDING_PURGE and active=False under lock."""
        sym = validate_symbol(symbol)
        with RegistryControlLock(self._root, timeout=self._lock_timeout):
            snapshot = self.load()
            if sym not in snapshot.symbols:
                raise SymbolNotFoundError(f"Symbol {sym} not found in registry")
            entry = snapshot.symbols[sym]
            entry.status = STATUS_PENDING_PURGE
            entry.active = False
            now_iso = datetime.now(timezone.utc).isoformat()
            entry.updated_at = now_iso
            snapshot.version += 1
            snapshot.updated_at = now_iso
            self._save_snapshot(snapshot)
            self.touch_signal()
            return entry

    def complete_purge(self, symbol: str) -> None:
        """Deletes symbol completely from active registry and archives generation under lock."""
        sym = validate_symbol(symbol)
        with RegistryControlLock(self._root, timeout=self._lock_timeout):
            snapshot = self.load()
            if sym in snapshot.symbols:
                entry = snapshot.symbols[sym]
                snapshot.generations[sym] = entry.generation
                del snapshot.symbols[sym]
                now_iso = datetime.now(timezone.utc).isoformat()
                snapshot.version += 1
                snapshot.updated_at = now_iso
                self._save_snapshot(snapshot)
                self.touch_signal()

    def touch_signal(self) -> Path:
        """Touches reload signal file to wake up background runners."""
        return touch_stream_reload_signal(signal_path=self.signal_path, root=self.root)


def init_registry(
    root: Union[str, Path],
    force: bool = False,
    registry_path: Optional[Union[str, Path]] = None,
    lock_timeout: float = 5.0,
) -> RegistrySnapshot:
    """Initialize a versioned symbol registry file idempotently."""
    reg = SymbolRegistry(root=root, registry_path=registry_path, lock_timeout=lock_timeout)
    if reg.path.exists() and not force:
        return reg.load()

    with RegistryControlLock(reg.root, timeout=reg._lock_timeout):
        if reg.path.exists() and not force:
            return reg.load()

        now_iso = datetime.now(timezone.utc).isoformat()
        snapshot = RegistrySnapshot(
            version=1,
            updated_at=now_iso,
            symbols={},
            generations={},
        )
        reg._save_snapshot(snapshot)
        return snapshot


def load_registry(
    root: Optional[Union[str, Path]] = None,
    registry_path: Optional[Union[str, Path]] = None,
) -> RegistrySnapshot:
    """Load registry snapshot without holding lock or mutating state."""
    reg = SymbolRegistry(root=root, registry_path=registry_path)
    return reg.load()


def get_registry_path(root: Optional[Union[str, Path]] = None) -> Path:
    """Resolve standard registry.json location."""
    reg = SymbolRegistry(root=root)
    return reg.path


def touch_stream_reload_signal(
    signal_path: Optional[Union[str, Path]] = None,
    root: Optional[Union[str, Path]] = None,
) -> Path:
    """Touch the reload signal file to wake up background streamers."""
    if signal_path is not None:
        target = Path(signal_path).resolve()
    elif root is not None:
        target = (Path(root) / DEFAULT_SIGNAL_FILENAME).resolve()
    else:
        try:
            lake_root = resolve_tick_lake_root()
            target = (lake_root / DEFAULT_SIGNAL_FILENAME).resolve()
        except Exception:
            target = (Path.cwd() / DEFAULT_SIGNAL_FILENAME).resolve()

    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())

    # Also touch control dir signal if inside a lake root with _control
    if root is not None:
        ctrl_signal = Path(root) / "_control" / DEFAULT_SIGNAL_FILENAME
        if ctrl_signal.parent.exists():
            try:
                with open(ctrl_signal, "w", encoding="utf-8") as f:
                    f.write(datetime.now(timezone.utc).isoformat())
            except Exception:
                pass

    return target


def get_symbol_registry(
    root: Optional[Union[str, Path]] = None,
    registry_path: Optional[Union[str, Path]] = None,
    lock_timeout: float = 5.0,
) -> SymbolRegistry:
    """Obtain a SymbolRegistry instance resolved from root or environment."""
    return SymbolRegistry(root=root, registry_path=registry_path, lock_timeout=lock_timeout)


def cleanup_orphaned_registry_staging_files(
    root: Union[str, Path],
    max_age_seconds: float = 60.0,
) -> int:
    """Scans _control/ for tmp_registry_*.json older than max_age_seconds and removes them.

    Returns the count of deleted orphaned files.
    """
    root_path = Path(root).resolve()
    control_dir = root_path / "_control"
    target_dirs = []
    if control_dir.is_dir():
        target_dirs.append(control_dir)
    elif root_path.name == "_control" and root_path.is_dir():
        target_dirs.append(root_path)
    elif root_path.is_dir():
        target_dirs.append(root_path)

    now = time.time()
    deleted_count = 0
    for d in target_dirs:
        for p in d.glob("tmp_registry_*.json"):
            if not p.is_file():
                continue
            try:
                mtime = p.stat().st_mtime
                if (now - mtime) >= max_age_seconds:
                    p.unlink(missing_ok=True)
                    deleted_count += 1
            except OSError:
                pass
    return deleted_count


# Backward-compatibility alias
cleanup_orphaned_staging_files = cleanup_orphaned_registry_staging_files

