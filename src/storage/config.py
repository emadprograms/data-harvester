"""
Storage configuration and path resolution for Tick Lake.
"""
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union
import urllib.parse
import uuid

# Environment variable names and filesystem constants
TICK_LAKE_ROOT_ENV = "TICK_LAKE_ROOT"
DATA_DIR_ENV = "DATA_DIR"
MICRON_DATA_DIR = "/Volumes/Micron-E 0256 A/data-harvester/data"
DEFAULT_LAKE_SUBDIR = "tick_lake"
LAKE_METADATA_FILENAME = "lake.json"
MAINTENANCE_GUARD_FILENAME = "in_progress.json"

SUBDIRECTORIES = [
    "ticks",
    "_staging",
    "_maintenance",
    "_migration",
    "_control",
    "_spool",
]


class StorageConfigError(Exception):
    """Base error for storage configuration and path resolution issues."""
    pass


class LakeNotFoundError(StorageConfigError):
    """Raised when lake.json cannot be found in the target lake root."""
    pass


class LakeMaintenanceInProgressError(StorageConfigError):
    """Raised when an operation is attempted while _maintenance/in_progress.json exists."""
    pass


class IncompatibleSchemaError(StorageConfigError):
    """Raised when lake schema version is not supported by this codebase."""
    pass


class PathTraversalError(ValueError):
    """Raised when a symbol or path escapes the intended storage hierarchy."""
    pass


@dataclass(frozen=True)
class LakeMetadata:
    lake_id: str
    created_at: str
    schema_version: int = 1
    format: str = "tick_lake"
    compatible_versions: List[int] = field(default_factory=lambda: [1])
    partition_layout: str = "ticks/symbol={symbol}/date={date}"
    ordering: List[str] = field(default_factory=lambda: ["timestamp", "ingest_id"])
    compression: str = "snappy"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lake_id": self.lake_id,
            "created_at": self.created_at,
            "schema_version": self.schema_version,
            "format": self.format,
            "compatible_versions": list(self.compatible_versions),
            "partition_layout": self.partition_layout,
            "ordering": list(self.ordering),
            "compression": self.compression,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LakeMetadata":
        return cls(
            lake_id=data["lake_id"],
            created_at=data["created_at"],
            schema_version=data.get("schema_version", 1),
            format=data.get("format", "tick_lake"),
            compatible_versions=data.get("compatible_versions", [1]),
            partition_layout=data.get("partition_layout", "ticks/symbol={symbol}/date={date}"),
            ordering=data.get("ordering", ["timestamp", "ingest_id"]),
            compression=data.get("compression", "snappy"),
        )


def resolve_tick_lake_root(custom_root: Optional[Union[str, Path]] = None) -> Path:
    """Resolve the root path of the tick lake according to strict precedence rules."""
    # 1: custom_root if provided
    if custom_root is not None:
        return Path(custom_root).resolve()

    # 2: TICK_LAKE_ROOT env var
    env_tick_lake = os.environ.get(TICK_LAKE_ROOT_ENV)
    if env_tick_lake:
        return Path(env_tick_lake).resolve()

    # 3: DATA_DIR env var / "tick_lake"
    env_data_dir = os.environ.get(DATA_DIR_ENV)
    if env_data_dir:
        return (Path(env_data_dir) / DEFAULT_LAKE_SUBDIR).resolve()

    # 4: MICRON_DATA_DIR / "tick_lake" if exists
    micron_path = Path(MICRON_DATA_DIR)
    if micron_path.exists():
        return (micron_path / DEFAULT_LAKE_SUBDIR).resolve()

    # 5: <repo_root>/data/tick_lake if exists and valid directory;
    # if data is a broken symlink, raise StorageConfigError
    repo_root = Path(__file__).resolve().parent.parent.parent
    repo_data = repo_root / "data"
    if repo_data.is_symlink() and not repo_data.exists():
        raise StorageConfigError(f"Storage mount missing: data symlink is broken at {repo_data}")
    if repo_data.exists() and repo_data.is_dir():
        return (repo_data / DEFAULT_LAKE_SUBDIR).resolve()

    # 6: Otherwise raise StorageConfigError
    raise StorageConfigError(
        "Unable to resolve tick lake root: no valid root specified or found in environment/hardware mounts"
    )


def init_tick_lake(root: Path, schema_version: int = 1, force: bool = False) -> LakeMetadata:
    """Initialize a tick lake directory hierarchy and write lake.json metadata idempotently."""
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    for subdir in SUBDIRECTORIES:
        (root / subdir).mkdir(parents=True, exist_ok=True)
    (root / "_control" / "receipts").mkdir(parents=True, exist_ok=True)
    (root / "_control" / "intent").mkdir(parents=True, exist_ok=True)

    lake_json_path = root / LAKE_METADATA_FILENAME
    if lake_json_path.is_file() and not force:
        with open(lake_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return LakeMetadata.from_dict(data)

    meta = LakeMetadata(
        lake_id=f"lake_{uuid.uuid4().hex[:12]}",
        created_at=datetime.now(timezone.utc).isoformat(),
        schema_version=schema_version,
    )

    staging_dir = root / "_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = staging_dir / f"tmp_lake_{uuid.uuid4().hex}.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(meta.to_dict(), f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_file, lake_json_path)
    return meta


def load_lake_metadata(root: Path, check_maintenance: bool = True) -> LakeMetadata:
    """Load and validate lake.json without creating files or directories."""
    root = Path(root).resolve()
    if check_maintenance:
        guard_path = root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
        if guard_path.exists():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")

    lake_json_path = root / LAKE_METADATA_FILENAME
    if not lake_json_path.is_file():
        raise LakeNotFoundError(f"Lake metadata file not found at {lake_json_path}")

    with open(lake_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("format") != "tick_lake":
        raise IncompatibleSchemaError(f"Unsupported lake format: {data.get('format')}")

    compatible_versions = data.get("compatible_versions", [1])
    if 1 not in compatible_versions and data.get("schema_version") != 1:
        raise IncompatibleSchemaError(
            f"Incompatible schema version: {data.get('schema_version')}"
        )

    return LakeMetadata.from_dict(data)


SAFE_SYMBOL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)


def encode_symbol(symbol: str) -> str:
    """Safely percent-encode a symbol for partition directory naming."""
    if not symbol or not isinstance(symbol, str):
        raise ValueError("Symbol cannot be empty")

    if len(symbol) > 64:
        raise ValueError("Symbol length exceeds 64 characters")

    if "\x00" in symbol:
        raise PathTraversalError("Null byte not allowed in symbol")

    if "\\" in symbol:
        raise PathTraversalError("Backslash not allowed in symbol")

    if symbol == "." or symbol == ".." or ".." in symbol or symbol.startswith("./") or "/." in symbol:
        raise PathTraversalError("Directory traversal detected in symbol")

    encoded_chars: List[str] = []
    for ch in symbol:
        if ch in SAFE_SYMBOL_CHARS:
            encoded_chars.append(ch)
        else:
            for b in ch.encode("utf-8"):
                encoded_chars.append(f"%{b:02X}")

    return "".join(encoded_chars)


def decode_symbol(encoded_symbol: str) -> str:
    """Decode a safely percent-encoded partition symbol."""
    if not encoded_symbol or not isinstance(encoded_symbol, str):
        raise ValueError("Encoded symbol cannot be empty")

    # Check for malformed hex encoding (e.g. %ZZ or trailing single hex char)
    i = 0
    n = len(encoded_symbol)
    while i < n:
        if encoded_symbol[i] == "%":
            if i + 2 >= n:
                raise ValueError("Malformed percent encoding: incomplete hex escape")
            hex_digits = encoded_symbol[i + 1 : i + 3]
            if not all(c in "0123456789ABCDEFabcdef" for c in hex_digits):
                raise ValueError(f"Malformed percent encoding: %{hex_digits}")
            i += 3
        else:
            i += 1

    decoded = urllib.parse.unquote(encoded_symbol)

    if "\x00" in decoded:
        raise PathTraversalError("Decoded symbol contains null byte")

    if "\\" in decoded:
        raise PathTraversalError("Decoded symbol contains backslash")

    if decoded == "." or decoded == ".." or ".." in decoded or decoded.startswith("./") or "/." in decoded:
        raise PathTraversalError("Decoded symbol contains directory traversal")

    re_encoded = encode_symbol(decoded)
    if re_encoded != encoded_symbol:
        raise ValueError(
            f"Decoded symbol roundtrip mismatch: expected {encoded_symbol}, got {re_encoded}"
        )

    return decoded


def get_partition_path(root: Path, symbol: str, dt: Union[date, datetime, str]) -> Path:
    """Assemble and validate partition path within root/ticks."""
    root = Path(root).resolve()
    ticks_dir = (root / "ticks").resolve()

    if isinstance(dt, datetime):
        date_str = dt.date().isoformat()
    elif isinstance(dt, date):
        date_str = dt.isoformat()
    elif isinstance(dt, str):
        if ".." in dt or "/" in dt or "\\" in dt:
            raise PathTraversalError(f"Invalid date component: {dt}")
        try:
            date_str = date.fromisoformat(dt).isoformat()
        except ValueError:
            raise PathTraversalError(f"Invalid date format: {dt}")
    else:
        raise ValueError(f"Invalid date type: {type(dt)}")

    encoded_sym = encode_symbol(symbol)
    partition_path = (ticks_dir / f"symbol={encoded_sym}" / f"date={date_str}").resolve()

    try:
        rel = partition_path.relative_to(ticks_dir)
    except ValueError:
        raise PathTraversalError(f"Partition path {partition_path} escapes {ticks_dir}")

    if not partition_path.is_relative_to(ticks_dir):
        raise PathTraversalError(f"Partition path {partition_path} escapes {ticks_dir}")

    if len(rel.parts) != 2 or not rel.parts[0].startswith("symbol=") or not rel.parts[1].startswith("date="):
        raise PathTraversalError(f"Invalid partition path structure: {rel}")

    return partition_path
