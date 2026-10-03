"""
Storage configuration and path resolution for Tick Lake.
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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
        raise NotImplementedError("LakeMetadata.to_dict not implemented yet")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LakeMetadata":
        raise NotImplementedError("LakeMetadata.from_dict not implemented yet")


def resolve_tick_lake_root(custom_root: Optional[Union[str, Path]] = None) -> Path:
    """Resolve the root path of the tick lake according to strict precedence rules."""
    raise NotImplementedError("resolve_tick_lake_root not implemented yet")


def init_tick_lake(root: Path, schema_version: int = 1, force: bool = False) -> LakeMetadata:
    """Initialize a tick lake directory hierarchy and write lake.json metadata idempotently."""
    raise NotImplementedError("init_tick_lake not implemented yet")


def load_lake_metadata(root: Path, check_maintenance: bool = True) -> LakeMetadata:
    """Load and validate lake.json without creating files or directories."""
    raise NotImplementedError("load_lake_metadata not implemented yet")


def encode_symbol(symbol: str) -> str:
    """Safely percent-encode a symbol for partition directory naming."""
    raise NotImplementedError("encode_symbol not implemented yet")


def decode_symbol(encoded_symbol: str) -> str:
    """Decode a safely percent-encoded partition symbol."""
    raise NotImplementedError("decode_symbol not implemented yet")


def get_partition_path(root: Path, symbol: str, dt: Union[date, datetime, str]) -> Path:
    """Assemble and validate partition path within root/ticks."""
    raise NotImplementedError("get_partition_path not implemented yet")
