"""
Historical DuckDB to Partitioned Parquet Tick Lake Migration Tool.
Milestone v4.0 - Phase 20 (P6: Zero-Loss Migration Tooling & Rehearsal).

Provides zero-loss streaming-to-parquet historical migration with:
- Deterministic ingest ID synthesis (mig_{symbol}_{date}_{idx:08d})
- Streaming chunked export with memory-bounded fetchmany
- Resumable export with partition checkpointing
- Two-way mathematical reconciliation (EXCEPT ALL) ensuring duplicate multiplicity preservation
- Fail-fast publication guard with atomic staging promotion and immutable receipts
"""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import uuid

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.storage.config import (
    LakeMaintenanceInProgressError,
    decode_symbol,
    encode_symbol,
    init_tick_lake,
    resolve_tick_lake_root,
)
from src.storage.publication import (
    FilePublicationReceipt,
    LakeOwnershipError,
    LakePublisherLock,
    PublishReceipt,
)
from src.storage.schema import SchemaValidationError, ticks_to_table, validate_table_v1

logger = logging.getLogger("migration_tool")


def _atomic_save_json(data: Dict[str, Any], target_path: Union[str, Path]) -> None:
    """Atomically persist JSON, including a best-effort parent-directory fsync."""
    target_path = Path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.parent / f"tmp_{target_path.stem}_{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, target_path)
        try:
            directory_fd = os.open(str(target_path.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        tmp_path.unlink(missing_ok=True)


class MigrationError(RuntimeError):
    """Base error for migration authorization and checkpoint failures."""


class MigrationIdentityError(MigrationError):
    """A source, scope, schema, or resume identity does not match."""


class MigrationOwnershipError(MigrationError):
    """Another cooperative migration process owns the shared staging tree."""


class MigrationVerificationError(MigrationError):
    """Staged or published bytes no longer match their verified authorization."""


class MigrationCollisionError(MigrationError):
    """A namespaced migration destination is already occupied by other bytes."""


class SourceSnapshotChangedError(MigrationError):
    """The source DuckDB changed while the read-only migration was running."""


class MigrationOwnershipLock:
    """Non-blocking OS lock serializing cooperative access to migration staging."""

    def __init__(self, migration_dir: Path, lock_path: Optional[Path] = None):
        self.migration_dir = Path(migration_dir).resolve()
        self.lock_path = Path(lock_path or (self.migration_dir / "migration.lock"))
        self._fd: Optional[int] = None

    def acquire(self) -> bool:
        if self._fd is not None:
            return True
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except (BlockingIOError, OSError, IOError) as exc:
            os.close(fd)
            raise MigrationOwnershipError(
                f"Another migration owns staging for {self.migration_dir}"
            ) from exc
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            else:
                import msvcrt
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "MigrationOwnershipLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


@dataclass
class MigrationConfig:
    """Configuration options for a source- and scope-bound migration run."""
    source_db: Optional[Union[str, Path]] = None
    lake_root: Optional[Union[str, Path]] = None
    source_table: Optional[str] = None
    mode: str = "all"
    chunk_size: int = 100_000
    symbols: Optional[Union[str, List[str]]] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    dry_run: bool = False
    resume: bool = False
    force: bool = False
    compression: str = "snappy"
    migration_id: Optional[str] = None

    def __post_init__(self):
        if self.source_db is not None:
            self.source_db = Path(self.source_db)
        if self.lake_root is not None:
            self.lake_root = Path(self.lake_root)
        if isinstance(self.symbols, str):
            self.symbols = [item.strip().upper() for item in self.symbols.split(",") if item.strip()]
        elif self.symbols is not None:
            self.symbols = [str(item).strip().upper() for item in self.symbols if str(item).strip()]
        self.chunk_size = int(self.chunk_size)
        self.dry_run = bool(self.dry_run)
        self.resume = bool(self.resume)
        self.force = bool(self.force)
        if self.migration_id is not None:
            self.migration_id = str(self.migration_id).lower()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_db": str(self.source_db) if self.source_db else None,
            "lake_root": str(self.lake_root) if self.lake_root else None,
            "source_table": self.source_table,
            "mode": self.mode,
            "chunk_size": self.chunk_size,
            "symbols": list(self.symbols) if self.symbols is not None else None,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "dry_run": self.dry_run,
            "resume": self.resume,
            "force": self.force,
            "compression": self.compression,
            "migration_id": self.migration_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationConfig":
        return cls(
            source_db=data.get("source_db"),
            lake_root=data.get("lake_root"),
            source_table=data.get("source_table"),
            mode=data.get("mode", "all"),
            chunk_size=data.get("chunk_size", 100_000),
            symbols=data.get("symbols"),
            date_start=data.get("date_start"),
            date_end=data.get("date_end"),
            dry_run=data.get("dry_run", False),
            resume=data.get("resume", False),
            force=data.get("force", False),
            compression=data.get("compression", "snappy"),
            migration_id=data.get("migration_id"),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationConfig":
        with open(Path(path), "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


@dataclass
class PartitionPlan:
    """Plan metadata for a single symbol-date partition."""
    symbol: str
    date: str
    row_count: int
    min_timestamp: str
    max_timestamp: str
    status: str = "PENDING"
    published_batch_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "row_count": self.row_count,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "status": self.status,
            "published_batch_id": self.published_batch_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionPlan":
        return cls(
            symbol=data["symbol"],
            date=data["date"],
            row_count=data["row_count"],
            min_timestamp=str(data.get("min_timestamp", "")),
            max_timestamp=str(data.get("max_timestamp", "")),
            status=data.get("status", "PENDING"),
            published_batch_id=data.get("published_batch_id"),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "PartitionPlan":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class MigrationPlan:
    """Execution plan bound to one immutable source snapshot and selection scope."""
    created_at: str = ""
    source_db: str = ""
    lake_root: str = ""
    total_rows: int = 0
    partitions: List[Dict[str, Any]] = field(default_factory=list)
    migration_id: str = ""
    source_path: str = ""
    source_snapshot_sha256: str = ""
    source_schema_sha256: str = ""
    source_table: str = ""
    projection_version: int = 1
    scope: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "created_at": self.created_at,
            "source_db": self.source_db,
            "lake_root": self.lake_root,
            "total_rows": self.total_rows,
            "partitions": [item.to_dict() if hasattr(item, "to_dict") else item for item in self.partitions],
            "migration_id": self.migration_id,
            "source_path": self.source_path,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "source_schema_sha256": self.source_schema_sha256,
            "source_table": self.source_table,
            "projection_version": self.projection_version,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationPlan":
        return cls(
            created_at=data.get("created_at", ""),
            source_db=data.get("source_db", ""),
            lake_root=data.get("lake_root", ""),
            total_rows=data.get("total_rows", 0),
            partitions=data.get("partitions", []),
            migration_id=data.get("migration_id", ""),
            source_path=data.get("source_path", data.get("source_db", "")),
            source_snapshot_sha256=data.get("source_snapshot_sha256", ""),
            source_schema_sha256=data.get("source_schema_sha256", ""),
            source_table=data.get("source_table", ""),
            projection_version=data.get("projection_version", 1),
            scope=data.get("scope", {}),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationPlan":
        with open(Path(path), "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


@dataclass
class PartitionState:
    """State tracking for a single partition's staged chunks."""
    status: str = "PENDING"  # "PENDING" | "COMPLETED" | "FAILED"
    chunks: List[str] = field(default_factory=list)
    row_count: int = 0
    file_sizes: Dict[str, int] = field(default_factory=dict)
    sha256: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "chunks": list(self.chunks),
            "row_count": self.row_count,
            "file_sizes": dict(self.file_sizes),
            "sha256": dict(self.sha256),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionState":
        return cls(
            status=data.get("status", "PENDING"),
            chunks=data.get("chunks", []),
            row_count=data.get("row_count", 0),
            file_sizes=data.get("file_sizes", {}),
            sha256=data.get("sha256", {}),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "PartitionState":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class MigrationState:
    """Durable migration checkpoint with source identity and per-chunk evidence."""
    updated_at: str = ""
    status: str = "IN_PROGRESS"
    partitions: Dict[str, Any] = field(default_factory=dict)
    migration_id: str = ""
    source_path: str = ""
    source_snapshot_sha256: str = ""
    source_schema_sha256: str = ""
    source_table: str = ""
    projection_version: int = 1
    scope: Dict[str, Any] = field(default_factory=dict)
    plan_sha256: str = ""
    export_options: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "updated_at": self.updated_at,
            "status": self.status,
            "partitions": {
                key: (value.to_dict() if hasattr(value, "to_dict") else value)
                for key, value in self.partitions.items()
            },
            "migration_id": self.migration_id,
            "source_path": self.source_path,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "source_schema_sha256": self.source_schema_sha256,
            "source_table": self.source_table,
            "projection_version": self.projection_version,
            "scope": self.scope,
            "plan_sha256": self.plan_sha256,
            "export_options": self.export_options,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationState":
        return cls(
            updated_at=data.get("updated_at", ""),
            status=data.get("status", "IN_PROGRESS"),
            partitions=data.get("partitions", {}),
            migration_id=data.get("migration_id", ""),
            source_path=data.get("source_path", ""),
            source_snapshot_sha256=data.get("source_snapshot_sha256", ""),
            source_schema_sha256=data.get("source_schema_sha256", ""),
            source_table=data.get("source_table", ""),
            projection_version=data.get("projection_version", 1),
            scope=data.get("scope", {}),
            plan_sha256=data.get("plan_sha256", ""),
            export_options=data.get("export_options", {}),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationState":
        with open(Path(path), "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


@dataclass
class PartitionVerificationResult:
    """Verification reconciliation result for a single partition."""
    symbol: str
    date: str
    status: str = "PASSED"  # "PASSED" | "FAILED"
    source_count: int = 0
    parquet_count: int = 0
    discrepancies: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "status": self.status,
            "source_count": self.source_count,
            "parquet_count": self.parquet_count,
            "discrepancies": self.discrepancies,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionVerificationResult":
        return cls(
            symbol=data["symbol"],
            date=data["date"],
            status=data.get("status", "PASSED"),
            source_count=data.get("source_count", 0),
            parquet_count=data.get("parquet_count", 0),
            discrepancies=data.get("discrepancies", []),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "PartitionVerificationResult":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class VerificationResult:
    """Two-way EXCEPT ALL result bound to the exact migration authorization."""
    status: str = "FAILED"
    total_source_rows: int = 0
    total_parquet_rows: int = 0
    discrepancies: List[Dict[str, Any]] = field(default_factory=list)
    verified_at: str = ""
    migration_id: str = ""
    source_path: str = ""
    source_snapshot_sha256: str = ""
    source_schema_sha256: str = ""
    source_table: str = ""
    projection_version: int = 1
    scope: Dict[str, Any] = field(default_factory=dict)
    plan_sha256: str = ""
    files: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "total_source_rows": self.total_source_rows,
            "total_parquet_rows": self.total_parquet_rows,
            "discrepancies": self.discrepancies,
            "verified_at": self.verified_at,
            "migration_id": self.migration_id,
            "source_path": self.source_path,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "source_schema_sha256": self.source_schema_sha256,
            "source_table": self.source_table,
            "projection_version": self.projection_version,
            "scope": self.scope,
            "plan_sha256": self.plan_sha256,
            "files": self.files,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VerificationResult":
        return cls(
            status=data.get("status", "FAILED"),
            total_source_rows=data.get("total_source_rows", 0),
            total_parquet_rows=data.get("total_parquet_rows", 0),
            discrepancies=data.get("discrepancies", []),
            verified_at=data.get("verified_at", ""),
            migration_id=data.get("migration_id", ""),
            source_path=data.get("source_path", ""),
            source_snapshot_sha256=data.get("source_snapshot_sha256", ""),
            source_schema_sha256=data.get("source_schema_sha256", ""),
            source_table=data.get("source_table", ""),
            projection_version=data.get("projection_version", 1),
            scope=data.get("scope", {}),
            plan_sha256=data.get("plan_sha256", ""),
            files=data.get("files", []),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "VerificationResult":
        with open(Path(path), "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


class MigrationOrchestrator:
    """Coordinates an immutable, source-bound DuckDB-to-Parquet migration."""

    PROJECTION_VERSION = 1

    def __init__(self, config: MigrationConfig):
        self.config = config
        if self.config.source_db is not None:
            self.source_db = Path(self.config.source_db).resolve()
        else:
            data_dir_env = os.environ.get("DATA_DIR")
            if data_dir_env:
                self.source_db = (Path(data_dir_env) / "streaming.duckdb").resolve()
            else:
                repo_root = Path(__file__).resolve().parent.parent
                self.source_db = (repo_root / "data" / "streaming.duckdb").resolve()

        self.lake_root = resolve_tick_lake_root(self.config.lake_root)
        self.migration_dir = self.lake_root / "_migration"
        self.staging_dir = self.migration_dir / "staging"
        self.plan_file = self.migration_dir / "plan.json"  # latest-run compatibility view
        self.state_file = self.migration_dir / "state.json"  # latest-run compatibility view
        self.verification_file = self.migration_dir / "verification.json"  # latest-run compatibility view
        self.runs_dir = self.migration_dir / "runs"
        self.active_file = self.migration_dir / "active.json"
        self.lock_file = self.migration_dir / "migration.lock"
        self.coverage_file = self.migration_dir / "coverage.json"

        self.migration_id: Optional[str] = None
        self.run_dir: Optional[Path] = None
        self.run_plan_file: Optional[Path] = None
        self.run_state_file: Optional[Path] = None
        self.run_verification_file: Optional[Path] = None
        self.publish_journal_file: Optional[Path] = None
        self.source_table: Optional[str] = None
        self.schema_info: Dict[str, Any] = {}
        self.scope: Dict[str, Any] = {}
        self.identity: Dict[str, Any] = {}

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + str(identifier).replace('"', '""') + '"'

    @staticmethod
    def _canonical_json(data: Any) -> bytes:
        return json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")

    @classmethod
    def _json_sha256(cls, data: Any) -> str:
        return hashlib.sha256(cls._canonical_json(data)).hexdigest()

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    @classmethod
    def _schema_sha256(cls, schema: pa.Schema) -> str:
        return cls._json_sha256([
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ])

    def _source_snapshot_sha256(self) -> str:
        """Fingerprint the closed DuckDB file and its WAL sidecar, if present."""
        if not self.source_db.is_file():
            raise FileNotFoundError(f"Source database not found at {self.source_db}")
        candidates = [self.source_db, Path(str(self.source_db) + ".wal")]
        manifest = []
        for path in candidates:
            if path.exists():
                manifest.append({
                    "name": path.name,
                    "size": path.stat().st_size,
                    "sha256": self._file_sha256(path),
                })
        return self._json_sha256(manifest)

    def _source_fingerprint(self) -> str:
        """Stable fingerprint of the source database snapshot, schema, table, and projection."""
        db_sha = self._file_sha256(self.source_db) if self.source_db and self.source_db.is_file() else ""
        wal_path = Path(str(self.source_db) + ".wal") if self.source_db else None
        wal_sha = self._file_sha256(wal_path) if wal_path and wal_path.is_file() else ""
        payload = {
            "db_sha256": db_sha,
            "wal_sha256": wal_sha,
            "source_schema_sha256": self.identity.get("source_schema_sha256", ""),
            "source_table": self.source_table or "",
            "projection_version": self.PROJECTION_VERSION,
        }
        return self._json_sha256(payload)

    def _load_coverage(self) -> Dict[str, Any]:
        """Load coverage ledger from <lake_root>/_migration/coverage.json or bootstrap from receipts."""
        if self.coverage_file.is_file():
            try:
                data = json.loads(self.coverage_file.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception as exc:
                raise MigrationError(f"Cannot read migration coverage ledger {self.coverage_file}: {exc}") from exc
        return self._bootstrap_coverage_from_receipts()

    def _bootstrap_coverage_from_receipts(self) -> Dict[str, Any]:
        """Bootstrap coverage ledger from valid published migration receipts if coverage.json is absent."""
        receipts_dir = self.lake_root / "_control" / "receipts"
        partitions: Dict[str, Any] = {}
        if receipts_dir.is_dir():
            for receipt_path in sorted(receipts_dir.glob("migration_*.json")):
                try:
                    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    if receipt.get("status") != "PUBLISHED":
                        continue
                    batch_id = receipt.get("batch_id", "")
                    snap = receipt.get("source_snapshot_sha256", "")
                    for detail in receipt.get("file_details", []):
                        sym = detail.get("symbol")
                        dt = detail.get("date")
                        if not sym or not dt:
                            continue
                        key = self._partition_key(sym, dt)
                        rel = detail.get("relative_path")
                        if not rel or not (self.lake_root / rel).is_file():
                            continue
                        if key not in partitions:
                            partitions[key] = {
                                "symbol": sym,
                                "date": dt,
                                "source_rows": 0,
                                "first_timestamp": "",
                                "last_timestamp": "",
                                "published_batch_id": batch_id,
                                "status": "COVERED",
                                "source_snapshot_sha256": snap,
                                "source_fingerprint": "",
                                "files": [],
                            }
                        partitions[key]["source_rows"] += int(detail.get("row_count", 0))
                        partitions[key]["files"].append({
                            "final_path": rel,
                            "row_count": int(detail.get("row_count", 0)),
                            "sha256": detail.get("sha256", ""),
                            "size_bytes": int(detail.get("file_size_bytes", 0)),
                        })
                except Exception:
                    continue
        return {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source_fingerprint": self._source_fingerprint() if (self.source_db and self.source_db.is_file()) else "",
            "partitions": partitions,
        }

    def _save_coverage(self, coverage: Dict[str, Any]) -> None:
        """Atomically persist coverage ledger with both 'partitions' dict and top-level keys."""
        coverage["updated_at"] = datetime.now(timezone.utc).isoformat()
        data = dict(coverage)
        partitions = data.get("partitions", {})
        for key, val in partitions.items():
            data[key] = val
        _atomic_save_json(data, self.coverage_file)

    def _check_partition_coverage(
        self,
        symbol: str,
        date_value: str,
        candidate: Dict[str, Any],
        coverage: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Check if a candidate partition is already marked COVERED for this source fingerprint
        and existing published files match.
        """
        key = self._partition_key(symbol, date_value)
        parts = coverage.get("partitions", coverage)
        record = parts.get(key)
        if not record or not isinstance(record, dict):
            return None
        if record.get("status") != "COVERED":
            return None

        # Verify source fingerprint or source snapshot match
        rec_fp = record.get("source_fingerprint")
        rec_snap = record.get("source_snapshot_sha256")
        cur_fp = self._source_fingerprint() if (self.source_db and self.source_db.is_file()) else ""
        cur_snap = self.identity.get("source_snapshot_sha256")
        if rec_fp and cur_fp and rec_fp != cur_fp:
            return None
        if not rec_fp and rec_snap and cur_snap and rec_snap != cur_snap:
            return None

        cand_rows = int(candidate.get("row_count", 0))
        rec_rows = int(record.get("source_rows", 0))
        if cand_rows != rec_rows:
            raise MigrationError(
                f"Source partition {key} has {cand_rows} rows but coverage ledger records {rec_rows} rows. "
                "Source data changed since migration; actionable reconciliation required."
            )

        files = record.get("files", [])
        if files:
            file_rows = 0
            for file_info in files:
                rel = file_info.get("final_path")
                if not rel:
                    continue
                file_path = self._safe_path(self.lake_root, rel)
                if not file_path.is_file():
                    raise MigrationError(
                        f"Partition {key} marked COVERED in ledger, but published file is missing: {file_path}"
                    )
                if file_info.get("size_bytes") and file_path.stat().st_size != int(file_info["size_bytes"]):
                    raise MigrationError(
                        f"Partition {key} published file size modified: {file_path}"
                    )
                if file_info.get("sha256") and self._file_sha256(file_path) != file_info["sha256"]:
                    raise MigrationError(
                        f"Partition {key} published file content modified: {file_path}"
                    )
                file_rows += int(file_info.get("row_count", 0))
            if file_rows != rec_rows:
                raise MigrationError(
                    f"Partition {key} published files total {file_rows} rows, expected {rec_rows}"
                )
        else:
            part_dir = self.lake_root / "ticks" / f"symbol={encode_symbol(symbol)}" / f"date={date_value}"
            if not part_dir.is_dir() or not list(part_dir.glob("*.parquet")):
                raise MigrationError(
                    f"Partition {key} marked COVERED but no published parquet files found in {part_dir}"
                )

        return record

    def _normalized_scope(self) -> Dict[str, Any]:
        symbols = sorted(set(self.config.symbols or [])) or None
        start = self.config.date_start
        end = self.config.date_end
        for label, value in (("date_start", start), ("date_end", end)):
            if value is not None:
                try:
                    if datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d") != value:
                        raise ValueError
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{label} must be an ISO date (YYYY-MM-DD), got {value!r}") from exc
        if start and end and start > end:
            raise ValueError("date_start must be less than or equal to date_end")
        return {"symbols": symbols, "date_start": start, "date_end": end}

    def _ensure_identity(self) -> None:
        if self.migration_id is not None:
            return
        if self.config.chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")

        scope = self._normalized_scope()
        before = self._source_snapshot_sha256()
        con = duckdb.connect(str(self.source_db), read_only=True)
        try:
            con.execute("BEGIN TRANSACTION")
            table = self._detect_source_table(con)
            exists = con.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='main' AND lower(table_name)=lower(?)",
                [table],
            ).fetchone()[0] > 0
            if exists:
                schema_info = self._inspect_source_schema(con, table)
            else:
                schema_info = {
                    "table": table,
                    "columns": {},
                    "source_columns": [],
                    "projections": [],
                    "projection_sql": "",
                    "ts_col": None,
                    "sym_col": None,
                    "price_col": None,
                }
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()

        after = self._source_snapshot_sha256()
        if before != after:
            raise SourceSnapshotChangedError(
                "Source DuckDB changed while migration identity was being established; "
                "close/checkpoint the source and retry from a frozen snapshot"
            )

        schema_identity = {
            "table": table,
            "source_columns": schema_info.get("source_columns", []),
            "resolved_columns": schema_info.get("columns", {}),
            "projection_version": self.PROJECTION_VERSION,
            "projection_sql": schema_info.get("projection_sql", ""),
        }
        schema_sha = self._json_sha256(schema_identity)
        identity = {
            "source_path": str(self.source_db),
            "source_snapshot_sha256": before,
            "source_schema_sha256": schema_sha,
            "source_table": table,
            "projection_version": self.PROJECTION_VERSION,
            "scope": scope,
        }
        requested_id = self.config.migration_id
        if requested_id is not None:
            requested_id = str(requested_id).lower()
            if not re.fullmatch(r"[0-9a-f]{32}", requested_id):
                raise ValueError("migration_id must be a 32-character hexadecimal UUID")
            migration_id = requested_id
        else:
            migration_id = self._json_sha256(identity)[:32]

        self.migration_id = migration_id
        self.run_dir = self.runs_dir / migration_id
        self.run_plan_file = self.run_dir / "plan.json"
        self.run_state_file = self.run_dir / "state.json"
        self.run_verification_file = self.run_dir / "verification.json"
        self.publish_journal_file = self.run_dir / "publish_journal.json"
        self.source_table = table
        self.schema_info = schema_info
        self.scope = scope
        self.identity = {**identity, "migration_id": migration_id}

    @contextmanager
    def _source_session(self):
        """Use one read transaction and reject source changes across the operation."""
        self._ensure_identity()
        before = self._source_snapshot_sha256()
        if before != self.identity["source_snapshot_sha256"]:
            raise SourceSnapshotChangedError(
                "Source fingerprint differs from the planned migration snapshot"
            )
        con = duckdb.connect(str(self.source_db), read_only=True)
        try:
            con.execute("BEGIN TRANSACTION")
            yield con
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()
        after = self._source_snapshot_sha256()
        if after != before:
            raise SourceSnapshotChangedError(
                "Source DuckDB changed during migration; staged output is not authorized"
            )

    def _detect_source_table(self, con: duckdb.DuckDBPyConnection) -> str:
        """Auto-detect the source tick table, preserving the historical precedence."""
        if self.config.source_table:
            return self.config.source_table
        tables = [row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()]
        if not tables:
            tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        by_lower = {name.lower(): name for name in tables}
        for candidate in ("tick_data", "ticks", "streaming_ticks"):
            if candidate in by_lower:
                return by_lower[candidate]
        return tables[0] if tables else "tick_data"

    def _inspect_source_schema(self, con: duckdb.DuckDBPyConnection, table: str) -> Dict[str, Any]:
        """Resolve legacy aliases and return a canonical eight-column projection."""
        cols_rows = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='main' AND lower(table_name)=lower(?) ORDER BY ordinal_position",
            [table],
        ).fetchall()
        if not cols_rows:
            raise SchemaValidationError(f"Source table or view {table!r} has no inspectable columns")
        cols_dict = {str(name).lower(): (str(name), str(dtype).upper()) for name, dtype in cols_rows}
        alias_map = {
            "timestamp": ["timestamp", "ts", "time", "datetime", "created_at"],
            "symbol": ["symbol", "sym", "ticker"],
            "price": ["price", "last", "rate", "px", "close"],
            "volume": ["volume", "vol", "size", "qty"],
            "bid": ["bid", "bid_price", "bid_px"],
            "ask": ["ask", "ask_price", "ask_px"],
            "source": ["source", "feed", "exchange", "src"],
            "session": ["session", "sess"],
        }
        resolved: Dict[str, Optional[str]] = {}
        for canonical, aliases in alias_map.items():
            resolved[canonical] = next(
                (cols_dict[alias][0] for alias in aliases if alias in cols_dict), None
            )
        for required in ("timestamp", "symbol", "price"):
            if not resolved[required]:
                raise SchemaValidationError(
                    f"Required column '{required}' missing in source table '{table}'. "
                    f"Available columns: {list(cols_dict)}"
                )

        projections = [
            f"CAST({self._quote_identifier(resolved['timestamp'])} AS TIMESTAMP) AS timestamp",
            f"CAST({self._quote_identifier(resolved['symbol'])} AS VARCHAR) AS symbol",
            f"CAST({self._quote_identifier(resolved['price'])} AS DOUBLE) AS price",
        ]
        for canonical in ("volume", "bid", "ask"):
            column = resolved[canonical]
            projections.append(
                f"CAST({self._quote_identifier(column)} AS DOUBLE) AS {canonical}"
                if column else f"CAST(NULL AS DOUBLE) AS {canonical}"
            )
        for canonical, fallback in (("source", "LEGACY"), ("session", "REG")):
            column = resolved[canonical]
            projections.append(
                f"CAST({self._quote_identifier(column)} AS VARCHAR) AS {canonical}"
                if column else f"CAST('{fallback}' AS VARCHAR) AS {canonical}"
            )
        return {
            "table": table,
            "columns": resolved,
            "source_columns": [{"name": name, "type": dtype} for name, dtype in cols_rows],
            "projections": projections,
            "projection_sql": ", ".join(projections),
            "ts_col": resolved["timestamp"],
            "sym_col": resolved["symbol"],
            "price_col": resolved["price"],
        }

    def _plan_sha256(self, plan: MigrationPlan) -> str:
        canonical_partitions = [
            {
                "symbol": str(p["symbol"]),
                "date": str(p["date"]),
                "row_count": int(p["row_count"]),
                "min_timestamp": str(p.get("min_timestamp", "")),
                "max_timestamp": str(p.get("max_timestamp", "")),
            }
            for p in plan.partitions
        ]
        return self._json_sha256({
            "migration_id": plan.migration_id,
            "source_path": plan.source_path,
            "source_snapshot_sha256": plan.source_snapshot_sha256,
            "source_schema_sha256": plan.source_schema_sha256,
            "source_table": plan.source_table,
            "projection_version": plan.projection_version,
            "scope": plan.scope,
            "partitions": canonical_partitions,
            "total_rows": plan.total_rows,
        })

    def _compute_plan(self, con: duckdb.DuckDBPyConnection) -> MigrationPlan:
        self._ensure_identity()
        partitions: List[Dict[str, Any]] = []
        table = self.source_table or "tick_data"
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='main' AND lower(table_name)=lower(?)",
            [table],
        ).fetchone()[0] > 0
        if table_exists:
            info = self.schema_info
            table_sql = self._quote_identifier(table)
            ts_sql = self._quote_identifier(info["ts_col"])
            symbol_sql = self._quote_identifier(info["sym_col"])
            dirty_query = (
                f"SELECT count(*) FROM {table_sql} WHERE {ts_sql} IS NULL OR {symbol_sql} IS NULL "
                f"OR length(trim(CAST({symbol_sql} AS VARCHAR)))=0"
            )
            dirty_count = con.execute(dirty_query).fetchone()[0]
            if dirty_count:
                raise SchemaValidationError(
                    f"Source table '{table}' contains {dirty_count} dirty rows with NULL timestamp or empty symbol"
                )

            query = (
                f"SELECT symbol, strftime(timestamp, '%Y-%m-%d') AS date, count(*) AS row_count, "
                f"min(timestamp) AS min_ts, max(timestamp) AS max_ts "
                f"FROM (SELECT {info['projection_sql']} FROM {table_sql}) AS src"
            )
            clauses = []
            params: List[Any] = []
            symbols = self.scope["symbols"]
            if symbols:
                clauses.append("symbol IN (" + ",".join("?" for _ in symbols) + ")")
                params.extend(symbols)
            if self.scope["date_start"]:
                clauses.append("strftime(timestamp, '%Y-%m-%d') >= ?")
                params.append(self.scope["date_start"])
            if self.scope["date_end"]:
                clauses.append("strftime(timestamp, '%Y-%m-%d') <= ?")
                params.append(self.scope["date_end"])
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " GROUP BY symbol, date ORDER BY symbol, date"
            coverage = self._load_coverage()
            for symbol, date_value, count, minimum, maximum in con.execute(query, params).fetchall():
                cand = {
                    "symbol": str(symbol),
                    "date": str(date_value),
                    "row_count": int(count),
                    "min_timestamp": minimum.isoformat() if hasattr(minimum, "isoformat") else str(minimum),
                    "max_timestamp": maximum.isoformat() if hasattr(maximum, "isoformat") else str(maximum),
                }
                cov_rec = self._check_partition_coverage(str(symbol), str(date_value), cand, coverage)
                if cov_rec is not None:
                    cand["status"] = "COVERED"
                    cand["published_batch_id"] = cov_rec.get("published_batch_id")
                else:
                    cand["status"] = "PENDING"
                    cand["published_batch_id"] = None
                partitions.append(PartitionPlan(**cand).to_dict())

        return MigrationPlan(
            created_at=datetime.now(timezone.utc).isoformat(),
            source_db=str(self.source_db),
            lake_root=str(self.lake_root),
            total_rows=sum(part["row_count"] for part in partitions),
            partitions=partitions,
            migration_id=self.migration_id or "",
            source_path=str(self.source_db),
            source_snapshot_sha256=self.identity["source_snapshot_sha256"],
            source_schema_sha256=self.identity["source_schema_sha256"],
            source_table=table,
            projection_version=self.PROJECTION_VERSION,
            scope=self.scope,
        )

    def _new_state(self, status: str, plan: Optional[MigrationPlan] = None) -> MigrationState:
        return MigrationState(
            updated_at=datetime.now(timezone.utc).isoformat(),
            status=status,
            partitions={},
            migration_id=self.migration_id or "",
            source_path=str(self.source_db),
            source_snapshot_sha256=self.identity.get("source_snapshot_sha256", ""),
            source_schema_sha256=self.identity.get("source_schema_sha256", ""),
            source_table=self.source_table or "",
            projection_version=self.PROJECTION_VERSION,
            scope=self.scope,
            plan_sha256=self._plan_sha256(plan) if plan else "",
            export_options={
                "chunk_size": self.config.chunk_size,
                "compression": self.config.compression,
                "projection_version": self.PROJECTION_VERSION,
            },
        )

    def _write_plan(self, plan: MigrationPlan) -> None:
        assert self.run_plan_file is not None
        self.run_plan_file.parent.mkdir(parents=True, exist_ok=True)
        plan.save(self.run_plan_file)
        plan.save(self.plan_file)

    def _write_state(self, state: MigrationState) -> None:
        assert self.run_state_file is not None
        state.updated_at = datetime.now(timezone.utc).isoformat()
        state.save(self.run_state_file)
        state.save(self.state_file)

    def _load_state_for(self, migration_id: str) -> Optional[MigrationState]:
        path = self.runs_dir / migration_id / "state.json"
        if not path.is_file():
            return None
        try:
            return MigrationState.load(path)
        except Exception as exc:
            raise MigrationError(f"Cannot read migration checkpoint {path}: {exc}") from exc

    def _load_state(self) -> Optional[MigrationState]:
        self._ensure_identity()
        assert self.run_state_file is not None
        if self.run_state_file.is_file():
            try:
                return MigrationState.load(self.run_state_file)
            except Exception as exc:
                raise MigrationError(f"Cannot read migration checkpoint {self.run_state_file}: {exc}") from exc
        # Read a prior single-run checkpoint only when it is already bound to this UUID.
        if self.state_file.is_file():
            try:
                compatibility = MigrationState.load(self.state_file)
            except Exception as exc:
                raise MigrationError(f"Cannot read latest migration checkpoint {self.state_file}: {exc}") from exc
            if compatibility.migration_id == self.migration_id:
                return compatibility
        return None

    def _validate_identity_fields(self, payload: Dict[str, Any], label: str) -> None:
        expected = self.identity
        for key in (
            "migration_id", "source_path", "source_snapshot_sha256",
            "source_schema_sha256", "source_table", "projection_version", "scope",
        ):
            if payload.get(key) != expected.get(key):
                raise MigrationIdentityError(
                    f"{label} identity mismatch for {key}: expected {expected.get(key)!r}, "
                    f"found {payload.get(key)!r}"
                )

    def _read_active(self) -> Optional[Dict[str, Any]]:
        if not self.active_file.is_file():
            return None
        try:
            payload = json.loads(self.active_file.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not payload.get("migration_id"):
                raise ValueError("missing migration_id")
            return payload
        except Exception as exc:
            raise MigrationError(f"Active migration marker is corrupt: {self.active_file}: {exc}") from exc

    def _write_active(self) -> None:
        _atomic_save_json({
            "migration_id": self.migration_id,
            "source_snapshot_sha256": self.identity["source_snapshot_sha256"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, self.active_file)

    def _assert_active_can_export(self) -> None:
        active = self._read_active()
        if not active or active.get("migration_id") == self.migration_id:
            return
        previous = self._load_state_for(str(active["migration_id"]))
        if previous is None or previous.status != "PUBLISHED":
            raise MigrationOwnershipError(
                f"Migration {active['migration_id']} owns shared staging and is not published; "
                "resume or finish it before exporting another migration"
            )
        self._cleanup_published_stage(previous)

    def _find_resume_conflict(self) -> Optional[str]:
        """Explain why --resume cannot attach to the current source/scope identity."""
        active = self._read_active()
        if active and active.get("migration_id") != self.migration_id:
            previous = self._load_state_for(str(active["migration_id"]))
            if previous is None or previous.status != "PUBLISHED":
                return f"active migration is {active.get('migration_id')}"
        if self.runs_dir.is_dir():
            for checkpoint in self.runs_dir.glob("*/state.json"):
                try:
                    state = MigrationState.load(checkpoint)
                except Exception as exc:
                    raise MigrationError(f"Cannot inspect prior checkpoint {checkpoint}: {exc}") from exc
                if state.source_path != str(self.source_db):
                    continue
                if state.scope != self.scope:
                    return "filter scope differs from the previous migration for this source"
                if state.source_snapshot_sha256 != self.identity["source_snapshot_sha256"]:
                    return "source snapshot fingerprint differs from the previous migration"
                if state.source_schema_sha256 != self.identity["source_schema_sha256"]:
                    return "source schema/projection differs from the previous migration"
        return None

    def _validate_resume_state(self, state: MigrationState) -> None:
        self._validate_identity_fields(state.to_dict(), "Resume checkpoint")
        if state.plan_sha256:
            plan = self._load_plan(required=True)
            if self._plan_sha256(plan) != state.plan_sha256:
                raise MigrationIdentityError("Resume checkpoint plan fingerprint changed")
        old_options = state.export_options or {}
        requested_options = {
            "chunk_size": self.config.chunk_size,
            "compression": self.config.compression,
            "projection_version": self.PROJECTION_VERSION,
        }
        if old_options and old_options != requested_options:
            raise MigrationIdentityError(
                f"Resume export options changed: checkpoint={old_options}, requested={requested_options}"
            )

    def _load_plan(self, required: bool = True) -> Optional[MigrationPlan]:
        self._ensure_identity()
        assert self.run_plan_file is not None
        if not self.run_plan_file.is_file():
            if required:
                raise MigrationError(f"Migration plan not found for {self.migration_id}")
            return None
        try:
            plan = MigrationPlan.load(self.run_plan_file)
        except Exception as exc:
            raise MigrationError(f"Cannot read migration plan {self.run_plan_file}: {exc}") from exc
        self._validate_identity_fields(plan.to_dict(), "Migration plan")
        if plan.lake_root != str(self.lake_root):
            raise MigrationIdentityError("Migration plan targets a different lake root")
        return plan

    def _ensure_plan_locked(self) -> MigrationPlan:
        plan = self._load_plan(required=False)
        if plan is None or self.config.force:
            with self._source_session() as con:
                plan = self._compute_plan(con)
            self._write_plan(plan)
        return plan

    def plan(self) -> MigrationPlan:
        """Analyze a frozen source snapshot and optionally persist the run-bound plan."""
        self._ensure_identity()
        if self.config.dry_run:
            with self._source_session() as con:
                return self._compute_plan(con)
        with MigrationOwnershipLock(self.migration_dir, self.lock_file):
            plan = self._ensure_plan_locked()
            # Keep the documented latest-run compatibility view repairable even
            # when its atomic write was interrupted after the run-bound plan saved.
            plan.save(self.plan_file)
            state = self._load_state()
            if state is None:
                self._write_state(self._new_state("PLANNED", plan))
            else:
                self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
                if not state.plan_sha256:
                    state.plan_sha256 = self._plan_sha256(plan)
                    self._write_state(state)
            return plan

    @staticmethod
    def _partition_key(symbol: str, date_value: str) -> str:
        return f"symbol={encode_symbol(symbol)}/date={date_value}"

    def _staging_relative(self, symbol: str, date_value: str, name: str) -> str:
        return (
            f"staging/ticks/symbol={encode_symbol(symbol)}/date={date_value}/{name}"
        )

    def _safe_path(self, base: Path, relative_value: str) -> Path:
        relative = Path(str(relative_value))
        if relative.is_absolute() or ".." in relative.parts or "\\" in str(relative_value):
            raise MigrationError(f"Unsafe migration path {relative_value!r}")
        resolved_base = Path(base).resolve()
        candidate = (resolved_base / relative).resolve(strict=False)
        if not candidate.is_relative_to(resolved_base):
            raise MigrationError(f"Migration path escapes its owner directory: {relative_value!r}")
        current = resolved_base
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise MigrationError(f"Symlink is not permitted in migration path: {current}")
        return candidate

    def _stage_directory(self, symbol: str, date_value: str) -> Path:
        return self.staging_dir / "ticks" / f"symbol={encode_symbol(symbol)}" / f"date={date_value}"

    def _remove_verification_authorization(self) -> None:
        assert self.run_verification_file is not None
        for path in (self.run_verification_file, self.verification_file):
            try:
                path.unlink(missing_ok=True)
                if path.parent.is_dir():
                    try:
                        directory_fd = os.open(str(path.parent), os.O_RDONLY)
                        try:
                            os.fsync(directory_fd)
                        finally:
                            os.close(directory_fd)
                    except OSError:
                        pass
            except OSError as exc:
                raise MigrationError(
                    f"Cannot invalidate stale verification authorization {path}; export aborted"
                ) from exc

    def _actual_stage_paths(self) -> List[Path]:
        ticks = self.staging_dir / "ticks"
        if not ticks.exists():
            return []
        return sorted(ticks.rglob("*.parquet"))

    def _clear_partition_stage(self, symbol: str, date_value: str) -> None:
        directory = self._stage_directory(symbol, date_value)
        if directory.exists():
            for path in directory.rglob("*.parquet"):
                if path.is_symlink():
                    raise MigrationError(f"Refusing to unlink symlinked staged chunk {path}")
                path.unlink()
            for path in sorted(directory.rglob("*"), reverse=True):
                if path.is_dir() and not path.is_symlink():
                    try:
                        path.rmdir()
                    except OSError:
                        pass
            try:
                directory.rmdir()
            except OSError:
                pass

    def _cleanup_published_stage(self, state: MigrationState) -> None:
        """Remove only files named by a completed migration checkpoint."""
        known = set()
        for partition_state in state.partitions.values():
            for detail in partition_state.get("files", []):
                rel = detail.get("staging_path")
                if rel:
                    known.add(str(rel))
        actual = {
            f"staging/{path.relative_to(self.staging_dir).as_posix()}"
            for path in self._actual_stage_paths()
        }
        if not actual:
            return
        if not actual.issubset(known):
            raise MigrationOwnershipError(
                "Shared migration staging contains files not owned by the published checkpoint"
            )
        for relative in actual:
            path = self._safe_path(self.migration_dir, relative)
            path.unlink(missing_ok=True)
        for path in sorted((self.staging_dir / "ticks").rglob("*"), reverse=True):
            if path.is_dir() and not path.is_symlink():
                try:
                    path.rmdir()
                except OSError:
                    pass

    def _checkpoint_file(self, path: Path, symbol: str, date_value: str, chunk_name: str) -> Dict[str, Any]:
        if path.is_symlink() or not path.is_file():
            raise MigrationError(f"Staged migration chunk is missing or unsafe: {path}")
        parquet = pq.ParquetFile(path)
        table = parquet.read()
        validate_table_v1(table)
        if any(row["symbol"] != symbol for row in table.select(["symbol"]).to_pylist()):
            raise MigrationError(f"Staged chunk contains rows outside symbol partition {symbol}: {path}")
        if any(row["timestamp"].date().isoformat() != date_value for row in table.select(["timestamp"]).to_pylist()):
            raise MigrationError(f"Staged chunk contains rows outside date partition {date_value}: {path}")
        return {
            "name": chunk_name,
            "staging_path": f"staging/{path.relative_to(self.staging_dir).as_posix()}",
            "symbol": symbol,
            "date": date_value,
            "row_count": table.num_rows,
            "size_bytes": path.stat().st_size,
            "sha256": self._file_sha256(path),
            "schema_sha256": self._schema_sha256(table.schema),
        }

    def _validate_completed_partition(self, key: str, part: Dict[str, Any], checkpoint: Dict[str, Any]) -> None:
        if checkpoint.get("status") != "COMPLETED":
            return
        symbol = part["symbol"]
        date_value = part["date"]
        files = checkpoint.get("files")
        if not isinstance(files, list) or not files:
            raise MigrationError(f"Completed checkpoint {key} has no verifiable chunk inventory")
        expected_names = list(checkpoint.get("chunks", []))
        if [detail.get("name") for detail in files] != expected_names:
            raise MigrationError(f"Completed checkpoint {key} has an inconsistent chunk inventory")
        actual = sorted(self._stage_directory(symbol, date_value).glob("*.parquet"))
        if [path.name for path in actual] != sorted(expected_names):
            raise MigrationError(f"Completed checkpoint {key} has missing or additional staged chunks")
        total = 0
        for detail, path in zip(sorted(files, key=lambda value: value["name"]), actual):
            observed = self._checkpoint_file(path, symbol, date_value, path.name)
            for field_name in ("staging_path", "row_count", "size_bytes", "sha256", "schema_sha256"):
                if observed.get(field_name) != detail.get(field_name):
                    raise MigrationError(
                        f"Completed checkpoint {key} failed {field_name} verification for {path.name}"
                    )
            total += observed["row_count"]
        if total != int(checkpoint.get("row_count", -1)) or total != int(part["row_count"]):
            raise MigrationError(f"Completed checkpoint {key} row count does not match the source plan")

    def export(self) -> MigrationState:
        """Export to shared staging under migration ownership with validated checkpoints."""
        self._ensure_identity()
        if self.config.dry_run:
            with self._source_session() as con:
                plan = self._compute_plan(con)
            return self._new_state("DRY_RUN", plan)

        with MigrationOwnershipLock(self.migration_dir, self.lock_file):
            self._assert_active_can_export()
            plan = self._ensure_plan_locked()
            state = self._load_state()
            if state is not None:
                self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
                if state.status == "PUBLISHED":
                    self._validate_published_receipt(state, plan)
                    return state
                if state.status == "PUBLISHING" and self.config.resume:
                    return state
                if self.config.resume:
                    if state.status not in {"PLANNED", "IN_PROGRESS", "VERIFICATION_FAILED", "VERIFIED"}:
                        raise MigrationError(f"Cannot resume migration in state {state.status!r}")
                    self._validate_resume_state(state)
                else:
                    if self.publish_journal_file is not None and self.publish_journal_file.exists():
                        raise MigrationError("A publish journal exists; use --resume to finish or inspect it")
                    state = self._new_state("IN_PROGRESS", plan)
            elif self.config.resume:
                conflict = self._find_resume_conflict()
                if conflict:
                    raise MigrationIdentityError(f"Cannot resume migration: {conflict}")
                raise MigrationIdentityError(
                    f"No checkpoint exists for migration {self.migration_id}; refusing an ambiguous resume"
                )
            else:
                state = self._new_state("IN_PROGRESS", plan)

            self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
            if state.plan_sha256 and state.plan_sha256 != self._plan_sha256(plan):
                raise MigrationIdentityError("Migration checkpoint is bound to a different plan")
            state.plan_sha256 = self._plan_sha256(plan)
            state.export_options = {
                "chunk_size": self.config.chunk_size,
                "compression": self.config.compression,
                "projection_version": self.PROJECTION_VERSION,
            }
            self._remove_verification_authorization()
            self._write_active()
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self.staging_dir.mkdir(parents=True, exist_ok=True)

            if not self.config.resume or state.status in {"PLANNED", "VERIFICATION_FAILED", "VERIFIED"}:
                if not self.config.resume:
                    state.partitions = {}
                state.status = "IN_PROGRESS"

            table = self.source_table or "tick_data"
            table_exists = bool(self.schema_info.get("projection_sql"))
            coverage = self._load_coverage()
            try:
                with self._source_session() as con:
                    if table_exists:
                        table_sql = self._quote_identifier(table)
                        projection_sql = self.schema_info["projection_sql"]
                        ordering = (
                            "timestamp ASC, symbol ASC, price ASC, volume ASC NULLS FIRST, "
                            "bid ASC NULLS FIRST, ask ASC NULLS FIRST, source ASC NULLS FIRST, "
                            "session ASC NULLS FIRST"
                        )
                        covered_count = 0
                        for part in plan.partitions:
                            symbol = part["symbol"]
                            date_value = part["date"]
                            key = self._partition_key(symbol, date_value)
                            previous = state.partitions.get(key, {})
                            if self.config.resume and previous.get("status") == "COMPLETED":
                                self._validate_completed_partition(key, part, previous)
                                continue

                            # Check coverage ledger
                            cov_rec = self._check_partition_coverage(symbol, date_value, part, coverage)
                            if cov_rec is not None or part.get("status") == "COVERED":
                                state.partitions[key] = {
                                    "status": "COVERED",
                                    "chunks": [],
                                    "files": cov_rec.get("files", []) if cov_rec else [],
                                    "row_count": int(part["row_count"]),
                                    "file_sizes": {},
                                    "sha256": {},
                                    "covered": True,
                                    "published_batch_id": cov_rec.get("published_batch_id") if cov_rec else "",
                                }
                                covered_count += 1
                                continue

                            self._clear_partition_stage(symbol, date_value)
                            stage_dir = self._stage_directory(symbol, date_value)
                            stage_dir.mkdir(parents=True, exist_ok=True)
                            state.partitions[key] = {
                                "status": "EXPORTING",
                                "chunks": [],
                                "files": [],
                                "row_count": 0,
                                "file_sizes": {},
                                "sha256": {},
                            }
                            self._write_state(state)
                            cur = con.cursor()
                            query = (
                                "SELECT timestamp, symbol, price, volume, bid, ask, source, session, "
                                f"row_number() OVER (ORDER BY {ordering}) AS row_ordinal "
                                f"FROM (SELECT {projection_sql} FROM {table_sql}) AS src "
                                "WHERE symbol = ? AND strftime(timestamp, '%Y-%m-%d') = ? "
                                f"ORDER BY {ordering}"
                            )
                            cur.execute(query, [symbol, date_value])
                            chunk_idx = 1
                            part_count = 0
                            chunk_names: List[str] = []
                            file_details: List[Dict[str, Any]] = []
                            while True:
                                rows = cur.fetchmany(self.config.chunk_size)
                                if not rows:
                                    break
                                chunk_name = f"chunk_{chunk_idx:06d}.parquet"
                                chunk_path = stage_dir / chunk_name
                                chunk_records = [
                                    (*row[:8], f"mig_{self.migration_id}_{int(row[8]):016d}")
                                    for row in rows
                                ]
                                table_arrow = ticks_to_table(chunk_records, validate=True)
                                symbol_dict = pc.dictionary_encode(table_arrow["symbol"])
                                table_arrow = table_arrow.set_column(
                                    table_arrow.schema.get_field_index("symbol"),
                                    pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                                    symbol_dict,
                                )
                                pq.write_table(table_arrow, chunk_path, compression=self.config.compression)
                                with open(chunk_path, "rb") as handle:
                                    os.fsync(handle.fileno())
                                detail = self._checkpoint_file(chunk_path, symbol, date_value, chunk_name)
                                file_details.append(detail)
                                chunk_names.append(chunk_name)
                                part_count += len(rows)
                                state.partitions[key] = {
                                    "status": "EXPORTING",
                                    "chunks": list(chunk_names),
                                    "files": list(file_details),
                                    "row_count": part_count,
                                    "file_sizes": {item["name"]: item["size_bytes"] for item in file_details},
                                    "sha256": {item["name"]: item["sha256"] for item in file_details},
                                }
                                self._write_state(state)
                                chunk_idx += 1
                            if part_count != int(part["row_count"]):
                                raise SourceSnapshotChangedError(
                                    f"Source plan expected {part['row_count']} rows for {key}, exported {part_count}"
                                )
                            state.partitions[key] = {
                                "status": "COMPLETED",
                                "chunks": chunk_names,
                                "files": file_details,
                                "row_count": part_count,
                                "file_sizes": {item["name"]: item["size_bytes"] for item in file_details},
                                "sha256": {item["name"]: item["sha256"] for item in file_details},
                            }
                            self._write_state(state)

                        if plan.partitions and covered_count == len(plan.partitions):
                            state.status = "PUBLISHED"
                            self._write_state(state)
                            return state

                    state.status = "IN_PROGRESS"
                    self._write_state(state)
            except Exception:
                # Keep the checkpoint/stage for an explicit --resume; no verification authorizes it.
                raise
            return state

    def _expected_state_files(self, state: MigrationState) -> Dict[str, Dict[str, Any]]:
        expected: Dict[str, Dict[str, Any]] = {}
        for partition_state in state.partitions.values():
            if partition_state.get("status") != "COMPLETED":
                continue
            for detail in partition_state.get("files", []):
                rel = str(detail.get("staging_path", ""))
                if not rel or rel in expected:
                    raise MigrationError(f"Migration checkpoint has an invalid/duplicate staged path: {rel!r}")
                expected[rel] = detail
        return expected

    def _partition_for_stage_path(self, path: Path) -> Optional[Tuple[str, str]]:
        try:
            relative = path.relative_to(self.staging_dir).parts
        except ValueError:
            return None
        if len(relative) < 4 or relative[0] != "ticks" or not relative[1].startswith("symbol=") or not relative[2].startswith("date="):
            return None
        try:
            return decode_symbol(relative[1].split("=", 1)[1]), relative[2].split("=", 1)[1]
        except Exception:
            return None

    def _inspect_staged_file(self, path: Path) -> Dict[str, Any]:
        relative = f"staging/{path.relative_to(self.staging_dir).as_posix()}"
        part = self._partition_for_stage_path(path)
        base = {
            "staging_path": relative,
            "symbol": part[0] if part else "",
            "date": part[1] if part else "",
            "name": path.name,
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "sha256": self._file_sha256(path) if path.is_file() and not path.is_symlink() else "",
            "row_count": None,
            "schema_sha256": "",
        }
        if path.is_symlink() or not path.is_file():
            raise MigrationError(f"Staged path is not a regular file: {path}")
        if part is None:
            raise MigrationError(f"Staged path is outside the planned partition layout: {path}")
        table = pq.ParquetFile(path).read()
        validate_table_v1(table)
        if any(row["symbol"] != part[0] for row in table.select(["symbol"]).to_pylist()):
            raise MigrationError(f"Staged file contains a symbol outside its path: {path}")
        if any(row["timestamp"].date().isoformat() != part[1] for row in table.select(["timestamp"]).to_pylist()):
            raise MigrationError(f"Staged file contains a date outside its path: {path}")
        base["row_count"] = table.num_rows
        base["schema_sha256"] = self._schema_sha256(table.schema)
        return base

    def _compute_verification(
        self,
        con: duckdb.DuckDBPyConnection,
        plan: MigrationPlan,
        state: MigrationState,
    ) -> VerificationResult:
        expected = self._expected_state_files(state)
        actual_paths = self._actual_stage_paths()
        actual_by_rel = {
            f"staging/{path.relative_to(self.staging_dir).as_posix()}": path
            for path in actual_paths
        }
        discrepancies: List[Dict[str, Any]] = []
        expected_names = set(expected)
        actual_names = set(actual_by_rel)
        if expected_names != actual_names:
            discrepancies.append({
                "type": "staging_inventory_mismatch",
                "missing_files": sorted(expected_names - actual_names),
                "extra_files": sorted(actual_names - expected_names),
            })

        inventory: List[Dict[str, Any]] = []
        readable_paths: Dict[Tuple[str, str], List[str]] = {}
        for rel, path in sorted(actual_by_rel.items()):
            part = self._partition_for_stage_path(path)
            try:
                detail = self._inspect_staged_file(path)
                inventory.append(detail)
                if part is not None:
                    readable_paths.setdefault(part, []).append(str(path))
                checkpoint = expected.get(rel)
                if checkpoint is None:
                    continue
                for field_name in ("sha256", "size_bytes", "row_count", "schema_sha256", "symbol", "date"):
                    if detail.get(field_name) != checkpoint.get(field_name):
                        discrepancies.append({
                            "type": "staging_checkpoint_mismatch",
                            "path": rel,
                            "field": field_name,
                            "expected": checkpoint.get(field_name),
                            "actual": detail.get(field_name),
                        })
            except Exception as exc:
                discrepancies.append({"type": "parquet_read_error", "path": rel, "error": str(exc)})
                if part is not None:
                    readable_paths.setdefault(part, []).append(str(path))
                inventory.append({
                    "staging_path": rel,
                    "symbol": part[0] if part else "",
                    "date": part[1] if part else "",
                    "name": path.name,
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                    "sha256": self._file_sha256(path) if path.is_file() and not path.is_symlink() else "",
                    "row_count": None,
                    "schema_sha256": "",
                })

        table = self.source_table or "tick_data"
        table_sql = self._quote_identifier(table)
        projection_sql = self.schema_info.get("projection_sql", "")
        total_source = 0
        total_parquet = 0
        for part in plan.partitions:
            symbol = str(part["symbol"])
            date_value = str(part["date"])
            key = self._partition_key(symbol, date_value)
            if state.partitions.get(key, {}).get("status") == "COVERED":
                continue
            source_count = int(con.execute(
                f"SELECT count(*) FROM (SELECT {projection_sql} FROM {table_sql}) AS src "
                "WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?",
                [symbol, date_value],
            ).fetchone()[0])
            paths = sorted(readable_paths.get((symbol, date_value), []))
            parquet_count = 0
            if not paths:
                if source_count:
                    discrepancies.append({
                        "type": "missing_parquet_files",
                        "symbol": symbol,
                        "date": date_value,
                        "error": f"No staged Parquet files for {symbol} on {date_value}, source has {source_count} rows",
                    })
            else:
                try:
                    parquet_count = int(con.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [paths]
                    ).fetchone()[0])
                    diff_source = int(con.execute(
                        f"""SELECT count(*) FROM (
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM (SELECT {projection_sql} FROM {table_sql}) AS src
                             WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?)
                            EXCEPT ALL
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM read_parquet(?, hive_partitioning=false))
                        )""",
                        [symbol, date_value, paths],
                    ).fetchone()[0])
                    diff_parquet = int(con.execute(
                        f"""SELECT count(*) FROM (
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM read_parquet(?, hive_partitioning=false))
                            EXCEPT ALL
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM (SELECT {projection_sql} FROM {table_sql}) AS src
                             WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?)
                        )""",
                        [paths, symbol, date_value],
                    ).fetchone()[0])
                    if source_count != parquet_count:
                        discrepancies.append({
                            "type": "row_count_mismatch", "symbol": symbol, "date": date_value,
                            "source_count": source_count, "parquet_count": parquet_count,
                        })
                    if diff_source:
                        discrepancies.append({
                            "type": "source_except_parquet_discrepancy", "symbol": symbol,
                            "date": date_value, "missing_in_parquet": diff_source,
                        })
                    if diff_parquet:
                        discrepancies.append({
                            "type": "parquet_except_source_discrepancy", "symbol": symbol,
                            "date": date_value, "missing_in_source": diff_parquet,
                        })
                except Exception as exc:
                    discrepancies.append({
                        "type": "parquet_read_error", "symbol": symbol, "date": date_value,
                        "error": str(exc),
                    })
            total_source += source_count
            total_parquet += parquet_count

        return VerificationResult(
            status="PASSED" if not discrepancies else "FAILED",
            total_source_rows=total_source,
            total_parquet_rows=total_parquet,
            discrepancies=discrepancies,
            verified_at=datetime.now(timezone.utc).isoformat(),
            migration_id=self.migration_id or "",
            source_path=str(self.source_db),
            source_snapshot_sha256=self.identity["source_snapshot_sha256"],
            source_schema_sha256=self.identity["source_schema_sha256"],
            source_table=self.source_table or "",
            projection_version=self.PROJECTION_VERSION,
            scope=self.scope,
            plan_sha256=self._plan_sha256(plan),
            files=sorted(inventory, key=lambda item: item["staging_path"]),
        )

    def _write_verification(self, result: VerificationResult) -> None:
        assert self.run_verification_file is not None
        result.save(self.run_verification_file)
        result.save(self.verification_file)

    def _load_verification(self, require_latest_alias: bool = True) -> VerificationResult:
        self._ensure_identity()
        assert self.run_verification_file is not None
        if not self.run_verification_file.is_file():
            raise RuntimeError("Publish aborted: verification must pass before publishing.")
        try:
            result = VerificationResult.load(self.run_verification_file)
        except Exception as exc:
            raise MigrationError(f"Cannot read verification authorization: {exc}") from exc
        self._validate_identity_fields(result.to_dict(), "Verification report")
        if require_latest_alias:
            if not self.verification_file.is_file():
                raise RuntimeError("Publish aborted: verification must pass before publishing.")
            try:
                latest = json.loads(self.verification_file.read_text(encoding="utf-8"))
            except Exception as exc:
                raise MigrationError(f"Cannot read latest verification authorization: {exc}") from exc
            self._validate_identity_fields(latest, "Latest verification report")
            if latest != result.to_dict():
                raise MigrationIdentityError("Latest verification alias does not match the migration-bound report")
        return result

    @staticmethod
    def _verification_proof(result: VerificationResult) -> Dict[str, Any]:
        data = result.to_dict()
        data.pop("verified_at", None)
        return data

    def _verify_ready_state(self, state: MigrationState, plan: MigrationPlan) -> VerificationResult:
        self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
        if state.status not in {"VERIFIED", "IN_PROGRESS", "PUBLISHING", "PUBLISHED"}:
            raise RuntimeError("Publish aborted: verification must pass before publishing.")
        if state.plan_sha256 != self._plan_sha256(plan):
            raise MigrationIdentityError("Migration checkpoint plan fingerprint changed")
        stored = self._load_verification(require_latest_alias=True)
        if stored.status != "PASSED" or stored.discrepancies:
            raise RuntimeError("Publish aborted: verification must pass before publishing.")
        if stored.plan_sha256 != self._plan_sha256(plan):
            raise MigrationIdentityError("Verification is bound to a different migration plan")
        with self._source_session() as con:
            current = self._compute_verification(con, plan, state)
        if current.status != "PASSED":
            raise MigrationVerificationError(
                "Publish aborted: staged migration no longer matches the verified source/inventory"
            )
        if self._verification_proof(current) != self._verification_proof(stored):
            raise MigrationVerificationError(
                "Publish aborted: staged contents, file inventory or reconciliation changed after verification"
            )
        return stored

    def assert_publish_ready(self) -> VerificationResult:
        """Read-only handoff preflight; valid while the live writer still owns the lake."""
        self._ensure_identity()
        with MigrationOwnershipLock(self.migration_dir, self.lock_file):
            state = self._load_state()
            plan = self._load_plan(required=True)
            if state is None:
                raise RuntimeError("Publish aborted: migration checkpoint is missing")
            return self._verify_ready_state(state, plan)

    def verify(self) -> VerificationResult:
        """Reconcile the exact run-owned staged inventory to its frozen DuckDB source."""
        self._ensure_identity()
        if self.config.dry_run:
            with self._source_session() as con:
                plan = self._compute_plan(con)
            return VerificationResult(
                status="DRY_RUN",
                total_source_rows=plan.total_rows,
                total_parquet_rows=0,
                discrepancies=[],
                verified_at=datetime.now(timezone.utc).isoformat(),
                migration_id=self.migration_id or "",
                source_path=str(self.source_db),
                source_snapshot_sha256=self.identity["source_snapshot_sha256"],
                source_schema_sha256=self.identity["source_schema_sha256"],
                source_table=self.source_table or "",
                projection_version=self.PROJECTION_VERSION,
                scope=self.scope,
                plan_sha256=self._plan_sha256(plan),
                files=[],
            )

        with MigrationOwnershipLock(self.migration_dir, self.lock_file):
            state = self._load_state()
            plan = self._load_plan(required=False)
            if plan is None:
                plan = self._ensure_plan_locked()

            if self.config.mode == "verify-published" or (state and state.status == "PUBLISHED"):
                return self.verify_published(plan=plan, state=state)

            if state is None:
                raise MigrationError("Cannot verify: migration export checkpoint is missing")
            self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
            active = self._read_active()
            if active and active.get("migration_id") != self.migration_id:
                raise MigrationOwnershipError("Another migration owns shared staging")
            with self._source_session() as con:
                result = self._compute_verification(con, plan, state)
            self._write_verification(result)
            state.status = "VERIFIED" if result.status == "PASSED" else "VERIFICATION_FAILED"
            state.plan_sha256 = self._plan_sha256(plan)
            self._write_state(state)
            return result

    def verify_published(
        self,
        plan: Optional[MigrationPlan] = None,
        state: Optional[MigrationState] = None,
    ) -> VerificationResult:
        """
        Provenance-Scoped Final Verification (MIGR-02):
        Reconcile mapped source fields and multiplicity strictly against the
        migration-owned receipt / coverage inventory using bidirectional EXCEPT ALL.
        Legitimate concurrent live rows or independent sources outside this inventory
        do NOT fail this reconciliation.
        """
        self._ensure_identity()
        if plan is None:
            plan = self._load_plan(required=False) or self._ensure_plan_locked()
        if state is None:
            state = self._load_state()

        receipt_path = self.lake_root / "_control" / "receipts" / f"migration_{self.migration_id}.json"
        owned_files: List[Dict[str, Any]] = []
        coverage = self._load_coverage()
        parts_cov = coverage.get("partitions", coverage)

        if receipt_path.is_file():
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                for detail in receipt.get("file_details", []):
                    owned_files.append({
                        "final_path": detail["relative_path"],
                        "symbol": detail["symbol"],
                        "date": detail["date"],
                        "row_count": int(detail["row_count"]),
                        "size_bytes": int(detail.get("file_size_bytes", 0)),
                        "sha256": detail.get("sha256", ""),
                    })
            except Exception as exc:
                raise MigrationError(f"Cannot read migration receipt {receipt_path}: {exc}") from exc
        else:
            for part in plan.partitions:
                key = self._partition_key(part["symbol"], part["date"])
                cov_rec = parts_cov.get(key)
                if cov_rec and cov_rec.get("status") == "COVERED":
                    for f in cov_rec.get("files", []):
                        owned_files.append({
                            "final_path": f["final_path"],
                            "symbol": part["symbol"],
                            "date": part["date"],
                            "row_count": int(f["row_count"]),
                            "size_bytes": int(f.get("size_bytes", 0)),
                            "sha256": f.get("sha256", ""),
                        })

        if not owned_files:
            raise MigrationError("No migration-owned published files found to verify")

        lineage_map: Dict[str, Any] = {}
        lineage_path = self.lake_root / "_control" / "lineage.json"
        if lineage_path.is_file():
            try:
                lineage_data = json.loads(lineage_path.read_text(encoding="utf-8"))
                lineage_map = lineage_data.get("file_lineage", {})
            except Exception:
                pass

        discrepancies: List[Dict[str, Any]] = []
        files_by_partition: Dict[Tuple[str, str], List[str]] = {}

        for detail in owned_files:
            rel = detail["final_path"]
            path = self._safe_path(self.lake_root, rel)
            sym = detail["symbol"]
            dt = detail["date"]

            is_compacted = False
            if not path.is_file() and rel in lineage_map:
                compacted_rel = lineage_map[rel].get("compacted_file")
                if compacted_rel:
                    path = self._safe_path(self.lake_root, compacted_rel)
                    is_compacted = True

            files_by_partition.setdefault((sym, dt), [])
            if str(path) not in files_by_partition[(sym, dt)]:
                files_by_partition[(sym, dt)].append(str(path))

            if not path.is_file() or path.is_symlink():
                discrepancies.append({
                    "type": "missing_published_file",
                    "path": rel,
                    "error": f"Published file missing or unsafe: {path}",
                })
                continue

            if not is_compacted:
                actual_size = path.stat().st_size
                if detail["size_bytes"] and actual_size != detail["size_bytes"]:
                    discrepancies.append({
                        "type": "file_size_mismatch",
                        "path": rel,
                        "expected": detail["size_bytes"],
                        "actual": actual_size,
                    })
                actual_sha = self._file_sha256(path)
                if detail["sha256"] and actual_sha != detail["sha256"]:
                    discrepancies.append({
                        "type": "file_checksum_mismatch",
                        "path": rel,
                        "expected": detail["sha256"],
                        "actual": actual_sha,
                    })
                try:
                    table = pq.ParquetFile(path).read()
                    validate_table_v1(table)
                    if table.num_rows != detail["row_count"]:
                        discrepancies.append({
                            "type": "row_count_mismatch",
                            "path": rel,
                            "expected": detail["row_count"],
                            "actual": table.num_rows,
                        })
                except Exception as exc:
                    discrepancies.append({
                        "type": "parquet_read_error",
                        "path": rel,
                        "error": str(exc),
                    })
            else:
                try:
                    table = pq.ParquetFile(path).read()
                    validate_table_v1(table)
                except Exception as exc:
                    discrepancies.append({
                        "type": "parquet_read_error",
                        "path": str(path),
                        "error": str(exc),
                    })

        table = self.source_table or "tick_data"
        table_sql = self._quote_identifier(table)
        projection_sql = self.schema_info.get("projection_sql", "")
        total_source = 0
        total_parquet = 0

        with self._source_session() as con:
            for part in plan.partitions:
                symbol = str(part["symbol"])
                date_value = str(part["date"])
                paths = sorted(files_by_partition.get((symbol, date_value), []))
                source_count = int(con.execute(
                    f"SELECT count(*) FROM (SELECT {projection_sql} FROM {table_sql}) AS src "
                    "WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?",
                    [symbol, date_value],
                ).fetchone()[0])
                total_source += source_count

                if not paths:
                    if source_count:
                        discrepancies.append({
                            "type": "missing_published_partition_files",
                            "symbol": symbol,
                            "date": date_value,
                            "error": f"No published files for {symbol} on {date_value}, source has {source_count} rows",
                        })
                    continue

                try:
                    parquet_count = int(con.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [paths]
                    ).fetchone()[0])
                    total_parquet += parquet_count

                    diff_source = int(con.execute(
                        f"""SELECT count(*) FROM (
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM (SELECT {projection_sql} FROM {table_sql}) AS src
                             WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?)
                            EXCEPT ALL
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM read_parquet(?, hive_partitioning=false))
                        )""",
                        [symbol, date_value, paths],
                    ).fetchone()[0])
                    diff_parquet = int(con.execute(
                        f"""SELECT count(*) FROM (
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM read_parquet(?, hive_partitioning=false))
                            EXCEPT ALL
                            (SELECT timestamp,symbol,price,volume,bid,ask,source,session
                             FROM (SELECT {projection_sql} FROM {table_sql}) AS src
                             WHERE symbol=? AND strftime(timestamp, '%Y-%m-%d')=?)
                        )""",
                        [paths, symbol, date_value],
                    ).fetchone()[0])

                    if source_count != parquet_count:
                        discrepancies.append({
                            "type": "partition_row_count_mismatch",
                            "symbol": symbol,
                            "date": date_value,
                            "source_count": source_count,
                            "parquet_count": parquet_count,
                        })
                    if diff_source:
                        discrepancies.append({
                            "type": "source_except_parquet_discrepancy",
                            "symbol": symbol,
                            "date": date_value,
                            "missing_in_parquet": diff_source,
                        })
                    if diff_parquet:
                        discrepancies.append({
                            "type": "parquet_except_source_discrepancy",
                            "symbol": symbol,
                            "date": date_value,
                            "missing_in_source": diff_parquet,
                        })
                except Exception as exc:
                    discrepancies.append({
                        "type": "parquet_read_error",
                        "symbol": symbol,
                        "date": date_value,
                        "error": str(exc),
                    })

        result = VerificationResult(
            status="PASSED" if not discrepancies else "FAILED",
            total_source_rows=total_source,
            total_parquet_rows=total_parquet,
            discrepancies=discrepancies,
            verified_at=datetime.now(timezone.utc).isoformat(),
            migration_id=self.migration_id or "",
            source_path=str(self.source_db),
            source_snapshot_sha256=self.identity["source_snapshot_sha256"],
            source_schema_sha256=self.identity["source_schema_sha256"],
            source_table=self.source_table or "",
            projection_version=self.PROJECTION_VERSION,
            scope=self.scope,
            plan_sha256=self._plan_sha256(plan),
            files=owned_files,
        )
        self._write_verification(result)
        return result

    def audit_lake(self) -> Dict[str, Any]:
        """
        Whole-Lake Integrity Audit (MIGR-02):
        Verifies that all Parquet files across ticks/ map to valid publication receipts,
        detecting unowned, malformed, or foreign additions across all namespaces.
        """
        receipts_dir = self.lake_root / "_control" / "receipts"
        ticks_dir = self.lake_root / "ticks"

        claimed_files: Dict[str, Dict[str, Any]] = {}
        receipt_errors: List[Dict[str, Any]] = []

        if receipts_dir.is_dir():
            for receipt_path in sorted(receipts_dir.glob("*.json")):
                try:
                    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    if receipt.get("status") != "PUBLISHED":
                        continue
                    batch_id = receipt.get("batch_id", receipt_path.stem)
                    for detail in receipt.get("file_details", []):
                        rel = detail.get("relative_path")
                        if rel:
                            claimed_files[rel] = {
                                "batch_id": batch_id,
                                "receipt_file": receipt_path.name,
                                "size_bytes": detail.get("file_size_bytes"),
                                "sha256": detail.get("sha256"),
                                "row_count": detail.get("row_count"),
                                "symbol": detail.get("symbol"),
                                "date": detail.get("date"),
                            }
                    for rel in receipt.get("file_paths", []):
                        if rel not in claimed_files:
                            claimed_files[rel] = {
                                "batch_id": batch_id,
                                "receipt_file": receipt_path.name,
                            }
                except Exception as exc:
                    receipt_errors.append({"receipt": receipt_path.name, "error": str(exc)})

        discovered_files: Dict[str, Path] = {}
        malformed_files: List[Dict[str, Any]] = []
        if ticks_dir.is_dir():
            for file_path in sorted(ticks_dir.rglob("*")):
                if file_path.is_file():
                    rel = file_path.relative_to(self.lake_root).as_posix()
                    if file_path.suffix == ".parquet":
                        discovered_files[rel] = file_path
                    else:
                        malformed_files.append({"path": rel, "error": "Non-parquet file in ticks namespace"})

        unowned_files: List[str] = []
        for rel in sorted(discovered_files):
            if rel not in claimed_files:
                unowned_files.append(rel)

        missing_files: List[str] = []
        corrupted_files: List[Dict[str, Any]] = []
        for rel, meta in sorted(claimed_files.items()):
            full_path = self.lake_root / rel
            if not full_path.is_file():
                missing_files.append(rel)
                continue
            expected_size = meta.get("size_bytes")
            if expected_size is not None and full_path.stat().st_size != int(expected_size):
                corrupted_files.append({
                    "path": rel,
                    "error": f"File size mismatch: expected {expected_size}, got {full_path.stat().st_size}",
                })
            expected_sha = meta.get("sha256")
            if expected_sha and self._file_sha256(full_path) != expected_sha:
                corrupted_files.append({
                    "path": rel,
                    "error": f"SHA256 mismatch for {rel}",
                })
            try:
                table = pq.ParquetFile(full_path).read()
                validate_table_v1(table)
            except Exception as exc:
                corrupted_files.append({"path": rel, "error": f"Parquet validation error: {exc}"})

        has_errors = bool(receipt_errors or malformed_files or unowned_files or missing_files or corrupted_files)
        status = "FAILED" if has_errors else "PASSED"

        report = {
            "status": status,
            "audited_at": datetime.now(timezone.utc).isoformat(),
            "total_receipts_checked": len(list(receipts_dir.glob("*.json"))) if receipts_dir.is_dir() else 0,
            "total_claimed_files": len(claimed_files),
            "total_discovered_parquet_files": len(discovered_files),
            "unowned_files": unowned_files,
            "missing_files": missing_files,
            "malformed_files": malformed_files,
            "corrupted_files": corrupted_files,
            "receipt_errors": receipt_errors,
        }
        return report

    def _journal_entries(self, state: MigrationState) -> List[Dict[str, Any]]:
        assert self.migration_id is not None
        entries = []
        for key in sorted(state.partitions):
            part_state = state.partitions[key]
            if part_state.get("status") == "COVERED":
                continue
            if part_state.get("status") != "COMPLETED":
                raise MigrationError(f"Cannot publish incomplete partition checkpoint {key}")
            for detail in sorted(part_state.get("files", []), key=lambda item: item["name"]):
                staged = str(detail["staging_path"])
                target = (
                    f"ticks/symbol={encode_symbol(detail['symbol'])}/date={detail['date']}/"
                    f"migration_{self.migration_id}_{detail['name']}"
                )
                entries.append({
                    **detail,
                    "final_path": target,
                    "status": "PENDING",
                })
        return entries

    def _validate_final_file(self, path: Path, detail: Dict[str, Any]) -> None:
        if path.is_symlink() or not path.is_file():
            raise MigrationError(f"Published migration file is missing or unsafe: {path}")
        if path.stat().st_size != int(detail["size_bytes"]):
            raise MigrationError(f"Published migration file size mismatch: {path}")
        if self._file_sha256(path) != detail["sha256"]:
            raise MigrationError(f"Published migration file checksum mismatch: {path}")
        table = pq.ParquetFile(path).read()
        validate_table_v1(table)
        if table.num_rows != int(detail["row_count"]):
            raise MigrationError(f"Published migration file row count mismatch: {path}")
        if any(row["symbol"] != detail["symbol"] for row in table.select(["symbol"]).to_pylist()):
            raise MigrationError(f"Published migration file symbol mismatch: {path}")
        if any(row["timestamp"].date().isoformat() != detail["date"] for row in table.select(["timestamp"]).to_pylist()):
            raise MigrationError(f"Published migration file date mismatch: {path}")

    def _validate_published_receipt(self, state: MigrationState, plan: MigrationPlan) -> None:
        assert self.migration_id is not None
        receipt_path = self.lake_root / "_control" / "receipts" / f"migration_{self.migration_id}.json"
        if state.partitions and all(p.get("status") == "COVERED" for p in state.partitions.values()):
            coverage = self._load_coverage()
            parts = coverage.get("partitions", coverage)
            for key in state.partitions:
                if key not in parts or parts[key].get("status") != "COVERED":
                    raise MigrationError(f"Published partition {key} has no valid coverage record")
                cov_entry = parts[key]
                for file_entry in cov_entry.get("files", []):
                    final_path = self._safe_path(self.lake_root, file_entry["final_path"])
                    if not final_path.is_file():
                        raise MigrationError(f"Covered file {final_path} does not exist on disk")
            if receipt_path.is_file():
                try:
                    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    raise MigrationError(f"Cannot read migration receipt {receipt_path}: {exc}") from exc
                if (
                    receipt.get("migration_id") != self.migration_id
                    or receipt.get("source_snapshot_sha256") != self.identity["source_snapshot_sha256"]
                    or receipt.get("plan_sha256") != self._plan_sha256(plan)
                ):
                    raise MigrationIdentityError("Migration receipt provenance does not match its checkpoint")
                for detail in receipt.get("file_details", []):
                    relative = detail.get("relative_path")
                    if relative:
                        final_path = self._safe_path(self.lake_root, relative)
                        self._validate_final_file(final_path, detail)
            return

        if not receipt_path.is_file():
            raise MigrationError(f"Published checkpoint has no migration receipt: {receipt_path}")
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise MigrationError(f"Cannot read migration receipt {receipt_path}: {exc}") from exc
        if (
            receipt.get("migration_id") != self.migration_id
            or receipt.get("source_snapshot_sha256") != self.identity["source_snapshot_sha256"]
            or receipt.get("plan_sha256") != self._plan_sha256(plan)
        ):
            raise MigrationIdentityError("Migration receipt provenance does not match its checkpoint")
        expected = self._journal_entries(state)
        details = receipt.get("file_details", [])
        expected_by_path = {item["final_path"]: item for item in expected}
        if set(receipt.get("file_paths", [])) != set(expected_by_path):
            raise MigrationError("Migration receipt references files outside this migration")
        if len(details) != len(expected_by_path):
            raise MigrationError("Migration receipt has an incomplete file inventory")
        for detail in details:
            relative = detail.get("relative_path")
            planned = expected_by_path.get(relative)
            if planned is None:
                raise MigrationError(f"Migration receipt claims an unowned file {relative!r}")
            final_path = self._safe_path(self.lake_root, relative)
            self._validate_final_file(final_path, planned)
            for field_name, receipt_name in (
                ("symbol", "symbol"), ("date", "date"), ("row_count", "row_count"),
                ("size_bytes", "file_size_bytes"), ("sha256", "sha256"),
            ):
                if detail.get(receipt_name) != planned.get(field_name):
                    raise MigrationError(f"Migration receipt detail mismatch for {relative}")
        expected_rows = sum(int(item["row_count"]) for item in expected)
        if int(receipt.get("row_count", -1)) != expected_rows:
            raise MigrationError("Migration receipt total row count is invalid")

    def _read_journal(self, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        assert self.publish_journal_file is not None
        expected_by_path = {entry["final_path"]: entry for entry in entries}
        if self.publish_journal_file.is_file():
            try:
                journal = json.loads(self.publish_journal_file.read_text(encoding="utf-8"))
            except Exception as exc:
                raise MigrationError(f"Publish journal is corrupt: {exc}") from exc
            if journal.get("migration_id") != self.migration_id:
                raise MigrationIdentityError("Publish journal belongs to another migration")
            recorded = {entry.get("final_path"): entry for entry in journal.get("files", [])}
            if set(recorded) != set(expected_by_path):
                raise MigrationError("Publish journal inventory differs from the verified migration")
            for target, planned in expected_by_path.items():
                for field_name in ("staging_path", "sha256", "size_bytes", "row_count", "symbol", "date"):
                    if recorded[target].get(field_name) != planned.get(field_name):
                        raise MigrationError(f"Publish journal authorization changed for {target}")
            journal["files"] = [recorded[entry["final_path"]] for entry in entries]
            return journal
        return {
            "migration_id": self.migration_id,
            "status": "PUBLISHING",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "files": entries,
        }

    def _copy_verified_file_no_replace(self, staged_path: Path, final_path: Path, detail: Dict[str, Any]) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            self._validate_final_file(final_path, detail)
            return

        temp_root = self.lake_root / "_staging"
        temp_root.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f"migration_{self.migration_id}_", suffix=".tmp", dir=temp_root)
        temp_path = Path(temp_name)
        input_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            source_fd = os.open(str(staged_path), input_flags)
            try:
                source_stat = os.fstat(source_fd)
                if not stat.S_ISREG(source_stat.st_mode):
                    raise MigrationError(f"Staged migration source is not a regular file: {staged_path}")
                digest = hashlib.sha256()
                size = 0
                with os.fdopen(source_fd, "rb", closefd=True) as source, os.fdopen(fd, "wb", closefd=True) as target:
                    fd = -1
                    while True:
                        block = source.read(1024 * 1024)
                        if not block:
                            break
                        digest.update(block)
                        size += len(block)
                        target.write(block)
                    target.flush()
                    os.fsync(target.fileno())
            except Exception:
                try:
                    os.close(source_fd)
                except OSError:
                    pass
                raise
            if size != int(detail["size_bytes"]) or digest.hexdigest() != detail["sha256"]:
                raise MigrationVerificationError(
                    f"Staged content changed during promotion: {staged_path}"
                )
            os.chmod(temp_path, 0o444)
            self._validate_final_file(temp_path, detail)
            try:
                os.link(temp_path, final_path)
            except FileExistsError:
                self._validate_final_file(final_path, detail)
            try:
                dir_fd = os.open(str(final_path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            temp_path.unlink(missing_ok=True)

    def _write_receipt(self, state: MigrationState, plan: MigrationPlan, journal: Dict[str, Any]) -> PublishReceipt:
        assert self.migration_id is not None
        entries = journal["files"]
        file_details = []
        for entry in entries:
            file_details.append({
                "relative_path": entry["final_path"],
                "symbol": entry["symbol"],
                "date": entry["date"],
                "row_count": int(entry["row_count"]),
                "file_size_bytes": int(entry["size_bytes"]),
                "sha256": entry["sha256"],
            })
        row_count = sum(item["row_count"] for item in file_details)
        batch_id = f"migration_{self.migration_id}"
        receipt_payload = {
            "batch_id": batch_id,
            "writer_id": f"migrator_{self.migration_id}",
            "sequence": 0,
            "row_count": row_count,
            "file_paths": [item["relative_path"] for item in file_details],
            "file_details": file_details,
            "status": "PUBLISHED",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "migration_id": self.migration_id,
            "source_path": str(self.source_db),
            "source_snapshot_sha256": self.identity["source_snapshot_sha256"],
            "source_schema_sha256": self.identity["source_schema_sha256"],
            "scope": self.scope,
            "plan_sha256": self._plan_sha256(plan),
            "verification_sha256": self._json_sha256(self._verification_proof(self._load_verification())),
        }
        receipt_path = self.lake_root / "_control" / "receipts" / f"{batch_id}.json"
        if receipt_path.exists():
            current = json.loads(receipt_path.read_text(encoding="utf-8"))
            comparable = dict(receipt_payload)
            comparable.pop("published_at", None)
            saved = dict(current)
            saved.pop("published_at", None)
            if comparable != saved:
                raise MigrationIdentityError("Existing migration receipt conflicts with this migration")
        else:
            _atomic_save_json(receipt_payload, receipt_path)
        return PublishReceipt(
            batch_id=batch_id,
            writer_id=receipt_payload["writer_id"],
            sequence=0,
            row_count=row_count,
            file_paths=[item["relative_path"] for item in file_details],
            file_details=[FilePublicationReceipt(
                relative_path=item["relative_path"],
                symbol=item["symbol"],
                date=item["date"],
                row_count=item["row_count"],
                file_size_bytes=item["file_size_bytes"],
                sha256=item["sha256"],
            ) for item in file_details],
            status="PUBLISHED",
            published_at=receipt_payload["published_at"],
        )

    def _cleanup_published_files(self, state: MigrationState) -> None:
        for partition_state in state.partitions.values():
            if partition_state.get("status") == "COVERED":
                continue
            for detail in partition_state.get("files", []):
                staging_rel = detail.get("staging_path")
                if staging_rel:
                    path = self._safe_path(self.migration_dir, staging_rel)
                    path.unlink(missing_ok=True)
        if (self.staging_dir / "ticks").exists():
            for directory in sorted((self.staging_dir / "ticks").rglob("*"), reverse=True):
                if directory.is_dir() and not directory.is_symlink():
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
            try:
                (self.staging_dir / "ticks").rmdir()
            except OSError:
                pass

    def _update_coverage_on_publish(self, state: MigrationState, plan: MigrationPlan, journal: Dict[str, Any]) -> None:
        coverage = self._load_coverage()
        if "partitions" not in coverage:
            coverage["partitions"] = {}
        coverage["source_fingerprint"] = self._source_fingerprint() if (self.source_db and self.source_db.is_file()) else ""

        files_by_partition: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for entry in journal.get("files", []):
            files_by_partition.setdefault((entry["symbol"], entry["date"]), []).append({
                "final_path": entry["final_path"],
                "row_count": int(entry["row_count"]),
                "sha256": entry["sha256"],
                "size_bytes": int(entry["size_bytes"]),
            })

        for part in plan.partitions:
            sym = part["symbol"]
            dt = part["date"]
            key = self._partition_key(sym, dt)
            if (sym, dt) in files_by_partition:
                part_files = files_by_partition[(sym, dt)]
                coverage["partitions"][key] = {
                    "symbol": sym,
                    "date": dt,
                    "source_rows": int(part["row_count"]),
                    "first_timestamp": str(part.get("min_timestamp", "")),
                    "last_timestamp": str(part.get("max_timestamp", "")),
                    "published_batch_id": f"migration_{self.migration_id}",
                    "status": "COVERED",
                    "source_fingerprint": self._source_fingerprint() if (self.source_db and self.source_db.is_file()) else "",
                    "source_snapshot_sha256": self.identity["source_snapshot_sha256"],
                    "files": part_files,
                }
        self._save_coverage(coverage)

    def publish(self) -> List[PublishReceipt]:
        """Revalidate verified scope and contents, then append immutable namespaced files."""
        self._ensure_identity()
        if self.config.dry_run:
            return []

        with MigrationOwnershipLock(self.migration_dir, self.lock_file):
            state = self._load_state()
            if state is None:
                raise RuntimeError("Publish aborted: verification must pass before publishing.")
            self._validate_identity_fields(state.to_dict(), "Migration checkpoint")
            plan = self._load_plan(required=True)
            if state.status == "PUBLISHED":
                self._validate_published_receipt(state, plan)
                return []

            if plan.partitions and all(
                state.partitions.get(self._partition_key(p["symbol"], p["date"]), {}).get("status") == "COVERED"
                for p in plan.partitions
            ):
                state.status = "PUBLISHED"
                self._write_state(state)
                return []

            # LakePublisherLock is intentionally acquired only for the final cutover.
            # Export and two-way verification can safely run while capture is active.
            with LakePublisherLock(self.lake_root, writer_id=f"migrator:{self.migration_id}"):
                verification = self._verify_ready_state(state, plan)
                entries = self._journal_entries(state)
                if not entries:
                    state.status = "PUBLISHED"
                    self._write_state(state)
                    return []

                journal = self._read_journal(entries)
                receipt_path = self.lake_root / "_control" / "receipts" / f"migration_{self.migration_id}.json"

                # Preflight every destination before making any finalized file visible.
                owned_paths = {item.get("final_path") for item in journal.get("files", [])}
                for entry in entries:
                    final_path = self._safe_path(self.lake_root, entry["final_path"])
                    if final_path.exists():
                        if entry["final_path"] not in owned_paths and not self.publish_journal_file.exists():
                            raise MigrationCollisionError(
                                f"Unowned destination already exists: {entry['final_path']}"
                            )
                        self._validate_final_file(final_path, entry)
                if receipt_path.exists():
                    try:
                        existing_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    except Exception as exc:
                        raise MigrationError(f"Existing migration receipt is corrupt: {exc}") from exc
                    if existing_receipt.get("migration_id") != self.migration_id:
                        raise MigrationCollisionError("Migration receipt path is occupied by another migration")

                assert self.publish_journal_file is not None
                journal["status"] = "PUBLISHING"
                journal["updated_at"] = datetime.now(timezone.utc).isoformat()
                _atomic_save_json(journal, self.publish_journal_file)
                state.status = "PUBLISHING"
                self._write_state(state)

                journal_by_path = {entry["final_path"]: entry for entry in journal["files"]}
                for entry in entries:
                    current = journal_by_path[entry["final_path"]]
                    staged_path = self._safe_path(self.migration_dir, entry["staging_path"])
                    final_path = self._safe_path(self.lake_root, entry["final_path"])
                    if final_path.exists():
                        self._validate_final_file(final_path, entry)
                    else:
                        self._copy_verified_file_no_replace(staged_path, final_path, entry)
                    current["status"] = "PUBLISHED"
                    journal["updated_at"] = datetime.now(timezone.utc).isoformat()
                    _atomic_save_json(journal, self.publish_journal_file)

                # Recheck the complete owned set before writing the one migration receipt.
                for entry in entries:
                    self._validate_final_file(self._safe_path(self.lake_root, entry["final_path"]), entry)
                receipt = self._write_receipt(state, plan, journal)
                self._update_coverage_on_publish(state, plan, journal)
                journal["status"] = "PUBLISHED"
                journal["updated_at"] = datetime.now(timezone.utc).isoformat()
                _atomic_save_json(journal, self.publish_journal_file)
                state.status = "PUBLISHED"
                self._write_state(state)
                self._cleanup_published_files(state)
                return [receipt]

    def run(self) -> int:
        """Run a selected lifecycle stage; dry-run is strictly read-only."""
        try:
            if self.config.mode == "plan":
                self.plan()
            elif self.config.mode == "export":
                self.export()
            elif self.config.mode in ("verify", "verify-published"):
                result = self.verify()
                if result.status not in {"PASSED", "DRY_RUN"}:
                    return 1
            elif self.config.mode == "publish":
                self.publish()
            elif self.config.mode in ("audit-lake", "audit"):
                report = self.audit_lake()
                if report["status"] != "PASSED":
                    logger.error("Whole-lake integrity audit failed:\n%s", json.dumps(report, indent=2))
                    return 1
                logger.info("Whole-lake integrity audit passed")
                return 0
            elif self.config.mode == "all":
                self.plan()
                state = self.export()
                if not self.config.dry_run and state.status == "PUBLISHED":
                    return 0
                result = self.verify()
                if result.status not in {"PASSED", "DRY_RUN"}:
                    return 1
                self.publish()
            else:
                raise ValueError(f"Unknown mode: {self.config.mode}")
            return 0
        except Exception as exc:
            logger.error("Migration lifecycle failure: %s", exc, exc_info=True)
            return 1


class MigrationHandoffCoordinator:
    """Persisted, bounded writer-stop / history-publish / writer-restart protocol."""

    def __init__(
        self,
        supervisor: Any,
        migration: MigrationOrchestrator,
        drain_timeout: float = 15.0,
        restart_timeout: float = 20.0,
    ):
        if drain_timeout <= 0 or restart_timeout <= 0:
            raise ValueError("handoff timeouts must be positive")
        self.supervisor = supervisor
        self.migration = migration
        self.drain_timeout = float(drain_timeout)
        self.restart_timeout = float(restart_timeout)
        self.migration._ensure_identity()
        self.state_file = self.migration.migration_dir / "handoff.json"
        self._paused = False

    def _read_status(self) -> Dict[str, Any]:
        status_path = self.migration.lake_root / "_control" / "writer_status.json"
        try:
            return json.loads(status_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Cannot read live writer status {status_path}: {exc}") from exc

    def _save(self, phase: str, **fields: Any) -> Dict[str, Any]:
        prior: Dict[str, Any] = {}
        if self.state_file.is_file():
            try:
                prior = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception:
                prior = {}
        payload = {
            **prior,
            **fields,
            "schema_version": 1,
            "migration_id": self.migration.migration_id,
            "phase": phase,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_save_json(payload, self.state_file)
        return payload

    def _wait_status(self, predicate, timeout: float) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_error: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                status = self._read_status()
                if predicate(status):
                    return status
            except Exception as exc:
                last_error = exc
            time.sleep(0.02)
        if last_error:
            raise RuntimeError(f"Writer did not reach the required handoff state: {last_error}")
        raise RuntimeError("Writer did not reach the required handoff state before the deadline")

    def execute(self) -> List[PublishReceipt]:
        phase = "PREPARING"
        old_pid: Optional[int] = None
        try:
            self._save("PREPARING", drain_timeout=self.drain_timeout)
            # This reads and rechecks the full authorization without competing for the
            # publisher lock that the live writer intentionally owns.
            self.migration.assert_publish_ready()
            initial = self._read_status()
            old_pid = int(initial.get("pid")) if initial.get("pid") is not None else None
            if initial.get("status") != "RUNNING" or old_pid is None:
                raise RuntimeError("Supervised writer is not healthy/running before cutover")
            if self.supervisor.child_pid is not None and self.supervisor.child_pid != old_pid:
                raise RuntimeError("Writer status PID does not match the supervised child")

            phase = "DRAINING"
            self._save(phase, old_writer_pid=old_pid, pre_cutover_rows=int(initial.get("total_rows_written", 0)))
            exit_code = self.supervisor.suspend_for_handoff(timeout=self.drain_timeout)
            self._paused = True
            if exit_code not in (0, None):
                raise RuntimeError(f"Supervised writer exited unsuccessfully during drain: {exit_code}")
            drained = self._wait_status(
                lambda status: status.get("status") == "STOPPED" and int(status.get("queue_depth", -1)) == 0,
                self.drain_timeout,
            )
            self._save(
                "DRAINED",
                drained_rows=int(drained.get("total_rows_written", 0)),
                drain_status=drained.get("status"),
            )

            phase = "PUBLISHING"
            self._save(phase)
            receipts = self.migration.publish()

            phase = "RESTARTING"
            self._save(phase)
            self.supervisor.resume_after_handoff()
            self._paused = False
            restarted = self._wait_status(
                lambda status: (
                    status.get("status") == "RUNNING"
                    and status.get("pid") != old_pid
                    and int(status.get("total_rows_written", 0)) > 0
                ),
                self.restart_timeout,
            )
            self._save(
                "COMPLETE",
                new_writer_pid=int(restarted["pid"]),
                post_cutover_rows=int(restarted.get("total_rows_written", 0)),
                published_batches=sum(1 for _ in receipts),
                capture_gap_expected=True,
            )
            return receipts
        except Exception as exc:
            failed_phase = phase
            recovery_error = None
            if self._paused or getattr(self.supervisor, "is_handoff_suspended", False):
                try:
                    self.supervisor.resume_after_handoff()
                    self._paused = False
                except Exception as restart_exc:
                    recovery_error = str(restart_exc)
            self._save(
                "FAILED",
                failed_phase=failed_phase,
                error=f"{type(exc).__name__}: {exc}",
                restart_error=recovery_error,
                old_writer_pid=old_pid,
            )
            raise

def parse_args(args: Optional[Sequence[str]] = None) -> MigrationConfig:
    """Parse command line arguments into MigrationConfig."""
    parser = argparse.ArgumentParser(
        description="Historical DuckDB to Partitioned Parquet Tick Lake Migration Tool."
    )
    parser.add_argument("--source-db", type=str, default=None, help="Path to source DuckDB database")
    parser.add_argument("--source-table", type=str, default=None, help="Source table name")
    parser.add_argument("--lake-root", type=str, default=None, help="Path to destination Tick Lake root")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["plan", "export", "verify", "verify-published", "publish", "all", "audit-lake", "audit"],
        default="all",
        help="Lifecycle execution mode",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Chunk size (row count per parquet file)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to migrate (e.g. AAPL,MSFT)",
    )
    parser.add_argument(
        "--date-start",
        type=str,
        default=None,
        help="Start date filter (YYYY-MM-DD inclusive)",
    )
    parser.add_argument(
        "--date-end",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD inclusive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Simulate execution without modifying disk",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume previous export skipping completed partitions",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force overwrite existing staging or plan files",
    )
    parser.add_argument(
        "--compression",
        type=str,
        default="snappy",
        help="Parquet compression codec (default: snappy)",
    )
    parser.add_argument("--migration-id", type=str, default=None, help="Optional explicit 32-hex migration UUID")

    parsed = parser.parse_args(args)
    return MigrationConfig(
        source_db=parsed.source_db,
        lake_root=parsed.lake_root,
        source_table=parsed.source_table,
        mode=parsed.mode,
        chunk_size=parsed.chunk_size,
        symbols=parsed.symbols,
        date_start=parsed.date_start,
        date_end=parsed.date_end,
        dry_run=parsed.dry_run,
        resume=parsed.resume,
        force=parsed.force,
        compression=parsed.compression,
        migration_id=parsed.migration_id,
    )


def main(args: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint for migration tool."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    config = parse_args(args)
    orchestrator = MigrationOrchestrator(config)
    return orchestrator.run()


if __name__ == "__main__":
    sys.exit(main())
