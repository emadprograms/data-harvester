"""
Atomic publication state machine, crash recovery, and publisher ownership locking.
"""
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import uuid

import errno
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.storage.barriers import trigger_persistence_barrier
from src.storage.config import (
    LakeMaintenanceInProgressError,
    StorageConfigError,
    encode_symbol,
)
from src.storage.schema import (
    LAKE_SCHEMA_V1,
    SCHEMA_V2_COLUMNS,
    record_is_quote_v2,
    ticks_to_table,
    ticks_to_table_v2,
    validate_published_table,
    validate_schema_v1,
    validate_schema_v2,
)


class LakeOwnershipError(StorageConfigError):
    """Raised when another publisher holds the single-writer lock on the lake root."""
    pass


class BatchCollisionError(StorageConfigError):
    """Raised when a batch targets an existing partition file with differing contents."""
    pass


class PublishError(StorageConfigError):
    """Raised when an unexpected error occurs during publication."""
    pass


@dataclass(frozen=True)
class FilePublicationReceipt:
    relative_path: str = ""
    symbol: str = ""
    date: str = ""
    row_count: int = 0
    file_size_bytes: int = 0
    sha256: str = ""


@dataclass(frozen=True)
class PublishReceipt:
    batch_id: str = ""
    writer_id: str = ""
    sequence: int = 0
    row_count: int = 0
    file_paths: List[str] = field(default_factory=list)
    file_details: List[FilePublicationReceipt] = field(default_factory=list)
    status: str = "PUBLISHED"  # "PUBLISHED" | "ALREADY_PUBLISHED"
    published_at: str = ""
    payload_sha256: str = ""
    # Optional caller-supplied request scope (a named gap-fill interval, for example).
    # It lets a lost coverage ledger be rebuilt without inferring bounds from a hash.
    request_scope: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PublishIntent:
    batch_id: str = ""
    writer_id: str = ""
    sequence: int = 0
    expected_row_count: int = 0
    targets: List[Dict[str, Any]] = field(default_factory=list)
    created_at: str = ""
    state: str = "PENDING"  # "PENDING" | "STAGED" | "COMMITTED"
    payload_sha256: str = ""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


_V1_FLOAT_COLUMNS = {"price", "volume", "bid", "ask"}
_V2_FLOAT_COLUMNS = {"bid_price", "ask_price"}


def _fingerprint_contract(rows_or_table: Any) -> Tuple[int, List[str], set]:
    """Keep the schema v1 hash unchanged. Schema v2 rows hash their own columns."""
    if isinstance(rows_or_table, pa.Table):
        if "bid_price" in rows_or_table.schema.names:
            return 2, list(SCHEMA_V2_COLUMNS), _V2_FLOAT_COLUMNS
        return 1, list(LAKE_SCHEMA_V1.names), _V1_FLOAT_COLUMNS
    rows = list(rows_or_table)
    if rows and isinstance(rows[0], dict) and ("bid_price" in rows[0] or "ask_price" in rows[0]):
        return 2, list(SCHEMA_V2_COLUMNS), _V2_FLOAT_COLUMNS
    return 1, list(LAKE_SCHEMA_V1.names), _V1_FLOAT_COLUMNS


def _canonical_row(row: Dict[str, Any], columns: List[str], float_columns: set) -> List[Any]:
    canonical = []
    for column in columns:
        value = row.get(column)
        if column == "timestamp" and value is not None:
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            canonical.append(value.isoformat(timespec="microseconds"))
        elif column in float_columns and value is not None:
            canonical.append(float(value).hex())
        else:
            canonical.append(value)
    return canonical


def _payload_fingerprint(rows_or_table: Any) -> str:
    """Hash canonical logical rows as a multiset; retain duplicate multiplicity."""
    version, columns, float_columns = _fingerprint_contract(rows_or_table)
    if isinstance(rows_or_table, pa.Table):
        rows = rows_or_table.to_pylist()
    else:
        rows = list(rows_or_table)
    canonical_rows = [
        json.dumps(
            _canonical_row(row, columns, float_columns),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        for row in rows
    ]
    canonical_rows.sort()
    payload = json.dumps(
        {"schema_version": version, "columns": columns, "rows": canonical_rows},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _batch_is_quote_v2(records: List[Any]) -> bool:
    flags = [record_is_quote_v2(record) for record in records]
    if any(flags) and not all(flags):
        raise PublishError("Refusing a batch that mixes schema v1 and schema v2 rows")
    return bool(flags) and all(flags)


def _safe_batch_id(batch_id: str) -> str:
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", batch_id):
        raise PublishError(f"Invalid batch_id {batch_id!r}; only safe filename characters are allowed")
    if batch_id in {".", ".."}:
        raise PublishError(f"Invalid batch_id {batch_id!r}")
    return batch_id


def _verify_receipt_files(root: Path, receipt_data: Dict[str, Any]) -> Tuple[List[FilePublicationReceipt], List[Dict[str, Any]], str]:
    """Verify receipt-owned files independently and return details, rows and logical digest."""
    root = Path(root).resolve()
    raw_details = receipt_data.get("file_details")
    if not isinstance(raw_details, list):
        raise PublishError(
            f"Receipt {receipt_data.get('batch_id', '<unknown>')} has malformed file checksums"
        )
    if not raw_details and int(receipt_data.get("row_count", -1)) != 0:
        raise PublishError(
            f"Receipt {receipt_data.get('batch_id', '<unknown>')} has no file checksums; integrity cannot be established"
        )

    lineage_map: Dict[str, Any] = {}
    lineage_file = root / "_control" / "lineage.json"
    if lineage_file.is_file():
        try:
            lineage_data = json.loads(lineage_file.read_text(encoding="utf-8"))
            lineage_map = lineage_data.get("file_lineage", {})
        except Exception:
            pass

    details: List[FilePublicationReceipt] = []
    rows: List[Dict[str, Any]] = []
    seen_paths = set()
    resolved_via_lineage = False

    for raw in raw_details:
        try:
            relative = Path(raw["relative_path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise PublishError(f"Receipt path escapes lake: {raw.get('relative_path')!r}")
            target = (root / relative).resolve()
            if not target.is_relative_to(root):
                raise PublishError(f"Receipt path escapes lake: {raw.get('relative_path')!r}")
            rel_path = relative.as_posix()
            if rel_path in seen_paths:
                raise PublishError(f"Receipt references file more than once: {rel_path}")
            seen_paths.add(rel_path)
            if not target.is_file():
                if rel_path in lineage_map:
                    compacted_rel = lineage_map[rel_path].get("compacted_file")
                    compacted_target = (root / compacted_rel).resolve() if compacted_rel else None
                    if compacted_target and compacted_target.is_file():
                        table = pq.ParquetFile(compacted_target).read()
                        validate_published_table(table)
                        details.append(FilePublicationReceipt(
                            relative_path=rel_path,
                            symbol=str(raw["symbol"]),
                            date=str(raw["date"]),
                            row_count=int(raw["row_count"]),
                            file_size_bytes=int(raw["file_size_bytes"]),
                            sha256=str(raw["sha256"]),
                        ))
                        resolved_via_lineage = True
                        continue
                raise PublishError(f"Receipt file is missing: {rel_path}")
            size = target.stat().st_size
            digest = _sha256_file(target)
            if size != int(raw["file_size_bytes"]) or digest != str(raw["sha256"]):
                raise PublishError(f"Receipt checksum/size mismatch for {rel_path}")
            # Read the physical file schema without Hive partition columns appended by read_table().
            table = pq.ParquetFile(target).read()
            validate_published_table(table)
            count = table.num_rows
            if count != int(raw["row_count"]):
                raise PublishError(f"Receipt row count mismatch for {rel_path}")
            table_rows = table.to_pylist()
            expected_symbol = str(raw["symbol"])
            expected_date = str(raw["date"])
            for row in table_rows:
                if row["symbol"] != expected_symbol:
                    raise PublishError(f"Receipt symbol mismatch for {rel_path}")
                if row["timestamp"].date().isoformat() != expected_date:
                    raise PublishError(f"Receipt date mismatch for {rel_path}")
            rows.extend(table_rows)
            details.append(FilePublicationReceipt(
                relative_path=rel_path,
                symbol=expected_symbol,
                date=expected_date,
                row_count=count,
                file_size_bytes=size,
                sha256=digest,
            ))
        except PublishError:
            raise
        except Exception as exc:
            label = raw.get("relative_path", "<unknown>") if isinstance(raw, dict) else "<malformed>"
            raise PublishError(f"Cannot verify receipt file {label}: {exc}") from exc

    raw_paths = receipt_data.get("file_paths")
    if raw_paths is not None:
        if (
            not isinstance(raw_paths, list)
            or len(raw_paths) != len(seen_paths)
            or set(map(str, raw_paths)) != seen_paths
        ):
            raise PublishError(f"Receipt file_paths do not match its file_details for {receipt_data.get('batch_id')}")

    stored_fingerprint = receipt_data.get("payload_sha256")
    if resolved_via_lineage:
        actual_fingerprint = stored_fingerprint or ""
    else:
        expected_rows = int(receipt_data.get("row_count", -1))
        if expected_rows != len(rows):
            raise PublishError(
                f"Receipt row count mismatch for {receipt_data.get('batch_id')}: expected {expected_rows}, found {len(rows)}"
            )
        actual_fingerprint = _payload_fingerprint(rows)

    if stored_fingerprint and stored_fingerprint != actual_fingerprint:
        raise PublishError(f"Receipt logical payload fingerprint mismatch for {receipt_data.get('batch_id')}")
    return details, rows, actual_fingerprint


class LakePublisherLock:
    """Single-publisher process ownership lock using advisory file locking."""

    def __init__(self, root: Path, writer_id: str = "writer_1", ignore_maintenance: bool = False):
        self.root = Path(root).resolve()
        self.writer_id = writer_id
        self.ignore_maintenance = ignore_maintenance
        self.lock_dir = self.root / "_control"
        self.lock_path = self.lock_dir / "publisher.lock"
        self._fd: Optional[int] = None
        self._is_locked: bool = False

    def acquire(self, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """Acquire the shared exclusive lake-owner lock and honor maintenance fencing.

        Publisher entry points default to fail-fast ownership. Idempotent lake
        initialization may wait for an in-progress publisher to finish before it
        inspects or creates metadata.
        """
        if self._is_locked:
            return True
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")

        guard_path = self.root / "_maintenance" / "in_progress.json"
        if not self.ignore_maintenance and not self.writer_id.startswith("maintenance"):
            if guard_path.exists():
                raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")

        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        should_block = blocking or timeout is not None
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            try:
                if sys.platform != "win32":
                    import fcntl
                    nonblocking = not should_block or deadline is not None
                    flags = fcntl.LOCK_EX | fcntl.LOCK_NB if nonblocking else fcntl.LOCK_EX
                    fcntl.flock(fd, flags)
                else:
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except (BlockingIOError, OSError, IOError) as exc:
                if not should_block or (deadline is not None and time.monotonic() >= deadline):
                    os.close(fd)
                    raise LakeOwnershipError(
                        f"Publisher lock already held for lake at {self.root} (file: {self.lock_path})"
                    ) from exc
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())) if deadline is not None else 0.01)

        # Recheck after taking ownership: maintenance may have set its marker between
        # the optimistic precheck and this lock acquisition.
        if not self.ignore_maintenance and not self.writer_id.startswith("maintenance"):
            if guard_path.exists():
                try:
                    if sys.platform != "win32":
                        import fcntl
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                finally:
                    raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")

        try:
            os.ftruncate(fd, 0)
            payload = (
                f"writer_id={self.writer_id}\n"
                f"pid={os.getpid()}\n"
                f"acquired_at={datetime.now(timezone.utc).isoformat()}\n"
            ).encode("utf-8")
            os.write(fd, payload)
        except Exception:
            pass

        self._fd = fd
        self._is_locked = True
        return True

    def release(self) -> None:
        """Release the advisory file lock if held."""
        if not self._is_locked or self._fd is None:
            return

        try:
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
        except Exception:
            pass
        finally:
            self._fd = None
            self._is_locked = False

    def __enter__(self) -> "LakePublisherLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


class LakeMaintenanceLock:
    """Exclusive offline-maintenance owner sharing the normal publisher lock."""

    def __init__(self, root: Path, operation: str, owner: str = ""):
        self.root = Path(root).resolve()
        self.operation = str(operation)
        self.owner = str(owner or f"pid:{os.getpid()}")
        self.marker_path = self.root / "_maintenance" / "in_progress.json"
        self._owner_lock = LakePublisherLock(self.root, writer_id=f"maintenance:{self.operation}")
        self._is_locked = False

    def acquire(self) -> bool:
        if self._is_locked:
            return True
        if self.marker_path.exists():
            raise LakeMaintenanceInProgressError(f"Lake maintenance already in progress: {self.marker_path}")
        self._owner_lock.acquire()
        try:
            if self.marker_path.exists():
                raise LakeMaintenanceInProgressError(f"Lake maintenance already in progress: {self.marker_path}")
            self.marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker = {
                "operation": self.operation,
                "owner": self.owner,
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            fd = os.open(str(self.marker_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(marker, handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                directory_fd = os.open(str(self.marker_path.parent), os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
            self._is_locked = True
            return True
        except Exception:
            self._owner_lock.release()
            raise

    def release(self, successful: bool = False) -> None:
        if not self._is_locked:
            self._owner_lock.release()
            return
        marker_error = None
        if successful:
            try:
                self.marker_path.unlink(missing_ok=True)
                try:
                    directory_fd = os.open(str(self.marker_path.parent), os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError:
                    pass
            except Exception as exc:
                marker_error = exc
        self._is_locked = False
        self._owner_lock.release()
        if marker_error is not None:
            raise marker_error

    def __enter__(self) -> "LakeMaintenanceLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release(successful=exc_type is None)


class LakePublisher:
    """Atomic batch publisher for partitioned Parquet tick lake."""

    def __init__(
        self,
        root: Path,
        writer_id: str = "writer_1",
        compression: str = "snappy",
        file_namespace: Optional[str] = None,
        ownership_lock: Optional[LakePublisherLock] = None,
    ):
        self.root = Path(root).resolve()
        self.writer_id = writer_id
        self.compression = compression
        if file_namespace is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", file_namespace):
            raise PublishError(f"Unsafe publication file namespace: {file_namespace!r}")
        self.file_namespace = file_namespace
        self._writer_file_token = re.sub(r"[^A-Za-z0-9_-]+", "_", str(writer_id)).strip("_") or "writer"

        # Reject an existing maintenance marker before creating control artifacts,
        # then acquire the shared lock before initialization and publication setup.
        guard_path = self.root / "_maintenance" / "in_progress.json"
        if guard_path.exists():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")
        self.control_dir = self.root / "_control"
        self.staging_dir = self.root / "_staging"
        self.receipts_dir = self.control_dir / "receipts"
        self.intent_dir = self.control_dir / "intent"
        if ownership_lock is not None:
            if not ownership_lock._is_locked or ownership_lock.root != self.root:
                raise LakeOwnershipError(
                    "Publication requires the active publisher lock for the same lake root"
                )
            self.lock = ownership_lock
            self._owns_lock = False
        else:
            self.lock = LakePublisherLock(root=self.root, writer_id=self.writer_id)
            self.lock.acquire()
            self._owns_lock = True
        try:
            self.control_dir.mkdir(parents=True, exist_ok=True)
            self.staging_dir.mkdir(parents=True, exist_ok=True)
            self.receipts_dir.mkdir(parents=True, exist_ok=True)
            self.intent_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            if self._owns_lock:
                self.lock.release()
            raise

    def publish_batch(
        self,
        records_or_table: Any,
        batch_id: str,
        sequence: int,
        request_scope: Optional[Dict[str, Any]] = None,
        dedupe_on: Optional[str] = None,
    ) -> PublishReceipt:
        """Publish one immutable, payload-identified batch atomically.

        `request_scope`, when supplied, is stored in the intent and in the receipt so a
        caller can rebuild its own coverage state after losing it. Omitting it keeps the
        published payload byte-shape unchanged.

        `dedupe_on` names a column whose values identify a row (`ingest_id` for quotes).
        Named batches can legitimately re-request an interval another scope already
        filled, and distinct batch identities would otherwise give the same quote a
        second physical file. With `dedupe_on` set, rows whose identity is already
        stored in this batch's target partitions are dropped before staging, so a retry
        can never append a copy. Replay still comes first: the receipt accepts either the
        payload as written or the caller's pre-deduplication rows, so a batch that was
        published with rows already dropped keeps replaying instead of colliding.
        """
        batch_id = _safe_batch_id(batch_id)
        scope: Dict[str, Any] = dict(request_scope) if request_scope else {}
        if scope.get("batch_id") not in (None, batch_id):
            raise PublishError(
                f"Request scope names batch {scope.get('batch_id')!r}, not {batch_id!r}"
            )
        if isinstance(records_or_table, pa.Table):
            table = records_or_table
            validate_published_table(table)
        else:
            records = list(records_or_table)
            if records and _batch_is_quote_v2(records):
                table = ticks_to_table_v2(records, validate=True)
            else:
                table = ticks_to_table(records, validate=True)

        total_row_count = table.num_rows
        payload_sha256 = _payload_fingerprint(table)
        # The same batch identity may be handed back either as written or as downloaded.
        source_payload_sha256 = payload_sha256 if dedupe_on else ""
        source_row_count = total_row_count
        receipt_file = self.receipts_dir / f"{batch_id}.json"
        intent_file = self.intent_dir / f"{batch_id}.json"

        def replay_receipt() -> PublishReceipt:
            try:
                with open(receipt_file, "r", encoding="utf-8") as handle:
                    rdata = json.load(handle)
            except Exception as exc:
                raise PublishError(f"Cannot read publication receipt for {batch_id}: {exc}") from exc
            if rdata.get("batch_id") != batch_id:
                raise PublishError(f"Receipt identity mismatch for {batch_id}")
            if rdata.get("writer_id", self.writer_id) != self.writer_id:
                raise BatchCollisionError(f"Batch {batch_id} was already published by a different writer")
            if int(rdata.get("sequence", sequence)) != int(sequence):
                raise BatchCollisionError(f"Batch {batch_id} was already published at a different sequence")
            accepted_row_counts = {int(rdata.get("row_count", -1))}
            if rdata.get("source_row_count") is not None:
                accepted_row_counts.add(int(rdata["source_row_count"]))
            if total_row_count not in accepted_row_counts:
                raise BatchCollisionError(f"Batch {batch_id} was already published with a different row count")
            file_details, _, stored_payload_sha256 = _verify_receipt_files(self.root, rdata)
            accepted_fingerprints = {stored_payload_sha256}
            if rdata.get("source_payload_sha256"):
                accepted_fingerprints.add(str(rdata["source_payload_sha256"]))
            if payload_sha256 not in accepted_fingerprints:
                raise BatchCollisionError(f"Batch {batch_id} was already published with a different payload")
            declared_payload_sha256 = rdata.get("payload_sha256")
            if declared_payload_sha256 and declared_payload_sha256 not in accepted_fingerprints:
                raise BatchCollisionError(f"Batch {batch_id} was already published with a different payload")
            file_paths = rdata.get("file_paths", [fd.relative_path for fd in file_details])
            stored_scope = rdata.get("request") if isinstance(rdata.get("request"), dict) else {}
            if scope and stored_scope and stored_scope != scope:
                raise BatchCollisionError(
                    f"Batch {batch_id} was already published with a different request scope"
                )
            return PublishReceipt(
                batch_id=batch_id,
                writer_id=rdata.get("writer_id", self.writer_id),
                sequence=int(rdata.get("sequence", sequence)),
                row_count=int(rdata.get("row_count", 0)),
                file_paths=list(file_paths),
                file_details=file_details,
                status="ALREADY_PUBLISHED",
                published_at=rdata.get("published_at", ""),
                payload_sha256=stored_payload_sha256,
                request_scope=dict(stored_scope),
            )

        if receipt_file.is_file():
            return replay_receipt()

        # Resume a prepared publication instead of silently creating new target names.
        if intent_file.is_file():
            try:
                with open(intent_file, "r", encoding="utf-8") as handle:
                    pending = json.load(handle)
            except Exception as exc:
                raise PublishError(f"Cannot read pending publication intent for {batch_id}: {exc}") from exc
            if pending.get("batch_id") != batch_id:
                raise PublishError(f"Pending intent identity mismatch for {batch_id}")
            if pending.get("writer_id", self.writer_id) != self.writer_id:
                raise BatchCollisionError(f"Batch {batch_id} has a pending intent owned by another writer")
            if int(pending.get("sequence", sequence)) != int(sequence):
                raise BatchCollisionError(f"Batch {batch_id} has a pending intent at a different sequence")
            prepared_fingerprints = {
                value
                for value in (
                    pending.get("payload_sha256"),
                    pending.get("source_payload_sha256"),
                )
                if value
            }
            if prepared_fingerprints and payload_sha256 not in prepared_fingerprints:
                raise BatchCollisionError(f"Batch {batch_id} has a pending intent for a different payload")
            prepared_scope = pending.get("request") if isinstance(pending.get("request"), dict) else {}
            if scope and prepared_scope and prepared_scope != scope:
                raise BatchCollisionError(
                    f"Batch {batch_id} has a pending intent for a different request scope"
                )
            recover_pending_publications(self.root, batch_ids={batch_id}, ownership_lock=self.lock)
            if receipt_file.is_file():
                return replay_receipt()
            raise PublishError(
                f"Batch {batch_id} has an incomplete prepared publication; intent retained for recovery"
            )

        # Identity-level idempotency, applied only to a new publication: replay and resume
        # above must see the caller's rows exactly as they were handed in.
        if dedupe_on:
            table = self._drop_already_stored(table, dedupe_on)
            total_row_count = table.num_rows
            payload_sha256 = _payload_fingerprint(table)

        # An empty batch is still an idempotent publication and receives a payload-bound receipt.
        if total_row_count == 0:
            published_at = datetime.now(timezone.utc).isoformat()
            receipt_payload = {
                "batch_id": batch_id,
                "writer_id": self.writer_id,
                "sequence": sequence,
                "row_count": 0,
                "file_paths": [],
                "file_details": [],
                "payload_sha256": payload_sha256,
                "status": "PUBLISHED",
                "published_at": published_at,
            }
            if scope:
                receipt_payload["request"] = scope
            if dedupe_on:
                receipt_payload["source_payload_sha256"] = source_payload_sha256
                receipt_payload["source_row_count"] = source_row_count
            tmp_receipt = self.staging_dir / f"tmp_receipt_{uuid.uuid4().hex}.json"
            with open(tmp_receipt, "w", encoding="utf-8") as handle:
                json.dump(receipt_payload, handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_receipt, receipt_file)
            return PublishReceipt(
                batch_id=batch_id,
                writer_id=self.writer_id,
                sequence=sequence,
                row_count=0,
                status="PUBLISHED",
                published_at=published_at,
                payload_sha256=payload_sha256,
                request_scope=scope,
            )

        # Partition rows by (symbol, UTC date).
        symbols = table["symbol"].to_pylist()
        timestamps = table["timestamp"].to_pylist()
        partition_groups: Dict[Tuple[str, str], List[int]] = defaultdict(list)
        for idx in range(total_row_count):
            partition_groups[(symbols[idx], timestamps[idx].date().isoformat())].append(idx)

        staged_targets: List[Dict[str, Any]] = []
        current_staging_file: Optional[Path] = None
        intent_written = False
        try:
            for (symbol, date_str) in sorted(partition_groups):
                part_table = table.take(partition_groups[(symbol, date_str)])
                sort_idx = pc.sort_indices(
                    part_table,
                    sort_keys=[("timestamp", "ascending"), ("ingest_id", "ascending")],
                )
                sorted_table = pc.take(part_table, sort_idx)
                symbol_dict = pc.dictionary_encode(sorted_table["symbol"])
                file_table = sorted_table.set_column(
                    sorted_table.schema.get_field_index("symbol"),
                    pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                    symbol_dict,
                )

                encoded_symbol = encode_symbol(symbol)
                sequence_token = f"{sequence:06d}"
                if self.file_namespace:
                    filename_token = f"{self._writer_file_token}_{self.file_namespace}_{sequence_token}"
                else:
                    filename_token = f"{self._writer_file_token}_{sequence_token}"
                relative_path = (
                    f"ticks/symbol={encoded_symbol}/date={date_str}/batch_{filename_token}.parquet"
                )
                staging_relative_path = (
                    f"_staging/tmp_{self._writer_file_token}_{uuid.uuid4().hex}.parquet.tmp"
                )
                staging_full_path = self.root / staging_relative_path
                current_staging_file = staging_full_path
                pq.write_table(file_table, staging_full_path, compression=self.compression)
                trigger_persistence_barrier("staged_fsync", path=staging_full_path, symbol=symbol, date=date_str)
                with open(staging_full_path, "rb") as handle:
                    os.fsync(handle.fileno())

                file_size_bytes = staging_full_path.stat().st_size
                if file_size_bytes <= 0:
                    raise PublishError(f"Staged Parquet file {staging_full_path} is empty")
                parquet_file = pq.ParquetFile(staging_full_path)
                if parquet_file.metadata.num_rows != sorted_table.num_rows:
                    raise PublishError(f"Staged Parquet row count mismatch for {staging_full_path}")
                if "bid_price" in parquet_file.schema_arrow.names:
                    validate_schema_v2(parquet_file.schema_arrow)
                else:
                    validate_schema_v1(parquet_file.schema_arrow)

                staged_targets.append({
                    "relative_path": relative_path,
                    "staging_path": staging_relative_path,
                    "symbol": symbol,
                    "date": date_str,
                    "row_count": sorted_table.num_rows,
                    "file_size_bytes": file_size_bytes,
                    "sha256": _sha256_file(staging_full_path),
                })
                current_staging_file = None

            intent_payload = {
                "batch_id": batch_id,
                "writer_id": self.writer_id,
                "sequence": sequence,
                "expected_row_count": total_row_count,
                "payload_sha256": payload_sha256,
                "state": "STAGED",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "targets": staged_targets,
            }
            if scope:
                intent_payload["request"] = scope
            if dedupe_on:
                intent_payload["source_payload_sha256"] = source_payload_sha256
                intent_payload["source_row_count"] = source_row_count
            tmp_intent = self.staging_dir / f"tmp_intent_{uuid.uuid4().hex}.json"
            with open(tmp_intent, "w", encoding="utf-8") as handle:
                json.dump(intent_payload, handle, indent=2)
                handle.flush()
                trigger_persistence_barrier("intent_fsync", path=tmp_intent, payload=intent_payload)
                os.fsync(handle.fileno())
            trigger_persistence_barrier("intent_durability", path=intent_file, tmp_path=tmp_intent, payload=intent_payload)
            os.replace(tmp_intent, intent_file)
            intent_written = True

            # Reject any pre-existing target before moving any staged file.
            for target in staged_targets:
                target_dest = self.root / target["relative_path"]
                if target_dest.exists():
                    if not target_dest.is_file() or _sha256_file(target_dest) != target["sha256"]:
                        for staged in staged_targets:
                            (self.root / staged["staging_path"]).unlink(missing_ok=True)
                        intent_file.unlink(missing_ok=True)
                        intent_written = False
                        raise BatchCollisionError(
                            f"Destination file {target['relative_path']} already exists with differing content"
                        )

            for target in staged_targets:
                target_dest = self.root / target["relative_path"]
                staging_file = self.root / target["staging_path"]
                if target_dest.is_file():
                    staging_file.unlink(missing_ok=True)
                else:
                    target_dest.parent.mkdir(parents=True, exist_ok=True)
                    trigger_persistence_barrier("staged_promotion", src=staging_file, dst=target_dest, symbol=target["symbol"])
                    os.replace(staging_file, target_dest)

                trigger_persistence_barrier("directory_fsync", path=target_dest.parent, symbol=target["symbol"])
                if sys.platform != "win32":
                    try:
                        dir_fd = os.open(str(target_dest.parent), os.O_RDONLY)
                        try:
                            os.fsync(dir_fd)
                        finally:
                            os.close(dir_fd)
                    except OSError as err:
                        if err.errno in (errno.ENOSPC, errno.EIO):
                            raise

            published_at = datetime.now(timezone.utc).isoformat()
            file_details = [
                FilePublicationReceipt(
                    relative_path=target["relative_path"],
                    symbol=target["symbol"],
                    date=target["date"],
                    row_count=target["row_count"],
                    file_size_bytes=target["file_size_bytes"],
                    sha256=target["sha256"],
                )
                for target in staged_targets
            ]
            receipt_payload = {
                "batch_id": batch_id,
                "writer_id": self.writer_id,
                "sequence": sequence,
                "row_count": total_row_count,
                "file_paths": [target["relative_path"] for target in staged_targets],
                "file_details": [
                    {
                        "relative_path": detail.relative_path,
                        "symbol": detail.symbol,
                        "date": detail.date,
                        "row_count": detail.row_count,
                        "file_size_bytes": detail.file_size_bytes,
                        "sha256": detail.sha256,
                    }
                    for detail in file_details
                ],
                "payload_sha256": payload_sha256,
                "status": "PUBLISHED",
                "published_at": published_at,
            }
            if scope:
                receipt_payload["request"] = scope
            if dedupe_on:
                receipt_payload["source_payload_sha256"] = source_payload_sha256
                receipt_payload["source_row_count"] = source_row_count
            tmp_receipt = self.staging_dir / f"tmp_receipt_{uuid.uuid4().hex}.json"
            with open(tmp_receipt, "w", encoding="utf-8") as handle:
                json.dump(receipt_payload, handle, indent=2)
                handle.flush()
                trigger_persistence_barrier("receipt_fsync", path=tmp_receipt, payload=receipt_payload)
                os.fsync(handle.fileno())
            trigger_persistence_barrier("receipt_durability", path=receipt_file, tmp_path=tmp_receipt, payload=receipt_payload)
            os.replace(tmp_receipt, receipt_file)
            intent_file.unlink(missing_ok=True)

            return PublishReceipt(
                batch_id=batch_id,
                writer_id=self.writer_id,
                sequence=sequence,
                row_count=total_row_count,
                file_paths=[target["relative_path"] for target in staged_targets],
                file_details=file_details,
                status="PUBLISHED",
                published_at=published_at,
                payload_sha256=payload_sha256,
                request_scope=scope,
            )
        except Exception:
            if not intent_written:
                if current_staging_file is not None and current_staging_file.is_file():
                    current_staging_file.unlink(missing_ok=True)
                for target in staged_targets:
                    (self.root / target["staging_path"]).unlink(missing_ok=True)
            raise

    def _drop_already_stored(self, table: pa.Table, identity_column: str) -> pa.Table:
        """Keep only rows whose identity is not already stored in that partition.

        A row that cannot be identified (a null identity) is always kept: dropping it
        would need proof of duplication that the row itself does not carry.
        """
        if table.num_rows == 0:
            return table
        if identity_column not in table.column_names:
            raise PublishError(
                f"Deduplication column {identity_column!r} is missing from the batch"
            )
        identities = table[identity_column].to_pylist()
        symbols = table["symbol"].to_pylist()
        dates = [timestamp.date().isoformat() for timestamp in table["timestamp"].to_pylist()]
        stored: Dict[Tuple[str, str], set] = {}
        keep: List[int] = []
        for idx, identity in enumerate(identities):
            key = (symbols[idx], dates[idx])
            if key not in stored:
                stored[key] = self._stored_identities(key[0], key[1], identity_column)
            if identity is None or identity not in stored[key]:
                keep.append(idx)
        if len(keep) == table.num_rows:
            return table
        if not keep:
            return table.slice(0, 0)  # every row was already stored
        return table.take(pa.array(keep, type=pa.int64()))

    def _stored_identities(self, symbol: str, date_str: str, identity_column: str) -> set:
        """Identities already published in one symbol/day partition."""
        partition = self.root / "ticks" / f"symbol={encode_symbol(symbol)}" / f"date={date_str}"
        if not partition.is_dir():
            return set()
        identities: set = set()
        for path in sorted(partition.glob("*.parquet")):
            try:
                names = pq.read_schema(path).names
            except Exception as exc:
                raise PublishError(
                    f"Cannot read stored partition schema at {path}: {exc}"
                ) from exc
            if identity_column not in names:
                continue  # a legacy file that cannot carry this identity
            try:
                column = pq.read_table(path, columns=[identity_column])[identity_column]
            except Exception as exc:
                raise PublishError(
                    f"Cannot read stored identities from {path}: {exc}"
                ) from exc
            identities.update(value for value in column.to_pylist() if value is not None)
        return identities

    def close(self) -> None:
        """Release single-writer ownership lock when this publisher acquired it."""
        if getattr(self, "_owns_lock", True):
            self.lock.release()
            self._owns_lock = False

    def __enter__(self) -> "LakePublisher":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def verify_published_receipt(root: Path, batch_id: str) -> Optional[PublishReceipt]:
    """Read and fully verify one published receipt without publishing anything.

    Returns None when no receipt exists. Raises PublishError when the receipt is
    unreadable, names a different batch, or its files fail verification, so a caller
    that treats a receipt as recovery evidence can refuse instead of guessing.
    """
    root = Path(root).resolve()
    batch_id = _safe_batch_id(batch_id)
    receipt_file = root / "_control" / "receipts" / f"{batch_id}.json"
    if not receipt_file.is_file():
        return None
    try:
        with open(receipt_file, "r", encoding="utf-8") as handle:
            receipt_data = json.load(handle)
    except Exception as exc:
        raise PublishError(f"Cannot read publication receipt for {batch_id}: {exc}") from exc
    if not isinstance(receipt_data, dict) or receipt_data.get("batch_id") != batch_id:
        raise PublishError(f"Receipt identity mismatch for {batch_id}")
    file_details, _, actual_payload_sha256 = _verify_receipt_files(root, receipt_data)
    scope = receipt_data.get("request") if isinstance(receipt_data.get("request"), dict) else {}
    return PublishReceipt(
        batch_id=batch_id,
        writer_id=str(receipt_data.get("writer_id", "")),
        sequence=int(receipt_data.get("sequence", 0)),
        row_count=int(receipt_data.get("row_count", 0)),
        file_paths=list(receipt_data.get("file_paths", [detail.relative_path for detail in file_details])),
        file_details=file_details,
        status="PUBLISHED",
        published_at=str(receipt_data.get("published_at", "")),
        payload_sha256=actual_payload_sha256,
        request_scope=dict(scope),
    )


def recover_pending_publications(
    root: Path,
    batch_ids: Optional[Iterable[str]] = None,
    ownership_lock: Optional[LakePublisherLock] = None,
) -> List[PublishReceipt]:
    """Recover intents under the same exclusive owner lock used by publishers."""
    root = Path(root).resolve()
    guard_path = root / "_maintenance" / "in_progress.json"
    if guard_path.exists():
        raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")

    owns_lock = ownership_lock is None
    lock = ownership_lock or LakePublisherLock(root, writer_id="publication-recovery")
    if owns_lock:
        lock.acquire()
    elif not lock._is_locked or lock.root != root:
        raise LakeOwnershipError("Recovery requires the active publisher lock for the same lake root")
    try:
        if guard_path.exists():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard_path}")
        return _recover_pending_publications_locked(root, batch_ids=batch_ids)
    finally:
        if owns_lock:
            lock.release()


def _recover_pending_publications_locked(
    root: Path,
    batch_ids: Optional[Iterable[str]] = None,
) -> List[PublishReceipt]:
    """Recover only intents whose immutable staged/final targets verify completely."""
    root = Path(root).resolve()
    intent_dir = root / "_control" / "intent"
    receipts_dir = root / "_control" / "receipts"
    staging_dir = root / "_staging"
    receipts_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    if not intent_dir.is_dir():
        return []

    wanted = set(batch_ids) if batch_ids is not None else None
    recovered_receipts: List[PublishReceipt] = []

    def safe_target(relative_value: Any, label: str) -> Tuple[Path, str]:
        relative = Path(str(relative_value))
        if relative.is_absolute() or ".." in relative.parts:
            raise PublishError(f"Unsafe {label} path in publication intent: {relative_value!r}")
        target = (root / relative).resolve()
        if not target.is_relative_to(root):
            raise PublishError(f"{label} path escapes lake: {relative_value!r}")
        return target, relative.as_posix()

    for intent_file in sorted(intent_dir.glob("*.json")):
        if wanted is not None and intent_file.stem not in wanted:
            continue
        try:
            with open(intent_file, "r", encoding="utf-8") as handle:
                intent_data = json.load(handle)
        except Exception:
            continue
        if not isinstance(intent_data, dict):
            continue

        try:
            batch_id = _safe_batch_id(intent_data.get("batch_id"))
        except PublishError:
            continue
        if intent_file.name != f"{batch_id}.json":
            raise PublishError(f"Intent filename does not match batch identity {batch_id}")
        writer_id = str(intent_data.get("writer_id", "writer_1"))
        sequence = int(intent_data.get("sequence", 0))
        expected_row_count = int(intent_data.get("expected_row_count", 0))
        expected_payload_sha256 = intent_data.get("payload_sha256")
        receipt_file = receipts_dir / f"{batch_id}.json"

        if receipt_file.is_file():
            try:
                with open(receipt_file, "r", encoding="utf-8") as handle:
                    receipt_data = json.load(handle)
                if receipt_data.get("batch_id") != batch_id:
                    raise PublishError(f"Receipt identity mismatch for {batch_id}")
                details, _, actual_payload_sha256 = _verify_receipt_files(root, receipt_data)
                if expected_payload_sha256 and expected_payload_sha256 != actual_payload_sha256:
                    raise PublishError(f"Intent/receipt payload mismatch for {batch_id}")
                if expected_row_count != int(receipt_data.get("row_count", -1)):
                    raise PublishError(f"Intent/receipt row-count mismatch for {batch_id}")
            except PublishError:
                raise
            except Exception as exc:
                raise PublishError(f"Cannot verify receipt for pending intent {batch_id}: {exc}") from exc
            intent_scope = intent_data.get("request") if isinstance(intent_data.get("request"), dict) else {}
            receipt_scope = receipt_data.get("request") if isinstance(receipt_data.get("request"), dict) else {}
            if intent_scope and receipt_scope and intent_scope != receipt_scope:
                raise PublishError(f"Intent/receipt request scope mismatch for {batch_id}")
            intent_file.unlink(missing_ok=True)
            recovered_receipts.append(PublishReceipt(
                batch_id=batch_id,
                writer_id=receipt_data.get("writer_id", writer_id),
                sequence=int(receipt_data.get("sequence", sequence)),
                row_count=int(receipt_data.get("row_count", expected_row_count)),
                file_paths=list(receipt_data.get("file_paths", [detail.relative_path for detail in details])),
                file_details=details,
                status="PUBLISHED",
                published_at=receipt_data.get("published_at", ""),
                payload_sha256=actual_payload_sha256,
                request_scope=dict(receipt_scope or intent_scope),
            ))
            continue

        targets = intent_data.get("targets", [])
        if not isinstance(targets, list):
            continue
        if not targets and expected_row_count != 0:
            continue

        all_targets_ready = True
        target_candidates: List[Tuple[Dict[str, Any], Path, Path, str, List[Dict[str, Any]]]] = []
        file_details: List[FilePublicationReceipt] = []
        all_rows: List[Dict[str, Any]] = []

        for target in targets:
            if not isinstance(target, dict):
                all_targets_ready = False
                break
            try:
                target_dest, rel_path = safe_target(target["relative_path"], "target")
                expected_sha = str(target["sha256"])
                if not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha):
                    raise PublishError(f"Intent for {batch_id} has no valid checksum for {rel_path}")
                expected_bytes = int(target["file_size_bytes"])
                expected_rows = int(target["row_count"])
                symbol = str(target["symbol"])
                date_str = str(target["date"])
                staging_value = target.get("staging_path")
                staging_full = safe_target(staging_value, "staging")[0] if staging_value else None
            except (KeyError, TypeError, ValueError, PublishError):
                all_targets_ready = False
                break

            if target_dest.exists():
                candidate = target_dest
                if not candidate.is_file():
                    all_targets_ready = False
                    break
            elif staging_full is not None and staging_full.is_file():
                candidate = staging_full
            else:
                all_targets_ready = False
                break

            try:
                if candidate.stat().st_size != expected_bytes or _sha256_file(candidate) != expected_sha:
                    all_targets_ready = False
                    break
                # Avoid Hive partition discovery; receipt verification is against file contents only.
                table = pq.ParquetFile(candidate).read()
                validate_published_table(table)
                if table.num_rows != expected_rows:
                    all_targets_ready = False
                    break
                table_rows = table.to_pylist()
                if any(
                    row["symbol"] != symbol or row["timestamp"].date().isoformat() != date_str
                    for row in table_rows
                ):
                    all_targets_ready = False
                    break
            except Exception:
                all_targets_ready = False
                break

            detail = FilePublicationReceipt(
                relative_path=rel_path,
                symbol=symbol,
                date=date_str,
                row_count=expected_rows,
                file_size_bytes=expected_bytes,
                sha256=expected_sha,
            )
            file_details.append(detail)
            all_rows.extend(table_rows)
            target_candidates.append((target, target_dest, candidate, rel_path, table_rows))

        actual_payload_sha256 = _payload_fingerprint(all_rows)
        if expected_payload_sha256 and expected_payload_sha256 != actual_payload_sha256:
            all_targets_ready = False
        actual_total_rows = len(all_rows)
        if actual_total_rows != expected_row_count:
            all_targets_ready = False

        if not all_targets_ready:
            continue

        # Validate every candidate before promoting any staging file. Preserve an existing
        # destination on collision; recovery must never replace a published object.
        for target, target_dest, candidate, rel_path, _ in target_candidates:
            if candidate != target_dest:
                target_dest.parent.mkdir(parents=True, exist_ok=True)
                if target_dest.exists():
                    if not target_dest.is_file() or _sha256_file(target_dest) != str(target["sha256"]):
                        all_targets_ready = False
                        break
                    candidate.unlink(missing_ok=True)
                else:
                    try:
                        trigger_persistence_barrier("staged_promotion", src=candidate, dst=target_dest, symbol=target["symbol"])
                        os.replace(candidate, target_dest)
                    except Exception:
                        all_targets_ready = False
                        break

            try:
                trigger_persistence_barrier("directory_fsync", path=target_dest.parent, symbol=target["symbol"])
            except Exception:
                all_targets_ready = False
                break
        if not all_targets_ready:
            continue

        published_at = datetime.now(timezone.utc).isoformat()
        receipt_payload = {
            "batch_id": batch_id,
            "writer_id": writer_id,
            "sequence": sequence,
            "row_count": actual_total_rows,
            "file_paths": [detail.relative_path for detail in file_details],
            "file_details": [
                {
                    "relative_path": detail.relative_path,
                    "symbol": detail.symbol,
                    "date": detail.date,
                    "row_count": detail.row_count,
                    "file_size_bytes": detail.file_size_bytes,
                    "sha256": detail.sha256,
                }
                for detail in file_details
            ],
            "payload_sha256": actual_payload_sha256,
            "status": "PUBLISHED",
            "published_at": published_at,
        }
        recovered_scope = intent_data.get("request") if isinstance(intent_data.get("request"), dict) else {}
        if recovered_scope:
            receipt_payload["request"] = recovered_scope
        tmp_receipt = staging_dir / f"tmp_rec_{uuid.uuid4().hex}.json"
        with open(tmp_receipt, "w", encoding="utf-8") as handle:
            json.dump(receipt_payload, handle, indent=2)
            handle.flush()
            trigger_persistence_barrier("receipt_fsync", path=tmp_receipt, payload=receipt_payload)
            os.fsync(handle.fileno())
        trigger_persistence_barrier("receipt_durability", path=receipt_file, tmp_path=tmp_receipt, payload=receipt_payload)
        os.replace(tmp_receipt, receipt_file)
        intent_file.unlink(missing_ok=True)
        recovered_receipts.append(PublishReceipt(
            batch_id=batch_id,
            writer_id=writer_id,
            sequence=sequence,
            row_count=actual_total_rows,
            file_paths=[detail.relative_path for detail in file_details],
            file_details=file_details,
            status="PUBLISHED",
            published_at=published_at,
            payload_sha256=actual_payload_sha256,
            request_scope=dict(recovered_scope),
        ))

    return recovered_receipts


def cleanup_orphaned_staging_files(root: Path, max_age_seconds: int = 3600) -> int:
    """Clean up dangling .tmp files in _staging/ that exceed max_age_seconds and are not locked."""
    root = Path(root).resolve()
    staging_dir = root / "_staging"
    if not staging_dir.is_dir():
        return 0

    active_staging_files = set()
    intent_dir = root / "_control" / "intent"
    if intent_dir.is_dir():
        for ifile in intent_dir.glob("*.json"):
            try:
                with open(ifile, "r", encoding="utf-8") as f:
                    idata = json.load(f)
                for target in idata.get("targets", []):
                    sp = target.get("staging_path")
                    if sp:
                        active_staging_files.add(Path(sp).name)
                        active_staging_files.add(str(Path(sp)))
            except Exception:
                pass

    now = time.time()
    deleted_count = 0

    for entry in staging_dir.iterdir():
        if not entry.is_file():
            continue

        # Check if protected by active intent
        rel_to_root = str(entry.relative_to(root))
        if entry.name in active_staging_files or rel_to_root in active_staging_files:
            continue

        try:
            mtime = entry.stat().st_mtime
            if (now - mtime) > max_age_seconds:
                entry.unlink()
                deleted_count += 1
        except OSError:
            pass

    return deleted_count
