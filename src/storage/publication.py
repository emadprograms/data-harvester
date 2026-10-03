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
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.storage.config import StorageConfigError, encode_symbol
from src.storage.schema import LAKE_SCHEMA_V1, ticks_to_table, validate_schema_v1, validate_table_v1


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


@dataclass
class PublishIntent:
    batch_id: str = ""
    writer_id: str = ""
    sequence: int = 0
    expected_row_count: int = 0
    targets: List[Dict[str, Any]] = field(default_factory=list)
    created_at: str = ""
    state: str = "PENDING"  # "PENDING" | "STAGED" | "COMMITTED"


class LakePublisherLock:
    """Single-publisher process ownership lock using advisory file locking."""

    def __init__(self, root: Path, writer_id: str = "writer_1"):
        self.root = Path(root).resolve()
        self.writer_id = writer_id
        self.lock_dir = self.root / "_control"
        self.lock_path = self.lock_dir / "publisher.lock"
        self._fd: Optional[int] = None
        self._is_locked: bool = False

    def acquire(self) -> bool:
        """Acquire an exclusive non-blocking advisory file lock."""
        if self._is_locked:
            return True

        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o644)

        try:
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except (BlockingIOError, OSError, IOError) as exc:
            os.close(fd)
            raise LakeOwnershipError(
                f"Publisher lock already held for lake at {self.root} (file: {self.lock_path})"
            ) from exc

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


class LakePublisher:
    """Atomic batch publisher for partitioned Parquet tick lake."""

    def __init__(
        self,
        root: Path,
        writer_id: str = "writer_1",
        compression: str = "snappy",
    ):
        self.root = Path(root).resolve()
        self.writer_id = writer_id
        self.compression = compression

        # Control directories
        self.control_dir = self.root / "_control"
        self.staging_dir = self.root / "_staging"
        self.receipts_dir = self.control_dir / "receipts"
        self.intent_dir = self.control_dir / "intent"

        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        self.intent_dir.mkdir(parents=True, exist_ok=True)

        # Acquire single-writer lock
        self.lock = LakePublisherLock(root=self.root, writer_id=self.writer_id)
        self.lock.acquire()

    def publish_batch(
        self,
        records_or_table: Any,
        batch_id: str,
        sequence: int,
    ) -> PublishReceipt:
        """Publish a batch of records atomically across date/symbol partitions."""
        receipt_file = self.receipts_dir / f"{batch_id}.json"

        # 1. Idempotency check: if receipt already exists, return ALREADY_PUBLISHED
        if receipt_file.is_file():
            with open(receipt_file, "r", encoding="utf-8") as f:
                rdata = json.load(f)
            file_details = [
                FilePublicationReceipt(
                    relative_path=fd["relative_path"],
                    symbol=fd["symbol"],
                    date=fd["date"],
                    row_count=fd["row_count"],
                    file_size_bytes=fd["file_size_bytes"],
                    sha256=fd["sha256"],
                )
                for fd in rdata.get("file_details", [])
            ]
            return PublishReceipt(
                batch_id=rdata["batch_id"],
                writer_id=rdata.get("writer_id", self.writer_id),
                sequence=rdata.get("sequence", sequence),
                row_count=rdata.get("row_count", 0),
                file_paths=rdata.get("file_paths", [fd.relative_path for fd in file_details]),
                file_details=file_details,
                status="ALREADY_PUBLISHED",
                published_at=rdata.get("published_at", ""),
            )

        # 2. Convert to PyArrow Table and validate
        if isinstance(records_or_table, pa.Table):
            table = records_or_table
            validate_table_v1(table)
        else:
            table = ticks_to_table(records_or_table, validate=True)

        total_row_count = table.num_rows
        if total_row_count == 0:
            return PublishReceipt(
                batch_id=batch_id,
                writer_id=self.writer_id,
                sequence=sequence,
                row_count=0,
                status="PUBLISHED",
                published_at=datetime.now(timezone.utc).isoformat(),
            )

        # 3. Partition rows by (symbol, UTC date)
        symbols = table["symbol"].to_pylist()
        timestamps = table["timestamp"].to_pylist()

        partition_groups: Dict[Tuple[str, str], List[int]] = defaultdict(list)
        for idx in range(total_row_count):
            sym = symbols[idx]
            ts = timestamps[idx]
            dt_str = ts.date().isoformat()
            partition_groups[(sym, dt_str)].append(idx)

        # 4. Sort each partition by (timestamp ASC, ingest_id ASC) and write to staging
        staged_targets: List[Dict[str, Any]] = []
        current_staging_file: Optional[Path] = None
        intent_written = False
        try:
            for (sym, dt_str) in sorted(partition_groups.keys()):
                indices = partition_groups[(sym, dt_str)]
                part_table = table.take(indices)

                # Tiebreak sorting: (timestamp ASC, ingest_id ASC)
                sort_idx = pc.sort_indices(
                    part_table,
                    sort_keys=[("timestamp", "ascending"), ("ingest_id", "ascending")],
                )
                sorted_table = pc.take(part_table, sort_idx)

                # Dictionary encode symbol column to ensure compatibility with pyarrow dataset HivePartitioning
                symbol_dict = pc.dictionary_encode(sorted_table["symbol"])
                file_table = sorted_table.set_column(
                    sorted_table.schema.get_field_index("symbol"),
                    pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                    symbol_dict,
                )

                # Paths
                encoded_sym = encode_symbol(sym)
                rel_path = f"ticks/symbol={encoded_sym}/date={dt_str}/batch_{self.writer_id}_{sequence:06d}.parquet"
                staging_fname = f"tmp_{self.writer_id}_{sequence:06d}_{uuid.uuid4().hex}.parquet.tmp"
                staging_rel_path = f"_staging/{staging_fname}"
                staging_full_path = self.root / staging_rel_path
                current_staging_file = staging_full_path

                # Write parquet with compression
                pq.write_table(file_table, staging_full_path, compression=self.compression)

                # Flush and fsync
                with open(staging_full_path, "a") as f:
                    f.flush()
                    os.fsync(f.fileno())

                # Validate staged file: size, footer, row count, schema, SHA256
                fsize = staging_full_path.stat().st_size
                assert fsize > 0, f"Staged parquet file {staging_full_path} is empty"

                pf = pq.ParquetFile(staging_full_path)
                assert pf.metadata.num_rows == sorted_table.num_rows
                validate_schema_v1(pf.schema_arrow)

                with open(staging_full_path, "rb") as f:
                    file_sha = hashlib.sha256(f.read()).hexdigest()

                staged_targets.append({
                    "relative_path": rel_path,
                    "staging_path": staging_rel_path,
                    "symbol": sym,
                    "date": dt_str,
                    "row_count": sorted_table.num_rows,
                    "file_size_bytes": fsize,
                    "sha256": file_sha,
                })
                current_staging_file = None

            # 5. Write Intent to _control/intent/<batch_id>.json
            intent_file = self.intent_dir / f"{batch_id}.json"
            intent_payload = {
                "batch_id": batch_id,
                "writer_id": self.writer_id,
                "sequence": sequence,
                "expected_row_count": total_row_count,
                "state": "STAGED",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "targets": staged_targets,
            }
            tmp_intent = self.staging_dir / f"tmp_intent_{uuid.uuid4().hex}.json"
            with open(tmp_intent, "w", encoding="utf-8") as f:
                json.dump(intent_payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_intent, intent_file)
            intent_written = True

            # 6. Check for destination collisions BEFORE renaming
            for target in staged_targets:
                target_dest = self.root / target["relative_path"]
                if target_dest.is_file():
                    with open(target_dest, "rb") as f:
                        dest_sha = hashlib.sha256(f.read()).hexdigest()
                    if dest_sha != target["sha256"]:
                        # Collision: target file exists with differing content!
                        # Clean up staging files and abort
                        for t in staged_targets:
                            (self.root / t["staging_path"]).unlink(missing_ok=True)
                        intent_file.unlink(missing_ok=True)
                        raise BatchCollisionError(
                            f"Destination file {target['relative_path']} already exists with differing checksum"
                        )

            # 7. Atomic rename: move staged files to final partition targets
            for target in staged_targets:
                target_dest = self.root / target["relative_path"]
                staging_file = self.root / target["staging_path"]
                if target_dest.is_file():
                    # Identical checksum already in place, remove duplicate staged file
                    staging_file.unlink(missing_ok=True)
                else:
                    target_dest.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(staging_file, target_dest)
                    try:
                        dir_fd = os.open(str(target_dest.parent), os.O_RDONLY)
                        os.fsync(dir_fd)
                        os.close(dir_fd)
                    except Exception:
                        pass

            # 8. Write immutable receipt to _control/receipts/<batch_id>.json and delete intent
            published_at = datetime.now(timezone.utc).isoformat()
            file_details = [
                FilePublicationReceipt(
                    relative_path=t["relative_path"],
                    symbol=t["symbol"],
                    date=t["date"],
                    row_count=t["row_count"],
                    file_size_bytes=t["file_size_bytes"],
                    sha256=t["sha256"],
                )
                for t in staged_targets
            ]
            receipt_payload = {
                "batch_id": batch_id,
                "writer_id": self.writer_id,
                "sequence": sequence,
                "row_count": total_row_count,
                "file_paths": [t["relative_path"] for t in staged_targets],
                "file_details": [
                    {
                        "relative_path": fd.relative_path,
                        "symbol": fd.symbol,
                        "date": fd.date,
                        "row_count": fd.row_count,
                        "file_size_bytes": fd.file_size_bytes,
                        "sha256": fd.sha256,
                    }
                    for fd in file_details
                ],
                "status": "PUBLISHED",
                "published_at": published_at,
            }

            tmp_receipt = self.staging_dir / f"tmp_receipt_{uuid.uuid4().hex}.json"
            with open(tmp_receipt, "w", encoding="utf-8") as f:
                json.dump(receipt_payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_receipt, receipt_file)

            intent_file.unlink(missing_ok=True)

            return PublishReceipt(
                batch_id=batch_id,
                writer_id=self.writer_id,
                sequence=sequence,
                row_count=total_row_count,
                file_paths=[t["relative_path"] for t in staged_targets],
                file_details=file_details,
                status="PUBLISHED",
                published_at=published_at,
            )
        except Exception:
            if not intent_written:
                if current_staging_file is not None and current_staging_file.is_file():
                    current_staging_file.unlink(missing_ok=True)
                for t in staged_targets:
                    (self.root / t["staging_path"]).unlink(missing_ok=True)
            raise

    def close(self) -> None:
        """Release single-writer ownership lock."""
        self.lock.release()

    def __enter__(self) -> "LakePublisher":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def recover_pending_publications(root: Path) -> List[PublishReceipt]:
    """Recover uncommitted or unacknowledged publication intents in _control/intent/."""
    root = Path(root).resolve()
    intent_dir = root / "_control" / "intent"
    receipts_dir = root / "_control" / "receipts"
    staging_dir = root / "_staging"

    receipts_dir.mkdir(parents=True, exist_ok=True)
    if not intent_dir.is_dir():
        return []

    recovered_receipts: List[PublishReceipt] = []

    for intent_file in sorted(intent_dir.glob("*.json")):
        try:
            with open(intent_file, "r", encoding="utf-8") as f:
                intent_data = json.load(f)
        except Exception:
            continue

        batch_id = intent_data.get("batch_id")
        writer_id = intent_data.get("writer_id", "writer_1")
        sequence = intent_data.get("sequence", 0)
        expected_row_count = intent_data.get("expected_row_count", 0)
        targets = intent_data.get("targets", [])

        receipt_file = receipts_dir / f"{batch_id}.json"
        if receipt_file.is_file():
            # Receipt already exists, intent was just not cleaned up
            intent_file.unlink(missing_ok=True)
            with open(receipt_file, "r", encoding="utf-8") as f:
                rdata = json.load(f)
            file_details = [
                FilePublicationReceipt(
                    relative_path=fd["relative_path"],
                    symbol=fd["symbol"],
                    date=fd["date"],
                    row_count=fd["row_count"],
                    file_size_bytes=fd["file_size_bytes"],
                    sha256=fd["sha256"],
                )
                for fd in rdata.get("file_details", [])
            ]
            recovered_receipts.append(
                PublishReceipt(
                    batch_id=rdata["batch_id"],
                    writer_id=rdata.get("writer_id", writer_id),
                    sequence=rdata.get("sequence", sequence),
                    row_count=rdata.get("row_count", expected_row_count),
                    file_paths=rdata.get("file_paths", [fd.relative_path for fd in file_details]),
                    file_details=file_details,
                    status="PUBLISHED",
                    published_at=rdata.get("published_at", ""),
                )
            )
            continue

        # Check each target in intent
        all_targets_ready = True
        file_details: List[FilePublicationReceipt] = []
        actual_total_rows = 0

        for target in targets:
            rel_path = target["relative_path"]
            target_dest = root / rel_path
            expected_sha = target.get("sha256")
            expected_rows = target.get("row_count", 0)
            expected_bytes = target.get("file_size_bytes", 0)
            symbol = target.get("symbol", "")
            date_str = target.get("date", "")

            staging_rel = target.get("staging_path")
            staging_full = (root / staging_rel) if staging_rel else None

            if target_dest.is_file():
                # Destination already exists
                with open(target_dest, "rb") as f:
                    dest_sha = hashlib.sha256(f.read()).hexdigest()
                if expected_sha and dest_sha != expected_sha:
                    all_targets_ready = False
                    break
                actual_bytes = target_dest.stat().st_size
                pf = pq.ParquetFile(target_dest)
                actual_rows = pf.metadata.num_rows
                file_details.append(
                    FilePublicationReceipt(
                        relative_path=rel_path,
                        symbol=symbol,
                        date=date_str,
                        row_count=actual_rows,
                        file_size_bytes=actual_bytes,
                        sha256=dest_sha,
                    )
                )
                actual_total_rows += actual_rows
                if staging_full and staging_full.is_file():
                    staging_full.unlink(missing_ok=True)
            elif staging_full and staging_full.is_file():
                # Destination does not exist, but staged file is available: move to destination
                with open(staging_full, "rb") as f:
                    stg_sha = hashlib.sha256(f.read()).hexdigest()
                if expected_sha and stg_sha != expected_sha:
                    all_targets_ready = False
                    break
                actual_bytes = staging_full.stat().st_size
                pf = pq.ParquetFile(staging_full)
                actual_rows = pf.metadata.num_rows

                target_dest.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staging_full, target_dest)

                file_details.append(
                    FilePublicationReceipt(
                        relative_path=rel_path,
                        symbol=symbol,
                        date=date_str,
                        row_count=actual_rows,
                        file_size_bytes=actual_bytes,
                        sha256=stg_sha,
                    )
                )
                actual_total_rows += actual_rows
            else:
                # Neither target nor staging file exists
                all_targets_ready = False
                break

        if all_targets_ready and file_details:
            published_at = datetime.now(timezone.utc).isoformat()
            receipt_payload = {
                "batch_id": batch_id,
                "writer_id": writer_id,
                "sequence": sequence,
                "row_count": actual_total_rows if actual_total_rows > 0 else expected_row_count,
                "file_paths": [fd.relative_path for fd in file_details],
                "file_details": [
                    {
                        "relative_path": fd.relative_path,
                        "symbol": fd.symbol,
                        "date": fd.date,
                        "row_count": fd.row_count,
                        "file_size_bytes": fd.file_size_bytes,
                        "sha256": fd.sha256,
                    }
                    for fd in file_details
                ],
                "status": "PUBLISHED",
                "published_at": published_at,
            }

            tmp_receipt = staging_dir / f"tmp_rec_{uuid.uuid4().hex}.json"
            with open(tmp_receipt, "w", encoding="utf-8") as f:
                json.dump(receipt_payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_receipt, receipt_file)

            intent_file.unlink(missing_ok=True)

            recovered_receipts.append(
                PublishReceipt(
                    batch_id=batch_id,
                    writer_id=writer_id,
                    sequence=sequence,
                    row_count=actual_total_rows if actual_total_rows > 0 else expected_row_count,
                    file_paths=[fd.relative_path for fd in file_details],
                    file_details=file_details,
                    status="PUBLISHED",
                    published_at=published_at,
                )
            )

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
