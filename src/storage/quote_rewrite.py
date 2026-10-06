"""Offline schema v1 → v2 rewrite for an explicit lake root.

This module never resolves a production lake. Callers must pass lake_root.
"""

from __future__ import annotations

from datetime import datetime, timezone
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sys
from typing import Any, Callable, Dict, List, Optional
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.config import (
    LAKE_METADATA_FILENAME,
    MAINTENANCE_GUARD_FILENAME,
    StorageConfigError,
    load_lake_metadata,
)
from src.storage.publication import (
    LakeOwnershipError,
    LakePublisherLock,
    _payload_fingerprint,
    _sha256_file,
    _verify_receipt_files,
)
from src.storage.schema import ticks_to_table_v2, validate_table_v2


class QuoteRewriteError(StorageConfigError):
    """Raised when the quote rewrite refuses to start or cannot finish safely."""


PROGRESS_NAME = "quote_rewrite_progress.json"
RETIRED_DIRNAME = "quote_rewrite_retired"
BACKUP_MARKER = "BACKUP_COMPLETE"
REWRITE_OPERATION = "quote_rewrite"
QUARANTINE_REPORT = "quote_rewrite_quarantine.json"
QUARANTINE_PARTS_DIRNAME = "quote_rewrite_quarantine_parts"
# Receipts above this many rows are not re-fingerprinted in memory.
LARGE_RECEIPT_ROWS = 2_000_000


def _require_path(value: Path, name: str) -> Path:
    if value is None:
        raise QuoteRewriteError(f"{name} is required")
    return Path(value).expanduser().resolve()


def _dir_size_bytes(root: Path) -> int:
    total = 0
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            path = Path(dirpath) / name
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total


def _free_bytes(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(path).free


def _atomic_write_json(path: Path, payload: Any, indent: Optional[int] = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=indent, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _quarantine_part_path(root: Path, relative: str) -> Path:
    digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:40]
    return root / "_maintenance" / QUARANTINE_PARTS_DIRNAME / f"{digest}.json"


def _write_quarantine_part(root: Path, relative: str, rows: List[Dict[str, Any]]) -> None:
    _atomic_write_json(
        _quarantine_part_path(root, relative),
        {"relative_path": relative, "quarantine": rows},
        indent=None,
    )


def _read_quarantine_part(root: Path, relative: str) -> List[Dict[str, Any]]:
    path = _quarantine_part_path(root, relative)
    if not path.is_file():
        raise QuoteRewriteError(f"quarantine part is missing for {relative}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("relative_path") != relative:
        raise QuoteRewriteError(f"quarantine part does not belong to {relative}")
    return list(data.get("quarantine") or [])


def _load_progress(root: Path) -> Dict[str, Any]:
    path = root / "_maintenance" / PROGRESS_NAME
    empty = {"run_id": "", "finished": [], "files": {}}
    if not path.is_file():
        return empty
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return empty
    finished = data.get("finished") or []
    files = data.get("files") or {}
    if not isinstance(files, dict):
        files = {}
    progress = {
        "run_id": str(data.get("run_id") or ""),
        "finished": list(finished),
        "files": files,
    }
    # An earlier checkpoint format embedded every quarantined row in the progress
    # file, which made each checkpoint rewrite hundreds of megabytes. Move those rows
    # into their parts first and only then replace the checkpoint, so a stop at any
    # point still leaves one complete copy.
    migrated = False
    for relative, info in files.items():
        if isinstance(info, dict) and "quarantine" in info:
            rows = list(info.pop("quarantine") or [])
            if rows:
                _write_quarantine_part(root, relative, rows)
            info["quarantined_rows"] = len(rows)
            migrated = True
    if migrated:
        _save_progress(root, progress)
    return progress


def _save_progress(root: Path, progress: Dict[str, Any]) -> None:
    payload = {
        "run_id": progress.get("run_id") or "",
        "finished": sorted(set(progress.get("finished") or [])),
        "files": progress.get("files") or {},
    }
    _atomic_write_json(root / "_maintenance" / PROGRESS_NAME, payload, indent=None)


def _list_tick_files(root: Path) -> List[Path]:
    ticks = root / "ticks"
    if not ticks.is_dir():
        return []
    return sorted(path for path in ticks.rglob("*.parquet") if path.is_file())


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    if math.isnan(number) or math.isinf(number):
        return False
    return number > 0.0


def _rewrite_table(table: pa.Table) -> tuple[pa.Table, List[Dict[str, Any]]]:
    kept: List[Dict[str, Any]] = []
    quarantine: List[Dict[str, Any]] = []
    source_rows = table.to_pylist()
    for row in source_rows:
        bid = row.get("bid")
        ask = row.get("ask")
        if _is_present(bid) and _is_present(ask):
            kept.append(
                {
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "bid_price": float(bid),
                    "ask_price": float(ask),
                    "source": row.get("source"),
                    "session": row.get("session"),
                    "ingest_id": row.get("ingest_id"),
                }
            )
        else:
            quarantine.append(
                {
                    "timestamp": row.get("timestamp"),
                    "symbol": row.get("symbol"),
                    "ingest_id": row.get("ingest_id"),
                    "bid": bid,
                    "ask": ask,
                    "price": row.get("price"),
                }
            )
    if len(kept) + len(quarantine) != table.num_rows:
        raise QuoteRewriteError("kept plus quarantined does not equal the old row count")
    for index, row in enumerate(kept):
        original = table.to_pylist()[index] if False else None
        _ = original
    original_rows = source_rows
    original_kept = [row for row in original_rows if _is_present(row.get("bid")) and _is_present(row.get("ask"))]
    if len(original_kept) != len(kept):
        raise QuoteRewriteError("kept row count mismatch against source bid/ask")
    for new_row, old_row in zip(kept, original_kept):
        if float(new_row["bid_price"]) != float(old_row["bid"]):
            raise QuoteRewriteError("bid_price does not equal the old bid")
        if float(new_row["ask_price"]) != float(old_row["ask"]):
            raise QuoteRewriteError("ask_price does not equal the old ask")
        if "price" in new_row:
            raise QuoteRewriteError("refusing to copy price into a v2 row")
    out = ticks_to_table_v2(kept, validate=True) if kept else ticks_to_table_v2([], validate=False)
    if kept:
        validate_table_v2(out)
    return out, quarantine


def _file_inventory(root: Path, *, exclude_names: Optional[set[str]] = None) -> Dict[str, Dict[str, Any]]:
    skip = exclude_names or set()
    inventory: Dict[str, Dict[str, Any]] = {}
    if not root.exists():
        return inventory
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name in skip:
            continue
        rel = path.relative_to(root).as_posix()
        inventory[rel] = {"size": path.stat().st_size, "sha256": _sha256_file(path)}
    return inventory


def _inventories_match(expected: Dict[str, Dict[str, Any]], actual: Dict[str, Dict[str, Any]]) -> bool:
    if set(expected) != set(actual):
        return False
    for rel, meta in expected.items():
        other = actual.get(rel) or {}
        if int(other.get("size", -1)) != int(meta.get("size", -2)):
            return False
        if str(other.get("sha256", "")) != str(meta.get("sha256", "")):
            return False
    return True


def _read_backup_marker(backup_root: Path) -> Optional[Dict[str, Any]]:
    marker = backup_root / BACKUP_MARKER
    if not marker.is_file():
        return None
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict) or "run_id" not in data or "lake_root" not in data:
        return None
    return data


def _copy_lake(
    lake_root: Path,
    backup_root: Path,
    copy_impl: Callable[..., Any],
    *,
    run_id: str,
) -> None:
    marker = backup_root / BACKUP_MARKER
    existing = _read_backup_marker(backup_root)
    if existing is not None:
        if existing.get("lake_root") == str(lake_root) and existing.get("run_id") == run_id:
            backup_files = _file_inventory(backup_root, exclude_names={BACKUP_MARKER})
            if _inventories_match(existing.get("files") or {}, backup_files):
                return
        raise QuoteRewriteError(f"backup marker does not match lake {lake_root} run {run_id}")
    if backup_root.exists() and any(path.name != BACKUP_MARKER for path in backup_root.iterdir()):
        raise QuoteRewriteError(f"backup copy is incomplete at {backup_root}")
    try:
        copy_impl(lake_root, backup_root, dirs_exist_ok=True)
    except TypeError:
        copy_impl(lake_root, backup_root)
    except OSError as exc:
        raise QuoteRewriteError(f"lake copy failed: {exc}") from exc
    live = _file_inventory(lake_root)
    backup_files = _file_inventory(backup_root, exclude_names={BACKUP_MARKER})
    if not _inventories_match(live, backup_files):
        raise QuoteRewriteError("backup copy is incomplete: size or checksum mismatch")
    payload = {"lake_root": str(lake_root), "run_id": run_id, "files": live}
    marker.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _receipt_index(root: Path) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    receipts_dir = root / "_control" / "receipts"
    if not receipts_dir.is_dir():
        return mapping
    for path in receipts_dir.glob("*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        for detail in data.get("file_details") or []:
            mapping[str(detail.get("relative_path"))] = {"path": path, "data": data}
    return mapping


def _load_lineage_map(root: Path) -> Dict[str, Any]:
    path = root / "_control" / "lineage.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    mapping = data.get("file_lineage") if isinstance(data, dict) else None
    return mapping if isinstance(mapping, dict) else {}


def _verify_large_receipt(root: Path, data: Dict[str, Any], via_lineage: set[str]) -> None:
    """Bounded-memory check for a receipt whose rows cannot be fingerprinted in memory."""
    details = data.get("file_details") or []
    paths = {str(item["relative_path"]) for item in details}
    raw_paths = data.get("file_paths")
    if raw_paths is not None and set(map(str, raw_paths)) != paths:
        raise QuoteRewriteError(f"receipt file_paths do not match file_details for {data.get('batch_id')}")
    total = 0
    for item in details:
        rel = str(item["relative_path"])
        total += int(item["row_count"])
        if rel in via_lineage:
            continue
        target = root / rel
        if target.stat().st_size != int(item["file_size_bytes"]):
            raise QuoteRewriteError(f"receipt size mismatch for {rel}")
        if pq.ParquetFile(target).metadata.num_rows != int(item["row_count"]):
            raise QuoteRewriteError(f"receipt row count mismatch for {rel}")
    if total != int(data.get("row_count", -1)):
        raise QuoteRewriteError(f"receipt row_count does not equal its file_details for {data.get('batch_id')}")


def _update_receipts_for_finished(
    root: Path,
    finished: set[str],
    journaled: Optional[set[str]] = None,
) -> None:
    receipts_dir = root / "_control" / "receipts"
    if not receipts_dir.is_dir():
        return
    lineage = _load_lineage_map(root)
    for receipt_path in sorted(receipts_dir.glob("*.json")):
        data = json.loads(receipt_path.read_text(encoding="utf-8"))
        details = data.get("file_details") or []
        relatives = [str(item["relative_path"]) for item in details]
        if not relatives:
            continue
        # A receipt entry is either a finished live file, or an original that
        # compaction retired in favour of a finished compacted file (lineage).
        via_lineage: set[str] = set()
        touched = journaled is None
        eligible = True
        for rel in relatives:
            if rel in finished:
                touched = touched or rel in journaled
                continue
            compacted = (lineage.get(rel) or {}).get("compacted_file")
            if compacted and compacted in finished and not (root / rel).is_file():
                via_lineage.add(rel)
                touched = touched or compacted in journaled
                continue
            eligible = False
            break
        if not eligible or not touched:
            continue

        new_details = []
        live_relatives: List[str] = []
        for item in details:
            rel = str(item["relative_path"])
            if rel in via_lineage:
                new_details.append(item)
                continue
            target = root / rel
            new_details.append(
                {
                    "relative_path": rel,
                    "symbol": item["symbol"],
                    "date": item["date"],
                    "row_count": pq.ParquetFile(target).metadata.num_rows,
                    "file_size_bytes": target.stat().st_size,
                    "sha256": _sha256_file(target),
                }
            )
            live_relatives.append(rel)
        total_rows = sum(int(item["row_count"]) for item in new_details)
        data["file_details"] = new_details
        data["row_count"] = total_rows

        if via_lineage or total_rows > LARGE_RECEIPT_ROWS:
            # The logical fingerprint hashes every row in memory, which cannot be
            # recomputed for a receipt this large, and the v1 hash no longer describes
            # the rewritten rows. Record that explicitly (owner decision) rather than
            # leave a hash that can never verify. Per-file size, checksum and row
            # counts remain the integrity evidence.
            previous = data.get("payload_sha256")
            data["payload_sha256"] = ""
            data["payload_sha256_invalidated"] = {
                "operation": REWRITE_OPERATION,
                "reason": "schema v1 to v2 quote rewrite changed the logical rows; receipt too large to re-fingerprint in memory",
                "previous_payload_sha256": previous,
                "invalidated_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_write_json(receipt_path, data, indent=None)
            _verify_large_receipt(root, json.loads(receipt_path.read_text(encoding="utf-8")), via_lineage)
            continue

        rows: List[Dict[str, Any]] = []
        for rel in live_relatives:
            rows.extend(pq.ParquetFile(root / rel).read().to_pylist())
        data["payload_sha256"] = _payload_fingerprint(rows) if rows else _payload_fingerprint([])
        _atomic_write_json(receipt_path, data)
        _verify_receipt_files(root, data)


def _retire_original(root: Path, relative: str, original: Path) -> None:
    dest = root / "_maintenance" / RETIRED_DIRNAME / relative
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        shutil.copy2(original, dest)


def _swap_file(root: Path, relative: str, new_table: pa.Table) -> None:
    live = root / relative
    staging = root / "_staging" / f"rewrite_{os.getpid()}_{live.name}"
    staging.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(new_table, staging, compression="snappy")
    os.replace(staging, live)


def _foreign_guard(root: Path) -> None:
    guard = root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
    if not guard.exists():
        return
    try:
        data = json.loads(guard.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise QuoteRewriteError(f"lake maintenance in progress: {guard}") from exc
    if data.get("operation") != REWRITE_OPERATION:
        raise QuoteRewriteError(f"lake maintenance in progress: {guard}")


def _hold_lock(root: Path) -> LakePublisherLock:
    guard = root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
    lock = LakePublisherLock(root, writer_id=f"maintenance:{REWRITE_OPERATION}")
    try:
        lock.acquire(blocking=False)
    except LakeOwnershipError as exc:
        raise QuoteRewriteError(f"publisher lock already held for {root}") from exc
    if not guard.exists():
        guard.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "operation": REWRITE_OPERATION,
            "owner": f"pid:{os.getpid()}",
            "pid": os.getpid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_write_json(guard, payload)
    return lock


def _release_lock(root: Path, lock: LakePublisherLock, successful: bool) -> None:
    guard = root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
    if successful and guard.exists():
        try:
            data = json.loads(guard.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
        if data.get("operation") == REWRITE_OPERATION:
            guard.unlink(missing_ok=True)
    lock.release()


def _journal_entry(kept_rows: int, quarantine: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "kept_rows": int(kept_rows),
        "quarantined_rows": len(quarantine),
    }


def _cumulative_from_journal(root: Path, progress: Dict[str, Any]) -> tuple[int, List[Dict[str, Any]]]:
    kept_total = 0
    quarantine_all: List[Dict[str, Any]] = []
    files = progress.get("files") or {}
    for relative in progress.get("finished") or []:
        info = files.get(relative) or {}
        kept_total += int(info.get("kept_rows") or 0)
        expected = int(info.get("quarantined_rows") or 0)
        if expected:
            rows = _read_quarantine_part(root, relative)
            if len(rows) != expected:
                raise QuoteRewriteError(
                    f"quarantine part for {relative} holds {len(rows)} rows, journal says {expected}"
                )
            quarantine_all.extend(rows)
    return kept_total, quarantine_all


def rewrite_quote_lake(
    *,
    lake_root: Path,
    backup_root: Path,
    free_bytes: Optional[int] = None,
    copy_impl: Optional[Callable[..., Any]] = None,
    after_file: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Rewrite every schema v1 file under an explicit lake_root."""
    lake_root = _require_path(lake_root, "lake_root")
    backup_root = _require_path(backup_root, "backup_root")
    if not (lake_root / LAKE_METADATA_FILENAME).is_file():
        raise QuoteRewriteError(f"lake.json missing at {lake_root}")

    _foreign_guard(lake_root)

    lake_bytes = _dir_size_bytes(lake_root)
    available = int(free_bytes) if free_bytes is not None else _free_bytes(backup_root.parent)
    if available < lake_bytes:
        raise QuoteRewriteError(
            f"backup will not fit: lake is {lake_bytes} bytes, free_bytes={available}"
        )

    lock = _hold_lock(lake_root)
    progress = _load_progress(lake_root)
    if not progress.get("run_id"):
        progress["run_id"] = uuid.uuid4().hex
        _save_progress(lake_root, progress)
    try:
        copier = copy_impl or shutil.copytree
        try:
            _copy_lake(lake_root, backup_root, copier, run_id=str(progress["run_id"]))
        except QuoteRewriteError:
            raise
        except Exception as exc:
            raise QuoteRewriteError(f"lake copy failed: {exc}") from exc

        finished = list(progress.get("finished") or [])
        finished_set = set(finished)
        files_journal: Dict[str, Any] = dict(progress.get("files") or {})
        # ``progress`` shares these live containers, so any checkpoint (including the
        # failure checkpoint below) carries the latest state without copying it.
        progress["finished"] = finished
        progress["files"] = files_journal
        rewritten = 0
        skipped = 0
        files = _list_tick_files(lake_root)
        for path in files:
            relative = path.relative_to(lake_root).as_posix()
            # Schema v2 is recognised from the Parquet footer alone. A file that is
            # already v2 needs no checkpoint of its own: every resume re-derives it.
            already_v2 = "bid_price" in pq.ParquetFile(path).schema_arrow.names
            if already_v2 or relative in finished_set:
                if relative not in finished_set:
                    finished.append(relative)
                    finished_set.add(relative)
                skipped += 1
                if after_file is not None:
                    after_file(relative)
                continue
            table = pq.ParquetFile(path).read()
            new_table, quarantined = _rewrite_table(table)
            # Quarantined rows are made durable first, then the checkpoint that names
            # them, and only then is the file retired and swapped.
            if quarantined:
                _write_quarantine_part(lake_root, relative, quarantined)
            files_journal[relative] = _journal_entry(new_table.num_rows, quarantined)
            _save_progress(lake_root, progress)
            _retire_original(lake_root, relative, path)
            _swap_file(lake_root, relative, new_table)
            finished.append(relative)
            finished_set.add(relative)
            rewritten += 1
            if after_file is not None:
                after_file(relative)

        all_relatives = {path.relative_to(lake_root).as_posix() for path in _list_tick_files(lake_root)}
        if not all_relatives.issubset(finished_set):
            raise QuoteRewriteError("not every tick file has passed the rewrite check")

        # Verify the quarantine evidence against the journal while the lake is still
        # marked schema v1, so a missing or short part fails the run before any
        # receipt or lake.json changes.
        _save_progress(lake_root, progress)
        kept_total, quarantine_all = _cumulative_from_journal(lake_root, progress)

        _update_receipts_for_finished(lake_root, finished_set, set(files_journal))

        retired_root = lake_root / "_maintenance" / RETIRED_DIRNAME
        if retired_root.exists():
            shutil.rmtree(retired_root)

        meta = load_lake_metadata(lake_root, check_maintenance=False)
        payload = meta.to_dict()
        payload["schema_version"] = 2
        versions = list(payload.get("compatible_versions") or [])
        if 2 not in versions:
            versions.append(2)
        payload["compatible_versions"] = versions
        _atomic_write_json(lake_root / LAKE_METADATA_FILENAME, payload)

        report = {
            "kept_rows": kept_total,
            "quarantined_rows": len(quarantine_all),
            "quarantine": quarantine_all,
            "files_rewritten": rewritten,
            "files_skipped": skipped,
            "run_id": progress["run_id"],
        }
        _atomic_write_json(lake_root / "_maintenance" / QUARANTINE_REPORT, report, indent=None)
        _release_lock(lake_root, lock, successful=True)
        return report
    except Exception:
        _save_progress(lake_root, progress)
        _release_lock(lake_root, lock, successful=False)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rewrite a schema v1 tick lake to bid_price/ask_price. Both roots are required.",
    )
    parser.add_argument("--lake-root", required=True, help="Path to the lake to rewrite. No default.")
    parser.add_argument("--backup-root", required=True, help="Path for the backup copy. No default.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    report = rewrite_quote_lake(
        lake_root=Path(args.lake_root),
        backup_root=Path(args.backup_root),
    )
    print(json.dumps({k: v for k, v in report.items() if k != "quarantine"}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
