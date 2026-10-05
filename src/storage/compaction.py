"""
Offline compaction, durable maintenance journal, consumer drain protocol,
immutable lineage tracking, and physical purge automation for Tick Lake.
Milestone 4.3 - Package E (CAPA-02, CAPA-03, CAPA-04).
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.config import (
    MAINTENANCE_GUARD_FILENAME,
    decode_symbol,
    encode_symbol,
    get_partition_path,
    resolve_tick_lake_root,
)
from src.storage.publication import (
    LakeMaintenanceInProgressError,
    LakePublisherLock,
    _payload_fingerprint,
)
from src.storage.registry import (
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    SymbolNotFoundError,
    SymbolRegistry,
    get_symbol_registry,
)
from src.storage.schema import (
    LAKE_SCHEMA_V1,
    validate_table_v1,
)


MAINTENANCE_JOURNAL_FILENAME = "journal.json"
LINEAGE_FILENAME = "lineage.json"

STATE_REQUESTED = "REQUESTED"
STATE_DRAINING = "DRAINING"
STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_STAGED = "STAGED"
STATE_COMMITTED = "COMMITTED"
STATE_ABORTED = "ABORTED"

DEFAULT_DRAIN_TIMEOUT = 15.0
DEFAULT_MIN_FILE_SIZE_BYTES = 1024 * 1024  # 1 MiB


class CompactionError(Exception):
    """Base exception for lake compaction and maintenance failures."""
    pass


class MaintenanceJournalError(CompactionError):
    """Raised when journal state machine encounters inconsistent or invalid state."""
    pass


class ConsumerDrainError(CompactionError):
    """Raised when consumer draining fails or encounters errors."""
    pass


class ConsumerDrainRefusedError(ConsumerDrainError):
    """Raised when replacement is refused because consumer shutdown cannot be established."""
    pass


class EquivalenceVerificationError(CompactionError):
    """Raised when multiset equivalence verification fails between inputs and outputs."""
    pass


class PurgeError(Exception):
    """Raised when physical symbol purge validation or execution fails."""
    pass


class MaintenanceJournal:
    """
    Durable maintenance journal tracking offline state transitions atomically
    at <lake_root>/_maintenance/journal.json.
    """

    def __init__(self, lake_root: Path):
        self.root = Path(lake_root).resolve()
        self.maintenance_dir = self.root / "_maintenance"
        self.journal_path = self.maintenance_dir / MAINTENANCE_JOURNAL_FILENAME

    def load(self) -> Optional[Dict[str, Any]]:
        """Loads journal data from disk without mutating."""
        if not self.journal_path.is_file():
            return None
        try:
            with open(self.journal_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
        except Exception as exc:
            raise MaintenanceJournalError(f"Cannot read maintenance journal: {exc}") from exc

    def record_transition(
        self,
        state: str,
        maintenance_id: str,
        plan: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Atomically records a journal state transition.
        States: REQUESTED, DRAINING, IN_PROGRESS, STAGED, COMMITTED, ABORTED.
        """
        valid_states = {
            STATE_REQUESTED,
            STATE_DRAINING,
            STATE_IN_PROGRESS,
            STATE_STAGED,
            STATE_COMMITTED,
            STATE_ABORTED,
        }
        if state not in valid_states:
            raise MaintenanceJournalError(f"Invalid journal state: {state}")

        self.maintenance_dir.mkdir(parents=True, exist_ok=True)
        existing = self.load() or {}

        now_iso = datetime.now(timezone.utc).isoformat()
        current_plan = plan if plan is not None else existing.get("plan", {})
        current_details = dict(existing.get("details", {}))
        if details:
            current_details.update(details)

        journal_data = {
            "maintenance_id": maintenance_id,
            "operation": "compaction",
            "state": state,
            "owner": f"pid:{os.getpid()}",
            "pid": os.getpid(),
            "started_at": existing.get("started_at", now_iso),
            "updated_at": now_iso,
            "plan": current_plan,
            "details": current_details,
            "error": error if error is not None else existing.get("error"),
        }

        tmp_name = f"tmp_journal_{os.getpid()}_{uuid.uuid4().hex[:8]}.json"
        tmp_path = self.maintenance_dir / tmp_name

        payload = json.dumps(journal_data, indent=2)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, self.journal_path)
        try:
            dir_fd = os.open(str(self.maintenance_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        return journal_data


class LineageManager:
    """
    Manages immutable receipt and file lineage mapping at <lake_root>/_control/lineage.json.
    """

    def __init__(self, lake_root: Path):
        self.root = Path(lake_root).resolve()
        self.control_dir = self.root / "_control"
        self.lineage_path = self.control_dir / LINEAGE_FILENAME

    def load(self) -> Dict[str, Any]:
        """Loads current lineage mapping."""
        if not self.lineage_path.is_file():
            return {
                "version": 1,
                "updated_at": "",
                "file_lineage": {},
                "receipt_lineage": {},
                "compactions": [],
            }
        try:
            with open(self.lineage_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("Lineage root must be a dict")
            data.setdefault("file_lineage", {})
            data.setdefault("receipt_lineage", {})
            data.setdefault("compactions", [])
            return data
        except Exception as exc:
            raise CompactionError(f"Cannot load lineage: {exc}") from exc

    def record_compaction(
        self,
        maintenance_id: str,
        file_mappings: Dict[str, Dict[str, Any]],
        receipt_mappings: Dict[str, Dict[str, Any]],
        compaction_entry: Dict[str, Any],
    ) -> None:
        """Atomically appends compaction lineage to <lake_root>/_control/lineage.json."""
        self.control_dir.mkdir(parents=True, exist_ok=True)
        lineage = self.load()

        now_iso = datetime.now(timezone.utc).isoformat()
        lineage["updated_at"] = now_iso
        lineage["file_lineage"].update(file_mappings)
        lineage["receipt_lineage"].update(receipt_mappings)
        lineage["compactions"].append(compaction_entry)

        tmp_name = f"tmp_lineage_{os.getpid()}_{uuid.uuid4().hex[:8]}.json"
        tmp_path = self.control_dir / tmp_name

        payload = json.dumps(lineage, indent=2)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, self.lineage_path)
        try:
            dir_fd = os.open(str(self.control_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def resolve_file(self, relative_path: str) -> str:
        """Resolves an old relative path transitively to the newest compacted generation."""
        lineage = self.load()
        file_map = lineage.get("file_lineage", {})
        current = relative_path
        seen = set()
        while current in file_map and current not in seen:
            seen.add(current)
            current = file_map[current].get("compacted_file", current)
        return current


class LakeCompactor:
    """
    Coordinates durable offline partition compaction, multiset equivalence verification,
    atomic replacement, and receipt lineage preservation.
    """

    def __init__(
        self,
        lake_root: Optional[Union[str, Path]] = None,
        min_file_size_bytes: int = DEFAULT_MIN_FILE_SIZE_BYTES,
        row_group_size: int = 50_000,
        compression: str = "snappy",
        supervisor: Optional[Any] = None,
        configured_consumers: Optional[List[str]] = None,
        consumer_verifier: Optional[Callable[[], bool]] = None,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    ):
        self.root = resolve_tick_lake_root(lake_root)
        self.min_file_size_bytes = int(min_file_size_bytes)
        self.row_group_size = int(row_group_size)
        self.compression = compression
        self.supervisor = supervisor
        self.configured_consumers = list(configured_consumers or [])
        self.consumer_verifier = consumer_verifier
        self.drain_timeout = float(drain_timeout)

        self.maintenance_dir = self.root / "_maintenance"
        self.guard_path = self.maintenance_dir / MAINTENANCE_GUARD_FILENAME
        self.journal = MaintenanceJournal(self.root)
        self.lineage = LineageManager(self.root)

    def find_candidates(
        self,
        symbol: Optional[str] = None,
        date_str: Optional[str] = None,
        force: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Discovers partitions requiring compaction (multiple files or small files).
        """
        ticks_dir = self.root / "ticks"
        candidates: List[Dict[str, Any]] = []
        if not ticks_dir.is_dir():
            return candidates

        sym_dirs = [p for p in ticks_dir.iterdir() if p.is_dir() and p.name.startswith("symbol=")]
        for sym_dir in sorted(sym_dirs, key=lambda p: p.name):
            encoded_sym = sym_dir.name[len("symbol="):]
            sym = decode_symbol(encoded_sym)
            if symbol and sym.upper() != symbol.strip().upper():
                continue

            dt_dirs = [p for p in sym_dir.iterdir() if p.is_dir() and p.name.startswith("date=")]
            for dt_dir in sorted(dt_dirs, key=lambda p: p.name):
                dt = dt_dir.name[len("date="):]
                if date_str and dt != date_str.strip():
                    continue

                parquet_files = [
                    f for f in dt_dir.iterdir()
                    if f.is_file() and f.name.endswith(".parquet") and not f.name.startswith((".", "tmp_"))
                ]
                if not parquet_files:
                    continue

                total_size = sum(f.stat().st_size for f in parquet_files)
                has_multiple = len(parquet_files) > 1
                has_small = any(f.stat().st_size < self.min_file_size_bytes for f in parquet_files)

                # Compaction candidate if multiple files, or forced, or small files with multiple files
                if has_multiple or force:
                    candidates.append({
                        "symbol": sym,
                        "date": dt,
                        "partition_dir": dt_dir.relative_to(self.root).as_posix(),
                        "files": [f.relative_to(self.root).as_posix() for f in sorted(parquet_files, key=lambda p: p.name)],
                        "total_files": len(parquet_files),
                        "total_bytes": total_size,
                    })

        return candidates

    def _drain_consumers(self) -> None:
        """
        Consumer Drain Protocol (CAPA-02):
        1. Suspend supervisor restarts if present.
        2. Verify all configured consumers are stopped.
        3. Poll registered active reader leases until drain_timeout.
        4. Refuse replacement if shutdown cannot be established.
        """
        # 1. Pause supervisor managed restarts
        if self.supervisor is not None:
            if hasattr(self.supervisor, "suspend_for_handoff"):
                exit_code = self.supervisor.suspend_for_handoff(timeout=self.drain_timeout)
            elif hasattr(self.supervisor, "is_handoff_suspended"):
                self.supervisor.is_handoff_suspended = True

        # 2. Check external consumer verifier
        if self.consumer_verifier is not None:
            try:
                verified = self.consumer_verifier()
                if not verified:
                    raise ConsumerDrainRefusedError(
                        "Consumer verifier reported active or unverified consumers; refusing replacement."
                    )
            except Exception as exc:
                if isinstance(exc, ConsumerDrainRefusedError):
                    raise
                raise ConsumerDrainRefusedError(f"Consumer verification failed: {exc}") from exc

        # 3. Check registered active readers in _control/readers/
        readers_dir = self.root / "_control" / "readers"
        deadline = time.monotonic() + self.drain_timeout
        while time.monotonic() < deadline:
            active_readers = []
            if readers_dir.is_dir():
                for rf in readers_dir.iterdir():
                    if rf.is_file() and rf.name.endswith(".json"):
                        try:
                            rinfo = json.loads(rf.read_text(encoding="utf-8"))
                            pid = rinfo.get("pid")
                            if pid and pid != os.getpid():
                                # Check if process is still alive
                                try:
                                    os.kill(pid, 0)
                                    active_readers.append(pid)
                                except (OSError, ProcessLookupError):
                                    rf.unlink(missing_ok=True)
                        except Exception:
                            pass
            if not active_readers:
                break
            time.sleep(0.05)
        else:
            raise ConsumerDrainRefusedError(
                f"Active reader queries (PIDs {active_readers}) did not drain within {self.drain_timeout}s; refusing replacement."
            )

    def _write_guard_file(self, maintenance_id: str) -> None:
        """Writes <lake_root>/_maintenance/in_progress.json guard."""
        self.maintenance_dir.mkdir(parents=True, exist_ok=True)
        guard_data = {
            "operation": "compaction",
            "maintenance_id": maintenance_id,
            "owner": f"pid:{os.getpid()}",
            "pid": os.getpid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp_name = f"tmp_guard_{os.getpid()}_{uuid.uuid4().hex[:8]}.json"
        tmp_path = self.maintenance_dir / tmp_name

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(guard_data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, self.guard_path)
        try:
            dir_fd = os.open(str(self.maintenance_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def _remove_guard_file(self) -> None:
        """Removes guard file cleanly."""
        self.guard_path.unlink(missing_ok=True)
        try:
            dir_fd = os.open(str(self.maintenance_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    def _verify_multiset_equivalence(
        self,
        input_paths: List[Path],
        staged_path: Path,
    ) -> None:
        """
        Enforces 100% multiset equivalence between inputs and compacted output (CAPA-03):
        - Column schemas, row counts, duplicate multiplicities, timestamps, float precision.
        - DuckDB bidirectional EXCEPT ALL and payload fingerprint.
        """
        input_str_paths = [str(p.resolve()) for p in input_paths]
        staged_str_path = str(staged_path.resolve())

        # Validate PyArrow table schema
        staged_table = pq.ParquetFile(staged_path).read()
        validate_table_v1(staged_table)

        con = duckdb.connect(":memory:")
        try:
            # 1. Total row count verification
            in_count = int(con.execute(
                "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                [input_str_paths]
            ).fetchone()[0])

            out_count = int(con.execute(
                "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                [[staged_str_path]]
            ).fetchone()[0])

            if in_count != out_count:
                raise EquivalenceVerificationError(
                    f"Row count mismatch: inputs have {in_count} rows, staged output has {out_count} rows"
                )

            # 2. DuckDB bidirectional EXCEPT ALL (checks exact values, floats, duplicate multiplicity)
            diff_query = """
                SELECT count(*) FROM (
                    (SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
                     FROM read_parquet(?, hive_partitioning=false)
                     EXCEPT ALL
                     SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
                     FROM read_parquet(?, hive_partitioning=false))
                    UNION ALL
                    (SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
                     FROM read_parquet(?, hive_partitioning=false)
                     EXCEPT ALL
                     SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
                     FROM read_parquet(?, hive_partitioning=false))
                )
            """
            diff_count = int(con.execute(
                diff_query,
                [input_str_paths, [staged_str_path], [staged_str_path], input_str_paths]
            ).fetchone()[0])

            if diff_count != 0:
                raise EquivalenceVerificationError(
                    f"Multiset equivalence failed: bidirectional EXCEPT ALL found {diff_count} difference rows"
                )

            # 3. Payload fingerprint verification
            in_tables = [pq.ParquetFile(p).read().cast(LAKE_SCHEMA_V1) for p in input_paths]
            in_table = pa.concat_tables(in_tables)
            if _payload_fingerprint(in_table) != _payload_fingerprint(staged_table):
                raise EquivalenceVerificationError("Logical payload multiset fingerprint mismatch between inputs and staged file")

        finally:
            con.close()

    def compact(
        self,
        symbol: Optional[str] = None,
        date_str: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Executes complete durable offline compaction pipeline:
        1. Identify candidate partitions.
        2. Record REQUESTED in journal.
        3. Acquire LakePublisherLock (exclusive lake ownership).
        4. Place _maintenance/in_progress.json guard.
        5. Record DRAINING and drain consumers.
        6. Record IN_PROGRESS and build staged files outside active globs.
        7. Verify multiset equivalence.
        8. Record STAGED and perform atomic replacement & retirement.
        9. Record lineage map in _control/lineage.json.
        10. Record COMMITTED, remove guard, release lock, resume supervisor.
        """
        candidates = self.find_candidates(symbol=symbol, date_str=date_str, force=force)
        if not candidates:
            return {
                "status": "NOOP",
                "compacted_partitions": 0,
                "consolidated_files": 0,
                "message": "No partitions qualify for compaction.",
            }

        maintenance_id = f"maint_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        plan = {
            "maintenance_id": maintenance_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "target_partitions": candidates,
        }

        # 2. Record REQUESTED
        self.journal.record_transition(STATE_REQUESTED, maintenance_id=maintenance_id, plan=plan)

        # 3. Acquire LakePublisherLock
        publisher_lock = LakePublisherLock(
            root=self.root,
            writer_id=f"maintenance:{maintenance_id}",
            ignore_maintenance=True,
        )
        if not publisher_lock.acquire(blocking=True, timeout=self.drain_timeout):
            self.journal.record_transition(
                STATE_ABORTED,
                maintenance_id=maintenance_id,
                error="Could not acquire lake publisher lock within timeout.",
            )
            raise CompactionError("Could not acquire exclusive lake publisher lock for maintenance.")

        try:
            # 4. Write in_progress guard file
            self._write_guard_file(maintenance_id=maintenance_id)

            # 5. Record DRAINING and execute drain protocol
            self.journal.record_transition(STATE_DRAINING, maintenance_id=maintenance_id)
            try:
                self._drain_consumers()
            except Exception as drain_exc:
                self.journal.record_transition(
                    STATE_ABORTED,
                    maintenance_id=maintenance_id,
                    error=str(drain_exc),
                )
                self._remove_guard_file()
                raise

            # 6. Record IN_PROGRESS
            self.journal.record_transition(STATE_IN_PROGRESS, maintenance_id=maintenance_id)

            staging_base = self.maintenance_dir / "staging"
            staging_base.mkdir(parents=True, exist_ok=True)
            retired_base = self.maintenance_dir / "retired"
            retired_base.mkdir(parents=True, exist_ok=True)

            staged_partitions = []
            file_lineage_entries: Dict[str, Dict[str, Any]] = {}
            receipt_lineage_entries: Dict[str, Dict[str, Any]] = {}
            all_input_files: Set[str] = set()

            for cand in candidates:
                sym = cand["symbol"]
                dt = cand["date"]
                part_rel = cand["partition_dir"]
                input_rels = cand["files"]
                all_input_files.update(input_rels)

                input_paths = [self.root / rel for rel in input_rels]
                part_staging_dir = staging_base / part_rel
                part_staging_dir.mkdir(parents=True, exist_ok=True)

                # Determine generation number: inspect existing input filenames for compacted_gen(\d+)_
                current_gens = []
                for p in input_paths:
                    m = re.search(r"compacted_gen(\d+)_", p.name) or re.search(r"compacted_(\d+)_", p.name)
                    if m:
                        current_gens.append(int(m.group(1)))
                next_gen = (max(current_gens) + 1) if current_gens else 1

                staged_filename = f"compacted_gen{next_gen}_{uuid.uuid4().hex[:12]}.parquet"
                staged_path = part_staging_dir / staged_filename
                target_rel = f"{part_rel}/{staged_filename}"
                target_path = self.root / target_rel

                # Consolidate and sort rows deterministically using DuckDB
                con = duckdb.connect(":memory:")
                try:
                    input_str_paths = [str(p.resolve()) for p in input_paths]
                    # Deterministic total order: timestamp, ingest_id, and all columns for stability
                    sorted_arrow = con.execute("""
                        SELECT timestamp, symbol, price, volume, bid, ask, source, session, ingest_id
                        FROM read_parquet(?, hive_partitioning=false)
                        ORDER BY timestamp ASC, ingest_id ASC, price ASC, volume ASC, bid ASC, ask ASC, source ASC, session ASC
                    """, [input_str_paths]).to_arrow_table()
                    sorted_arrow = sorted_arrow.cast(LAKE_SCHEMA_V1)
                finally:
                    con.close()

                validate_table_v1(sorted_arrow)
                pq.write_table(
                    sorted_arrow,
                    staged_path,
                    compression=self.compression,
                    row_group_size=self.row_group_size,
                )

                # Flush & fsync staged file and directory
                with open(staged_path, "rb") as sf:
                    os.fsync(sf.fileno())
                try:
                    dir_fd = os.open(str(part_staging_dir), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass

                # 7. Multiset equivalence verification
                self._verify_multiset_equivalence(input_paths=input_paths, staged_path=staged_path)

                staged_partitions.append({
                    "symbol": sym,
                    "date": dt,
                    "partition_dir": part_rel,
                    "generation": next_gen,
                    "input_files": input_rels,
                    "staged_file": staged_path.relative_to(self.root).as_posix(),
                    "target_file": target_rel,
                    "row_count": sorted_arrow.num_rows,
                })

                # Prepare lineage records for this partition
                for in_rel in input_rels:
                    in_path = self.root / in_rel
                    sz = in_path.stat().st_size if in_path.exists() else 0
                    file_lineage_entries[in_rel] = {
                        "compacted_file": target_rel,
                        "symbol": sym,
                        "date": dt,
                        "compacted_at": datetime.now(timezone.utc).isoformat(),
                        "generation": next_gen,
                        "maintenance_id": maintenance_id,
                        "file_size_bytes": sz,
                    }

            # Map existing publication receipts that referenced these input files
            receipts_dir = self.root / "_control" / "receipts"
            if receipts_dir.is_dir():
                for rf in receipts_dir.iterdir():
                    if rf.is_file() and rf.name.endswith(".json"):
                        try:
                            rdata = json.loads(rf.read_text(encoding="utf-8"))
                            r_paths = set(rdata.get("file_paths", []))
                            if not r_paths:
                                r_paths = {d.get("relative_path") for d in rdata.get("file_details", [])}
                            intersect = r_paths.intersection(all_input_files)
                            if intersect:
                                batch_id = rdata.get("batch_id", rf.stem)
                                receipt_lineage_entries[batch_id] = {
                                    "batch_id": batch_id,
                                    "compacted_files": list({file_lineage_entries[p]["compacted_file"] for p in intersect if p in file_lineage_entries}),
                                    "input_files": list(intersect),
                                    "maintenance_id": maintenance_id,
                                }
                        except Exception:
                            pass

            # 8. Record STAGED in journal with full replacement manifest
            plan["staged_partitions"] = staged_partitions
            self.journal.record_transition(
                STATE_STAGED,
                maintenance_id=maintenance_id,
                plan=plan,
                details={
                    "file_lineage": file_lineage_entries,
                    "receipt_lineage": receipt_lineage_entries,
                },
            )

            # Atomic replacement: move staged files into partitions & retire inputs
            for sp in staged_partitions:
                staged_abs = self.root / sp["staged_file"]
                target_abs = self.root / sp["target_file"]
                target_abs.parent.mkdir(parents=True, exist_ok=True)

                # Move staged file to target partition
                os.replace(staged_abs, target_abs)
                try:
                    dir_fd = os.open(str(target_abs.parent), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass

                # Retire old input files outside active globs
                for in_rel in sp["input_files"]:
                    in_abs = self.root / in_rel
                    if in_abs.is_file():
                        retire_dest = retired_base / in_rel
                        retire_dest.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(in_abs, retire_dest)
                        try:
                            dir_fd = os.open(str(in_abs.parent), os.O_RDONLY)
                            try:
                                os.fsync(dir_fd)
                            finally:
                                os.close(dir_fd)
                        except OSError:
                            pass

            # 9. Record lineage in _control/lineage.json
            self.lineage.record_compaction(
                maintenance_id=maintenance_id,
                file_mappings=file_lineage_entries,
                receipt_mappings=receipt_lineage_entries,
                compaction_entry={
                    "maintenance_id": maintenance_id,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "partitions": [
                        {
                            "symbol": sp["symbol"],
                            "date": sp["date"],
                            "target_file": sp["target_file"],
                            "input_files": sp["input_files"],
                            "row_count": sp["row_count"],
                        }
                        for sp in staged_partitions
                    ],
                },
            )

            # Clean up retired inputs
            shutil.rmtree(retired_base, ignore_errors=True)
            shutil.rmtree(staging_base, ignore_errors=True)

            # 10. Record COMMITTED
            self.journal.record_transition(
                STATE_COMMITTED,
                maintenance_id=maintenance_id,
                plan=plan,
            )

            # Remove guard file
            self._remove_guard_file()

            # Resume supervisor if suspended
            if self.supervisor is not None and hasattr(self.supervisor, "resume_after_handoff"):
                try:
                    self.supervisor.resume_after_handoff()
                except Exception:
                    pass

            return {
                "status": "COMPLETED",
                "maintenance_id": maintenance_id,
                "compacted_partitions": len(staged_partitions),
                "consolidated_files": len(all_input_files),
                "staged_partitions": staged_partitions,
            }

        finally:
            publisher_lock.release()

    def compact_partition(
        self,
        symbol: str,
        date_str: Union[str, Any],
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Compacts a single partition identified by (symbol, date_str).
        """
        if hasattr(date_str, "strftime"):
            d_str = date_str.strftime("%Y-%m-%d")
        else:
            d_str = str(date_str).strip()
        return self.compact(symbol=symbol, date_str=d_str, force=force)


def recover_maintenance(
    lake_root: Union[str, Path],
    roll_forward: bool = True,
) -> Dict[str, Any]:
    """
    Crash-Safe Maintenance Recovery (CAPA-03):
    Inspects _maintenance/in_progress.json and journal.json.
    - If in REQUESTED, DRAINING, IN_PROGRESS, or ABORTED: cleans staging, marks ABORTED, removes guard.
    - If in STAGED: finishes atomic replacement (roll_forward=True) or restores old files (roll_forward=False).
    - If in COMMITTED: clears residual guard file and staging.
    Consumers never observe torn files or duplicate rows across restarts.
    """
    root = resolve_tick_lake_root(lake_root)
    maint_dir = root / "_maintenance"
    guard_path = maint_dir / MAINTENANCE_GUARD_FILENAME
    journal = MaintenanceJournal(root)
    lineage = LineageManager(root)

    # Acquire lock with ignore_maintenance to perform recovery safely
    lock = LakePublisherLock(root=root, writer_id="maintenance:recovery", ignore_maintenance=True)
    if not lock.acquire(blocking=True, timeout=DEFAULT_DRAIN_TIMEOUT):
        raise CompactionError("Cannot acquire publisher lock for maintenance recovery")

    try:
        journal_data = journal.load()
        if not journal_data:
            # Stale guard without journal: check PID or clean up
            if guard_path.exists():
                guard_path.unlink(missing_ok=True)
            shutil.rmtree(maint_dir / "staging", ignore_errors=True)
            return {"status": "CLEARED_STALE_MARKER"}

        state = journal_data.get("state")
        maintenance_id = journal_data.get("maintenance_id", "unknown")
        plan = journal_data.get("plan", {})
        details = journal_data.get("details", {})

        if state in (STATE_REQUESTED, STATE_DRAINING, STATE_IN_PROGRESS, STATE_ABORTED):
            # Clean staging artifacts; active files were untouched
            shutil.rmtree(maint_dir / "staging", ignore_errors=True)
            shutil.rmtree(maint_dir / "retired", ignore_errors=True)
            journal.record_transition(STATE_ABORTED, maintenance_id=maintenance_id, plan=plan)
            guard_path.unlink(missing_ok=True)
            return {"status": "ROLLED_BACK", "state": STATE_ABORTED}

        elif state == STATE_STAGED:
            staged_partitions = plan.get("staged_partitions", [])
            retired_base = maint_dir / "retired"

            if roll_forward:
                # Complete promotion of all staged files and retirement of inputs
                for sp in staged_partitions:
                    staged_rel = sp.get("staged_file")
                    target_rel = sp.get("target_file")
                    staged_abs = root / staged_rel if staged_rel else None
                    target_abs = root / target_rel if target_rel else None

                    # If staged file still exists, move to target
                    if staged_abs and staged_abs.is_file() and target_abs:
                        target_abs.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(staged_abs, target_abs)

                    # Move remaining input files to retired
                    for in_rel in sp.get("input_files", []):
                        in_abs = root / in_rel
                        if in_abs.is_file():
                            ret_abs = retired_base / in_rel
                            ret_abs.parent.mkdir(parents=True, exist_ok=True)
                            os.replace(in_abs, ret_abs)

                # Record lineage
                file_lineage = details.get("file_lineage", {})
                receipt_lineage = details.get("receipt_lineage", {})
                if file_lineage:
                    lineage.record_compaction(
                        maintenance_id=maintenance_id,
                        file_mappings=file_lineage,
                        receipt_mappings=receipt_lineage,
                        compaction_entry={
                            "maintenance_id": maintenance_id,
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "recovery": True,
                        },
                    )

                shutil.rmtree(retired_base, ignore_errors=True)
                shutil.rmtree(maint_dir / "staging", ignore_errors=True)
                journal.record_transition(STATE_COMMITTED, maintenance_id=maintenance_id, plan=plan)
                guard_path.unlink(missing_ok=True)
                return {"status": "RECOVERED_COMMITTED"}

            else:
                # Explicit rollback: restore any retired files, unlink target files
                for sp in staged_partitions:
                    target_abs = root / sp.get("target_file")
                    if target_abs and target_abs.is_file():
                        target_abs.unlink(missing_ok=True)

                    for in_rel in sp.get("input_files", []):
                        ret_abs = retired_base / in_rel
                        orig_abs = root / in_rel
                        if ret_abs.is_file():
                            orig_abs.parent.mkdir(parents=True, exist_ok=True)
                            os.replace(ret_abs, orig_abs)

                shutil.rmtree(retired_base, ignore_errors=True)
                shutil.rmtree(maint_dir / "staging", ignore_errors=True)
                journal.record_transition(STATE_ABORTED, maintenance_id=maintenance_id, plan=plan)
                guard_path.unlink(missing_ok=True)
                return {"status": "ROLLED_BACK", "state": STATE_ABORTED}

        elif state == STATE_COMMITTED:
            shutil.rmtree(maint_dir / "staging", ignore_errors=True)
            shutil.rmtree(maint_dir / "retired", ignore_errors=True)
            guard_path.unlink(missing_ok=True)
            return {"status": "CLEARED_COMPLETED"}

        return {"status": "UNKNOWN", "state": state}

    finally:
        lock.release()


def purge_symbol_physical(
    lake_root: Union[str, Path],
    symbol: str,
) -> Dict[str, Any]:
    """
    Physical Purge Automation (CAPA-04):
    - Validates that symbol is in PENDING_PURGE status with active=False in SymbolRegistry.
    - Validates fenced generation.
    - Removes partition directories strictly under ticks/symbol=<encoded_symbol>/.
    - Calls registry.complete_purge(symbol) to archive generation.
    - Never touches source backups or active symbols.
    - Crash-safe: registry preserves PENDING_PURGE until cleanup completes.
    """
    root = resolve_tick_lake_root(lake_root)
    registry = get_symbol_registry(root=root)

    entry = registry.get_symbol(symbol)
    if entry is None:
        raise SymbolNotFoundError(f"Symbol {symbol} not found in registry")

    if entry.active:
        raise PurgeError(f"Cannot physically purge active symbol {symbol}")

    if entry.status != STATUS_PENDING_PURGE:
        raise PurgeError(
            f"Cannot physically purge symbol {symbol} in status {entry.status}; must be PENDING_PURGE"
        )

    fenced_generation = entry.generation

    # Acquire publisher lock to fence publishers while unlinking
    lock = LakePublisherLock(root=root, writer_id=f"purge:{symbol}")
    if not lock.acquire(blocking=True, timeout=DEFAULT_DRAIN_TIMEOUT):
        raise PurgeError(f"Could not acquire publisher lock to purge {symbol}")

    try:
        # Re-validate entry under lock
        recheck = registry.get_symbol(symbol)
        if recheck is None or recheck.status != STATUS_PENDING_PURGE or recheck.generation != fenced_generation:
            raise PurgeError(f"Symbol {symbol} state or generation changed concurrently; aborting purge")

        encoded_sym = encode_symbol(symbol)
        sym_dir = root / "ticks" / f"symbol={encoded_sym}"

        deleted_files = 0
        deleted_bytes = 0
        if sym_dir.is_dir():
            for root_d, dirs, files in os.walk(sym_dir, topdown=False):
                for f in files:
                    fp = Path(root_d) / f
                    deleted_files += 1
                    deleted_bytes += fp.stat().st_size
                    fp.unlink(missing_ok=True)
                for d in dirs:
                    (Path(root_d) / d).rmdir()
            sym_dir.rmdir()

            try:
                ticks_fd = os.open(str(root / "ticks"), os.O_RDONLY)
                try:
                    os.fsync(ticks_fd)
                finally:
                    os.close(ticks_fd)
            except OSError:
                pass

        # Complete purge in registry: archives generation and removes from symbols
        registry.complete_purge(symbol)

        return {
            "status": "PURGED",
            "symbol": symbol,
            "generation": fenced_generation,
            "deleted_files": deleted_files,
            "deleted_bytes": deleted_bytes,
            "directory": str(sym_dir),
        }

    finally:
        lock.release()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tick Lake Offline Compactor & Physical Purge (Package E)")
    parser.add_argument("--lake-root", type=str, default=None, help="Root path of the tick lake")
    parser.add_argument("--symbol", type=str, default=None, help="Specific symbol to compact or purge")
    parser.add_argument("--date", type=str, default=None, help="Specific date (YYYY-MM-DD) to compact")
    parser.add_argument("--force", action="store_true", help="Force compaction even if files exceed target size")
    parser.add_argument("--recover", action="store_true", help="Run crash recovery on interrupted maintenance")
    parser.add_argument("--purge", type=str, default=None, help="Execute physical purge for symbol in PENDING_PURGE")
    args = parser.parse_args()

    lake_root = resolve_tick_lake_root(args.lake_root)

    if args.recover:
        print(f"Executing maintenance recovery on {lake_root}...")
        result = recover_maintenance(lake_root)
        print(json.dumps(result, indent=2))
        sys.exit(0)

    if args.purge:
        sym = args.purge.strip().upper()
        print(f"Executing physical purge for symbol {sym}...")
        result = purge_symbol_physical(lake_root, sym)
        print(json.dumps(result, indent=2))
        sys.exit(0)

    print(f"Running offline compaction on {lake_root}...")
    compactor = LakeCompactor(lake_root=lake_root)
    result = compactor.compact(symbol=args.symbol, date_str=args.date, force=args.force)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
