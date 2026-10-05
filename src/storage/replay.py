"""
Bounded chronological snapshot replay iterator and resumable cursor for Tick Lake.
Milestone 4.3 - Package F: Market Rewind & Bounded Replay Iterator (RPLY-01, RPLY-02, RPLY-03).

Provides lock-free, bounded chronological replay over frozen immutable file inventories
yielding Arrow RecordBatches/Tables without loading full history into memory or using large OFFSET scans.
Enforces deterministic total ordering and supports stateful cursor resumption across process restarts.
"""
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union
import uuid

import duckdb
import pyarrow as pa

from src.storage.config import (
    LakeMaintenanceInProgressError,
    LakeNotFoundError,
    decode_symbol,
    encode_symbol,
    resolve_tick_lake_root,
)
from src.storage.reader import (
    LakeReaderError,
    LakeUnavailableError,
    _normalize_datetime_bound,
)
from src.storage.schema import (
    LAKE_SCHEMA_V1,
    SCHEMA_V1_COLUMNS,
    QuoteTick,
)


class ReplayError(Exception):
    """Base exception for lake replay operations."""
    pass


class ReplaySnapshotInvalidError(ReplayError):
    """Raised when replay snapshot is missing, corrupted, or incompatible with lake root."""
    pass


class ReplaySnapshotRetiredError(ReplayError):
    """Raised when snapshot files were retired by compaction or purge."""
    pass


class ReplayCursorCorruptedError(ReplayError):
    """Raised when a cursor token is malformed, corrupted, or has invalid state."""
    pass


def _compute_file_digest(path: Path) -> str:
    """Computes a SHA-256 digest of a file on disk."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def _parse_filter_bound(
    val: Optional[Union[str, date, datetime]],
    is_end: bool = False,
) -> Optional[datetime]:
    """
    Normalizes string, date, or datetime boundaries to naive UTC datetime.
    Pure date strings or date objects for end bounds default to end-of-day (23:59:59.999999).
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is not None:
            return val.astimezone(timezone.utc).replace(tzinfo=None)
        return val
    if isinstance(val, date):
        if is_end:
            return datetime.combine(val, datetime.max.time().replace(microsecond=999999))
        return datetime.combine(val, datetime.min.time())
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if len(s) == 10 and re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            d = date.fromisoformat(s)
            if is_end:
                return datetime.combine(d, datetime.max.time().replace(microsecond=999999))
            return datetime.combine(d, datetime.min.time())
        return _normalize_datetime_bound(s)
    return None


@dataclass
class ReplaySnapshot:
    """
    Freezes an immutable file inventory for replay queries.
    Captures snapshot_id, lake_root, created_at, relative file paths, and SHA-256 file digests.
    """
    snapshot_id: str
    lake_root: str
    created_at: str
    files: List[str]
    file_digests: Dict[str, str]
    symbols: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @classmethod
    def create(
        cls,
        lake_root: Union[str, Path],
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        persist: bool = True,
    ) -> "ReplaySnapshot":
        """
        Discovers candidate partition files matching query parameters and freezes them into an immutable snapshot.
        """
        root = resolve_tick_lake_root(lake_root)
        if not root.is_dir() or not (root / "lake.json").is_file():
            raise LakeNotFoundError(f"Tick lake not found at {root}")

        guard = root / "_maintenance" / "in_progress.json"
        if guard.is_file():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard}")

        clean_symbols: Optional[List[str]] = None
        if symbols is not None:
            if isinstance(symbols, str):
                clean_symbols = [symbols.strip().upper()]
            else:
                clean_symbols = [s.strip().upper() for s in symbols if s and s.strip()]

        s_dt = _parse_filter_bound(start_date, is_end=False)
        e_dt = _parse_filter_bound(end_date, is_end=True)

        s_date = s_dt.date() if s_dt else None
        e_date = e_dt.date() if e_dt else None

        from src.storage.reader import TickLakeReader
        reader = TickLakeReader(root=root, check_maintenance=True)

        candidate_files: List[Path] = []
        if clean_symbols:
            for sym in clean_symbols:
                sym_files = reader.resolve_partition_files(
                    symbol=sym,
                    start_date=s_date,
                    end_date=e_date,
                )
                candidate_files.extend(sym_files)
        else:
            candidate_files = reader.resolve_partition_files(
                symbol=None,
                start_date=s_date,
                end_date=e_date,
            )

        resolved_root = root.resolve()
        rel_files: List[str] = []
        for f in candidate_files:
            rel = str(f.resolve().relative_to(resolved_root))
            rel_files.append(rel)
        rel_files = sorted(set(rel_files))

        digests: Dict[str, str] = {}
        for rf in rel_files:
            abs_p = resolved_root / rf
            if not abs_p.is_file():
                raise ReplaySnapshotInvalidError(f"File missing during snapshot creation: {rf}")
            digests[rf] = _compute_file_digest(abs_p)

        snapshot_id = f"snap_{uuid.uuid4().hex[:16]}"
        now_iso = datetime.now(timezone.utc).isoformat()

        snapshot = cls(
            snapshot_id=snapshot_id,
            lake_root=str(resolved_root),
            created_at=now_iso,
            files=rel_files,
            file_digests=digests,
            symbols=clean_symbols,
            start_date=s_dt.isoformat() if s_dt else None,
            end_date=e_dt.isoformat() if e_dt else None,
        )

        if persist:
            snapshot.save()

        return snapshot

    def save(self, path_or_root: Optional[Union[str, Path]] = None) -> Path:
        """Atomically saves snapshot JSON metadata."""
        if path_or_root is None:
            dest_dir = Path(self.lake_root) / "_control" / "snapshots"
            target_file = dest_dir / f"{self.snapshot_id}.json"
        else:
            p = Path(path_or_root)
            if p.suffix == ".json":
                dest_dir = p.parent
                target_file = p
            else:
                dest_dir = p / "_control" / "snapshots"
                target_file = dest_dir / f"{self.snapshot_id}.json"

        dest_dir.mkdir(parents=True, exist_ok=True)
        tmp_file = dest_dir / f"tmp_{self.snapshot_id}_{os.getpid()}_{uuid.uuid4().hex[:6]}.json"

        payload = json.dumps(self.to_dict(), indent=2)
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_file, target_file)
        try:
            dir_fd = os.open(str(dest_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        return target_file

    @classmethod
    def load(cls, path: Union[str, Path]) -> "ReplaySnapshot":
        """Loads snapshot from file path."""
        p = Path(path).resolve()
        if not p.is_file():
            raise ReplaySnapshotInvalidError(f"Snapshot file not found: {p}")
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception as exc:
            if isinstance(exc, ReplayError):
                raise
            raise ReplaySnapshotInvalidError(f"Cannot load snapshot from {p}: {exc}") from exc

    @classmethod
    def load_by_id(cls, lake_root: Union[str, Path], snapshot_id: str) -> "ReplaySnapshot":
        """Loads a snapshot from <lake_root>/_control/snapshots/<snapshot_id>.json."""
        root = resolve_tick_lake_root(lake_root)
        snap_path = root / "_control" / "snapshots" / f"{snapshot_id}.json"
        if not snap_path.is_file():
            raise ReplaySnapshotInvalidError(f"Snapshot {snapshot_id} not found at {snap_path}")
        return cls.load(snap_path)

    def validate(self, lake_root: Optional[Union[str, Path]] = None) -> None:
        """
        Validates snapshot:
        - Confirms lake root exists and matches.
        - Fails fast if _maintenance/in_progress.json exists.
        - Raises ReplaySnapshotRetiredError if any file was retired by compaction.
        - Raises ReplaySnapshotInvalidError if any file is missing or digest mismatches.
        """
        expected_root = Path(self.lake_root).resolve()
        if lake_root is not None:
            actual_root = Path(lake_root).resolve()
            if actual_root != expected_root:
                raise ReplaySnapshotInvalidError(
                    f"Lake root mismatch for snapshot {self.snapshot_id}: expected {expected_root}, got {actual_root}"
                )
        else:
            actual_root = expected_root

        if not actual_root.is_dir() or not (actual_root / "lake.json").is_file():
            raise LakeUnavailableError(f"Lake root unavailable: {actual_root}")

        # Check maintenance lock
        guard = actual_root / "_maintenance" / "in_progress.json"
        if guard.is_file():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard}")

        # Check lineage for retired files
        lineage_path = actual_root / "_control" / "lineage.json"
        retired_set: Set[str] = set()
        if lineage_path.is_file():
            try:
                with open(lineage_path, "r", encoding="utf-8") as lf:
                    ldata = json.load(lf)
                file_map = ldata.get("file_lineage", {})
                for k in file_map:
                    retired_set.add(Path(k).as_posix())
                for comp in ldata.get("compactions", []):
                    for part in comp.get("partitions", []):
                        for in_f in part.get("input_files", []):
                            retired_set.add(Path(in_f).as_posix())
            except Exception as exc:
                raise ReplaySnapshotInvalidError(f"Cannot read lineage: {exc}") from exc

        # Check each file in snapshot
        for rf in self.files:
            norm_rf = Path(rf).as_posix()
            if norm_rf in retired_set:
                raise ReplaySnapshotRetiredError(
                    f"Snapshot {self.snapshot_id} file {rf} was retired by compaction"
                )

            abs_file = actual_root / rf
            if not abs_file.is_file():
                if norm_rf in retired_set:
                    raise ReplaySnapshotRetiredError(
                        f"Snapshot {self.snapshot_id} file {rf} was retired by compaction"
                    )
                raise ReplaySnapshotInvalidError(
                    f"Snapshot {self.snapshot_id} file missing on disk: {rf}"
                )

            expected_digest = self.file_digests.get(rf)
            if expected_digest:
                actual_digest = _compute_file_digest(abs_file)
                if actual_digest != expected_digest:
                    raise ReplaySnapshotInvalidError(
                        f"Snapshot {self.snapshot_id} file {rf} digest mismatch: "
                        f"expected {expected_digest}, got {actual_digest}"
                    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "lake_root": self.lake_root,
            "created_at": self.created_at,
            "files": list(self.files),
            "file_digests": dict(self.file_digests),
            "symbols": list(self.symbols) if self.symbols is not None else None,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ReplaySnapshot":
        if not isinstance(d, dict):
            raise ReplaySnapshotInvalidError("Snapshot data must be a dictionary")
        for req in ("snapshot_id", "lake_root", "created_at", "files", "file_digests"):
            if req not in d:
                raise ReplaySnapshotInvalidError(f"Snapshot missing required field '{req}'")
        return cls(
            snapshot_id=str(d["snapshot_id"]),
            lake_root=str(d["lake_root"]),
            created_at=str(d["created_at"]),
            files=list(d["files"]),
            file_digests=dict(d["file_digests"]),
            symbols=list(d["symbols"]) if d.get("symbols") is not None else None,
            start_date=d.get("start_date"),
            end_date=d.get("end_date"),
        )


@dataclass
class ReplayCursor:
    """
    Serializable replay cursor capturing current position in the chronological stream.
    State survives fresh process restarts and uses strict lexicographic successor pagination.
    """
    snapshot_id: str
    symbols: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    batch_size: int = 5000
    last_key: Optional[Tuple[str, str, str]] = None  # (timestamp_iso, symbol, ingest_id)
    offset_in_key: int = 0
    emitted_count: int = 0
    lake_root: Optional[str] = None

    def to_token(self) -> str:
        """Serializes cursor state into a URL-safe base64 token string."""
        d = {
            "v": 1,
            "snapshot_id": self.snapshot_id,
            "symbols": self.symbols,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "batch_size": self.batch_size,
            "last_key": list(self.last_key) if self.last_key is not None else None,
            "offset_in_key": self.offset_in_key,
            "emitted_count": self.emitted_count,
            "lake_root": self.lake_root,
        }
        raw_json = json.dumps(d, separators=(",", ":"))
        return base64.urlsafe_b64encode(raw_json.encode("utf-8")).decode("ascii")

    @classmethod
    def from_token(cls, token: str) -> "ReplayCursor":
        """Deserializes and validates cursor state from a base64 or JSON token string."""
        if not token or not isinstance(token, str):
            raise ReplayCursorCorruptedError("Cursor token must be a non-empty string")
        token_str = token.strip()
        data = None

        if token_str.startswith("{"):
            try:
                data = json.loads(token_str)
            except Exception as exc:
                raise ReplayCursorCorruptedError(f"Malformed JSON in cursor token: {exc}") from exc
        else:
            try:
                decoded_bytes = base64.urlsafe_b64decode(token_str.encode("ascii"))
                data = json.loads(decoded_bytes.decode("utf-8"))
            except Exception as exc:
                raise ReplayCursorCorruptedError(f"Malformed base64/JSON cursor token: {exc}") from exc

        if not isinstance(data, dict):
            raise ReplayCursorCorruptedError("Cursor token payload must be a JSON object")

        snapshot_id = data.get("snapshot_id")
        if not snapshot_id or not isinstance(snapshot_id, str):
            raise ReplayCursorCorruptedError("Cursor token missing or invalid 'snapshot_id'")

        batch_size = data.get("batch_size", 5000)
        try:
            batch_size = int(batch_size)
            if batch_size <= 0:
                raise ValueError()
        except Exception:
            raise ReplayCursorCorruptedError(f"Invalid batch_size in cursor token: {batch_size}")

        emitted_count = data.get("emitted_count", 0)
        try:
            emitted_count = int(emitted_count)
            if emitted_count < 0:
                raise ValueError()
        except Exception:
            raise ReplayCursorCorruptedError(f"Invalid emitted_count in cursor token: {emitted_count}")

        offset_in_key = data.get("offset_in_key", 0)
        try:
            offset_in_key = int(offset_in_key)
            if offset_in_key < 0:
                raise ValueError()
        except Exception:
            raise ReplayCursorCorruptedError(f"Invalid offset_in_key in cursor token: {offset_in_key}")

        last_key_raw = data.get("last_key")
        last_key: Optional[Tuple[str, str, str]] = None
        if last_key_raw is not None:
            if not isinstance(last_key_raw, (list, tuple)) or len(last_key_raw) != 3:
                raise ReplayCursorCorruptedError("Invalid last_key structure in cursor token; must be 3-tuple")
            last_key = (str(last_key_raw[0]), str(last_key_raw[1]), str(last_key_raw[2]))

        symbols = data.get("symbols")
        if symbols is not None and not isinstance(symbols, list):
            raise ReplayCursorCorruptedError("symbols must be a list or null in cursor token")

        return cls(
            snapshot_id=snapshot_id,
            symbols=symbols,
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            batch_size=batch_size,
            last_key=last_key,
            offset_in_key=offset_in_key,
            emitted_count=emitted_count,
            lake_root=data.get("lake_root"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ReplayCursor":
        return cls(**d)


class TickLakeReplayIterator:
    """
    Chronological replay iterator streaming Arrow batches over a frozen snapshot
    using keyset pagination and deterministic total ordering:
    ORDER BY timestamp ASC, symbol ASC, ingest_id ASC, price ASC, volume ASC, bid ASC, ask ASC, source ASC, session ASC.
    """

    def __init__(
        self,
        lake_root: Optional[Union[str, Path]] = None,
        snapshot: Optional[ReplaySnapshot] = None,
        symbols: Optional[Union[str, List[str]]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        batch_size: int = 5000,
        cursor: Optional[Union[ReplayCursor, str]] = None,
        output_format: str = "record_batch",
        max_threads: int = 2,
        max_memory: str = "512MB",
    ):
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._closed = False

        parsed_cursor: Optional[ReplayCursor] = None
        if cursor is not None:
            if isinstance(cursor, str):
                parsed_cursor = ReplayCursor.from_token(cursor)
            elif isinstance(cursor, ReplayCursor):
                parsed_cursor = cursor
            else:
                raise ReplayCursorCorruptedError("cursor must be ReplayCursor or string token")

        # 1. Resolve lake root
        if snapshot is not None:
            expected_root = Path(snapshot.lake_root).resolve()
            if lake_root is not None:
                provided_root = resolve_tick_lake_root(lake_root)
                if provided_root != expected_root:
                    raise ReplaySnapshotInvalidError(
                        f"Lake root mismatch: provided {provided_root}, snapshot has {expected_root}"
                    )
            self.root = expected_root
        elif parsed_cursor is not None and parsed_cursor.lake_root:
            if lake_root is not None:
                provided_root = resolve_tick_lake_root(lake_root)
                expected_root = Path(parsed_cursor.lake_root).resolve()
                if provided_root != expected_root:
                    raise ReplaySnapshotInvalidError(
                        f"Lake root mismatch: provided {provided_root}, cursor has {expected_root}"
                    )
                self.root = provided_root
            else:
                self.root = resolve_tick_lake_root(parsed_cursor.lake_root)
        else:
            self.root = resolve_tick_lake_root(lake_root)

        # 2. Resolve snapshot
        if snapshot is not None:
            self.snapshot = snapshot
            if parsed_cursor is not None and parsed_cursor.snapshot_id != snapshot.snapshot_id:
                raise ReplaySnapshotInvalidError(
                    f"Cursor snapshot_id {parsed_cursor.snapshot_id} does not match snapshot {snapshot.snapshot_id}"
                )
        elif parsed_cursor is not None:
            self.snapshot = ReplaySnapshot.load_by_id(self.root, parsed_cursor.snapshot_id)
        else:
            self.snapshot = ReplaySnapshot.create(
                lake_root=self.root,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                persist=True,
            )

        # 3. Validate snapshot immediately
        self.snapshot.validate(self.root)

        # 4. Filters resolution
        if parsed_cursor is not None and parsed_cursor.symbols is not None:
            self.symbols = parsed_cursor.symbols
        elif self.snapshot.symbols is not None:
            self.symbols = self.snapshot.symbols
        elif symbols is not None:
            if isinstance(symbols, str):
                self.symbols = [symbols.strip().upper()]
            else:
                self.symbols = [s.strip().upper() for s in symbols if s and s.strip()]
        else:
            self.symbols = None

        raw_start = (
            parsed_cursor.start_date if (parsed_cursor and parsed_cursor.start_date)
            else (self.snapshot.start_date if self.snapshot.start_date else start_date)
        )
        raw_end = (
            parsed_cursor.end_date if (parsed_cursor and parsed_cursor.end_date)
            else (self.snapshot.end_date if self.snapshot.end_date else end_date)
        )

        self.start_dt = _parse_filter_bound(raw_start, is_end=False)
        self.end_dt = _parse_filter_bound(raw_end, is_end=True)

        # 5. Cursor state
        if parsed_cursor is not None:
            self.batch_size = parsed_cursor.batch_size
            self.last_key = parsed_cursor.last_key
            self.offset_in_key = parsed_cursor.offset_in_key
            self.emitted_count = parsed_cursor.emitted_count
        else:
            self.batch_size = max(1, int(batch_size))
            self.last_key = None
            self.offset_in_key = 0
            self.emitted_count = 0

        self.output_format = str(output_format).lower()
        self.max_threads = max(1, int(max_threads))
        self.max_memory = str(max_memory)
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._closed = False

    def _check_maintenance(self) -> None:
        """Fails fast if lake maintenance is in progress."""
        guard = self.root / "_maintenance" / "in_progress.json"
        if guard.is_file():
            raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard}")

    def _get_con(self) -> duckdb.DuckDBPyConnection:
        if self._closed:
            raise ReplayError("Replay iterator is closed")
        self._check_maintenance()
        if self._con is None:
            self._con = duckdb.connect(":memory:")
            self._con.execute("SET TimeZone = 'UTC'")
            self._con.execute(f"SET threads = {self.max_threads}")
            self._con.execute(f"SET max_memory = '{self.max_memory}'")
        return self._con

    def __iter__(self) -> "TickLakeReplayIterator":
        return self

    def __next__(self) -> Any:
        if self._closed:
            raise StopIteration

        self._check_maintenance()

        if not self.snapshot.files:
            raise StopIteration

        con = self._get_con()

        abs_files = [str(self.root / f) for f in self.snapshot.files]

        where_clauses: List[str] = []
        params: List[Any] = [abs_files]

        if self.symbols:
            if len(self.symbols) == 1:
                where_clauses.append("symbol = ?")
                params.append(self.symbols[0])
            else:
                placeholders = ", ".join(["?"] * len(self.symbols))
                where_clauses.append(f"symbol IN ({placeholders})")
                params.extend(self.symbols)

        if self.start_dt is not None:
            where_clauses.append("timestamp >= ?::TIMESTAMP")
            params.append(self.start_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
        if self.end_dt is not None:
            where_clauses.append("timestamp <= ?::TIMESTAMP")
            params.append(self.end_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))

        if self.last_key is not None:
            where_clauses.append("(timestamp, symbol, ingest_id) >= (?::TIMESTAMP, ?, ?)")
            params.extend([str(self.last_key[0]), str(self.last_key[1]), str(self.last_key[2])])
            limit_rows = self.batch_size + self.offset_in_key
        else:
            limit_rows = self.batch_size

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        query = f"""
            SELECT 
                timestamp,
                symbol,
                price,
                volume,
                bid,
                ask,
                source,
                session,
                ingest_id
            FROM read_parquet(?, hive_partitioning=false)
            {where_sql}
            ORDER BY timestamp ASC, symbol ASC, ingest_id ASC, price ASC, volume ASC, bid ASC, ask ASC, source ASC, session ASC
            LIMIT ?
        """
        params.append(limit_rows)

        try:
            arrow_res = con.execute(query, params).to_arrow_table()
        except duckdb.Error as exc:
            self._check_maintenance()
            raise ReplayError(f"Replay query failed: {exc}") from exc

        try:
            arrow_table = arrow_res.cast(LAKE_SCHEMA_V1)
        except Exception:
            arrow_table = arrow_res

        if self.last_key is not None and self.offset_in_key > 0:
            table = arrow_table.slice(self.offset_in_key)
        else:
            table = arrow_table

        if table.num_rows == 0:
            raise StopIteration

        ts_col = table.column("timestamp")
        sym_col = table.column("symbol")
        id_col = table.column("ingest_id")

        last_idx = table.num_rows - 1
        tail_ts_dt = ts_col[last_idx].as_py()
        tail_ts_str = tail_ts_dt.strftime("%Y-%m-%d %H:%M:%S.%f") if isinstance(tail_ts_dt, datetime) else str(tail_ts_dt)
        tail_sym_str = str(sym_col[last_idx].as_py())
        tail_id_str = str(id_col[last_idx].as_py())
        tail_key = (tail_ts_str, tail_sym_str, tail_id_str)

        tail_count = 0
        for idx in range(last_idx, -1, -1):
            r_ts_dt = ts_col[idx].as_py()
            r_ts_str = r_ts_dt.strftime("%Y-%m-%d %H:%M:%S.%f") if isinstance(r_ts_dt, datetime) else str(r_ts_dt)
            r_sym_str = str(sym_col[idx].as_py())
            r_id_str = str(id_col[idx].as_py())
            if (r_ts_str, r_sym_str, r_id_str) == tail_key:
                tail_count += 1
            else:
                break

        if self.last_key == tail_key:
            self.offset_in_key += tail_count
        else:
            self.last_key = tail_key
            self.offset_in_key = tail_count

        self.emitted_count += table.num_rows

        fmt = self.output_format
        if fmt in ("record_batch", "batch"):
            return table.combine_chunks().to_batches()[0]
        elif fmt == "table":
            return table
        elif fmt in ("dict", "dicts", "dictionary"):
            return table.to_pylist()
        else:
            return table.combine_chunks().to_batches()[0]

    def get_cursor(self) -> ReplayCursor:
        """Returns the current serializable cursor."""
        return ReplayCursor(
            snapshot_id=self.snapshot.snapshot_id,
            symbols=self.symbols,
            start_date=self.start_dt.isoformat() if self.start_dt else None,
            end_date=self.end_dt.isoformat() if self.end_dt else None,
            batch_size=self.batch_size,
            last_key=self.last_key,
            offset_in_key=self.offset_in_key,
            emitted_count=self.emitted_count,
            lake_root=str(self.root),
        )

    def get_cursor_token(self) -> str:
        """Returns the base64 cursor token string."""
        return self.get_cursor().to_token()

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        """Convenience generator yielding PyArrow RecordBatches."""
        prev = self.output_format
        try:
            self.output_format = "record_batch"
            for item in self:
                yield item
        finally:
            self.output_format = prev

    def iter_tables(self) -> Iterator[pa.Table]:
        """Convenience generator yielding PyArrow Tables."""
        prev = self.output_format
        try:
            self.output_format = "table"
            for item in self:
                yield item
        finally:
            self.output_format = prev

    def iter_dicts(self) -> Iterator[List[Dict[str, Any]]]:
        """Convenience generator yielding lists of row dictionaries."""
        prev = self.output_format
        try:
            self.output_format = "dict"
            for item in self:
                yield item
        finally:
            self.output_format = prev

    def close(self) -> None:
        """Closes DuckDB connection and releases resources."""
        if not getattr(self, "_closed", True):
            self._closed = True
            con = getattr(self, "_con", None)
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
                self._con = None

    def __enter__(self) -> "TickLakeReplayIterator":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


def create_replay_snapshot(
    lake_root: Optional[Union[str, Path]] = None,
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[Union[str, date, datetime]] = None,
    end_date: Optional[Union[str, date, datetime]] = None,
    persist: bool = True,
) -> ReplaySnapshot:
    """Helper to freeze and return a ReplaySnapshot."""
    root = resolve_tick_lake_root(lake_root)
    return ReplaySnapshot.create(
        lake_root=root,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        persist=persist,
    )
