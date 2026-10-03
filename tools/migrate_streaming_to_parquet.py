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
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Union
import uuid

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.storage.config import (
    decode_symbol,
    encode_symbol,
    init_tick_lake,
    resolve_tick_lake_root,
)
from src.storage.publication import (
    FilePublicationReceipt,
    LakePublisherLock,
    PublishReceipt,
)
from src.storage.schema import ticks_to_table

logger = logging.getLogger("migration_tool")


def _atomic_save_json(data: Dict[str, Any], target_path: Union[str, Path]) -> None:
    """Atomically write dictionary as JSON using fsync and atomic replace."""
    target_path = Path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.parent / f"tmp_{target_path.stem}_{uuid.uuid4().hex}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, target_path)


@dataclass
class MigrationConfig:
    """Configuration options for historical migration run."""
    source_db: Optional[Union[str, Path]] = None
    lake_root: Optional[Union[str, Path]] = None
    source_table: Optional[str] = None
    mode: str = "all"  # "plan" | "export" | "verify" | "publish" | "all"
    chunk_size: int = 100_000
    symbols: Optional[Union[str, List[str]]] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    dry_run: bool = False
    resume: bool = False
    force: bool = False
    compression: str = "snappy"

    def __post_init__(self):
        if self.source_db is not None:
            self.source_db = Path(self.source_db)
        if self.lake_root is not None:
            self.lake_root = Path(self.lake_root)
        if isinstance(self.symbols, str):
            self.symbols = [s.strip().upper() for s in self.symbols.split(",") if s.strip()]
        elif self.symbols is not None:
            self.symbols = [s.strip().upper() for s in self.symbols if s.strip()]
        self.chunk_size = int(self.chunk_size)
        self.dry_run = bool(self.dry_run)
        self.resume = bool(self.resume)
        self.force = bool(self.force)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_db": str(self.source_db) if self.source_db else None,
            "lake_root": str(self.lake_root) if self.lake_root else None,
            "source_table": self.source_table,
            "mode": self.mode,
            "chunk_size": self.chunk_size,
            "symbols": list(self.symbols) if self.symbols else None,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "dry_run": self.dry_run,
            "resume": self.resume,
            "force": self.force,
            "compression": self.compression,
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
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationConfig":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class PartitionPlan:
    """Plan metadata for a single symbol-date partition."""
    symbol: str
    date: str
    row_count: int
    min_timestamp: str
    max_timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "row_count": self.row_count,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionPlan":
        return cls(
            symbol=data["symbol"],
            date=data["date"],
            row_count=data["row_count"],
            min_timestamp=str(data["min_timestamp"]),
            max_timestamp=str(data["max_timestamp"]),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "PartitionPlan":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


@dataclass
class MigrationPlan:
    """Execution plan describing symbols, date partitions, and chunk counts to export."""
    created_at: str = ""
    source_db: str = ""
    lake_root: str = ""
    total_rows: int = 0
    partitions: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "created_at": self.created_at,
            "source_db": self.source_db,
            "lake_root": self.lake_root,
            "total_rows": self.total_rows,
            "partitions": [p.to_dict() if hasattr(p, "to_dict") else p for p in self.partitions],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationPlan":
        return cls(
            created_at=data.get("created_at", ""),
            source_db=data.get("source_db", ""),
            lake_root=data.get("lake_root", ""),
            total_rows=data.get("total_rows", 0),
            partitions=data.get("partitions", []),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationPlan":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


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
    """State tracking for chunked export and resumption checkpoints."""
    updated_at: str = ""
    status: str = "IN_PROGRESS"  # "IN_PROGRESS" | "PUBLISHED"
    partitions: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "updated_at": self.updated_at,
            "status": self.status,
            "partitions": {
                k: (v.to_dict() if hasattr(v, "to_dict") else v)
                for k, v in self.partitions.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationState":
        return cls(
            updated_at=data.get("updated_at", ""),
            status=data.get("status", "IN_PROGRESS"),
            partitions=data.get("partitions", {}),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "MigrationState":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


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
    """Result of two-way EXCEPT ALL mathematical reconciliation."""
    status: str = "FAILED"  # "PASSED" | "FAILED"
    total_source_rows: int = 0
    total_parquet_rows: int = 0
    discrepancies: List[Dict[str, Any]] = field(default_factory=list)
    verified_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "total_source_rows": self.total_source_rows,
            "total_parquet_rows": self.total_parquet_rows,
            "discrepancies": self.discrepancies,
            "verified_at": self.verified_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VerificationResult":
        return cls(
            status=data.get("status", "FAILED"),
            total_source_rows=data.get("total_source_rows", 0),
            total_parquet_rows=data.get("total_parquet_rows", 0),
            discrepancies=data.get("discrepancies", []),
            verified_at=data.get("verified_at", ""),
        )

    def save(self, path: Union[str, Path]) -> None:
        _atomic_save_json(self.to_dict(), path)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "VerificationResult":
        with open(Path(path), "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


class MigrationOrchestrator:
    """
    Coordinates historical migration lifecycle:
    plan -> export -> verify -> publish
    """

    def __init__(self, config: MigrationConfig):
        self.config = config

        # Resolve source_db
        if self.config.source_db is not None:
            self.source_db = Path(self.config.source_db).resolve()
        else:
            data_dir_env = os.environ.get("DATA_DIR")
            if data_dir_env:
                self.source_db = (Path(data_dir_env) / "streaming.duckdb").resolve()
            else:
                repo_root = Path(__file__).resolve().parent.parent
                self.source_db = (repo_root / "data" / "streaming.duckdb").resolve()

        # Resolve lake_root
        self.lake_root = resolve_tick_lake_root(self.config.lake_root)

        # Migration directories and paths
        self.migration_dir = self.lake_root / "_migration"
        self.staging_dir = self.migration_dir / "staging"
        self.plan_file = self.migration_dir / "plan.json"
        self.state_file = self.migration_dir / "state.json"
        self.verification_file = self.migration_dir / "verification.json"

    def _detect_source_table(self, con: duckdb.DuckDBPyConnection) -> str:
        """Auto-detect source tick table name from duckdb metadata."""
        if self.config.source_table:
            return self.config.source_table
        tables = [
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        for candidate in ["tick_data", "ticks", "streaming_ticks"]:
            if candidate in tables:
                return candidate
        if tables:
            return tables[0]
        return "tick_data"

    def plan(self) -> MigrationPlan:
        """Analyze source DuckDB and generate execution plan."""
        if not self.source_db.is_file():
            raise FileNotFoundError(f"Source database not found at {self.source_db}")

        con = duckdb.connect(str(self.source_db), read_only=True)
        try:
            table = self._detect_source_table(con)
            table_exists = (
                con.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='main' AND table_name = ?",
                    [table],
                ).fetchone()[0]
                > 0
            )

            partition_plans: List[Dict[str, Any]] = []
            if table_exists:
                query = (
                    f"SELECT symbol, strftime(timestamp, '%Y-%m-%d') AS date, "
                    f"count(*) AS row_count, min(timestamp) AS min_ts, max(timestamp) AS max_ts "
                    f"FROM {table}"
                )
                where_clauses = []
                params: List[Any] = []

                if self.config.symbols:
                    placeholders = ",".join(["?"] * len(self.config.symbols))
                    where_clauses.append(f"symbol IN ({placeholders})")
                    params.extend(self.config.symbols)
                if self.config.date_start:
                    where_clauses.append("strftime(timestamp, '%Y-%m-%d') >= ?")
                    params.append(self.config.date_start)
                if self.config.date_end:
                    where_clauses.append("strftime(timestamp, '%Y-%m-%d') <= ?")
                    params.append(self.config.date_end)

                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                query += " GROUP BY symbol, date ORDER BY symbol, date"

                rows = con.execute(query, params).fetchall()
                for sym, dt, cnt, min_t, max_t in rows:
                    min_iso = min_t.isoformat() if hasattr(min_t, "isoformat") else str(min_t)
                    max_iso = max_t.isoformat() if hasattr(max_t, "isoformat") else str(max_t)
                    p_plan = PartitionPlan(
                        symbol=sym,
                        date=dt,
                        row_count=cnt,
                        min_timestamp=min_iso,
                        max_timestamp=max_iso,
                    )
                    partition_plans.append(p_plan.to_dict())

            total_rows = sum(p["row_count"] for p in partition_plans)
            plan = MigrationPlan(
                created_at=datetime.now(timezone.utc).isoformat(),
                source_db=str(self.source_db),
                lake_root=str(self.lake_root),
                total_rows=total_rows,
                partitions=partition_plans,
            )

            if not self.config.dry_run:
                self.migration_dir.mkdir(parents=True, exist_ok=True)
                plan.save(self.plan_file)
                if not self.state_file.exists():
                    initial_state = MigrationState(
                        updated_at=datetime.now(timezone.utc).isoformat(),
                        status="IN_PROGRESS",
                        partitions={},
                    )
                    initial_state.save(self.state_file)

            return plan
        finally:
            con.close()

    def export(self) -> MigrationState:
        """Export source DuckDB tick_data into staged Parquet chunks."""
        if not self.source_db.is_file():
            raise FileNotFoundError(f"Source database not found at {self.source_db}")

        # Load existing state if resuming
        if self.state_file.exists() and self.config.resume:
            state = MigrationState.load(self.state_file)
        else:
            state = MigrationState(
                updated_at=datetime.now(timezone.utc).isoformat(),
                status="IN_PROGRESS",
                partitions={},
            )

        # Obtain plan
        if (
            self.plan_file.is_file()
            and not self.config.force
            and not self.config.symbols
            and not self.config.date_start
            and not self.config.date_end
        ):
            plan = MigrationPlan.load(self.plan_file)
        else:
            plan = self.plan()

        target_partitions = plan.partitions
        if self.config.symbols:
            target_partitions = [p for p in target_partitions if p["symbol"] in self.config.symbols]
        if self.config.date_start:
            target_partitions = [p for p in target_partitions if p["date"] >= self.config.date_start]
        if self.config.date_end:
            target_partitions = [p for p in target_partitions if p["date"] <= self.config.date_end]

        con = duckdb.connect(str(self.source_db), read_only=True)
        try:
            table = self._detect_source_table(con)

            for p in target_partitions:
                symbol = p["symbol"]
                date_str = p["date"]
                encoded_symbol = encode_symbol(symbol)
                part_key = f"symbol={encoded_symbol}/date={date_str}"

                # Check resume
                if self.config.resume and part_key in state.partitions:
                    p_state = state.partitions[part_key]
                    if isinstance(p_state, dict) and p_state.get("status") == "COMPLETED":
                        continue
                    elif hasattr(p_state, "status") and p_state.status == "COMPLETED":
                        continue

                staging_part_dir = (
                    self.staging_dir / "ticks" / f"symbol={encoded_symbol}" / f"date={date_str}"
                )
                if not self.config.dry_run:
                    staging_part_dir.mkdir(parents=True, exist_ok=True)

                cur = con.cursor()
                query = (
                    f"SELECT timestamp, symbol, price, volume, bid, ask, source, session "
                    f"FROM {table} "
                    f"WHERE symbol = ? AND strftime(timestamp, '%Y-%m-%d') = ? "
                    f"ORDER BY timestamp ASC"
                )
                cur.execute(query, [symbol, date_str])

                chunk_idx = 1
                row_idx = 1
                chunk_files = []
                file_sizes = {}
                sha_map = {}
                part_row_count = 0

                while True:
                    rows = cur.fetchmany(self.config.chunk_size)
                    if not rows:
                        break

                    chunk_name = f"chunk_{chunk_idx:06d}.parquet"
                    chunk_path = staging_part_dir / chunk_name

                    if not self.config.dry_run:
                        chunk_records = []
                        for r in rows:
                            iid = f"mig_{symbol}_{date_str.replace('-', '')}_{row_idx:08d}"
                            chunk_records.append((
                                r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], iid
                            ))
                            row_idx += 1

                        table_arrow = ticks_to_table(chunk_records, validate=True)
                        symbol_dict = pc.dictionary_encode(table_arrow["symbol"])
                        table_arrow = table_arrow.set_column(
                            table_arrow.schema.get_field_index("symbol"),
                            pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                            symbol_dict,
                        )
                        pq.write_table(
                            table_arrow,
                            chunk_path,
                            compression=self.config.compression,
                        )
                        with open(chunk_path, "r+b") as f:
                            os.fsync(f.fileno())

                        f_size = chunk_path.stat().st_size
                        f_sha = hashlib.sha256(chunk_path.read_bytes()).hexdigest()
                        file_sizes[chunk_name] = f_size
                        sha_map[chunk_name] = f_sha
                    else:
                        row_idx += len(rows)

                    chunk_files.append(chunk_name)
                    part_row_count += len(rows)
                    chunk_idx += 1

                partition_state = PartitionState(
                    status="COMPLETED",
                    chunks=chunk_files,
                    row_count=part_row_count,
                    file_sizes=file_sizes,
                    sha256=sha_map,
                )
                state.partitions[part_key] = partition_state.to_dict()
                state.updated_at = datetime.now(timezone.utc).isoformat()

                if not self.config.dry_run:
                    state.save(self.state_file)

            return state
        finally:
            con.close()

    def verify(self) -> VerificationResult:
        """Execute two-way EXCEPT ALL reconciliation between DuckDB and staged/published Parquet."""
        if self.config.dry_run:
            if self.plan_file.is_file():
                plan = MigrationPlan.load(self.plan_file)
                total = plan.total_rows
            else:
                total = 0
            return VerificationResult(
                status="PASSED",
                total_source_rows=total,
                total_parquet_rows=total,
                discrepancies=[],
                verified_at=datetime.now(timezone.utc).isoformat(),
            )

        if not self.source_db.is_file():
            raise FileNotFoundError(f"Source database not found at {self.source_db}")

        if (
            self.plan_file.is_file()
            and not self.config.force
            and not self.config.symbols
            and not self.config.date_start
            and not self.config.date_end
        ):
            plan = MigrationPlan.load(self.plan_file)
        else:
            plan = self.plan()

        target_partitions = plan.partitions
        if self.config.symbols:
            target_partitions = [p for p in target_partitions if p["symbol"] in self.config.symbols]
        if self.config.date_start:
            target_partitions = [p for p in target_partitions if p["date"] >= self.config.date_start]
        if self.config.date_end:
            target_partitions = [p for p in target_partitions if p["date"] <= self.config.date_end]

        con = duckdb.connect(str(self.source_db), read_only=True)
        try:
            table = self._detect_source_table(con)
            all_discrepancies: List[Dict[str, Any]] = []
            total_source = 0
            total_parquet = 0

            for p in target_partitions:
                symbol = p["symbol"]
                date_str = p["date"]
                encoded_sym = encode_symbol(symbol)

                # Look for parquet chunks: staged first, fallback to published
                staged_dir = self.staging_dir / "ticks" / f"symbol={encoded_sym}" / f"date={date_str}"
                pfiles = sorted(staged_dir.glob("*.parquet")) if staged_dir.is_dir() else []
                if not pfiles:
                    prod_dir = self.lake_root / "ticks" / f"symbol={encoded_sym}" / f"date={date_str}"
                    pfiles = sorted(prod_dir.glob("*.parquet")) if prod_dir.is_dir() else []

                source_count = con.execute(
                    f"SELECT count(*) FROM {table} WHERE symbol = ? AND strftime(timestamp, '%Y-%m-%d') = ?",
                    [symbol, date_str],
                ).fetchone()[0]

                if not pfiles:
                    if source_count == 0:
                        parquet_count = 0
                    else:
                        parquet_count = 0
                        all_discrepancies.append({
                            "type": "missing_parquet_files",
                            "symbol": symbol,
                            "date": date_str,
                            "error": f"No Parquet files found for {symbol} on {date_str}, source has {source_count} rows",
                        })
                else:
                    file_paths = [str(f) for f in pfiles]
                    parquet_count = con.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                        [file_paths],
                    ).fetchone()[0]

                    # Direction 1: source EXCEPT ALL parquet
                    diff1 = con.execute(
                        f"""
                        SELECT count(*) FROM (
                            (SELECT timestamp, symbol, price, volume, bid, ask, source, session 
                             FROM {table} WHERE symbol = ? AND strftime(timestamp, '%Y-%m-%d') = ?)
                            EXCEPT ALL
                            (SELECT timestamp, symbol, price, volume, bid, ask, source, session 
                             FROM read_parquet(?, hive_partitioning=false))
                        )
                        """,
                        [symbol, date_str, file_paths],
                    ).fetchone()[0]

                    # Direction 2: parquet EXCEPT ALL source
                    diff2 = con.execute(
                        f"""
                        SELECT count(*) FROM (
                            (SELECT timestamp, symbol, price, volume, bid, ask, source, session 
                             FROM read_parquet(?, hive_partitioning=false) WHERE symbol = ?)
                            EXCEPT ALL
                            (SELECT timestamp, symbol, price, volume, bid, ask, source, session 
                             FROM {table} WHERE symbol = ? AND strftime(timestamp, '%Y-%m-%d') = ?)
                        )
                        """,
                        [file_paths, symbol, symbol, date_str],
                    ).fetchone()[0]

                    if source_count != parquet_count:
                        all_discrepancies.append({
                            "type": "row_count_mismatch",
                            "symbol": symbol,
                            "date": date_str,
                            "source_count": source_count,
                            "parquet_count": parquet_count,
                        })
                    if diff1 > 0:
                        all_discrepancies.append({
                            "type": "source_except_parquet_discrepancy",
                            "symbol": symbol,
                            "date": date_str,
                            "missing_in_parquet": diff1,
                        })
                    if diff2 > 0:
                        all_discrepancies.append({
                            "type": "parquet_except_source_discrepancy",
                            "symbol": symbol,
                            "date": date_str,
                            "missing_in_source": diff2,
                        })

                total_source += source_count
                total_parquet += parquet_count

            status = "PASSED" if len(all_discrepancies) == 0 else "FAILED"
            vres = VerificationResult(
                status=status,
                total_source_rows=total_source,
                total_parquet_rows=total_parquet,
                discrepancies=all_discrepancies,
                verified_at=datetime.now(timezone.utc).isoformat(),
            )

            if not self.config.dry_run:
                self.migration_dir.mkdir(parents=True, exist_ok=True)
                vres.save(self.verification_file)

            return vres
        finally:
            con.close()

    def publish(self) -> List[PublishReceipt]:
        """Atomically promote verified staged chunks to production lake and write receipts."""
        if self.config.dry_run:
            return []

        if not self.verification_file.is_file():
            raise RuntimeError("Publish aborted: verification must pass before publishing.")

        with open(self.verification_file, "r", encoding="utf-8") as f:
            vdata = json.load(f)
        if vdata.get("status") != "PASSED":
            raise RuntimeError("Publish aborted: verification must pass before publishing.")

        receipts: List[PublishReceipt] = []
        with LakePublisherLock(self.lake_root, writer_id="migrator"):
            staging_ticks = self.staging_dir / "ticks"
            if staging_ticks.is_dir():
                for symbol_dir in sorted(staging_ticks.glob("symbol=*")):
                    symbol_part = symbol_dir.name
                    symbol_enc = symbol_part.split("=", 1)[1]
                    symbol = decode_symbol(symbol_enc)

                    if self.config.symbols and symbol not in self.config.symbols:
                        continue

                    for date_dir in sorted(symbol_dir.glob("date=*")):
                        date_part = date_dir.name
                        date_str = date_part.split("=", 1)[1]

                        if self.config.date_start and date_str < self.config.date_start:
                            continue
                        if self.config.date_end and date_str > self.config.date_end:
                            continue

                        chunk_files = sorted(date_dir.glob("*.parquet"))
                        if not chunk_files:
                            continue

                        target_dir = self.lake_root / "ticks" / symbol_part / date_part
                        target_dir.mkdir(parents=True, exist_ok=True)

                        part_file_details: List[FilePublicationReceipt] = []
                        target_rel_paths: List[str] = []
                        part_row_count = 0

                        for cfile in chunk_files:
                            target_dest = target_dir / cfile.name
                            os.replace(cfile, target_dest)

                            f_size = target_dest.stat().st_size
                            f_sha = hashlib.sha256(target_dest.read_bytes()).hexdigest()
                            f_rows = pq.ParquetFile(target_dest).metadata.num_rows
                            rel_path = str(target_dest.relative_to(self.lake_root))

                            part_row_count += f_rows
                            target_rel_paths.append(rel_path)
                            part_file_details.append(
                                FilePublicationReceipt(
                                    relative_path=rel_path,
                                    symbol=symbol,
                                    date=date_str,
                                    row_count=f_rows,
                                    file_size_bytes=f_size,
                                    sha256=f_sha,
                                )
                            )

                        batch_id = f"batch_migrated_{symbol}_{date_str.replace('-', '')}"
                        published_at = datetime.now(timezone.utc).isoformat()
                        receipt = PublishReceipt(
                            batch_id=batch_id,
                            writer_id="migrator",
                            sequence=0,
                            row_count=part_row_count,
                            file_paths=target_rel_paths,
                            file_details=part_file_details,
                            status="PUBLISHED",
                            published_at=published_at,
                        )

                        receipts_dir = self.lake_root / "_control" / "receipts"
                        receipts_dir.mkdir(parents=True, exist_ok=True)
                        receipt_path = receipts_dir / f"{batch_id}.json"

                        receipt_dict = {
                            "batch_id": receipt.batch_id,
                            "writer_id": receipt.writer_id,
                            "sequence": receipt.sequence,
                            "row_count": receipt.row_count,
                            "file_paths": receipt.file_paths,
                            "file_details": [
                                {
                                    "relative_path": fd.relative_path,
                                    "symbol": fd.symbol,
                                    "date": fd.date,
                                    "row_count": fd.row_count,
                                    "file_size_bytes": fd.file_size_bytes,
                                    "sha256": fd.sha256,
                                }
                                for fd in receipt.file_details
                            ],
                            "status": receipt.status,
                            "published_at": receipt.published_at,
                        }
                        _atomic_save_json(receipt_dict, receipt_path)
                        receipts.append(receipt)

                        # Clean up date_dir if empty
                        try:
                            date_dir.rmdir()
                        except OSError:
                            pass

                    # Clean up symbol_dir if empty
                    try:
                        symbol_dir.rmdir()
                    except OSError:
                        pass

                # Clean up staging_ticks if empty
                try:
                    staging_ticks.rmdir()
                except OSError:
                    pass

            # Update state.json
            if self.state_file.is_file():
                state = MigrationState.load(self.state_file)
                state.status = "PUBLISHED"
                state.updated_at = datetime.now(timezone.utc).isoformat()
                state.save(self.state_file)
            else:
                state = MigrationState(
                    updated_at=datetime.now(timezone.utc).isoformat(),
                    status="PUBLISHED",
                    partitions={},
                )
                state.save(self.state_file)

        return receipts

    def run(self) -> int:
        """Execute configured lifecycle mode and return exit code."""
        try:
            if self.config.mode == "plan":
                self.plan()
            elif self.config.mode == "export":
                self.export()
            elif self.config.mode == "verify":
                res = self.verify()
                if res.status != "PASSED":
                    return 1
            elif self.config.mode == "publish":
                self.publish()
            elif self.config.mode == "all":
                self.plan()
                self.export()
                vres = self.verify()
                if vres.status != "PASSED":
                    return 1
                self.publish()
            else:
                raise ValueError(f"Unknown mode: {self.config.mode}")
            return 0
        except Exception as e:
            logger.error(f"Migration lifecycle failure: {e}", exc_info=True)
            return 1


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
        choices=["plan", "export", "verify", "publish", "all"],
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
