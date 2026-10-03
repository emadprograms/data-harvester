"""
Historical DuckDB to Partitioned Parquet Tick Lake Migration Tool.
Milestone v4.0 - Phase 20 (P6: Zero-Loss Migration Tooling & Rehearsal).

Stage 2 Interface Stub:
Defines configuration, plan, state, verification structures, and orchestrator interface.
Operational methods raise NotImplementedError to establish clean TDD RED state for Stage 3.
"""
import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union


@dataclass
class MigrationConfig:
    """Configuration options for historical migration run."""
    source_db: Path
    lake_root: Path
    mode: str = "all"  # "plan" | "export" | "verify" | "publish" | "all"
    chunk_size: int = 100_000
    symbols: Optional[List[str]] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    dry_run: bool = False
    resume: bool = False
    force: bool = False

    def __post_init__(self):
        if isinstance(self.source_db, str):
            self.source_db = Path(self.source_db)
        if isinstance(self.lake_root, str):
            self.lake_root = Path(self.lake_root)
        if isinstance(self.symbols, str):
            self.symbols = [s.strip().upper() for s in self.symbols.split(",") if s.strip()]
        elif self.symbols is not None:
            self.symbols = [s.strip().upper() for s in self.symbols if s.strip()]


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
            "partitions": self.partitions,
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


@dataclass
class MigrationState:
    """State tracking for chunked export and resumption checkpoints."""
    updated_at: str = ""
    partitions: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "updated_at": self.updated_at,
            "partitions": self.partitions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationState":
        return cls(
            updated_at=data.get("updated_at", ""),
            partitions=data.get("partitions", {}),
        )


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


class MigrationOrchestrator:
    """
    Coordinates historical migration lifecycle:
    plan -> export -> verify -> publish
    """

    def __init__(self, config: MigrationConfig):
        self.config = config

    def plan(self) -> MigrationPlan:
        """Analyze source DuckDB and generate execution plan."""
        raise NotImplementedError("Stage 2 interface stub")

    def export(self) -> MigrationState:
        """Export source DuckDB tick_data into staged Parquet chunks."""
        raise NotImplementedError("Stage 2 interface stub")

    def verify(self) -> VerificationResult:
        """Execute two-way EXCEPT ALL reconciliation between DuckDB and staged Parquet."""
        raise NotImplementedError("Stage 2 interface stub")

    def publish(self) -> List[Any]:
        """Atomically promote verified staged chunks to production lake and write receipts."""
        raise NotImplementedError("Stage 2 interface stub")

    def run(self) -> int:
        """Execute configured lifecycle mode and return exit code."""
        raise NotImplementedError("Stage 2 interface stub")


def parse_args(args: Optional[Sequence[str]] = None) -> MigrationConfig:
    """Parse command line arguments into MigrationConfig."""
    raise NotImplementedError("Stage 2 interface stub")


def main(args: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint for migration tool."""
    raise NotImplementedError("Stage 2 interface stub")


if __name__ == "__main__":
    import sys
    sys.exit(main())
