"""
In-memory DuckDB reader for Partitioned Parquet Tick Lake (Milestone v4.0 - Phase 19).

Provides lock-free, read-only analytical queries over immutable Parquet batches
using isolated DuckDB (:memory:) connections.
"""
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.storage.config import resolve_tick_lake_root


class TickLakeReader:
    """
    Reader interface for Partitioned Parquet Tick Lake.
    All analytical queries execute using private in-memory (:memory:) DuckDB sessions,
    guaranteeing zero disk database lock contention with live ingestion writers.
    """

    def __init__(
        self,
        root: Optional[Union[str, Path]] = None,
        max_threads: int = 4,
        memory_limit: str = "2GB",
    ):
        if root is not None:
            self.root = Path(root).resolve()
        else:
            try:
                self.root = resolve_tick_lake_root()
            except Exception:
                self.root = None
        self.max_threads = max_threads
        self.memory_limit = memory_limit

    def resolve_partition_files(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
    ) -> List[Path]:
        """Resolve candidate partition files for a symbol and date range before query."""
        raise NotImplementedError("Stage 2 interface stub")

    def query_candles(
        self,
        symbol: str,
        timeframe: str = "1m",
        start: Optional[Union[str, datetime]] = None,
        end: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deterministic OHLCV candle query using arg_min / arg_max on (timestamp, ingest_id).
        Returns list of candle dictionaries.
        """
        raise NotImplementedError("Stage 2 interface stub")

    def get_candles(
        self,
        symbol: str,
        timeframe: str = "1m",
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
        date: Optional[str] = None,
        hours: str = "extended",
    ) -> Dict[str, Any]:
        """
        Exchange-aligned OHLCV candle query formatted for dashboard consumption.
        Includes day_total_ticks, session_total_ticks, gaps, and America/New_York timestamps.
        """
        raise NotImplementedError("Stage 2 interface stub")

    def query_ticks(
        self,
        symbol: Optional[str] = None,
        start: Optional[Union[str, datetime]] = None,
        end: Optional[Union[str, datetime]] = None,
        limit: int = 10000,
        offset: int = 0,
        direction: str = "asc",
    ) -> List[Dict[str, Any]]:
        """Bounded tick inspection query with limit and offset support."""
        raise NotImplementedError("Stage 2 interface stub")

    def get_tape(
        self,
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Latest ticks in reverse chronological order (timestamp DESC, ingest_id DESC)
        with computed spread and formatting for the streaming tape UI.
        """
        raise NotImplementedError("Stage 2 interface stub")

    def get_latest_tick(
        self,
        symbol: str,
    ) -> Optional[Dict[str, Any]]:
        """Point lookup returning the most recent tick for a given symbol."""
        raise NotImplementedError("Stage 2 interface stub")

    def get_stream_status(self) -> Dict[str, Any]:
        """Read streamer status from _control/writer_status.json."""
        raise NotImplementedError("Stage 2 interface stub")

    def discover_available_weeks(self) -> List[Dict[str, Any]]:
        """Discover available trading weeks from lake partitions grouped Mon-Fri."""
        raise NotImplementedError("Stage 2 interface stub")

    def get_streaming_continuity_analysis(
        self,
        days: int = 5,
        symbol: str = "all",
        include_extended: bool = False,
        week_start: Optional[str] = None,
        target_date: Optional[str] = None,
        week_offset: Optional[int] = None,
        target_week: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Continuity and gap analysis engine over tick lake partitions."""
        raise NotImplementedError("Stage 2 interface stub")

    def snapshot(
        self,
        symbols: Optional[List[str]] = None,
        start_utc: Optional[datetime] = None,
        end_utc: Optional[datetime] = None,
    ) -> Any:
        """Create an immutable query snapshot over active partition files."""
        raise NotImplementedError("Stage 2 interface stub")


def get_tick_lake_reader(root: Optional[Union[str, Path]] = None) -> TickLakeReader:
    """Factory helper to obtain a TickLakeReader instance."""
    return TickLakeReader(root=root)
