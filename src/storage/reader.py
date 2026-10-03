"""
In-memory DuckDB reader for Partitioned Parquet Tick Lake (Milestone v4.0 - Phase 19).

Provides lock-free, read-only analytical queries over immutable Parquet batches
using isolated DuckDB (:memory:) connections.
"""
import collections
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

import duckdb
from pandas.tseries.holiday import USFederalHolidayCalendar
import psutil

from src.storage.config import (
    LakeMaintenanceInProgressError,
    decode_symbol,
    encode_symbol,
    resolve_tick_lake_root,
)
from src.storage.schema import LAKE_SCHEMA_V1

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

TIMEFRAME_INTERVAL_MAP = {
    "1s": "1 second",
    "5s": "5 seconds",
    "15s": "15 seconds",
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "4h": "4 hours",
    "1d": "1 day",
}

MONITORED_19_SYMBOLS = [
    "AAPL", "ADBE", "AMD", "AMZN", "APP",
    "AVGO", "BABA", "GOOGL", "META", "MSFT",
    "MU", "NDAQ", "NVDA", "ORCL", "PANW",
    "QCOM", "SHOP", "TSLA", "TSM"
]


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
        max_memory: Optional[str] = None,
        check_maintenance: bool = True,
    ):
        if root is not None:
            self.root = Path(root).resolve()
        else:
            try:
                self.root = resolve_tick_lake_root()
            except Exception:
                self.root = None

        self.max_threads = max_threads
        self.max_memory = max_memory or memory_limit or "2GB"
        self.memory_limit = self.max_memory
        self.check_maintenance = check_maintenance

    def _check_maintenance(self) -> None:
        """Raise LakeMaintenanceInProgressError if maintenance lock file is present."""
        if self.check_maintenance and self.root:
            guard = self.root / "_maintenance" / "in_progress.json"
            if guard.is_file():
                raise LakeMaintenanceInProgressError(f"Lake maintenance in progress: {guard}")

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Creates an isolated in-memory DuckDB connection configured for UTC and resource limits.
        Must be closed in a finally block by callers.
        """
        self._check_maintenance()
        con = duckdb.connect(":memory:")
        con.execute("SET TimeZone = 'UTC'")
        con.execute(f"SET threads = {self.max_threads}")
        con.execute(f"SET max_memory = '{self.max_memory}'")
        return con

    def resolve_partition_files(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
    ) -> List[Path]:
        """
        Resolve candidate partition files for a symbol and date range before query.
        Prunes at the filesystem directory level and never returns non-existent paths.
        """
        self._check_maintenance()
        if self.root is None:
            return []

        ticks_dir = self.root / "ticks"
        if not ticks_dir.is_dir():
            return []

        sym_dirs: List[Path] = []
        if symbol is not None:
            clean_sym = symbol.strip().upper()
            try:
                enc_sym = encode_symbol(clean_sym)
                sym_path = ticks_dir / f"symbol={enc_sym}"
                if sym_path.is_dir():
                    sym_dirs.append(sym_path)
                else:
                    # Fallback check for case-sensitive non-uppercase directory
                    raw_enc = encode_symbol(symbol.strip())
                    raw_path = ticks_dir / f"symbol={raw_enc}"
                    if raw_path.is_dir():
                        sym_dirs.append(raw_path)
            except Exception:
                return []
            if not sym_dirs:
                return []
        else:
            try:
                sym_dirs = [d for d in ticks_dir.iterdir() if d.is_dir() and d.name.startswith("symbol=")]
            except Exception:
                return []

        s_date: Optional[date] = None
        if start_date is not None:
            if isinstance(start_date, datetime):
                s_date = start_date.date()
            elif isinstance(start_date, date):
                s_date = start_date
            elif isinstance(start_date, str) and len(start_date) >= 10:
                try:
                    s_date = date.fromisoformat(start_date[:10])
                except ValueError:
                    s_date = None

        e_date: Optional[date] = None
        if end_date is not None:
            if isinstance(end_date, datetime):
                e_date = end_date.date()
            elif isinstance(end_date, date):
                e_date = end_date
            elif isinstance(end_date, str) and len(end_date) >= 10:
                try:
                    e_date = date.fromisoformat(end_date[:10])
                except ValueError:
                    e_date = None

        matched_files: List[Path] = []
        for s_dir in sym_dirs:
            try:
                for d_dir in s_dir.iterdir():
                    if not d_dir.is_dir() or not d_dir.name.startswith("date="):
                        continue
                    try:
                        d_val = date.fromisoformat(d_dir.name.split("=")[1])
                    except ValueError:
                        continue
                    if s_date is not None and d_val < s_date:
                        continue
                    if e_date is not None and d_val > e_date:
                        continue
                    for f in d_dir.glob("*.parquet"):
                        if f.is_file():
                            matched_files.append(f)
            except Exception:
                continue

        return sorted(matched_files)

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
        Returns list of candle dictionaries strictly matching calculate_expected_candles oracle.
        """
        start_dt: Optional[datetime] = None
        if start is not None:
            if isinstance(start, datetime):
                start_dt = start
            elif isinstance(start, str):
                try:
                    clean_s = start.replace("Z", "+00:00")
                    start_dt = datetime.fromisoformat(clean_s)
                    if start_dt.tzinfo is not None:
                        start_dt = start_dt.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    start_dt = None

        end_dt: Optional[datetime] = None
        if end is not None:
            if isinstance(end, datetime):
                end_dt = end
            elif isinstance(end, str):
                try:
                    clean_e = end.replace("Z", "+00:00")
                    end_dt = datetime.fromisoformat(clean_e)
                    if end_dt.tzinfo is not None:
                        end_dt = end_dt.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    end_dt = None

        s_date = start_dt.date() if start_dt else None
        e_date = end_dt.date() if end_dt else None

        files = self.resolve_partition_files(symbol=symbol, start_date=s_date, end_date=e_date)
        if not files:
            return []

        interval_str = TIMEFRAME_INTERVAL_MAP.get(str(timeframe).lower(), "1 minute")
        clean_sym = symbol.strip().upper()

        where_clauses = ["symbol = ?"]
        params: List[Any] = [clean_sym]

        if start_dt is not None:
            where_clauses.append("timestamp >= ?::TIMESTAMP")
            params.append(start_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
        if end_dt is not None:
            where_clauses.append("timestamp <= ?::TIMESTAMP")
            params.append(end_dt.strftime("%Y-%m-%d %H:%M:%S.%f"))

        where_sql = " AND ".join(where_clauses)
        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

        file_paths = [str(f) for f in files]

        con = self.connect()
        try:
            query = f"""
                SELECT
                    time_bucket(INTERVAL '{interval_str}', timestamp) AS time,
                    symbol,
                    arg_min(price, (timestamp, ingest_id)) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    arg_max(price, (timestamp, ingest_id)) AS close,
                    sum(coalesce(volume, 1.0)) AS volume,
                    count(*) AS tick_count
                FROM read_parquet(?, hive_partitioning=false)
                WHERE {where_sql}
                GROUP BY time, symbol
                ORDER BY time ASC, symbol ASC
                {limit_sql}
            """
            rows = con.execute(query, [file_paths] + params).fetchall()

            candles: List[Dict[str, Any]] = []
            for r in rows:
                candles.append({
                    "time": r[0],
                    "symbol": r[1],
                    "open": float(r[2]),
                    "high": float(r[3]),
                    "low": float(r[4]),
                    "close": float(r[5]),
                    "volume": float(r[6]),
                    "tick_count": int(r[7]),
                })
            return candles
        finally:
            con.close()

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
        symbol = (symbol or "").strip().upper()
        if not symbol:
            return {
                "error": "symbol parameter is required",
                "candles": [],
                "count": 0,
                "database": "streaming",
            }

        timeframe = (timeframe or "1m").lower()
        interval_str = TIMEFRAME_INTERVAL_MAP.get(timeframe, "1 minute")
        limit = min(max(1, int(limit or 1000)), 10000)
        if date:
            limit = max(limit, 2000)

        session_start_epoch: Optional[int] = None
        session_end_epoch: Optional[int] = None
        day_total_ticks = 0
        session_total_ticks = 0
        d_obj: Optional[date] = None

        start_date_bound: Optional[date] = None
        end_date_bound: Optional[date] = None

        if date:
            d_parts = date.strip().split("-")
            d_obj = datetime(int(d_parts[0]), int(d_parts[1]), int(d_parts[2])).date()
            if str(hours).lower() == "regular":
                s_dt = datetime(d_obj.year, d_obj.month, d_obj.day, 9, 30, 0, tzinfo=ET)
                e_dt = datetime(d_obj.year, d_obj.month, d_obj.day, 16, 0, 0, tzinfo=ET)
            else:
                s_dt = datetime(d_obj.year, d_obj.month, d_obj.day, 4, 0, 0, tzinfo=ET)
                e_dt = datetime(d_obj.year, d_obj.month, d_obj.day, 20, 0, 0, tzinfo=ET)

            session_start_epoch = int(s_dt.timestamp())
            session_end_epoch = int(e_dt.timestamp())

            s_dt_utc = s_dt.astimezone(timezone.utc)
            e_dt_utc = e_dt.astimezone(timezone.utc)

            day_start_et = datetime(d_obj.year, d_obj.month, d_obj.day, 0, 0, 0, tzinfo=ET)
            day_end_et = day_start_et + timedelta(days=1)
            day_start_utc = day_start_et.astimezone(timezone.utc)
            day_end_utc = day_end_et.astimezone(timezone.utc)

            start_date_bound = day_start_utc.date()
            end_date_bound = day_end_utc.date()
        else:
            if start:
                try:
                    start_date_bound = date.fromisoformat(start[:10])
                except Exception:
                    pass
            if end:
                try:
                    end_date_bound = date.fromisoformat(end[:10])
                except Exception:
                    pass

        files = self.resolve_partition_files(
            symbol=symbol,
            start_date=start_date_bound,
            end_date=end_date_bound,
        )

        base_resp: Dict[str, Any] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "database": "streaming",
            "timezone": "America/New_York",
            "time_epoch_basis": "utc",
            "storage_timestamp_type": "TIMESTAMP",
            "count": 0,
            "candles": [],
        }

        if date and session_start_epoch is not None and session_end_epoch is not None:
            base_resp["session_start_epoch"] = session_start_epoch
            base_resp["session_end_epoch"] = session_end_epoch
            base_resp["date"] = date
            base_resp["hours"] = hours
            base_resp["day_total_ticks"] = 0
            base_resp["session_total_ticks"] = 0
            base_resp["gaps"] = []

        if not files:
            return base_resp

        file_paths = [str(f) for f in files]
        con = self.connect()

        try:
            if date and session_start_epoch is not None and session_end_epoch is not None:
                # Count day and session ticks
                counts_query = """
                    SELECT 
                        count(*) as day_total_ticks,
                        count(CASE WHEN timestamp >= ?::TIMESTAMP AND timestamp < ?::TIMESTAMP THEN 1 END) as session_total_ticks
                    FROM read_parquet(?, hive_partitioning=false)
                    WHERE symbol = ?
                      AND timestamp >= ?::TIMESTAMP
                      AND timestamp < ?::TIMESTAMP
                """
                c_row = con.execute(counts_query, [
                    s_dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    e_dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    file_paths,
                    symbol,
                    day_start_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    day_end_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                ]).fetchone()

                day_total_ticks = int(c_row[0]) if c_row and c_row[0] is not None else 0
                session_total_ticks = int(c_row[1]) if c_row and c_row[1] is not None else 0

                query = f"""
                    SELECT 
                        epoch(timezone('America/New_York', bucket)) as time_sec,
                        strftime(bucket, '%Y-%m-%d %H:%M:%S') as time_str,
                        arg_min(price, (timestamp, ingest_id)) as open,
                        max(price) as high,
                        min(price) as low,
                        arg_max(price, (timestamp, ingest_id)) as close,
                        sum(coalesce(volume, 1.0)) as volume,
                        arg_min(coalesce(source, 'CAPITAL'), (timestamp, ingest_id)) as source,
                        arg_min(coalesce(session, 'REG'), (timestamp, ingest_id)) as session,
                        count(*) as tick_count
                    FROM (
                        SELECT 
                            time_bucket(INTERVAL '{interval_str}', timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP))::TIMESTAMP) as bucket,
                            timestamp, price, volume, source, session, ingest_id
                        FROM read_parquet(?, hive_partitioning=false)
                        WHERE symbol = ?
                          AND timestamp >= ?::TIMESTAMP
                          AND timestamp < ?::TIMESTAMP
                    )
                    GROUP BY bucket
                    ORDER BY bucket ASC
                    LIMIT ?
                """
                params = [
                    file_paths,
                    symbol,
                    s_dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    e_dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    limit,
                ]
            else:
                where_clauses = ["symbol = ?"]
                params = [file_paths, symbol]
                if start:
                    where_clauses.append("timestamp >= ?::TIMESTAMP")
                    params.append(start.strip())
                if end:
                    where_clauses.append("timestamp <= ?::TIMESTAMP")
                    params.append(end.strip())

                where_sql = " AND ".join(where_clauses)
                query = f"""
                    SELECT 
                        epoch(timezone('America/New_York', bucket)) as time_sec,
                        strftime(bucket, '%Y-%m-%d %H:%M:%S') as time_str,
                        arg_min(price, (timestamp, ingest_id)) as open,
                        max(price) as high,
                        min(price) as low,
                        arg_max(price, (timestamp, ingest_id)) as close,
                        sum(coalesce(volume, 1.0)) as volume,
                        arg_min(coalesce(source, 'CAPITAL'), (timestamp, ingest_id)) as source,
                        arg_min(coalesce(session, 'REG'), (timestamp, ingest_id)) as session,
                        count(*) as tick_count
                    FROM (
                        SELECT 
                            time_bucket(INTERVAL '{interval_str}', timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP))::TIMESTAMP) as bucket,
                            timestamp, price, volume, source, session, ingest_id
                        FROM read_parquet(?, hive_partitioning=false)
                        WHERE {where_sql}
                    )
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT ?
                """
                params.append(limit)

            rows = con.execute(query, params).fetchall()

            candles: List[Dict[str, Any]] = []
            order_rows = rows if date else list(reversed(rows))
            for r in order_rows:
                candles.append({
                    "time": int(r[0]),
                    "time_str": str(r[1]),
                    "open": round(float(r[2]), 4) if r[2] is not None else None,
                    "high": round(float(r[3]), 4) if r[3] is not None else None,
                    "low": round(float(r[4]), 4) if r[4] is not None else None,
                    "close": round(float(r[5]), 4) if r[5] is not None else None,
                    "volume": round(float(r[6]), 2) if r[6] is not None else 0.0,
                    "source": r[7] or "CAPITAL",
                    "session": r[8] or "REG",
                    "tick_count": int(r[9]) if len(r) > 9 and r[9] is not None else 0,
                })

            resp: Dict[str, Any] = {
                "symbol": symbol,
                "timeframe": timeframe,
                "database": "streaming",
                "timezone": "America/New_York",
                "time_epoch_basis": "utc",
                "storage_timestamp_type": "TIMESTAMP",
                "count": len(candles),
                "candles": candles,
            }

            if date and session_start_epoch is not None and session_end_epoch is not None and d_obj is not None:
                resp["session_start_epoch"] = session_start_epoch
                resp["session_end_epoch"] = session_end_epoch
                resp["date"] = date
                resp["hours"] = hours
                resp["day_total_ticks"] = day_total_ticks
                resp["session_total_ticks"] = session_total_ticks

                gaps: List[Dict[str, Any]] = []

                def is_candle_gap_flagged(start_ep: int, end_ep: int, missing_m: int) -> bool:
                    if str(hours).lower() == "regular":
                        return missing_m >= 1
                    rth_start = int(datetime(d_obj.year, d_obj.month, d_obj.day, 9, 30, 0, tzinfo=ET).timestamp())
                    rth_end = int(datetime(d_obj.year, d_obj.month, d_obj.day, 15, 59, 0, tzinfo=ET).timestamp())
                    ov_s = max(start_ep, rth_start)
                    ov_e = min(end_ep, rth_end)
                    rth_m = ((ov_e - ov_s) // 60 + 1) if ov_s <= ov_e else 0
                    if rth_m >= 1:
                        return True
                    return missing_m >= 5

                if len(candles) >= 2:
                    for i in range(len(candles) - 1):
                        t1 = candles[i]["time"]
                        t2 = candles[i + 1]["time"]
                        diff_sec = t2 - t1
                        missing_min = (diff_sec // 60) - 1
                        if missing_min >= 1:
                            gap_start_epoch = t1 + 60
                            gap_end_epoch = t2 - 60
                            if is_candle_gap_flagged(gap_start_epoch, gap_end_epoch, missing_min):
                                s_dt = datetime.fromtimestamp(gap_start_epoch, tz=timezone.utc).astimezone(ET)
                                e_dt = datetime.fromtimestamp(gap_end_epoch, tz=timezone.utc).astimezone(ET)
                                start_str = s_dt.strftime("%H:%M")
                                end_str = e_dt.strftime("%H:%M")
                                gaps.append({
                                    "start_epoch": gap_start_epoch,
                                    "end_epoch": gap_end_epoch,
                                    "duration": missing_min,
                                    "start_str": start_str,
                                    "end_str": end_str,
                                    "description": f"{missing_min}m Gap ({start_str} - {end_str})",
                                })

                # Detect leading boundary gap
                if len(candles) > 0:
                    first_candle_time = candles[0]["time"]
                    if first_candle_time > session_start_epoch:
                        missing_min = (first_candle_time - session_start_epoch) // 60
                        leading_gap_start = session_start_epoch
                        leading_gap_end = first_candle_time - 60
                        if missing_min >= 1 and is_candle_gap_flagged(leading_gap_start, leading_gap_end, missing_min):
                            s_dt = datetime.fromtimestamp(leading_gap_start, tz=timezone.utc).astimezone(ET)
                            e_dt = datetime.fromtimestamp(leading_gap_end, tz=timezone.utc).astimezone(ET)
                            start_str = s_dt.strftime("%H:%M")
                            end_str = e_dt.strftime("%H:%M")
                            gaps.insert(0, {
                                "start_epoch": leading_gap_start,
                                "end_epoch": leading_gap_end,
                                "duration": missing_min,
                                "start_str": start_str,
                                "end_str": end_str,
                                "description": f"{missing_min}m Gap ({start_str} - {end_str})",
                            })

                    # Detect trailing boundary gap
                    last_candle_time = candles[-1]["time"]
                    if last_candle_time + 60 < session_end_epoch:
                        missing_min = (session_end_epoch - (last_candle_time + 60)) // 60
                        trailing_gap_start = last_candle_time + 60
                        trailing_gap_end = session_end_epoch - 60
                        if missing_min >= 1 and is_candle_gap_flagged(trailing_gap_start, trailing_gap_end, missing_min):
                            s_dt = datetime.fromtimestamp(trailing_gap_start, tz=timezone.utc).astimezone(ET)
                            e_dt = datetime.fromtimestamp(trailing_gap_end, tz=timezone.utc).astimezone(ET)
                            start_str = s_dt.strftime("%H:%M")
                            end_str = e_dt.strftime("%H:%M")
                            gaps.append({
                                "start_epoch": trailing_gap_start,
                                "end_epoch": trailing_gap_end,
                                "duration": missing_min,
                                "start_str": start_str,
                                "end_str": end_str,
                                "description": f"{missing_min}m Gap ({start_str} - {end_str})",
                            })
                else:
                    missing_min = (session_end_epoch - session_start_epoch) // 60
                    if missing_min >= 1 and is_candle_gap_flagged(session_start_epoch, session_end_epoch - 60, missing_min):
                        s_dt = datetime.fromtimestamp(session_start_epoch, tz=timezone.utc).astimezone(ET)
                        e_dt = datetime.fromtimestamp(session_end_epoch - 60, tz=timezone.utc).astimezone(ET)
                        start_str = s_dt.strftime("%H:%M")
                        end_str = e_dt.strftime("%H:%M")
                        gaps.append({
                            "start_epoch": session_start_epoch,
                            "end_epoch": session_end_epoch - 60,
                            "duration": missing_min,
                            "start_str": start_str,
                            "end_str": end_str,
                            "description": f"{missing_min}m Gap ({start_str} - {end_str})",
                        })

                gaps.sort(key=lambda g: g["start_epoch"])
                resp["gaps"] = gaps

            return resp
        finally:
            con.close()

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
        limit = min(max(1, int(limit or 10000)), 100000)
        offset = max(0, int(offset or 0))
        dir_sql = "DESC" if str(direction).lower() == "desc" else "ASC"

        s_date = None
        if start is not None:
            if isinstance(start, (date, datetime)):
                s_date = start.date() if isinstance(start, datetime) else start
            elif isinstance(start, str) and len(start) >= 10:
                try:
                    s_date = date.fromisoformat(start[:10])
                except Exception:
                    pass

        e_date = None
        if end is not None:
            if isinstance(end, (date, datetime)):
                e_date = end.date() if isinstance(end, datetime) else end
            elif isinstance(end, str) and len(end) >= 10:
                try:
                    e_date = date.fromisoformat(end[:10])
                except Exception:
                    pass

        files = self.resolve_partition_files(symbol=symbol, start_date=s_date, end_date=e_date)
        if not files:
            return []

        file_paths = [str(f) for f in files]
        where_clauses: List[str] = []
        params: List[Any] = [file_paths]

        if symbol:
            where_clauses.append("symbol = ?")
            params.append(symbol.strip().upper())
        if start:
            where_clauses.append("timestamp >= ?::TIMESTAMP")
            params.append(str(start).strip())
        if end:
            where_clauses.append("timestamp <= ?::TIMESTAMP")
            params.append(str(end).strip())

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        query = f"""
            SELECT 
                strftime(timestamp, '%Y-%m-%d %H:%M:%S.%f') as time_str,
                symbol, price, coalesce(volume, 1.0) as volume, bid, ask, source, session, ingest_id
            FROM read_parquet(?, hive_partitioning=false)
            {where_sql}
            ORDER BY timestamp {dir_sql}, ingest_id {dir_sql}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        con = self.connect()
        try:
            rows = con.execute(query, params).fetchall()
            ticks: List[Dict[str, Any]] = []
            for r in rows:
                bid_val = float(r[4]) if r[4] is not None else None
                ask_val = float(r[5]) if r[5] is not None else None
                spread_val = round(ask_val - bid_val, 4) if (ask_val is not None and bid_val is not None) else None

                ticks.append({
                    "timestamp": str(r[0])[:-3],
                    "symbol": r[1],
                    "price": round(float(r[2]), 4) if r[2] is not None else None,
                    "volume": round(float(r[3]), 2) if r[3] is not None else 1.0,
                    "bid": round(bid_val, 4) if bid_val is not None else None,
                    "ask": round(ask_val, 4) if ask_val is not None else None,
                    "spread": spread_val,
                    "source": r[6] or "CAPITAL",
                    "session": r[7] or "REG",
                    "ingest_id": r[8],
                })
            return ticks
        finally:
            con.close()

    def get_tape(
        self,
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Latest ticks in reverse chronological order (timestamp DESC, ingest_id DESC)
        with computed spread and formatting for the streaming tape UI.
        """
        limit = min(max(1, int(limit or 50)), 500)
        all_files = self.resolve_partition_files(symbol=symbol)
        clean_sym = symbol.strip().upper() if symbol else "ALL"

        if not all_files:
            return {"symbol": clean_sym, "count": 0, "ticks": []}

        # Select files from most recent dates to optimize query execution
        date_groups = collections.defaultdict(list)
        for f in all_files:
            date_groups[f.parent.name].append(f)

        sorted_dates = sorted(date_groups.keys(), reverse=True)
        target_files: List[Path] = []
        for d in sorted_dates:
            target_files.extend(date_groups[d])
            if len(target_files) >= max(limit * 2, 50):
                break

        file_paths = [str(f) for f in target_files]
        where_clauses: List[str] = []
        params: List[Any] = [file_paths]

        if symbol:
            where_clauses.append("symbol = ?")
            params.append(clean_sym)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        query = f"""
            SELECT 
                strftime(timestamp, '%Y-%m-%d %H:%M:%S.%f') as time_str,
                symbol, price, coalesce(volume, 1.0) as volume, bid, ask,
                case when bid is not null and ask is not null then (ask - bid) else null end as spread,
                source, session, ingest_id
            FROM read_parquet(?, hive_partitioning=false)
            {where_sql}
            ORDER BY timestamp DESC, ingest_id DESC
            LIMIT ?
        """
        params.append(limit)

        con = self.connect()
        try:
            rows = con.execute(query, params).fetchall()
            ticks: List[Dict[str, Any]] = []
            for r in rows:
                ts_str = str(r[0])[:-3]
                bid_val = float(r[4]) if r[4] is not None else None
                ask_val = float(r[5]) if r[5] is not None else None
                spread_val = round(float(r[6]), 4) if r[6] is not None else None

                ticks.append({
                    "timestamp": ts_str,
                    "time_str": ts_str,
                    "symbol": r[1],
                    "price": round(float(r[2]), 4) if r[2] is not None else None,
                    "volume": round(float(r[3]), 2) if r[3] is not None else 1.0,
                    "bid": round(bid_val, 4) if bid_val is not None else None,
                    "ask": round(ask_val, 4) if ask_val is not None else None,
                    "spread": spread_val,
                    "source": r[7] or "CAPITAL",
                    "session": r[8] or "REG",
                    "ingest_id": r[9],
                })

            return {
                "symbol": clean_sym,
                "count": len(ticks),
                "ticks": ticks,
            }
        finally:
            con.close()

    def get_latest_tick(
        self,
        symbol: str,
    ) -> Optional[Dict[str, Any]]:
        """Point lookup returning the most recent tick for a given symbol."""
        tape = self.get_tape(symbol=symbol, limit=1)
        ticks = tape.get("ticks", [])
        if ticks:
            return ticks[0]
        return None

    def get_stream_status(self) -> Dict[str, Any]:
        """Read streamer status from _control/writer_status.json."""
        if self.root:
            status_file = self.root / "_control" / "writer_status.json"
            if status_file.is_file():
                try:
                    with open(status_file, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    raw_status = data.get("status", "RUNNING")
                    is_alive = str(raw_status).upper() in ("RUNNING", "HEALTHY", "LIVE")
                    writer_id = data.get("writer_id")
                    pid = data.get("pid")
                    ticks_total = data.get("total_rows_written", data.get("ticks_total", 0))

                    latest_ts = None
                    seconds_ago = None
                    last_pub = data.get("last_publish_time")
                    if last_pub is not None:
                        try:
                            dt = datetime.fromtimestamp(float(last_pub), tz=timezone.utc)
                            latest_ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                            seconds_ago = round((datetime.now(timezone.utc) - dt).total_seconds(), 1)
                        except Exception:
                            pass

                    return {
                        "is_alive": is_alive,
                        "status": raw_status,
                        "status_label": "LIVE" if is_alive else "STOPPED",
                        "writer_id": writer_id,
                        "pid": pid,
                        "all_pids": [pid] if pid else [],
                        "ticks_total": ticks_total,
                        "batches_published": data.get("batches_published", 0),
                        "latest_tick_timestamp": latest_ts,
                        "seconds_since_last_tick": seconds_ago,
                        "ticks_last_minute": data.get("last_batch_rows", 0),
                    }
                except Exception:
                    pass

            return {
                "is_alive": False,
                "status": "STOPPED",
                "status_label": "STOPPED",
                "writer_id": None,
                "pid": None,
                "all_pids": [],
                "ticks_total": 0,
                "batches_published": 0,
                "latest_tick_timestamp": None,
                "seconds_since_last_tick": None,
                "ticks_last_minute": 0,
            }

        return {
            "is_alive": False,
            "status": "STOPPED",
            "status_label": "STOPPED",
            "writer_id": None,
            "pid": None,
            "all_pids": [],
            "ticks_total": 0,
            "batches_published": 0,
            "latest_tick_timestamp": None,
            "seconds_since_last_tick": None,
            "ticks_last_minute": 0,
        }

    def discover_available_weeks(self) -> List[Dict[str, Any]]:
        """Discover available trading weeks from lake partitions grouped Mon-Fri."""
        if self.root is None:
            return []

        ticks_dir = self.root / "ticks"
        if not ticks_dir.is_dir():
            return []

        unique_dates = set()
        try:
            for date_dir in ticks_dir.glob("symbol=*/date=*"):
                if date_dir.is_dir():
                    d_name = date_dir.name
                    if d_name.startswith("date="):
                        try:
                            d = date.fromisoformat(d_name.split("=")[1])
                            if d.weekday() < 5:
                                unique_dates.add(d)
                        except ValueError:
                            pass
        except Exception:
            return []

        if not unique_dates:
            return []

        week_days_map = collections.defaultdict(set)
        for d in unique_dates:
            monday = d - timedelta(days=d.weekday())
            mon_str = monday.strftime("%Y-%m-%d")
            week_days_map[mon_str].add(d)

        sorted_mondays = sorted(week_days_map.keys(), reverse=True)
        weeks: List[Dict[str, Any]] = []
        for i, mon_str in enumerate(sorted_mondays):
            is_current = (i == 0)
            mon_date = datetime.strptime(mon_str, "%Y-%m-%d").date()
            fri_date = mon_date + timedelta(days=4)
            fri_str = fri_date.strftime("%Y-%m-%d")

            mon_lbl = mon_date.strftime("%b %d")
            fri_lbl = fri_date.strftime("%b %d, %Y")
            label = f"{mon_lbl} – {fri_lbl}" + (" (Current)" if is_current else "")

            weeks.append({
                "week_start": mon_str,
                "week_end": fri_str,
                "start_date": mon_str,
                "end_date": fri_str,
                "label": label,
                "is_current": is_current,
                "trading_days_count": len(week_days_map[mon_str]),
            })

        return weeks

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
        try:
            days = max(1, int(days or 5))
        except (ValueError, TypeError):
            days = 5

        symbol = (symbol or "all").strip().upper()
        is_all = (symbol == "ALL")
        view_mode = "all" if is_all else symbol
        include_extended = bool(include_extended)

        available_weeks = self.discover_available_weeks()
        if target_week and not week_start:
            week_start = target_week
        if end_date and not target_date:
            target_date = end_date
        if week_offset is not None and not week_start:
            try:
                w_off = int(week_offset)
                if 0 <= w_off < len(available_weeks):
                    week_start = available_weeks[w_off].get("week_start") or available_weeks[w_off].get("start_date")
            except Exception:
                pass
        active_week_start = week_start or (available_weeks[0]["week_start"] if available_weeks else None)

        eval_symbols = MONITORED_19_SYMBOLS if is_all else [symbol]
        monitored_count = len(eval_symbols)

        empty_spec = {
            s: {
                "symbol": s,
                "coverage_pct": 100.0,
                "status": "healthy",
                "total_gaps": 0,
                "total_outage_minutes": 0,
                "gaps": [],
            }
            for s in eval_symbols
        }

        empty_resp = {
            "status": "healthy",
            "database": "streaming",
            "view_mode": view_mode,
            "symbol": symbol,
            "monitored_symbols_count": monitored_count,
            "extended_hours": include_extended,
            "hours": "extended" if include_extended else "regular",
            "days": [],
            "summary": {"total_gaps": 0, "total_outage_minutes": 0, "average_coverage": 100.0, "gaps": []},
            "spectrogram": empty_spec if is_all else {},
            "symbols_breakdown": empty_spec if is_all else {},
            "symbols": empty_spec if is_all else {},
            "available_weeks": available_weeks,
            "week_start": active_week_start,
            "target_date": target_date,
        }

        if self.root is None or not (self.root / "ticks").is_dir():
            return empty_resp

        # Target dates discovery
        cal = USFederalHolidayCalendar()
        holidays = set(cal.holidays(start="2020-01-01", end="2035-01-01").date)

        target_dates: List[date] = []
        if target_date:
            try:
                t_parts = target_date.strip().split("-")
                target_dates = [date(int(t_parts[0]), int(t_parts[1]), int(t_parts[2]))]
                active_week_start = (target_dates[0] - timedelta(days=target_dates[0].weekday())).strftime("%Y-%m-%d")
            except Exception:
                target_dates = []
        elif week_start:
            try:
                ws_parts = week_start.strip().split("-")
                ws_date = date(int(ws_parts[0]), int(ws_parts[1]), int(ws_parts[2]))
                mon = ws_date - timedelta(days=ws_date.weekday())
                active_week_start = mon.strftime("%Y-%m-%d")
                target_dates = [mon + timedelta(days=i) for i in range(5) if (mon + timedelta(days=i)) not in holidays]
            except Exception:
                target_dates = []
        else:
            # Discover trading dates from active weeks
            all_lake_dates: List[date] = []
            for w in available_weeks:
                s_d = date.fromisoformat(w["week_start"])
                for i in range(5):
                    d_candidate = s_d + timedelta(days=i)
                    if d_candidate not in holidays:
                        all_lake_dates.append(d_candidate)
            all_lake_dates = sorted(set(all_lake_dates))
            if all_lake_dates:
                target_dates = all_lake_dates[-days:]
                active_week_start = (target_dates[0] - timedelta(days=target_dates[0].weekday())).strftime("%Y-%m-%d")

        if not target_dates:
            return empty_resp

        min_d = target_dates[0]
        max_d = target_dates[-1]

        # Resolve partition files covering the target range
        files = self.resolve_partition_files(
            symbol=None if is_all else symbol,
            start_date=min_d,
            end_date=max_d + timedelta(days=1),
        )
        if not files:
            return empty_resp

        file_paths = [str(f) for f in files]
        start_time_bucket = "04:00:00" if include_extended else "09:30:00"
        end_time_bucket = "19:59:59" if include_extended else "15:59:59"

        where_clauses = [
            "CAST(timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP)) AS DATE) >= ?::DATE",
            "CAST(timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP)) AS DATE) <= ?::DATE",
            f"CAST(timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP)) AS TIME) >= TIME '{start_time_bucket}'",
            f"CAST(timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP)) AS TIME) <= TIME '{end_time_bucket}'",
        ]
        params: List[Any] = [file_paths, min_d.isoformat(), max_d.isoformat()]

        if not is_all:
            where_clauses.append("symbol = ?")
            params.append(symbol)

        where_sql = " AND ".join(where_clauses)
        query = f"""
            SELECT 
                CAST(timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP)) AS DATE) as d,
                strftime(time_bucket(INTERVAL '1 minute', timezone('America/New_York', timezone('UTC', timestamp::TIMESTAMP))::TIMESTAMP), '%H:%M') as m,
                symbol,
                count(*) as tick_count
            FROM read_parquet(?, hive_partitioning=false)
            WHERE {where_sql}
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """

        con = self.connect()
        try:
            rows = con.execute(query, params).fetchall()
        finally:
            con.close()

        day_minute_symbols = collections.defaultdict(lambda: collections.defaultdict(set))
        for d_val, m_val, sym_val, _ in rows:
            d_str = d_val.strftime("%Y-%m-%d") if hasattr(d_val, "strftime") else str(d_val)
            day_minute_symbols[d_str][m_val].add(sym_val)

        session_minutes: List[str] = []
        if include_extended:
            cur_t = datetime(2000, 1, 1, 4, 0)
            end_t = datetime(2000, 1, 1, 20, 0)
        else:
            cur_t = datetime(2000, 1, 1, 9, 30)
            end_t = datetime(2000, 1, 1, 16, 0)

        while cur_t < end_t:
            session_minutes.append(cur_t.strftime("%H:%M"))
            cur_t += timedelta(minutes=1)
        session_day_minutes = len(session_minutes)

        def next_minute_str(m_str: str) -> str:
            hh, mm = map(int, m_str.split(":"))
            nxt = datetime(2000, 1, 1, hh, mm) + timedelta(minutes=1)
            return nxt.strftime("%H:%M")

        def is_continuity_gap_flagged(missing_minutes_list: list, is_ext: bool) -> bool:
            if not missing_minutes_list:
                return False
            dur = len(missing_minutes_list)
            if not is_ext:
                return dur >= 1
            rth_missing = sum(1 for m in missing_minutes_list if "09:30" <= m < "16:00")
            if rth_missing >= 1:
                return True
            return dur >= 5

        day_objs: List[Dict[str, Any]] = []
        all_gaps: List[Dict[str, Any]] = []
        spectrogram: Dict[str, Any] = {}

        if is_all:
            for s in eval_symbols:
                spectrogram[s] = {
                    "symbol": s,
                    "active_minutes": 0,
                    "total_minutes": len(target_dates) * session_day_minutes,
                    "coverage_pct": 100.0,
                    "status": "healthy",
                    "total_gaps": 0,
                    "total_outage_minutes": 0,
                    "gaps": [],
                }

        for td in target_dates:
            td_str = td.strftime("%Y-%m-%d")
            day_name = td.strftime("%A")
            min_data = day_minute_symbols[td_str]

            day_buckets: List[Dict[str, Any]] = []
            day_gaps: List[Dict[str, Any]] = []
            minute_statuses: Dict[str, str] = {}

            for m in session_minutes:
                active_syms = min_data.get(m, set()).intersection(eval_symbols)
                cnt = len(active_syms)
                if is_all:
                    if cnt == len(eval_symbols):
                        m_status = "healthy"
                    elif cnt > 0:
                        m_status = "partial"
                    else:
                        m_status = "outage"
                else:
                    m_status = "healthy" if cnt > 0 else "outage"

                minute_statuses[m] = m_status

                b_hh, b_mm = map(int, m.split(":"))
                b_dt_et = datetime(td.year, td.month, td.day, b_hh, b_mm, tzinfo=ET)
                b_start_epoch = int(b_dt_et.timestamp())
                b_end_epoch = b_start_epoch + 60

                day_buckets.append({
                    "time": m,
                    "status": m_status,
                    "active_count": cnt,
                    "total_count": len(eval_symbols),
                    "start_epoch": b_start_epoch,
                    "end_epoch": b_end_epoch,
                })

                if is_all:
                    for s in active_syms:
                        spectrogram[s]["active_minutes"] += 1

            outage_segments: List[List[str]] = []
            cur_outage: List[str] = []
            for m in session_minutes:
                if minute_statuses[m] == "outage":
                    cur_outage.append(m)
                else:
                    if cur_outage:
                        outage_segments.append(cur_outage)
                        cur_outage = []
            if cur_outage:
                outage_segments.append(cur_outage)

            global_blackout_minutes = set()
            for seg in outage_segments:
                if not is_continuity_gap_flagged(seg, include_extended):
                    continue
                dur = len(seg)
                start_m = seg[0]
                end_m = next_minute_str(seg[-1])
                for m in seg:
                    global_blackout_minutes.add(m)

                s_hh, s_mm = map(int, start_m.split(":"))
                g_dt_et = datetime(td.year, td.month, td.day, s_hh, s_mm, tzinfo=ET)
                g_start_epoch = int(g_dt_et.timestamp())
                g_end_epoch = g_start_epoch + (dur * 60)

                gap_obj = {
                    "date": td_str,
                    "start_time": f"{td_str} {start_m}:00",
                    "end_time": f"{td_str} {end_m}:00",
                    "start_str": start_m,
                    "end_str": end_m,
                    "start_epoch": g_start_epoch,
                    "end_epoch": g_end_epoch,
                    "duration": dur,
                    "duration_minutes": dur,
                    "missing_minutes": dur,
                    "status": "outage",
                    "type": "outage",
                    "severity": "outage",
                    "symbol": "ALL" if is_all else symbol,
                    "impacted_symbols": list(eval_symbols),
                    "description": f"{dur}m global blackout ({start_m} - {end_m} ET)" if is_all else f"{dur}m gap on {symbol} ({start_m} - {end_m} ET)",
                }
                day_gaps.append(gap_obj)
                all_gaps.append(gap_obj)

            if is_all:
                for s in eval_symbols:
                    cur_sym_gap: List[str] = []
                    sym_gap_segments: List[List[str]] = []
                    for m in session_minutes:
                        if s not in min_data.get(m, set()):
                            cur_sym_gap.append(m)
                        else:
                            if cur_sym_gap:
                                sym_gap_segments.append(cur_sym_gap)
                                cur_sym_gap = []
                    if cur_sym_gap:
                        sym_gap_segments.append(cur_sym_gap)

                    for seg in sym_gap_segments:
                        if not is_continuity_gap_flagged(seg, include_extended):
                            continue
                        dur = len(seg)
                        start_m = seg[0]
                        end_m = next_minute_str(seg[-1])
                        is_blackout = all(m in global_blackout_minutes for m in seg)
                        gap_status = "outage" if is_blackout else "partial"

                        sg_hh, sg_mm = map(int, start_m.split(":"))
                        sg_dt_et = datetime(td.year, td.month, td.day, sg_hh, sg_mm, tzinfo=ET)
                        sg_start_epoch = int(sg_dt_et.timestamp())
                        sg_end_epoch = sg_start_epoch + (dur * 60)

                        s_gap_obj = {
                            "date": td_str,
                            "start_time": f"{td_str} {start_m}:00",
                            "end_time": f"{td_str} {end_m}:00",
                            "start_str": start_m,
                            "end_str": end_m,
                            "start_epoch": sg_start_epoch,
                            "end_epoch": sg_end_epoch,
                            "duration": dur,
                            "duration_minutes": dur,
                            "missing_minutes": dur,
                            "status": gap_status,
                            "type": gap_status,
                            "severity": gap_status,
                            "symbol": s,
                            "impacted_symbols": [s],
                            "description": f"{dur}m gap on {s} ({start_m} - {end_m} ET)",
                        }
                        spectrogram[s]["gaps"].append(s_gap_obj)
                        if not is_blackout:
                            day_gaps.append(s_gap_obj)
                            all_gaps.append(s_gap_obj)

            if is_all:
                day_sym_coverages = []
                for s in eval_symbols:
                    s_active = sum(1 for m in session_minutes if s in min_data.get(m, set()))
                    day_sym_coverages.append((s_active / float(session_day_minutes)) * 100.0)
                day_cov = round(sum(day_sym_coverages) / len(day_sym_coverages), 2)
            else:
                active_cnt = sum(1 for m in session_minutes if len(min_data.get(m, set())) > 0)
                day_cov = round((active_cnt / float(session_day_minutes)) * 100.0, 2)

            has_outage = any(g.get("status") == "outage" for g in day_gaps)
            has_partial = any(g.get("status") == "partial" for g in day_gaps)
            if has_outage:
                day_status = "outage"
            elif has_partial:
                day_status = "partial"
            else:
                day_status = "healthy"

            day_objs.append({
                "date": td_str,
                "day_name": day_name,
                "coverage_pct": day_cov,
                "status": day_status,
                "buckets": day_buckets,
                "gaps": day_gaps,
            })

        if is_all:
            for s in eval_symbols:
                tot_min = spectrogram[s]["total_minutes"]
                act_min = spectrogram[s]["active_minutes"]
                cov = round((act_min / tot_min) * 100.0, 2) if tot_min > 0 else 100.0
                spectrogram[s]["coverage_pct"] = cov
                spectrogram[s]["total_gaps"] = len(spectrogram[s]["gaps"])
                spectrogram[s]["total_outage_minutes"] = sum(g["duration"] for g in spectrogram[s]["gaps"])
                if spectrogram[s]["total_outage_minutes"] == 0:
                    spectrogram[s]["status"] = "healthy"
                elif any(g["status"] == "outage" for g in spectrogram[s]["gaps"]):
                    spectrogram[s]["status"] = "outage"
                else:
                    spectrogram[s]["status"] = "partial"

        total_gaps = len(all_gaps)
        if is_all:
            total_outage_mins = sum(g["duration"] for g in all_gaps if g.get("status") == "outage")
        else:
            total_outage_mins = sum(g["duration"] for g in all_gaps)

        avg_cov = round(sum(d["coverage_pct"] for d in day_objs) / len(day_objs), 2) if day_objs else 100.0
        summary = {
            "total_gaps": total_gaps,
            "total_outage_minutes": total_outage_mins,
            "average_coverage": avg_cov,
            "gaps": all_gaps,
        }

        overall_status = "healthy"
        if total_outage_mins > 0:
            overall_status = "outage" if any(g.get("status") == "outage" for g in all_gaps) else "partial"

        return {
            "status": overall_status,
            "database": "streaming",
            "view_mode": view_mode,
            "symbol": symbol,
            "monitored_symbols_count": monitored_count,
            "extended_hours": include_extended,
            "hours": "extended" if include_extended else "regular",
            "days": day_objs,
            "summary": summary,
            "spectrogram": spectrogram if is_all else {},
            "symbols_breakdown": spectrogram if is_all else {},
            "symbols": spectrogram if is_all else {},
            "available_weeks": available_weeks,
            "week_start": active_week_start,
            "target_date": target_date,
        }

    def snapshot(
        self,
        symbols: Optional[List[str]] = None,
        start_utc: Optional[datetime] = None,
        end_utc: Optional[datetime] = None,
    ) -> Any:
        """Create an immutable query snapshot over active partition files."""
        import pyarrow.dataset as ds
        self._check_maintenance()
        files: List[Path] = []
        if symbols:
            for s in symbols:
                s_files = self.resolve_partition_files(
                    symbol=s,
                    start_date=start_utc.date() if start_utc else None,
                    end_date=end_utc.date() if end_utc else None,
                )
                files.extend(s_files)
        else:
            files = self.resolve_partition_files(
                symbol=None,
                start_date=start_utc.date() if start_utc else None,
                end_date=end_utc.date() if end_utc else None,
            )

        if not files:
            return ds.dataset([], schema=LAKE_SCHEMA_V1, format="parquet")
        return ds.dataset([str(f) for f in sorted(set(files))], format="parquet")

    def get_lake_health_report(self) -> Dict[str, Any]:
        """Computes active files count, total size, row count, status."""
        if self.root is None or not (self.root / "ticks").is_dir():
            return {
                "status": "empty",
                "root": str(self.root) if self.root else None,
                "total_files": 0,
                "total_size_bytes": 0,
                "active_symbols": 0,
                "healthy": True,
            }

        ticks_dir = self.root / "ticks"
        total_files = 0
        total_size = 0
        active_symbols = set()
        for f in ticks_dir.glob("symbol=*/date=*/*.parquet"):
            if f.is_file():
                total_files += 1
                total_size += f.stat().st_size
                try:
                    sym_part = f.parent.parent.name
                    if sym_part.startswith("symbol="):
                        active_symbols.add(decode_symbol(sym_part.split("=")[1]))
                except Exception:
                    pass

        return {
            "status": "healthy",
            "root": str(self.root),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "active_symbols": len(active_symbols),
            "healthy": True,
        }


_READER_CACHE: Dict[str, TickLakeReader] = {}


def get_tick_lake_reader(root: Optional[Union[str, Path]] = None) -> TickLakeReader:
    """Factory helper caching reader instance by resolved root."""
    resolved_root: Optional[Path] = None
    if root is not None:
        resolved_root = Path(root).resolve()
    else:
        try:
            resolved_root = resolve_tick_lake_root()
        except Exception:
            resolved_root = None

    cache_key = str(resolved_root) if resolved_root else "__default__"
    if cache_key not in _READER_CACHE:
        _READER_CACHE[cache_key] = TickLakeReader(root=resolved_root)
    return _READER_CACHE[cache_key]
