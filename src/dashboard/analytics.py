"""
Analytics and Query Engine for the Data Harvester Dashboard.
Provides high-performance DuckDB query execution for:
- Candlestick data extraction with dynamic time-bucketing (1m, 5m, 15m, 1h, 1d)
- Comprehensive symbol coverage, bar counts, and data source distribution
- Real-time tick stream tape and streamer daemon status
- Live US market session clock and countdowns
"""
import os
import time
import collections
from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
import psutil
from pandas.tseries.holiday import USFederalHolidayCalendar

from src.database.connection import (
    get_historical_db_connection,
    get_streaming_db_connection,
    DEFAULT_HISTORICAL_DB_PATH,
    DEFAULT_STREAMING_DB_PATH,
)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

TIMEFRAME_MAP = {
    "1m": None,  # Raw 1-minute
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "4h": "4 hours",
    "1d": "1 day",
}

# --- Exchange-time rendering contract -------------------------------------------------------
# Storage mandate: every timestamp in historical.duckdb / streaming.duckdb is pure UTC.
# Presentation mandate: the Historical Database page is an exchange-local chart, so all labels
# (X-axis ticks, crosshair, legend, inspector table, CSV) are rendered on the NYSE clock where
# the regular session opens at 09:30 and closes at 16:00 America/New_York.
#
# The conversion below is deliberately built from the *physical* column type and never relies on
# an implicit TIMESTAMPTZ -> TIMESTAMP cast: DuckDB resolves such casts with the session
# `TimeZone` setting, which it inherits from the host OS. On a host running in US Eastern, the
# previously used `((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York')::TIMESTAMP`
# expression silently round-tripped back to the raw UTC wall clock over a tz-aware column, so the
# 09:30 opening bell (13:30 UTC) was charted and labelled as "13:30 ET" with its volume spike.
EXCHANGE_TZ = "America/New_York"
TIME_EPOCH_BASIS = "utc"  # candle["time"] values are true UTC epoch seconds (not shifted wall clock)


def detect_timestamp_column_type(client, table: str, column: str = "timestamp") -> str:
    """
    Introspects the physical DuckDB type of a timestamp column.
    Returns 'TIMESTAMP' (canonical naive-UTC storage) or 'TIMESTAMP WITH TIME ZONE'
    (e.g. a database created by a legacy tz-aware pandas backfill).
    """
    try:
        res = client.execute(
            """
            SELECT data_type
            FROM duckdb_columns()
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table, column],
        )
        row = res.fetchone()
        if row and row[0]:
            return str(row[0]).upper()
    except Exception:
        pass
    return "TIMESTAMP"


def is_tz_aware_column(column_type: str) -> bool:
    """
    True when the stored column already carries a timezone.
    Accepts every spelling DuckDB and its clients use: 'TIMESTAMP WITH TIME ZONE' (canonical
    duckdb_columns() output), 'TIMESTAMPTZ', 'TIMESTAMP_TZ', 'TIMESTAMP WITH TIMEZONE'.
    """
    normalized = " ".join((column_type or "").upper().replace("_", " ").split())
    compact = normalized.replace(" ", "")
    return "TIMEZONE" in compact or "TIMESTAMPTZ" in compact


def build_utc_instant_sql(column_type: str, column: str = "timestamp") -> str:
    """
    SQL expression (TIMESTAMPTZ) resolving to the true UTC instant of a stored bar timestamp,
    independent of the DuckDB session TimeZone.
    """
    if is_tz_aware_column(column_type):
        return f"{column}::TIMESTAMPTZ"
    # Naive TIMESTAMP columns hold UTC wall-clock values: attach UTC explicitly.
    return f"timezone('UTC', {column}::TIMESTAMP)"


def build_exchange_local_sql(column_type: str, column: str = "timestamp") -> str:
    """
    SQL expression (naive TIMESTAMP) resolving to the exchange-local (America/New_York) wall clock
    of a stored bar timestamp. Used for human-readable labels and for time_bucket alignment so
    intraday and daily buckets snap to NYSE session boundaries instead of UTC ones.
    """
    return f"timezone('{EXCHANGE_TZ}', {build_utc_instant_sql(column_type, column)})"


def build_utc_epoch_sql(column_type: str, column: str = "timestamp") -> str:
    """SQL expression resolving to the true UTC epoch seconds of a stored bar timestamp."""
    return f"epoch({build_utc_instant_sql(column_type, column)})"


def build_instant_from_exchange_local_sql(local_expr: str) -> str:
    """SQL expression (TIMESTAMPTZ) converting an exchange-local wall clock back to its instant."""
    return f"timezone('{EXCHANGE_TZ}', {local_expr})"


def build_timestamp_range_clause(column_type: str, column: str, operator: str) -> str:
    """
    WHERE clause fragment comparing a stored timestamp against a UTC wall-clock string parameter.
    Kept explicit per column type so naive-UTC storage still benefits from index/zone-map pruning.
    """
    if is_tz_aware_column(column_type):
        return f"{column} {operator} timezone('UTC', ?::TIMESTAMP)"
    return f"{column}::TIMESTAMP {operator} ?::TIMESTAMP"


def get_historical_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000) -> dict:
    """
    Fetches canonical OHLCV candles exclusively from data/historical.duckdb.
    Zero blending with streaming data.

    Timestamp contract:
      - stored bars are UTC; `time` is returned as true UTC epoch seconds (chart positioning),
      - `time_str` is the same instant rendered on the NYSE clock (America/New_York, EST/EDT),
      - `timezone` / `time_epoch_basis` describe that contract to the frontend.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {"error": "symbol parameter is required", "candles": [], "count": 0, "database": "historical"}

    timeframe = (timeframe or "1m").lower()
    interval_str = TIMEFRAME_MAP.get(timeframe)
    if timeframe not in TIMEFRAME_MAP:
        timeframe = "1m"
        interval_str = None

    limit = min(max(1, int(limit or 1000)), 10000)

    client = get_historical_db_connection(read_only=True)
    if not client:
        return {"error": "Historical DuckDB unavailable", "candles": [], "count": 0, "database": "historical"}

    try:
        # Resolve the table and physical storage type first
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "minute_data" if "minute_data" in tables else "market_data"
        ts_type = detect_timestamp_column_type(client, table_name)
        instant_sql = build_utc_instant_sql(ts_type)
        exchange_local_sql = build_exchange_local_sql(ts_type)

        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start:
            where_clauses.append(build_timestamp_range_clause(ts_type, "timestamp", ">="))
            params.append(start.strip())
        if end:
            where_clauses.append(build_timestamp_range_clause(ts_type, "timestamp", "<="))
            params.append(end.strip())

        where_sql = " AND ".join(where_clauses)

        if interval_str is None:
            # Raw 1-minute candles: true UTC epoch for chart positioning + NYSE wall clock for labels
            query = f"""
                SELECT 
                    epoch({instant_sql}) as time_sec,
                    strftime({exchange_local_sql}, '%Y-%m-%d %H:%M:%S') as time_str,
                    open, high, low, close, COALESCE(volume, 0) as volume, source, session
                FROM {table_name}
                WHERE {where_sql}
                ORDER BY timestamp DESC
                LIMIT ?
            """
        else:
            # Aggregated buckets via time_bucket() aligned to NYSE session boundaries, so 1h/4h/1D
            # bars start at exchange-local hour/day edges (09:30 open bell stays its own bar).
            query = f"""
                SELECT 
                    epoch({build_instant_from_exchange_local_sql('bucket')}) as time_sec,
                    strftime(bucket, '%Y-%m-%d %H:%M:%S') as time_str,
                    first(open ORDER BY timestamp ASC) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close ORDER BY timestamp ASC) as close,
                    COALESCE(sum(volume), 0) as volume,
                    string_agg(DISTINCT source, ', ') as source,
                    string_agg(DISTINCT session, ', ') as session
                FROM (
                    SELECT 
                        time_bucket(INTERVAL '{interval_str}', {exchange_local_sql}) as bucket,
                        timestamp, open, high, low, close, volume, source, session
                    FROM {table_name}
                    WHERE {where_sql}
                )
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT ?
            """

        params.append(limit)
        res = client.execute(query, params)
        rows = res.rows or []

        candles = []
        for r in reversed(rows):
            candles.append({
                "time": int(r[0]),
                "time_str": str(r[1]),
                "open": round(float(r[2]), 4) if r[2] is not None else None,
                "high": round(float(r[3]), 4) if r[3] is not None else None,
                "low": round(float(r[4]), 4) if r[4] is not None else None,
                "close": round(float(r[5]), 4) if r[5] is not None else None,
                "volume": round(float(r[6]), 2) if r[6] is not None else 0.0,
                "source": r[7] or "UNKNOWN",
                "session": r[8] or "REG"
            })

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "database": "historical",
            "timezone": EXCHANGE_TZ,
            "time_epoch_basis": TIME_EPOCH_BASIS,
            "storage_timestamp_type": ts_type,
            "count": len(candles),
            "candles": candles
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol, "candles": [], "count": 0, "database": "historical", "timezone": EXCHANGE_TZ}
    finally:
        client.close()


def get_streaming_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000) -> dict:
    """
    Fetches OHLCV candles resampled on-the-fly exclusively from raw ticks in data/streaming.duckdb.
    Zero dependency on historical.duckdb.
    Buckets are aligned to the NYSE clock (America/New_York) and follow the same timestamp
    contract as get_historical_candles(): UTC epoch in `time`, exchange-local label in `time_str`.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {"error": "symbol parameter is required", "candles": [], "count": 0, "database": "streaming"}

    timeframe = (timeframe or "1m").lower()
    interval_map = {
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
    interval_str = interval_map.get(timeframe, "1 minute")
    limit = min(max(1, int(limit or 1000)), 10000)

    s_client = get_streaming_db_connection(read_only=True)
    if not s_client:
        return {"error": "Streaming DuckDB unavailable", "candles": [], "count": 0, "database": "streaming"}

    try:
        tables = [t[0] for t in s_client.execute("SHOW TABLES").fetchall()]
        table_name = "tick_data" if "tick_data" in tables else "ticks"
        ts_type = detect_timestamp_column_type(s_client, table_name)
        exchange_local_sql = build_exchange_local_sql(ts_type)

        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start:
            where_clauses.append(build_timestamp_range_clause(ts_type, "timestamp", ">="))
            params.append(start.strip())
        if end:
            where_clauses.append(build_timestamp_range_clause(ts_type, "timestamp", "<="))
            params.append(end.strip())

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                epoch({build_instant_from_exchange_local_sql('bucket')}) as time_sec,
                strftime(bucket, '%Y-%m-%d %H:%M:%S') as time_str,
                first(price ORDER BY timestamp ASC) as open,
                max(price) as high,
                min(price) as low,
                last(price ORDER BY timestamp ASC) as close,
                COALESCE(sum(volume), count(*)) as volume,
                COALESCE(first(source ORDER BY timestamp ASC), 'CAPITAL_STREAM') as source,
                COALESCE(first(session ORDER BY timestamp ASC), 'REG') as session,
                count(*) as tick_count
            FROM (
                SELECT 
                    time_bucket(INTERVAL '{interval_str}', {exchange_local_sql}) as bucket,
                    timestamp, price, volume, source, session
                FROM {table_name}
                WHERE {where_sql}
            )
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT ?
        """
        params.append(limit)
        res = s_client.execute(query, params)
        rows = res.rows or []

        candles = []
        for r in reversed(rows):
            candles.append({
                "time": int(r[0]),
                "time_str": str(r[1]),
                "open": round(float(r[2]), 4) if r[2] is not None else None,
                "high": round(float(r[3]), 4) if r[3] is not None else None,
                "low": round(float(r[4]), 4) if r[4] is not None else None,
                "close": round(float(r[5]), 4) if r[5] is not None else None,
                "volume": round(float(r[6]), 2) if r[6] is not None else 0.0,
                "source": r[7] or "CAPITAL_STREAM",
                "session": r[8] or "REG",
                "tick_count": int(r[9]) if len(r) > 9 and r[9] is not None else 0
            })

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "database": "streaming",
            "timezone": EXCHANGE_TZ,
            "time_epoch_basis": TIME_EPOCH_BASIS,
            "storage_timestamp_type": ts_type,
            "count": len(candles),
            "candles": candles
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol, "candles": [], "count": 0, "database": "streaming", "timezone": EXCHANGE_TZ}
    finally:
        s_client.close()


def get_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000, db_source: str = "historical") -> dict:
    """
    Unified entry point routing to either the canonical historical archive or the live streaming buffer.
    Never blends both databases silently.
    """
    db_source = (db_source or "historical").lower().strip()
    if db_source in ["streaming", "live", "stream", "ticks"]:
        return get_streaming_candles(symbol, timeframe=timeframe, start=start, end=end, limit=limit)
    return get_historical_candles(symbol, timeframe=timeframe, start=start, end=end, limit=limit)


def get_historical_overview() -> dict:
    """
    Returns high-level metadata for data/historical.duckdb:
    total rows, unique symbols, overall date span, and breakdown by source tier.
    """
    client = get_historical_db_connection(read_only=True)
    if not client:
        return {"error": "Historical database unavailable", "database": "data/historical.duckdb"}

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "minute_data" if "minute_data" in tables else "market_data"

        res_summary = client.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM {table_name}
        """).fetchone()

        res_sources = client.execute(f"""
            SELECT source, COUNT(*) as cnt
            FROM {table_name}
            GROUP BY source
            ORDER BY cnt DESC
        """).fetchall()

        total = res_summary[0] if res_summary else 0
        sources_breakdown = {}
        for src, cnt in (res_sources or []):
            src_name = src or "UNKNOWN"
            sources_breakdown[src_name] = {
                "count": cnt,
                "percentage": round((cnt / total * 100), 2) if total > 0 else 0
            }

        return {
            "total_rows": total,
            "unique_symbols": res_summary[1] if res_summary else 0,
            "min_timestamp": str(res_summary[2]) if res_summary and res_summary[2] else None,
            "max_timestamp": str(res_summary[3]) if res_summary and res_summary[3] else None,
            "sources": sources_breakdown,
            "database": "data/historical.duckdb"
        }
    except Exception as e:
        return {"error": str(e), "database": "data/historical.duckdb"}
    finally:
        client.close()


def get_symbols_coverage() -> dict:
    """
    Provides aggregated metadata across all symbols in symbol_map and minute_data:
    total bars, first/last timestamps, latest price, and data source distribution.
    """
    client = get_historical_db_connection()
    if not client:
        return {"symbols": [], "total_symbols": 0, "error": "Database unavailable"}

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "minute_data" if "minute_data" in tables else "market_data"
        symbol_table = "symbol_map"
        if "symbol_map" not in tables and "historical_database_symbols" in tables:
            symbol_table = "historical_database_symbols"

        query = f"""
            WITH stats AS (
                SELECT 
                    symbol,
                    COUNT(*) as bar_count,
                    MIN(timestamp) as first_ts,
                    MAX(timestamp) as last_ts
                FROM {table_name}
                GROUP BY symbol
            ),
            latest_rows AS (
                SELECT 
                    symbol,
                    close as latest_close,
                    source as latest_source,
                    timestamp as latest_ts
                FROM (
                    SELECT 
                        symbol, close, source, timestamp,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                    FROM {table_name}
                ) WHERE rn = 1
            ),
            sources AS (
                SELECT 
                    symbol,
                    string_agg(DISTINCT source, ', ') as sources_list
                FROM {table_name}
                GROUP BY symbol
            )
            SELECT 
                sm.display_name,
                sm.capital_ticker,
                sm.massive_ticker,
                sm.yahoo_ticker,
                sm.binance_ticker,
                COALESCE(s.bar_count, 0) as bar_count,
                s.first_ts,
                s.last_ts,
                lr.latest_close,
                lr.latest_source,
                COALESCE(src.sources_list, 'NONE') as sources_list
            FROM {symbol_table} sm
            LEFT JOIN stats s ON sm.display_name = s.symbol
            LEFT JOIN latest_rows lr ON sm.display_name = lr.symbol
            LEFT JOIN sources src ON sm.display_name = src.symbol
            ORDER BY bar_count DESC, sm.display_name ASC
        """
        res = client.execute(query)
        rows = res.rows or []

        symbols = []
        total_bars_all = 0

        now_utc = datetime.now(timezone.utc)

        for r in rows:
            bar_cnt = int(r[5])
            total_bars_all += bar_cnt
            last_ts_str = str(r[7]) if r[7] else None
            
            # Freshness calculation
            freshness = "EMPTY"
            if last_ts_str:
                try:
                    dt = datetime.strptime(last_ts_str.split('.')[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    age_hours = (now_utc - dt).total_seconds() / 3600.0
                    freshness = "FRESH" if age_hours <= 36 else "STALE"
                except Exception:
                    freshness = "RECORDED"

            # Asset class detection heuristic
            disp = r[0]
            if disp.endswith("USDT") or disp in ["BTC", "ETH"]:
                asset_class = "Crypto"
            elif disp in ["SPY", "QQQ", "IWM", "DIA", "XLC", "XLK", "XLF"]:
                asset_class = "ETF"
            elif disp in ["CL=F", "GC=F", "VIX", "UUP"]:
                asset_class = "Commodity/Index"
            else:
                asset_class = "Equity"

            symbols.append({
                "display_name": r[0],
                "capital_ticker": r[1],
                "massive_ticker": r[2],
                "yahoo_ticker": r[3],
                "binance_ticker": r[4],
                "bar_count": bar_cnt,
                "first_timestamp": str(r[6]) if r[6] else None,
                "last_timestamp": last_ts_str,
                "latest_close": round(float(r[8]), 4) if r[8] is not None else None,
                "latest_source": r[9],
                "sources_list": r[10],
                "freshness": freshness,
                "asset_class": asset_class
            })

        return {
            "total_symbols": len(symbols),
            "total_bars_database": total_bars_all,
            "symbols": symbols
        }
    except Exception as e:
        return {"symbols": [], "total_symbols": 0, "error": str(e)}
    finally:
        client.close()


def get_stream_tape(symbol: str = None, limit: int = 50) -> dict:
    """
    Returns latest raw ticks from streaming.duckdb, calculating spread and throughput.
    """
    limit = min(max(1, int(limit or 50)), 200)
    client = get_streaming_db_connection(read_only=True)
    if not client:
        return {"ticks": [], "count": 0, "error": "Streaming DB unavailable"}

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "tick_data" if "tick_data" in tables else "ticks"
        where_clause = "WHERE symbol = ?" if symbol else ""
        params = [symbol.strip().upper()] if symbol else []

        query = f"""
            SELECT 
                strftime(timestamp::TIMESTAMP, '%Y-%m-%d %H:%M:%S.%f') as time_str,
                symbol, price, COALESCE(volume, 1.0) as volume, bid, ask, source, session
            FROM {table_name}
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        res = client.execute(query, params)
        rows = res.rows or []

        ticks = []
        for r in rows:
            bid = float(r[4]) if r[4] is not None else None
            ask = float(r[5]) if r[5] is not None else None
            spread = round(ask - bid, 4) if (ask is not None and bid is not None) else None

            ticks.append({
                "timestamp": str(r[0])[:-3],  # Millisecond precision
                "symbol": r[1],
                "price": round(float(r[2]), 4) if r[2] is not None else None,
                "volume": round(float(r[3]), 2) if r[3] is not None else 1.0,
                "bid": round(bid, 4) if bid is not None else None,
                "ask": round(ask, 4) if ask is not None else None,
                "spread": spread,
                "source": r[6] or "CAPITAL",
                "session": r[7] or "REG"
            })

        return {
            "symbol": symbol or "ALL",
            "count": len(ticks),
            "ticks": ticks
        }
    except Exception as e:
        return {"ticks": [], "count": 0, "error": str(e)}
    finally:
        client.close()


def get_ticks(symbol: str = None, start: str = None, end: str = None, limit: int = 10000, offset: int = 0, direction: str = "asc") -> dict:
    """
    Queries raw ticks from streaming.duckdb with filtering by symbol, date/time range, limit, offset, and direction.
    """
    limit = min(max(1, int(limit or 10000)), 100000)
    offset = max(0, int(offset or 0))
    direction = "DESC" if str(direction).lower() == "desc" else "ASC"
    
    client = get_streaming_db_connection(read_only=True)
    if not client:
        return {"ticks": [], "count": 0, "error": "Streaming DB unavailable"}

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "tick_data" if "tick_data" in tables else "ticks"
        where_clauses = []
        params = []
        if symbol:
            where_clauses.append("symbol = ?")
            params.append(symbol.strip().upper())
        if start:
            where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
            params.append(start.strip())
        if end:
            where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
            params.append(end.strip())

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        query = f"""
            SELECT 
                strftime(timestamp::TIMESTAMP, '%Y-%m-%d %H:%M:%S.%f') as time_str,
                symbol, price, COALESCE(volume, 1.0) as volume, bid, ask, source, session
            FROM {table_name}
            {where_sql}
            ORDER BY timestamp {direction}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        res = client.execute(query, params)
        rows = res.rows or []

        ticks = []
        for r in rows:
            bid = float(r[4]) if r[4] is not None else None
            ask = float(r[5]) if r[5] is not None else None
            spread = round(ask - bid, 4) if (ask is not None and bid is not None) else None

            ticks.append({
                "timestamp": str(r[0])[:-3],
                "symbol": r[1],
                "price": round(float(r[2]), 4) if r[2] is not None else None,
                "volume": round(float(r[3]), 2) if r[3] is not None else 1.0,
                "bid": round(bid, 4) if bid is not None else None,
                "ask": round(ask, 4) if ask is not None else None,
                "spread": spread,
                "source": r[6] or "CAPITAL",
                "session": r[7] or "REG"
            })

        return {
            "symbol": symbol or "ALL",
            "count": len(ticks),
            "ticks": ticks
        }
    except Exception as e:
        return {"ticks": [], "count": 0, "error": str(e)}
    finally:
        client.close()


def get_stream_status() -> dict:
    """
    Inspects process table for src.stream.runner and checks recent tick throughput.
    """
    current_pid = os.getpid()
    running_pids = []
    
    try:
        for p in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if p.pid == current_pid:
                    continue
                cmdline = " ".join(p.info["cmdline"] or [])
                if "src.stream.runner" in cmdline:
                    running_pids.append(p.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except Exception:
        pass

    is_alive = len(running_pids) > 0
    active_pid = running_pids[0] if is_alive else None

    # Database tick activity
    client = get_streaming_db_connection(read_only=True)
    ticks_total = 0
    latest_ts = None
    seconds_ago = None
    ticks_last_min = 0

    if client:
        try:
            tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
            table_name = "tick_data" if "tick_data" in tables else "ticks"
            res = client.execute(f"SELECT COUNT(*), MAX(timestamp) FROM {table_name}").fetchone()
            if res:
                ticks_total = res[0] or 0
                latest_ts = str(res[1]) if res[1] else None

            if latest_ts:
                try:
                    dt = datetime.strptime(latest_ts.split('.')[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    seconds_ago = round((datetime.now(timezone.utc) - dt).total_seconds(), 1)
                except Exception:
                    pass

            # Count ticks in last 60 seconds
            one_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            res_m = client.execute(f"SELECT COUNT(*) FROM {table_name} WHERE timestamp >= ?::TIMESTAMP", [one_min_ago]).fetchone()
            if res_m:
                ticks_last_min = res_m[0] or 0
        except Exception:
            pass
        finally:
            client.close()

    return {
        "is_alive": is_alive,
        "pid": active_pid,
        "all_pids": running_pids,
        "ticks_total": ticks_total,
        "latest_tick_timestamp": latest_ts,
        "seconds_since_last_tick": seconds_ago,
        "ticks_last_minute": ticks_last_min,
        "status_label": "LIVE" if is_alive else "STOPPED"
    }


def get_market_session_info() -> dict:
    """
    Calculates US Eastern market session phase, holiday awareness, and next session boundaries.
    """
    now_et = datetime.now(ET)
    now_utc = datetime.now(timezone.utc)
    t = now_et.time()
    weekday = now_et.weekday()

    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=now_et.date() - timedelta(days=5), end=now_et.date() + timedelta(days=10)).date

    is_holiday = now_et.date() in holidays
    is_weekend = weekday >= 5

    if is_weekend or is_holiday:
        phase = "CLOSED"
        phase_label = "Market Closed (Weekend / Holiday)"
    elif t >= dtime(9, 30) and t < dtime(16, 0):
        phase = "REGULAR"
        phase_label = "Regular Trading Hours (9:30 AM - 4:00 PM ET)"
    elif t >= dtime(4, 0) and t < dtime(9, 30):
        phase = "PRE_MARKET"
        phase_label = "Pre-Market Session (4:00 AM - 9:30 AM ET)"
    elif t >= dtime(16, 0) and t < dtime(20, 0):
        phase = "AFTER_HOURS"
        phase_label = "After-Hours Session (4:00 PM - 8:00 PM ET)"
    else:
        phase = "OVERNIGHT"
        phase_label = "Overnight Session (Market Closed)"

    # Target session determination: 8:00 PM cutoff
    cutoff_time = dtime(20, 0)
    if t > cutoff_time:
        target_date = now_et.date() + timedelta(days=1)
    else:
        target_date = now_et.date()

    while target_date.weekday() > 4 or target_date in holidays:
        target_date += timedelta(days=1)

    # Next session cutoff timestamp (8 PM ET today or target date)
    today_cutoff = datetime.combine(now_et.date(), dtime(20, 0), tzinfo=ET)
    if now_et >= today_cutoff:
        next_cutoff = datetime.combine(now_et.date() + timedelta(days=1), dtime(20, 0), tzinfo=ET)
    else:
        next_cutoff = today_cutoff

    seconds_to_cutoff = max(0, int((next_cutoff - now_et).total_seconds()))

    return {
        "time_et": now_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "time_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "phase": phase,
        "phase_label": phase_label,
        "is_regular_open": phase == "REGULAR",
        "active_session_date": target_date.strftime("%Y-%m-%d"),
        "seconds_to_session_cutoff": seconds_to_cutoff,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday
    }


# 19 Monitored Single-Stock Equities (AAPL to TSM)
MONITORED_19_SYMBOLS = [
    "AAPL", "ADBE", "AMD", "AMZN", "APP",
    "AVGO", "BABA", "GOOGL", "META", "MSFT",
    "MU", "NDAQ", "NVDA", "ORCL", "PANW",
    "QCOM", "SHOP", "TSLA", "TSM"
]


def get_streaming_continuity_analysis(days: int = 5, symbol: str = "all", client=None, include_extended: bool = False) -> dict:
    """
    Bird's Eye View Data Continuity & Integrity Visualizer Analysis Engine.
    Exclusively queries streaming.duckdb (tick_data / ticks table). Zero access to historical.duckdb.

    Evaluates regular market session hours (09:30 to 16:00 ET, 390 min) or extended hours
    (04:00 to 20:00 ET, 960 min) across the last N trading days (Mon-Fri, excluding holidays and weekends).
    Non-market hours (overnight 20:00 to 04:00 ET and weekends) never trigger false gap alerts.

    Returns:
      - Master pulse health status: 'healthy' (green), 'partial' (amber), 'outage' (red).
      - Day-by-day minute-level buckets and detected gap incidents with exact UTC start_epoch and end_epoch.
      - 19-symbol spectrogram breakdown when symbol == 'all'.
      - Single-symbol continuity tracking when symbol != 'all'.
      - "extended_hours": bool indicating active window mode.
    """
    try:
        days = max(1, int(days or 5))
    except (ValueError, TypeError):
        days = 5

    symbol = (symbol or "all").strip().upper()
    is_all = (symbol == "ALL")
    view_mode = "all" if is_all else symbol
    include_extended = bool(include_extended)

    own_client = False
    if client is None:
        client = get_streaming_db_connection(read_only=True)
        own_client = True

    try:
        if not client:
            return {
                "database": "streaming",
                "view_mode": view_mode,
                "symbol": symbol,
                "monitored_symbols_count": 19 if is_all else 1,
                "extended_hours": include_extended,
                "hours": "extended" if include_extended else "regular",
                "days": [],
                "summary": {"total_gaps": 0, "total_outage_minutes": 0, "average_coverage": 100.0, "gaps": []},
                "spectrogram": {},
                "symbols_breakdown": {},
                "symbols": {},
            }

        # Resolve monitored symbols inventory from streaming_database_symbols
        monitored_symbols = []
        try:
            tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
            if "streaming_database_symbols" in tables:
                s_rows = client.execute("SELECT display_name FROM streaming_database_symbols WHERE is_active = true ORDER BY display_name").fetchall()
                monitored_symbols = [r[0] for r in s_rows if r[0]]
        except Exception:
            pass

        if not monitored_symbols:
            monitored_symbols = list(MONITORED_19_SYMBOLS)

        if not is_all:
            eval_symbols = [symbol]
            monitored_count = 1
        else:
            eval_symbols = monitored_symbols
            monitored_count = len(monitored_symbols)

        # Check table presence
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "tick_data" if "tick_data" in tables else ("ticks" if "ticks" in tables else None)
        if not table_name:
            return {
                "database": "streaming",
                "view_mode": view_mode,
                "symbol": symbol,
                "monitored_symbols_count": monitored_count,
                "extended_hours": include_extended,
                "hours": "extended" if include_extended else "regular",
                "days": [],
                "summary": {"total_gaps": 0, "total_outage_minutes": 0, "average_coverage": 100.0, "gaps": []},
                "spectrogram": {},
                "symbols_breakdown": {},
                "symbols": {},
            }

        count_row = client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        if not count_row or count_row[0] == 0:
            empty_spec = {s: {"symbol": s, "coverage_pct": 100.0, "status": "healthy", "total_gaps": 0, "total_outage_minutes": 0, "gaps": []} for s in eval_symbols}
            return {
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
            }

        # Timezone conversion SQL for exchange-local time
        ts_type = detect_timestamp_column_type(client, table_name)
        local_ts_sql = build_exchange_local_sql(ts_type, "timestamp")

        # Discover trading dates in streaming database
        start_time_sql = "04:00:00" if include_extended else "09:30:00"
        end_time_sql = "20:00:00" if include_extended else "16:00:00"

        # Fast path: query MAX(timestamp) and look back enough days to find target trading days
        max_ts_row = client.execute(f"SELECT MAX(timestamp) FROM {table_name}").fetchone()
        if not max_ts_row or not max_ts_row[0]:
            return {
                "database": "streaming",
                "view_mode": view_mode,
                "symbol": symbol,
                "monitored_symbols_count": monitored_count,
                "extended_hours": include_extended,
                "hours": "extended" if include_extended else "regular",
                "days": [],
                "summary": {"total_gaps": 0, "total_outage_minutes": 0, "average_coverage": 100.0, "gaps": []},
                "spectrogram": {},
                "symbols_breakdown": {},
                "symbols": {},
            }

        max_ts_val = max_ts_row[0]
        if isinstance(max_ts_val, str):
            max_ts_val = datetime.strptime(max_ts_val.split('.')[0], "%Y-%m-%d %H:%M:%S")

        lookback_days = max(30, days * 4)
        cutoff_dt = max_ts_val - timedelta(days=lookback_days)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

        dates_res = client.execute(f"""
            SELECT DISTINCT CAST({local_ts_sql} AS DATE) as d
            FROM {table_name}
            WHERE {build_timestamp_range_clause(ts_type, 'timestamp', '>=')}
              AND CAST({local_ts_sql} AS TIME) >= TIME '{start_time_sql}'
              AND CAST({local_ts_sql} AS TIME) <= TIME '{end_time_sql}'
            ORDER BY d ASC
        """, [cutoff_str]).fetchall()

        cal = USFederalHolidayCalendar()
        holidays = set(cal.holidays(start="2020-01-01", end="2035-01-01").date)

        trading_dates = []
        for r in dates_res:
            d = r[0]
            if isinstance(d, datetime):
                d = d.date()
            if d and d.weekday() < 5 and d not in holidays:
                trading_dates.append(d)

        # Fallback if fewer than `days` trading dates found in pruned window
        if len(trading_dates) < days:
            dates_res_full = client.execute(f"""
                SELECT DISTINCT CAST({local_ts_sql} AS DATE) as d
                FROM {table_name}
                WHERE CAST({local_ts_sql} AS TIME) >= TIME '{start_time_sql}'
                  AND CAST({local_ts_sql} AS TIME) <= TIME '{end_time_sql}'
                ORDER BY d ASC
            """).fetchall()
            trading_dates = []
            for r in dates_res_full:
                d = r[0]
                if isinstance(d, datetime):
                    d = d.date()
                if d and d.weekday() < 5 and d not in holidays:
                    trading_dates.append(d)

        if not trading_dates:
            return {
                "database": "streaming",
                "view_mode": view_mode,
                "symbol": symbol,
                "monitored_symbols_count": monitored_count,
                "extended_hours": include_extended,
                "hours": "extended" if include_extended else "regular",
                "days": [],
                "summary": {"total_gaps": 0, "total_outage_minutes": 0, "average_coverage": 100.0, "gaps": []},
                "spectrogram": {},
                "symbols_breakdown": {},
                "symbols": {},
            }

        target_dates = trading_dates[-days:]

        # Query 1-minute buckets across target dates
        min_date_str = target_dates[0].strftime("%Y-%m-%d")
        max_date_str = target_dates[-1].strftime("%Y-%m-%d")

        sym_filter = ""
        params = [min_date_str + " 00:00:00", min_date_str, max_date_str]
        if not is_all:
            sym_filter = "AND symbol = ?"
            params.append(symbol)

        start_time_bucket = "04:00:00" if include_extended else "09:30:00"
        end_time_bucket = "19:59:59" if include_extended else "15:59:59"

        q = f"""
            SELECT 
                CAST({local_ts_sql} AS DATE) as d,
                strftime(time_bucket(INTERVAL '1 minute', {local_ts_sql}), '%H:%M') as m,
                symbol,
                count(*) as tick_count
            FROM {table_name}
            WHERE {build_timestamp_range_clause(ts_type, 'timestamp', '>=')}
              AND CAST({local_ts_sql} AS DATE) >= ?::DATE
              AND CAST({local_ts_sql} AS DATE) <= ?::DATE
              AND CAST({local_ts_sql} AS TIME) >= TIME '{start_time_bucket}'
              AND CAST({local_ts_sql} AS TIME) <= TIME '{end_time_bucket}'
              {sym_filter}
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        rows = client.execute(q, params).fetchall()

        # Build minute map: day_str -> minute_str -> set of symbols
        day_minute_symbols = collections.defaultdict(lambda: collections.defaultdict(set))
        for d_val, m_val, sym_val, _ in rows:
            d_str = d_val.strftime("%Y-%m-%d") if hasattr(d_val, "strftime") else str(d_val)
            day_minute_symbols[d_str][m_val].add(sym_val)

        # Build session minutes (390 min for regular 09:30-15:59, 960 min for extended 04:00-19:59)
        session_minutes = []
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

        day_objs = []
        all_gaps = []
        spectrogram = {}

        # Initialize spectrogram trackers for all monitored symbols if is_all
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

            day_buckets = []
            day_gaps = []

            # 1. Evaluate per-minute status and attach exact UTC epoch seconds
            minute_statuses = {}
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

                # Calculate start_epoch and end_epoch in UTC seconds for minute m on date td
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

            # 2. Detect global outage gaps (all monitored symbols silent)
            outage_segments = []
            cur_outage = []
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
                    "description": f"{dur}m global blackout ({start_m} - {end_m} ET)" if is_all else f"{dur}m gap on {symbol} ({start_m} - {end_m} ET)"
                }
                day_gaps.append(gap_obj)
                all_gaps.append(gap_obj)

            # 3. Detect per-symbol gaps
            if is_all:
                for s in eval_symbols:
                    cur_sym_gap = []
                    sym_gap_segments = []
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
                            "description": f"{dur}m gap on {s} ({start_m} - {end_m} ET)"
                        }
                        spectrogram[s]["gaps"].append(s_gap_obj)
                        if not is_blackout:
                            day_gaps.append(s_gap_obj)
                            all_gaps.append(s_gap_obj)

            # Day coverage calculation
            if is_all:
                day_sym_coverages = []
                for s in eval_symbols:
                    s_active = sum(1 for m in session_minutes if s in min_data.get(m, set()))
                    day_sym_coverages.append((s_active / float(session_day_minutes)) * 100.0)
                day_cov = round(sum(day_sym_coverages) / len(day_sym_coverages), 2)
            else:
                active_cnt = sum(1 for m in session_minutes if len(min_data.get(m, set())) > 0)
                day_cov = round((active_cnt / float(session_day_minutes)) * 100.0, 2)

            # Day status
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

        # Finalize spectrogram if is_all
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

        # Overall summary
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

        return {
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
        }
    finally:
        if own_client and client:
            try:
                client.close()
            except Exception:
                pass

