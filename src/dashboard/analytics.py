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


def get_historical_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000) -> dict:
    """
    Fetches canonical OHLCV candles exclusively from data/historical.duckdb.
    Zero blending with streaming data.
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

    where_clauses = ["symbol = ?"]
    params = [symbol]

    if start:
        where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
        params.append(start.strip())
    if end:
        where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
        params.append(end.strip())

    where_sql = " AND ".join(where_clauses)

    client = get_historical_db_connection(read_only=True)
    if not client:
        return {"error": "Historical DuckDB unavailable", "candles": [], "count": 0, "database": "historical"}

    try:
        if interval_str is None:
            # Raw 1-minute candles
            query = f"""
                SELECT 
                    epoch(timestamp::TIMESTAMP) as time_sec,
                    strftime(timestamp::TIMESTAMP, '%Y-%m-%d %H:%M:%S') as time_str,
                    open, high, low, close, COALESCE(volume, 0) as volume, source, session
                FROM market_data
                WHERE {where_sql}
                ORDER BY timestamp DESC
                LIMIT ?
            """
            params.append(limit)
            res = client.execute(query, params)
            rows = res.rows or []
        else:
            # Aggregated buckets via time_bucket()
            query = f"""
                SELECT 
                    epoch(bucket) as time_sec,
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
                        time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP) as bucket,
                        timestamp, open, high, low, close, volume, source, session
                    FROM market_data
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
            "count": len(candles),
            "candles": candles
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol, "candles": [], "count": 0, "database": "historical"}
    finally:
        client.close()


def get_streaming_candles(symbol: str, timeframe: str = "1m", start: str = None, end: str = None, limit: int = 1000) -> dict:
    """
    Fetches OHLCV candles resampled on-the-fly exclusively from raw ticks in data/streaming.duckdb.
    Zero dependency on historical.duckdb.
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
        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start:
            where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
            params.append(start.strip())
        if end:
            where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
            params.append(end.strip())

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                epoch(time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP)) as time_sec,
                strftime(time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP), '%Y-%m-%d %H:%M:%S') as time_str,
                first(price ORDER BY timestamp ASC) as open,
                max(price) as high,
                min(price) as low,
                last(price ORDER BY timestamp ASC) as close,
                COALESCE(sum(volume), count(*)) as volume,
                'CAPITAL_STREAM' as source,
                'REG' as session,
                count(*) as tick_count
            FROM ticks
            WHERE {where_sql}
            GROUP BY time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP)
            ORDER BY time_sec DESC
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
            "count": len(candles),
            "candles": candles
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol, "candles": [], "count": 0, "database": "streaming"}
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
        res_summary = client.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM market_data
        """).fetchone()

        res_sources = client.execute("""
            SELECT source, COUNT(*) as cnt
            FROM market_data
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
    Provides aggregated metadata across all symbols in symbol_map and market_data:
    total bars, first/last timestamps, latest price, and data source distribution.
    """
    client = get_historical_db_connection()
    if not client:
        return {"symbols": [], "total_symbols": 0, "error": "Database unavailable"}

    try:
        query = """
            WITH stats AS (
                SELECT 
                    symbol,
                    COUNT(*) as bar_count,
                    MIN(timestamp) as first_ts,
                    MAX(timestamp) as last_ts
                FROM market_data
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
                    FROM market_data
                ) WHERE rn = 1
            ),
            sources AS (
                SELECT 
                    symbol,
                    string_agg(DISTINCT source, ', ') as sources_list
                FROM market_data
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
            FROM symbol_map sm
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
    client = get_streaming_db_connection()
    if not client:
        return {"ticks": [], "count": 0, "error": "Streaming DB unavailable"}

    try:
        where_clause = "WHERE symbol = ?" if symbol else ""
        params = [symbol.strip().upper()] if symbol else []

        query = f"""
            SELECT 
                strftime(timestamp::TIMESTAMP, '%Y-%m-%d %H:%M:%S.%f') as time_str,
                symbol, price, COALESCE(volume, 1.0) as volume, bid, ask, source, session
            FROM ticks
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
    client = get_streaming_db_connection()
    ticks_total = 0
    latest_ts = None
    seconds_ago = None
    ticks_last_min = 0

    if client:
        try:
            res = client.execute("SELECT COUNT(*), MAX(timestamp) FROM ticks").fetchone()
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
            res_m = client.execute("SELECT COUNT(*) FROM ticks WHERE timestamp >= ?::TIMESTAMP", [one_min_ago]).fetchone()
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
