"""
Data Integrity & Health Engine.
Provides comprehensive verification, gap detection, quiet interval detection,
OHLCV anomaly validation, and cross-database price drift reconciliation.
"""
import os
import hashlib
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from src.config import SCHEMA_COLS, UTC, US_EASTERN
from src.database.connection import (
    get_historical_db_connection,
    get_streaming_db_connection,
    DuckDBClient,
    DEFAULT_HISTORICAL_DB_PATH,
    DEFAULT_STREAMING_DB_PATH,
)


def _range_params(start_utc, end_utc):
    """Convert datetime boundaries to SQL-safe string params."""
    start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_utc, datetime) else str(start_utc)
    end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_utc, datetime) else str(end_utc)
    return start_str, end_str


# --- Fingerprint & MD5 Verification (Legacy & Batch Integrity) ---

def compute_fingerprint(client, start_utc, end_utc):
    """
    Returns a fingerprint dict (count, volume_sum, max_ts, min_ts)
    for all market_data rows within the session range [start_utc, end_utc).
    """
    try:
        start_str, end_str = _range_params(start_utc, end_utc)
        res = client.execute(
            "SELECT COUNT(*), COALESCE(SUM(CAST(volume AS DOUBLE)), 0), "
            "MAX(timestamp), MIN(timestamp) "
            "FROM market_data WHERE timestamp >= ?::TIMESTAMP AND timestamp < ?::TIMESTAMP",
            [start_str, end_str]
        )
        row = res.rows[0]
        return {
            "count": row[0] or 0,
            "volume_sum": row[1] or 0,
            "max_ts": str(row[2] or ""),
            "min_ts": str(row[3] or ""),
        }
    except Exception as e:
        return {"count": -1, "volume_sum": -1, "max_ts": "ERR", "min_ts": "ERR", "error": str(e)}


def calculate_df_md5(df: pd.DataFrame) -> str:
    """Calculates MD5 hash of a DataFrame's essential columns."""
    if df.empty:
        return ""

    df = df.copy()
    target_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session', 'source']

    for col in target_cols:
        if col not in df.columns:
            df[col] = 0.0 if col in ['open', 'high', 'low', 'close', 'volume'] else ""

    df_sorted = df[target_cols].sort_values(['timestamp', 'symbol'])

    if pd.api.types.is_datetime64_any_dtype(df_sorted['timestamp']):
        df_sorted['timestamp'] = df_sorted['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    hash_obj = hashlib.md5(df_sorted.to_csv(index=False).encode('utf-8'))
    return hash_obj.hexdigest()


def verify_db_md5(client, df: pd.DataFrame, start_utc, end_utc, logger=None) -> tuple[bool, str]:
    """
    Verifies database integrity by reading back session data and comparing MD5.
    Uses the full session range [start_utc, end_utc) to capture all rows.
    """
    if df.empty:
        return True, "Empty Data"

    try:
        df_md5 = calculate_df_md5(df)

        start_str, end_str = _range_params(start_utc, end_utc)
        col_list = ', '.join(SCHEMA_COLS)
        res = client.execute(
            f"SELECT {col_list} FROM market_data WHERE timestamp >= ?::TIMESTAMP AND timestamp < ?::TIMESTAMP",
            [start_str, end_str]
        )

        if not res.rows:
            return False, "❌ DB Empty for session range"

        db_df = pd.DataFrame([list(row) for row in res.rows], columns=SCHEMA_COLS)
        db_md5 = calculate_df_md5(db_df)

        if df_md5 == db_md5:
            return True, f"✅ MD5 MATCH ({df_md5[:8]})"
        else:
            return False, f"❌ MD5 MISMATCH (DF: {df_md5[:8]}, DB: {db_md5[:8]})"

    except Exception as e:
        msg = f"⚠️ MD5 Verification Error: {e}"
        if logger: logger.log(msg)
        return False, msg


# --- 1-Minute Historical Gap Detection (INTG-01) ---

def detect_1m_gaps(symbol: str, start_utc: datetime, end_utc: datetime, client=None) -> dict:
    """
    Scans historical 1-minute market data for missing candle intervals.
    Returns gap intervals, count of missing bars, and continuity percentage.
    """
    own_client = False
    if not client:
        client = get_historical_db_connection()
        own_client = True

    if not client:
        return {"symbol": symbol, "error": "Database unavailable", "passed": False}

    try:
        start_str, end_str = _range_params(start_utc, end_utc)
        res = client.execute("""
            SELECT timestamp::TIMESTAMP as ts
            FROM market_data
            WHERE symbol = ? AND timestamp::TIMESTAMP >= ?::TIMESTAMP AND timestamp::TIMESTAMP < ?::TIMESTAMP
            ORDER BY timestamp ASC
        """, [symbol, start_str, end_str])

        timestamps = [row[0] for row in res.rows]
        actual_bars = len(timestamps)

        if not timestamps:
            return {
                "symbol": symbol,
                "start_utc": start_str,
                "end_utc": end_str,
                "actual_bars": 0,
                "expected_bars": 0,
                "missing_minutes": 0,
                "coverage_pct": 0.0,
                "gaps": [],
                "passed": False
            }

        gaps = []
        total_missing = 0

        for i in range(len(timestamps) - 1):
            curr_ts = timestamps[i]
            next_ts = timestamps[i + 1]
            diff_seconds = (next_ts - curr_ts).total_seconds()

            if diff_seconds > 60:
                missing = int((diff_seconds - 60) // 60)
                total_missing += missing
                gaps.append({
                    "gap_start": curr_ts.strftime('%Y-%m-%d %H:%M:%S'),
                    "gap_end": next_ts.strftime('%Y-%m-%d %H:%M:%S'),
                    "duration_seconds": int(diff_seconds),
                    "missing_minutes": missing
                })

        total_span_minutes = max(1, int((timestamps[-1] - timestamps[0]).total_seconds() // 60) + 1)
        coverage_pct = round((actual_bars / total_span_minutes) * 100, 2)

        return {
            "symbol": symbol,
            "start_utc": start_str,
            "end_utc": end_str,
            "actual_bars": actual_bars,
            "expected_span_bars": total_span_minutes,
            "missing_minutes": total_missing,
            "coverage_pct": min(100.0, coverage_pct),
            "gaps": gaps,
            "passed": total_missing == 0
        }
    finally:
        if own_client and client:
            client.close()


# --- Stream Continuity & Quiet Interval Detection (INTG-02) ---

def detect_stream_quiet_intervals(symbol: str, lookback_minutes: int = 60, threshold_seconds: int = 120, client=None) -> dict:
    """
    Scans live streaming ticks for quiet periods where no tick arrived for > threshold_seconds.
    Also returns latest tick timestamp and stream staleness.
    """
    own_client = False
    if not client:
        client = get_streaming_db_connection()
        own_client = True

    if not client:
        return {"symbol": symbol, "error": "Streaming DB unavailable", "passed": False}

    try:
        now_utc = datetime.now(timezone.utc)
        since_utc = now_utc - timedelta(minutes=lookback_minutes)
        since_str = since_utc.strftime('%Y-%m-%d %H:%M:%S')

        res = client.execute("""
            SELECT timestamp::TIMESTAMP as ts, price
            FROM ticks
            WHERE symbol = ? AND timestamp >= ?::TIMESTAMP
            ORDER BY timestamp ASC
        """, [symbol, since_str])

        ticks = res.rows
        if not ticks:
            # Check latest recorded tick ever for this symbol
            latest_res = client.execute("SELECT MAX(timestamp) FROM ticks WHERE symbol = ?", [symbol])
            latest_ts_row = latest_res.fetchone()
            last_recorded = str(latest_ts_row[0]) if latest_ts_row and latest_ts_row[0] else None

            return {
                "symbol": symbol,
                "ticks_in_window": 0,
                "quiet_intervals": [],
                "last_tick_time": last_recorded,
                "seconds_since_last_tick": -1,
                "is_stalled": True,
                "passed": False
            }

        quiet_intervals = []
        for i in range(len(ticks) - 1):
            t1 = ticks[i][0]
            t2 = ticks[i + 1][0]
            gap = (t2 - t1).total_seconds()
            if gap > threshold_seconds:
                quiet_intervals.append({
                    "interval_start": t1.strftime('%Y-%m-%d %H:%M:%S'),
                    "interval_end": t2.strftime('%Y-%m-%d %H:%M:%S'),
                    "gap_seconds": int(gap)
                })

        last_tick_dt = ticks[-1][0]
        # Attach timezone if naive
        if last_tick_dt.tzinfo is None:
            last_tick_dt = last_tick_dt.replace(tzinfo=timezone.utc)
        seconds_ago = (now_utc - last_tick_dt).total_seconds()

        return {
            "symbol": symbol,
            "ticks_in_window": len(ticks),
            "quiet_intervals": quiet_intervals,
            "last_tick_time": last_tick_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
            "seconds_since_last_tick": round(seconds_ago, 1),
            "is_stalled": seconds_ago > threshold_seconds,
            "passed": len(quiet_intervals) == 0 and seconds_ago <= threshold_seconds
        }
    finally:
        if own_client and client:
            client.close()


# --- OHLCV Sanity & Anomaly Validation (INTG-03) ---

def validate_ohlcv_anomalies(symbol: str = None, limit: int = 10000, client=None) -> dict:
    """
    Validates logical integrity of historical OHLCV candles:
    - high >= low
    - high >= open and high >= close
    - low <= open and low <= close
    - positive prices (open > 0, high > 0, low > 0, close > 0)
    - non-negative volume
    """
    own_client = False
    if not client:
        client = get_historical_db_connection()
        own_client = True

    if not client:
        return {"error": "Database unavailable", "passed": False}

    try:
        where_clause = "WHERE symbol = ?" if symbol else ""
        params = [symbol] if symbol else []

        query = f"""
            SELECT 
                timestamp, symbol, open, high, low, close, volume,
                CASE 
                    WHEN high < low THEN 'HIGH_LESS_THAN_LOW'
                    WHEN high < open THEN 'HIGH_LESS_THAN_OPEN'
                    WHEN high < close THEN 'HIGH_LESS_THAN_CLOSE'
                    WHEN low > open THEN 'LOW_GREATER_THAN_OPEN'
                    WHEN low > close THEN 'LOW_GREATER_THAN_CLOSE'
                    WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 'NON_POSITIVE_PRICE'
                    WHEN volume < 0 THEN 'NEGATIVE_VOLUME'
                    ELSE 'OK'
                END AS anomaly_type
            FROM market_data
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT {int(limit)}
        """
        res = client.execute(query, params)
        rows = res.rows

        total_checked = len(rows)
        anomalies = []

        for r in rows:
            anomaly = r[7]
            if anomaly != "OK":
                anomalies.append({
                    "timestamp": str(r[0]),
                    "symbol": r[1],
                    "open": r[2],
                    "high": r[3],
                    "low": r[4],
                    "close": r[5],
                    "volume": r[6],
                    "reason": anomaly
                })

        return {
            "symbol": symbol or "ALL",
            "total_bars_inspected": total_checked,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:50],  # Return up to 50 samples
            "passed": len(anomalies) == 0
        }
    finally:
        if own_client and client:
            client.close()


# --- Cross-Database Drift Reconciliation (INTG-04) ---

def analyze_price_drift(symbol: str, tolerance: float = 0.50, client=None, hist_path=None, stream_path=None) -> dict:
    """
    Compares 1-minute candles from historical.duckdb against resampled ticks in streaming.duckdb
    for overlapping time windows to verify data accuracy and price fidelity.
    """
    hp = hist_path or DEFAULT_HISTORICAL_DB_PATH
    sp = stream_path or DEFAULT_STREAMING_DB_PATH

    own_client = False
    if not client:
        client = DuckDBClient(":memory:")
        own_client = True

    try:
        client.attach(hp, "hist", read_only=True)
        client.attach(sp, "live", read_only=True)

        query = f"""
            WITH stream_1m AS (
                SELECT 
                    time_bucket(INTERVAL '1 minute', timestamp::TIMESTAMP) AS time,
                    symbol,
                    last(price ORDER BY timestamp) AS close_stream
                FROM live.ticks
                WHERE symbol = ?
                GROUP BY time, symbol
            )
            SELECT 
                h.timestamp,
                h.symbol,
                h.close AS rest_close,
                s.close_stream AS stream_close,
                ABS(h.close - s.close_stream) AS drift
            FROM hist.market_data h
            JOIN stream_1m s ON h.symbol = s.symbol AND h.timestamp::TIMESTAMP = s.time
            WHERE h.symbol = ?
            ORDER BY h.timestamp ASC
        """
        res = client.execute(query, [symbol, symbol])
        rows = res.rows

        if not rows:
            return {
                "symbol": symbol,
                "overlapping_bars": 0,
                "mean_absolute_drift": 0.0,
                "max_drift": 0.0,
                "max_drift_timestamp": None,
                "passed": True,
                "note": "No overlapping timestamps between REST and Streaming tables."
            }

        drifts = [r[4] for r in rows if r[4] is not None]
        mean_drift = float(np.mean(drifts)) if drifts else 0.0
        max_drift = float(np.max(drifts)) if drifts else 0.0
        max_idx = int(np.argmax(drifts)) if drifts else -1
        max_ts = str(rows[max_idx][0]) if max_idx >= 0 else None

        passed = bool(mean_drift <= tolerance)

        return {
            "symbol": symbol,
            "overlapping_bars": len(rows),
            "mean_absolute_drift": round(mean_drift, 4),
            "max_drift": round(max_drift, 4),
            "max_drift_timestamp": max_ts,
            "tolerance": tolerance,
            "passed": passed
        }
    except Exception as e:
        return {
            "symbol": symbol,
            "error": str(e),
            "passed": False
        }
    finally:
        if own_client:
            client.close()


# --- Database Health Overview (DASH-01 / Health) ---

def get_database_health_report(historical_path=None, streaming_path=None) -> dict:
    """
    Generates a full operational health report across both DuckDB databases:
    file sizes, total rows, active tables, min/max timestamps, and overall status.
    """
    hp = historical_path or DEFAULT_HISTORICAL_DB_PATH
    sp = streaming_path or DEFAULT_STREAMING_DB_PATH

    report = {
        "status": "HEALTHY",
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        "historical": {},
        "streaming": {},
        "issues": []
    }

    # 1. Historical DB check
    if os.path.exists(hp):
        size_mb = round(os.path.getsize(hp) / (1024 * 1024), 2)
        try:
            from src.database.connection import get_duckdb_connection
            h_client = get_duckdb_connection(hp, read_only=True)
            if h_client:
                res_md = h_client.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM market_data").fetchone()
                res_sym = h_client.execute("SELECT COUNT(*) FROM symbol_map").fetchone()
                h_client.close()

                report["historical"] = {
                    "exists": True,
                    "path": hp,
                    "size_mb": size_mb,
                    "market_data_rows": res_md[0] if res_md else 0,
                    "min_timestamp": str(res_md[1]) if res_md and res_md[1] else None,
                    "max_timestamp": str(res_md[2]) if res_md and res_md[2] else None,
                    "symbols_count": res_sym[0] if res_sym else 0
                }
            else:
                raise RuntimeError("Could not establish connection to historical DuckDB")
        except Exception as e:
            report["historical"] = {"exists": True, "path": hp, "size_mb": size_mb, "error": str(e)}
            report["issues"].append(f"Historical DB query error: {e}")
            report["status"] = "DEGRADED"
    else:
        report["historical"] = {"exists": False, "path": hp}
        report["issues"].append("Historical DuckDB file does not exist on disk.")
        report["status"] = "CRITICAL"

    # 2. Streaming DB check
    if os.path.exists(sp):
        size_mb = round(os.path.getsize(sp) / (1024 * 1024), 2)
        try:
            from src.database.connection import get_duckdb_connection
            s_client = get_duckdb_connection(sp, read_only=True)
            if s_client:
                res_ticks = s_client.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM ticks").fetchone()
                s_client.close()

                report["streaming"] = {
                    "exists": True,
                    "path": sp,
                    "size_mb": size_mb,
                    "ticks_rows": res_ticks[0] if res_ticks else 0,
                    "min_timestamp": str(res_ticks[1]) if res_ticks and res_ticks[1] else None,
                    "max_timestamp": str(res_ticks[2]) if res_ticks and res_ticks[2] else None
                }
            else:
                raise RuntimeError("Could not establish connection to streaming DuckDB")
        except Exception as e:
            report["streaming"] = {"exists": True, "path": sp, "size_mb": size_mb, "error": str(e)}
            report["issues"].append(f"Streaming DB query error: {e}")
            report["status"] = "DEGRADED" if report["status"] != "CRITICAL" else "CRITICAL"
    else:
        report["streaming"] = {"exists": False, "path": sp, "note": "Streaming DB file not created yet"}

    return report
