import hashlib
import pandas as pd
import time
from datetime import datetime
from src.config import SCHEMA_COLS


def _range_params(start_utc, end_utc):
    """Convert datetime boundaries to SQL-safe string params."""
    start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_utc, datetime) else str(start_utc)
    end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_utc, datetime) else str(end_utc)
    return start_str, end_str


def compute_fingerprint(client, start_utc, end_utc):
    """
    Returns a fingerprint dict (count, volume_sum, max_ts, min_ts)
    for all market_data rows within the session range [start_utc, end_utc).
    """
    try:
        start_str, end_str = _range_params(start_utc, end_utc)
        res = client.execute(
            "SELECT COUNT(*), COALESCE(SUM(CAST(volume AS INTEGER)), 0), "
            "MAX(timestamp), MIN(timestamp) "
            "FROM market_data WHERE timestamp >= ? AND timestamp < ?",
            [start_str, end_str]
        )
        row = res.rows[0]
        return {
            "count": row[0] or 0,
            "volume_sum": row[1] or 0,
            "max_ts": row[2] or "",
            "min_ts": row[3] or "",
        }
    except Exception as e:
        return {"count": -1, "volume_sum": -1, "max_ts": "ERR", "min_ts": "ERR", "error": str(e)}

def calculate_df_md5(df: pd.DataFrame) -> str:
    """Calculates MD5 hash of a DataFrame's essential columns."""
    if df.empty:
        return ""
    
    # Work on a copy to avoid mutating the caller's DataFrame
    df = df.copy()
    
    # Target columns for hash consistency (Now including source/session for 9-column parity)
    target_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session', 'source']
    
    # Ensure all target columns exist
    for col in target_cols:
        if col not in df.columns:
            df[col] = 0.0 if col in ['open', 'high', 'low', 'close', 'volume'] else ""

    # Sort and convert to string for stable hashing
    df_sorted = df[target_cols].sort_values(['timestamp', 'symbol'])
    
    # Normalize timestamp to string if it's not already
    if pd.api.types.is_datetime64_any_dtype(df_sorted['timestamp']):
        df_sorted['timestamp'] = df_sorted['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Hash the CSV representation
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
            f"SELECT {col_list} FROM market_data WHERE timestamp >= ? AND timestamp < ?",
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

def ensure_database_parity(archive_client, mirror_client, start_utc, end_utc, logger=None) -> tuple[bool, str]:
    """
    Compares Archive and Mirror for the full session range [start_utc, end_utc)
    and repairs Mirror if they differ. Session-scoped to avoid missing rows
    that span multiple calendar dates.
    Returns (Success, Message).
    """
    try:
        start_str, end_str = _range_params(start_utc, end_utc)
        range_label = f"{start_str} → {end_str}"

        # 1. Fetch from Archive
        col_list = ", ".join(SCHEMA_COLS)
        res_a = archive_client.execute(
            f"SELECT {col_list} FROM market_data WHERE timestamp >= ? AND timestamp < ?",
            [start_str, end_str]
        )
        df_a = pd.DataFrame([list(row) for row in res_a.rows], columns=SCHEMA_COLS)
        md5_a = calculate_df_md5(df_a)
        
        if logger: logger.log(f"   📊 Archive: {len(df_a)} rows | MD5: {md5_a[:8]}")

        # 2. Fetch from Mirror
        res_m = mirror_client.execute(
            f"SELECT {col_list} FROM market_data WHERE timestamp >= ? AND timestamp < ?",
            [start_str, end_str]
        )
        df_m = pd.DataFrame([list(row) for row in res_m.rows], columns=SCHEMA_COLS)
        md5_m = calculate_df_md5(df_m)
        
        if logger: logger.log(f"   📊 Mirror : {len(df_m)} rows | MD5: {md5_m[:8]}")

        if md5_a == md5_m:
            if logger: logger.log(f"   ✅ Parity Confirmed for session {range_label}.")
            return True, f"✅ PARITY MATCH ({md5_a[:8]})"

        # 3. Repair if different
        if logger: logger.log(f"   ⚠️ Desync detected for session {range_label}. Repairing Mirror from Archive...")
        
        # Delete existing session data in Mirror
        mirror_client.execute(
            "DELETE FROM market_data WHERE timestamp >= ? AND timestamp < ?",
            [start_str, end_str]
        )
        
        if not df_a.empty:
            # Sanitize for Turso (Replace NaN/Inf with None)
            import numpy as np
            import math
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df_a.columns:
                    df_a[col] = df_a[col].replace([np.inf, -np.inf], np.nan)
                    df_a[col] = df_a[col].where(df_a[col].notnull(), None)

            rows_to_insert = []
            for row in df_a.itertuples(index=False):
                sanitized_row = []
                for item in row:
                    if isinstance(item, float) and not math.isfinite(item):
                        sanitized_row.append(None)
                    else:
                        sanitized_row.append(item)
                rows_to_insert.append(tuple(sanitized_row))

            from src.database.operations import _save_to_client
            _save_to_client(mirror_client, rows_to_insert, logger, "Mirror-Repair")
            
        return True, f"🛠 REPAIRED ({md5_a[:8]})"

    except Exception as e:
        msg = f"❌ Parity Check/Repair Failed: {e}"
        if logger: logger.log(f"   {msg}")
        return False, msg
