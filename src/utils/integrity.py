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
