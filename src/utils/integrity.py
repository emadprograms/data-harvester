import hashlib
import pandas as pd

def compute_fingerprint(client, date_str):
    """
    Returns a fingerprint tuple (count, volume_sum, max_ts, min_ts)
    for all market_data rows on or after the given date string.
    """
    try:
        res = client.execute(
            "SELECT COUNT(*), COALESCE(SUM(CAST(volume AS INTEGER)), 0), "
            "MAX(timestamp), MIN(timestamp) "
            "FROM market_data WHERE timestamp >= ?",
            [f"{date_str} 00:00:00"]
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
    
    # Target columns for hash consistency
    target_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    # Sort and convert to string for stable hashing
    df_sorted = df[target_cols].sort_values(['timestamp', 'symbol'])
    
    # Normalize timestamp to string if it's not already
    if pd.api.types.is_datetime64_any_dtype(df_sorted['timestamp']):
        df_sorted['timestamp'] = df_sorted['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Hash the CSV representation
    hash_obj = hashlib.md5(df_sorted.to_csv(index=False).encode('utf-8'))
    return hash_obj.hexdigest()

def verify_db_md5(client, df: pd.DataFrame, date_str: str, logger=None) -> tuple[bool, str]:
    """
    Verifies database integrity by reading back data and comparing MD5.
    Optimized for minimum reads by targeting only the harvested date.
    """
    if df.empty:
        return True, "Empty Data"
    
    try:
        df_md5 = calculate_df_md5(df)
        
        # Read back from DB
        # We only read the rows we expect (target date)
        res = client.execute(
            "SELECT timestamp, symbol, open, high, low, close, volume FROM market_data WHERE timestamp LIKE ?",
            [f"{date_str}%"]
        )
        
        if not res.rows:
            return False, "❌ DB Empty for date"
        
        db_df = pd.DataFrame([list(row) for row in res.rows], columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
        db_md5 = calculate_df_md5(db_df)
        
        if df_md5 == db_md5:
            return True, f"✅ MD5 MATCH ({df_md5[:8]})"
        else:
            return False, f"❌ MD5 MISMATCH (DF: {df_md5[:8]}, DB: {db_md5[:8]})"
            
    except Exception as e:
        msg = f"⚠️ MD5 Verification Error: {e}"
        if logger: logger.log(msg)
        return False, msg
