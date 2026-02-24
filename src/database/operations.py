"""
Database operations - Reverted to Stable Sequential Dual-Write.
"""
import pandas as pd
import numpy as np
import time
import math
from datetime import datetime
from src.database.connection import get_archive_db_connection, get_mirror_db_connection
from src.config import UTC, US_EASTERN

# --- Basic CRUD for Symbol Mapping ---

def get_symbol_map_from_db(client=None):
    """Fetches the complete symbol inventory from the symbol_map table."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True
        
    if not client:
        client = get_mirror_db_connection()
        if not client: return {}
        own_client = True
        
    try:
        res = client.execute("""
            SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker
            FROM symbol_map
            ORDER BY display_name
        """)
        inventory = {}
        for row in res.rows:
            inventory[row[0]] = {
                'yahoo_ticker': row[1],
                'massive_ticker': row[2],
                'binance_ticker': row[3],
                'capital_ticker': row[4]
            }
        return inventory
    except Exception:
        return {}
    finally:
        if own_client and client:
            client.close()

# --- MARKET DATA OPERATIONS ---

def clear_market_data_for_range(client, start_utc: datetime, end_utc: datetime, logger=None, label="DB", symbols=None):
    """
    Surgically deletes records within a specific UTC range.
    If symbols is provided, only deletes for those specific symbols.
    """
    try:
        start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S')
        
        if symbols:
            # Handle list of symbols
            placeholders = ",".join(["?"] * len(symbols))
            query = f"DELETE FROM market_data WHERE timestamp >= ? AND timestamp < ? AND symbol IN ({placeholders})"
            params = [start_str, end_str] + list(symbols)
            client.execute(query, params)
            if logger:
                logger.log(f"   ✅ {label}: Cleaned {len(symbols)} symbols for range: {start_str} to {end_str}")
        else:
            # Global wipe for range
            client.execute(
                "DELETE FROM market_data WHERE timestamp >= ? AND timestamp < ?",
                [start_str, end_str]
            )
            if logger:
                logger.log(f"   ✅ {label}: Cleaned ALL records for range: {start_str} to {end_str}")
    except Exception as e:
        if logger: logger.log(f"   ⚠️ {label} Range Clean-up warning: {e}")

def _save_to_client(client, rows_to_insert, logger=None, label="DB", mode="REPLACE"):
    """
    Stable sequential saver using bulk placeholders.
    """
    if not client or not rows_to_insert:
        return False

    verb = "REPLACE" if mode.upper() == "REPLACE" else "IGNORE"
    BATCH_SIZE = 100 # Stable batch size for HTTP
    total_rows = len(rows_to_insert)
    
    try:
        for i in range(0, total_rows, BATCH_SIZE):
            batch = rows_to_insert[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [item for sublist in batch for item in sublist]
            
            query = f"INSERT OR {verb} INTO market_data (timestamp, symbol, open, high, low, close, volume, session) VALUES {placeholders}"
            client.execute(query, flat_values)
            
            if logger and i % 5000 == 0:
                logger.log(f"      ➡️ {label}: Progress {min(i + BATCH_SIZE, total_rows)}/{total_rows}...")
                
        if logger: logger.log(f"   ✅ {label}: Successfully committed {total_rows} rows.")
        return True
    except Exception as e:
        if logger: logger.log(f"   ❌ {label} Save Error: {e}")
        return False

def save_data_to_storage(df: pd.DataFrame, logger=None, archive_client=None, mirror_client=None, mode="REPLACE") -> bool:
    """
    Saves market data to BOTH Turso Archive and Turso Mirror sequentially.
    """
    if df.empty:
        return False # Tests expect False on empty

    own_archive = False
    own_mirror = False
    
    try:
        if not archive_client:
            archive_client = get_archive_db_connection()
            own_archive = True
        if not mirror_client:
            mirror_client = get_mirror_db_connection()
            own_mirror = True

        if not archive_client or not mirror_client:
            if logger: logger.log("   ❌ Save failed: Could not establish dual connections.")
            return False

        # 1. Prepare Rows
        batch_df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(batch_df['timestamp']):
            batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], utc=True)

        batch_df['ts_str'] = batch_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Sanitize
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in batch_df.columns:
                batch_df[col] = batch_df[col].replace([np.inf, -np.inf], np.nan)
                batch_df[col] = batch_df[col].where(batch_df[col].notnull(), None)

        rows_to_insert = []
        for row in batch_df.itertuples(index=False):
            rows_to_insert.append((
                row.ts_str, row.symbol, 
                row.open, row.high, row.low, row.close, row.volume, 
                getattr(row, 'session', 'REG')
            ))

        if logger:
            logger.log(f"   💾 Dual Committing {len(rows_to_insert)} records (Mode: {mode})...")

        # 2. Sequential Commit (Stable)
        # Archive
        if logger: logger.log("   ➡️ Committing to ARCHIVE...")
        if not _save_to_client(archive_client, rows_to_insert, logger, "Archive", mode):
            return False
            
        # Mirror
        if logger: logger.log("   ➡️ Committing to MIRROR...")
        if not _save_to_client(mirror_client, rows_to_insert, logger, "Mirror", mode):
            # Rollback Archive if Mirror fails
            try:
                target_date_str = rows_to_insert[0][0].split(" ")[0]
                logger.log(f"   ⚠️ Mirror failed. Rolling back Archive for {target_date_str} to maintain parity...")
                archive_client.execute("DELETE FROM market_data WHERE timestamp LIKE ?", [f"{target_date_str}%"])
            except: pass
            return False
        
        return True

    except Exception as e:
        if logger: logger.log(f"   ❌ Storage Global Error: {e}")
        return False
    finally:
        if own_archive and archive_client:
            try: archive_client.close()
            except: pass
        if own_mirror and mirror_client:
            try: mirror_client.close()
            except: pass
