"""
Database operations.
STRATEGY: 
1. Save everything as UTC Strings (Unique, Clean).
2. When viewing Health Matrix, convert back to US/Eastern to count 'Trading Days' correctly.
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
        # Try mirror as fallback for inventory
        client = get_mirror_db_connection()
        if not client:
            return {}
        own_client = True
        
    try:
        # Fetch from table (Strictly Tickers)
        res = client.execute("""
            SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker
            FROM symbol_map
            ORDER BY display_name
        """)
        
        # Return a dictionary structured for the app
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

def clear_market_data_for_dates(client, dates: list, logger=None, label="DB"):
    """
    Deletes all records from market_data for the specified dates.
    """
    if not dates:
        return
    
    try:
        for d in dates:
            d_str = str(d)
            client.execute("DELETE FROM market_data WHERE timestamp LIKE ?", [f"{d_str}%"])
        if logger:
            date_str = ", ".join(map(str, dates))
            logger.log(f"   ✅ {label}: Cleaned existing records for: {date_str}")
    except Exception as e:
        if logger: logger.log(f"   ⚠️ {label} Clean-up warning: {e}")

def clear_market_data_for_range(client, start_utc: datetime, end_utc: datetime, logger=None, label="DB"):
    """
    Surgically deletes records within a specific UTC range.
    """
    try:
        start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S')
        client.execute(
            "DELETE FROM market_data WHERE timestamp >= ? AND timestamp < ?",
            [start_str, end_str]
        )
        if logger:
            logger.log(f"   ✅ {label}: Cleaned existing records for range: {start_str} to {end_str}")
    except Exception as e:
        if logger: logger.log(f"   ⚠️ {label} Range Clean-up warning: {e}")

def _save_to_client(client, rows_to_insert, logger=None, label="DB", mode="REPLACE"):
    """Generic helper to save a batch of rows to a specific database client."""
    BATCH_SIZE = 100
    total_rows = len(rows_to_insert)
    try:
        for i in range(0, total_rows, BATCH_SIZE):
            batch = rows_to_insert[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [item for sublist in batch for item in sublist]
            
            query = f"""
                INSERT OR {mode} INTO market_data 
                (timestamp, symbol, open, high, low, close, volume, session) 
                VALUES {placeholders}
            """
            client.execute(query, flat_values)
            # Minimal delay for Turso reliability
            time.sleep(0.01)
            
            if logger and i % 500 == 0:
                logger.log(f"      ➡️ {label}: Progress {min(i + BATCH_SIZE, total_rows)}/{total_rows}...")
                
        if logger: logger.log(f"   ✅ {label}: Successfully committed {total_rows} rows.")
        return True
    except Exception as e:
        err = f"{label} Save Error: {e}"
        if logger: logger.log(f"   ❌ {err}")
        else: print(f"❌ {err}")
        return False


def save_data_to_storage(df: pd.DataFrame, logger=None, archive_client=None, mirror_client=None, mode="REPLACE"):
    """
    Saves market data to BOTH Turso Archive and Turso Mirror.
    Implements a compensating rollback if the mirror write fails after archive success
    to ensure 1-on-1 parity.
    """
    if df.empty:
        return False

    own_archive = False
    own_mirror = False

    try:
        # 1. Prepare Batch
        batch_df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(batch_df['timestamp']):
            batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], utc=True)

        if batch_df['timestamp'].dt.tz is not None:
            batch_df['timestamp'] = batch_df['timestamp'].dt.tz_convert(UTC)
        else:
            batch_df['timestamp'] = batch_df['timestamp'].dt.tz_localize(UTC)

        batch_df['timestamp_str'] = batch_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in batch_df.columns:
                batch_df[col] = batch_df[col].replace([np.inf, -np.inf], np.nan)
                batch_df[col] = batch_df[col].where(batch_df[col].notnull(), None)

        rows_to_insert = []
        for row in batch_df.itertuples(index=False):
            sanitized_row = []
            for item in [
                row.timestamp_str, row.symbol, 
                row.open, row.high, row.low, row.close, row.volume, 
                getattr(row, 'session', 'REG')
            ]:
                if isinstance(item, float) and not math.isfinite(item):
                    sanitized_row.append(None)
                else:
                    sanitized_row.append(item)
            
            rows_to_insert.append(tuple(sanitized_row))

        if logger:
            logger.log(f"   💾 Dual Comitting {len(rows_to_insert)} records to Turso Archive & Mirror (Mode: {mode})...")

        # --- Dual-Write with Compensating Rollback ---
        
        # A. Try Archive First
        archive_success = False
        if not archive_client:
            archive_client = get_archive_db_connection()
            own_archive = True
        
        if archive_client:
            if logger: logger.log("   ➡️ Committing to ARCHIVE...")
            archive_success = _save_to_client(archive_client, rows_to_insert, logger, "Archive", mode=mode)

        if not archive_success:
            if logger: logger.log("   ❌ Archive write failed. Aborting dual-write to prevent desync.")
            return False

        # B. Try Mirror Second
        mirror_success = False
        if not mirror_client:
            mirror_client = get_mirror_db_connection()
            own_mirror = True
        
        if mirror_client:
            if logger: logger.log("   ➡️ Committing to MIRROR...")
            mirror_success = _save_to_client(mirror_client, rows_to_insert, logger, "Mirror", mode=mode)

        # C. Handle Desync (Mirror failed after Archive success)
        if not mirror_success:
            if logger: logger.log("   🚨 CRITICAL: Mirror write failed after Archive success. Performing compensating ROLLBACK on Archive to maintain parity...")
            try:
                # Target the specific symbols and the date range involved in this batch
                symbols = df['symbol'].unique().tolist()
                placeholders = ",".join("?" * len(symbols))
                # Identify the date from the first row of data
                target_date_str = rows_to_insert[0][0].split(" ")[0]
                
                archive_client.execute(
                    f"DELETE FROM market_data WHERE symbol IN ({placeholders}) AND timestamp LIKE ?",
                    symbols + [f"{target_date_str}%"]
                )
                if logger: logger.log(f"   ✅ Archive Rollback successful. Parity maintained (Both databases missing {target_date_str} data).")
            except Exception as e:
                if logger: logger.log(f"   ❌ FATAL: Archive rollback FAILED: {e}. Databases are now DESYNCED.")
            return False

        return True

    except Exception as e:
        err = f"Storage Global Error: {e}"
        if logger: logger.log(f"   ❌ {err}")
        else: print(f"❌ {err}")
        return False
    finally:
        if own_archive and archive_client:
            archive_client.close()
        if own_mirror and mirror_client:
            mirror_client.close()
