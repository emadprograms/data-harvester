"""
Database operations.
STRATEGY: 
1. Save everything as UTC Strings (Unique, Clean).
2. When viewing Health Matrix, convert back to US/Eastern to count 'Trading Days' correctly.
"""

import pandas as pd
import numpy as np
import time
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
            SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker
            FROM symbol_map
            ORDER BY display_name
        """)
        
        # Return a dictionary structured for the app
        inventory = {}
        for row in res.rows:
            inventory[row[0]] = {
                'yahoo_ticker': row[1],
                'massive_ticker': row[2],
                'binance_ticker': row[3]
            }
        return inventory
    except Exception:
        return {}
    finally:
        if own_client and client:
            client.close()

def upsert_symbol_mapping(display_name, y_ticker, m_ticker, b_ticker, client=None):
    """Adds or updates a symbol's rules in BOTH databases."""
    archive_client = client or get_archive_db_connection()
    mirror_client = get_mirror_db_connection()
    
    success = True
    for c in [archive_client, mirror_client]:
        if not c: continue
        try:
            c.execute(
                """INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker) 
                   VALUES (?, ?, ?, ?) 
                   ON CONFLICT(display_name) DO UPDATE SET 
                     yahoo_ticker=excluded.yahoo_ticker, 
                     massive_ticker=excluded.massive_ticker,
                     binance_ticker=excluded.binance_ticker""",
                [display_name, y_ticker, m_ticker, b_ticker]
            )
        except Exception as e:
            print(f"❌ Error saving symbol to DB: {e}")
            success = False
    
    if archive_client and not client: archive_client.close()
    if mirror_client: mirror_client.close()
    return success

def delete_symbol_mapping(ticker, client=None):
    """Deletes a symbol from BOTH databases."""
    archive_client = client or get_archive_db_connection()
    mirror_client = get_mirror_db_connection()
    
    success = True
    for c in [archive_client, mirror_client]:
        if not c: continue
        try:
            c.execute("DELETE FROM symbol_map WHERE display_name = ?", [ticker])
        except Exception as e:
            print(f"❌ Error deleting symbol from DB: {e}")
            success = False
            
    if archive_client and not client: archive_client.close()
    if mirror_client: mirror_client.close()
    return success

# --- MARKET DATA OPERATIONS ---

def _save_to_client(client, rows_to_insert, logger=None, label="DB"):
    """Generic helper to save a batch of rows to a specific database client."""
    BATCH_SIZE = 100
    try:
        for i in range(0, len(rows_to_insert), BATCH_SIZE):
            batch = rows_to_insert[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [item for sublist in batch for item in sublist]
            
            query = f"""
                INSERT OR REPLACE INTO market_data 
                (timestamp, symbol, open, high, low, close, volume, session) 
                VALUES {placeholders}
            """
            client.execute(query, flat_values)
            # Minimal delay for Turso reliability
            time.sleep(0.01)
        return True
    except Exception as e:
        err = f"{label} Save Error: {e}"
        if logger: logger.log(f"   ❌ {err}")
        else: print(f"❌ {err}")
        return False


def save_data_to_storage(df: pd.DataFrame, logger=None, archive_client=None, mirror_client=None):
    """
    Saves market data to BOTH Turso Archive and Turso Mirror.
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

        import math
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
            logger.log(f"   💾 Dual Comitting {len(rows_to_insert)} records to Turso Archive & Mirror...")

        # Explicit dual-write behavior
        archive_success = False
        if not archive_client:
            archive_client = get_archive_db_connection()
            own_archive = True
        if archive_client:
            archive_success = _save_to_client(archive_client, rows_to_insert, logger, "Archive")

        mirror_success = False
        if not mirror_client:
            mirror_client = get_mirror_db_connection()
            own_mirror = True
        if mirror_client:
            mirror_success = _save_to_client(mirror_client, rows_to_insert, logger, "Mirror")

        return archive_success and mirror_success

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

def fetch_data_health_matrix(tickers: list, start_date, end_date, session_filter="Total"):
    """
    Fetches data, CONVERTS TO US/EASTERN, and then groups by day.
    """
    client = get_archive_db_connection()
    if not client:
        return pd.DataFrame()

    start_str = f"{start_date} 00:00:00" 
    end_dt_buffer = end_date + pd.Timedelta(days=1)
    end_str = f"{end_dt_buffer} 23:59:59"

    placeholders = ",".join("?" * len(tickers))
    query = f"""
        SELECT timestamp, symbol, session
        FROM market_data 
        WHERE symbol IN ({placeholders}) 
          AND timestamp >= ? 
          AND timestamp <= ?
    """
    params = tickers + [start_str, end_str]
    
    try:
        res = client.execute(query, params)
        if not res.rows:
            return pd.DataFrame()
            
        df = pd.DataFrame([list(row) for row in res.rows], columns=['timestamp', 'symbol', 'session'])
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(UTC)
        df['timestamp_et'] = df['timestamp'].dt.tz_convert(US_EASTERN)
        df['day'] = df['timestamp_et'].dt.date
        
        if session_filter != "Total":
            df = df[df['session'] == session_filter]
            
        df = df[(df['day'] >= start_date) & (df['day'] <= end_date)]
        
        if df.empty:
            return pd.DataFrame()

        grouped = df.groupby(['symbol', 'day']).size().reset_index(name='candle_count')
        pivot_df = grouped.pivot(index='symbol', columns='day', values='candle_count')
        
        return pivot_df

    except Exception as e:
        print(f"❌ Error fetching health matrix: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()
