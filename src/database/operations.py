"""
Database operations.
STRATEGY: 
1. Save everything as UTC Strings (Unique, Clean).
2. When viewing Health Matrix, convert back to US/Eastern to count 'Trading Days' correctly.
"""

import pandas as pd
import time
from src.database.connection import get_db_connection, get_local_db_connection
from src.config import UTC, US_EASTERN

# --- Basic CRUD for Symbol Mapping ---

def get_symbol_map_from_db(client=None):
    """Fetches the complete symbol inventory from the new table."""
    own_client = False
    if not client:
        client = get_db_connection()
        own_client = True
        
    if not client:
        return {}
    try:
        # Fetch from new table
        res = client.execute("""
            SELECT display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2, priority_3 
            FROM market_symbols 
            ORDER BY display_name
        """)
        
        # Return a dictionary structured for the app
        inventory = {}
        for row in res.rows:
            inventory[row[0]] = {
                'yahoo_ticker': row[1],
                'capital_epic': row[2],
                'binance_ticker': row[3],
                'p1': row[4],
                'p2': row[5],
                'p3': row[6]
            }
        return inventory
    except Exception:
        return {}
    finally:
        if own_client and client:
            client.close()

def upsert_symbol_mapping(display_name, y_ticker, m_ticker, b_ticker, p1, p2, p3=None, client=None):
    """Adds or updates a symbol's rules."""
    own_client = False
    if not client:
        client = get_db_connection()
        own_client = True
        
    if not client:
        return False
    try:
        # Check if column exists, if not, migration handles it, but safe insert:
        client.execute(
            """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2, priority_3) 
               VALUES (?, ?, ?, ?, ?, ?, ?) 
               ON CONFLICT(display_name) DO UPDATE SET 
                 yahoo_ticker=excluded.yahoo_ticker, 
                 capital_epic=excluded.capital_epic,
                 binance_ticker=excluded.binance_ticker,
                 priority_1=excluded.priority_1,
                 priority_2=excluded.priority_2,
                 priority_3=excluded.priority_3""",
            [display_name, y_ticker, m_ticker, b_ticker, p1, p2, p3]
        )
        return True
    except Exception as e:
        print(f"❌ Error saving symbol: {e}")
        return False
    finally:
        if own_client and client:
            client.close()

def delete_symbol_mapping(ticker, client=None):
    """Deletes a symbol."""
    own_client = False
    if not client:
        client = get_db_connection()
        own_client = True
        
    if not client:
        return False
    try:
        client.execute("DELETE FROM market_symbols WHERE display_name = ?", [ticker])
        return True
    except Exception as e:
        print(f"❌ Error deleting symbol: {e}")
        return False
    finally:
        if own_client and client:
            client.close()

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
            time.sleep(0.02) # Faster for batch inserts
        return True
    except Exception as e:
        err = f"{label} Save Error: {e}"
        if logger: logger.log(f"   ❌ {err}")
        else: print(f"❌ {err}")
        return False


def save_data_to_storage(df: pd.DataFrame, logger=None, turso_client=None, local_client=None):
    """
    Saves market data to BOTH Turso (remote) and Local SQLite.
    CRITICAL: Normalizes timestamps to UTC strings to ensure uniqueness.
    """
    if df.empty:
        return False

    own_turso = False
    own_local = False

    try:
        # 1. Copy and Normalize Timestamp
        batch_df = df.copy()
        
        # FIX: Added utc=True to handle timezone-aware inputs gracefully
        if not pd.api.types.is_datetime64_any_dtype(batch_df['timestamp']):
            batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'], utc=True)

        # 2. FORCE UTC CONVERSION (Double safety)
        if batch_df['timestamp'].dt.tz is not None:
            batch_df['timestamp'] = batch_df['timestamp'].dt.tz_convert(UTC)
        else:
            batch_df['timestamp'] = batch_df['timestamp'].dt.tz_localize(UTC)

        # 3. Create String for SQLite (Removes Offset confusion)
        batch_df['timestamp_str'] = batch_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 4. Prepare Batch
        rows_to_insert = []
        for row in batch_df.itertuples(index=False):
            rows_to_insert.append((
                row.timestamp_str,
                row.symbol,
                row.open, row.high, row.low, row.close, row.volume,
                row.session
            ))

        if logger:
            logger.log(f"   💾 Dual Comitting {len(rows_to_insert)} records...")

        # 5. Save to Turso
        turso_success = False
        if not turso_client:
            turso_client = get_db_connection()
            own_turso = True
        if turso_client:
            turso_success = _save_to_client(turso_client, rows_to_insert, logger, "Turso")
        
        # 6. Save to Local
        local_success = False
        if not local_client:
            local_client = get_local_db_connection()
            own_local = True
        if local_client:
            local_success = _save_to_client(local_client, rows_to_insert, logger, "Local")

        return turso_success or local_success # Success if at least one worked

    except Exception as e:
        err = f"Storage Global Error: {e}"
        if logger: logger.log(f"   ❌ {err}")
        else: print(f"❌ {err}")
        return False
    finally:
        if own_turso and turso_client:
            turso_client.close()
        if own_local and local_client:
            local_client.close()

# Keep old name for backward compatibility if needed, but redirects to new dual storage
def save_data_to_turso(df: pd.DataFrame, logger=None):
    return save_data_to_storage(df, logger)


def fetch_data_health_matrix(tickers: list, start_date, end_date, session_filter="Total"):
    """
    Fetches data, CONVERTS TO US/EASTERN, and then groups by day.
    This solves the issue where post-market data (8 PM ET) looks like tomorrow in UTC.
    """
    client = get_db_connection()
    if not client:
        return pd.DataFrame()

    # Fetch slightly wider range to account for TZ shifts
    # We fetch the Raw UTC data first
    start_str = f"{start_date} 00:00:00" 
    # End date + 1 day to catch the UTC spillover
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
            
        # Convert to Pandas
        df = pd.DataFrame([list(row) for row in res.rows], columns=['timestamp', 'symbol', 'session'])
        
        # 1. Parse UTC String
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(UTC)
        
        # 2. Convert to US Eastern (The "Trading View")
        df['timestamp_et'] = df['timestamp'].dt.tz_convert(US_EASTERN)
        
        # 3. Extract the Date from the EASTERN time
        # This ensures 8 PM ET stays on "Today"
        df['day'] = df['timestamp_et'].dt.date
        
        # 4. Apply Session Filter
        if session_filter != "Total":
            df = df[df['session'] == session_filter]
            
        # 5. Filter strictly for requested date range (based on ET date)
        df = df[(df['day'] >= start_date) & (df['day'] <= end_date)]
        
        if df.empty:
            return pd.DataFrame()

        # 6. Group and Pivot
        grouped = df.groupby(['symbol', 'day']).size().reset_index(name='candle_count')
        pivot_df = grouped.pivot(index='symbol', columns='day', values='candle_count')
        
        return pivot_df

    except Exception as e:
        print(f"❌ Error fetching health matrix: {e}")
        return pd.DataFrame()