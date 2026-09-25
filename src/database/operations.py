"""
Database operations — High-performance DuckDB local storage.
Features Source-Tiering Protection, fast range cleaning, and instant time-series resampling.
"""
import pandas as pd
import numpy as np
import time
import math
from datetime import datetime
from src.database.connection import get_duckdb_connection, DuckDBClient, get_archive_db_connection
from src.config import UTC, US_EASTERN

# --- Symbol Inventory Operations ---

def get_symbol_map_from_db(client=None):
    """Fetches the complete symbol inventory from the symbol_map table."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

    if not client:
        return {}

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

# --- Market Data Operations ---

def clear_market_data_for_range(client, start_utc: datetime, end_utc: datetime, logger=None, label="DuckDB", symbols=None):
    """
    Deletes records within a specific UTC range.
    If symbols is provided, only deletes for those specific symbols.
    """
    try:
        start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_utc, datetime) else str(start_utc)
        end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_utc, datetime) else str(end_utc)

        if symbols:
            placeholders = ",".join(["?"] * len(symbols))
            query = f"DELETE FROM market_data WHERE timestamp::TIMESTAMP >= ?::TIMESTAMP AND timestamp::TIMESTAMP < ?::TIMESTAMP AND symbol IN ({placeholders})"
            params = [start_str, end_str] + list(symbols)
            client.execute(query, params)
            if logger:
                logger.log(f"   ✅ {label}: Cleaned {len(symbols)} symbols for range: {start_str} to {end_str}")
        else:
            client.execute(
                "DELETE FROM market_data WHERE timestamp::TIMESTAMP >= ?::TIMESTAMP AND timestamp::TIMESTAMP < ?::TIMESTAMP",
                [start_str, end_str]
            )
            if logger:
                logger.log(f"   ✅ {label}: Cleaned ALL records for range: {start_str} to {end_str}")
    except Exception as e:
        if logger:
            logger.log(f"   ⚠️ {label} Range Clean-up warning: {e}")

def _save_to_client(client, rows_to_insert, logger=None, label="DuckDB"):
    """
    Saves data with Source-Tiering Protection into DuckDB.
    Tier 1 (MASSIVE, BINANCE) will NOT be overwritten by Tier 2 (YAHOO, CAPITAL).
    """
    if not client or not rows_to_insert:
        return False

    BATCH_SIZE = 1000
    total_rows = len(rows_to_insert)

    try:
        for i in range(0, total_rows, BATCH_SIZE):
            batch = rows_to_insert[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [item for sublist in batch for item in sublist]

            query = f"""
                INSERT INTO market_data 
                (timestamp, symbol, open, high, low, close, volume, session, source) 
                VALUES {placeholders}
                ON CONFLICT(symbol, timestamp) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    session=excluded.session,
                    source=excluded.source
                WHERE 
                    (market_data.source NOT IN ('MASSIVE', 'BINANCE')) OR 
                    (excluded.source IN ('MASSIVE', 'BINANCE'))
            """
            client.execute(query, flat_values)

            if logger and i % 10000 == 0 and total_rows > 10000:
                logger.log(f"      ➡️ {label}: Progress {min(i + BATCH_SIZE, total_rows)}/{total_rows}...")

        if logger:
            logger.log(f"   ✅ {label}: Successfully committed {total_rows} rows.")
        return True
    except Exception as e:
        if logger:
            logger.log(f"   ❌ {label} Save Error: {e}")
        return False

def save_data_to_storage(df: pd.DataFrame, logger=None, archive_client=None) -> bool:
    """
    Saves market data DataFrame directly to DuckDB with Source-Tiering.
    """
    if df.empty:
        return False

    own_client = False
    client = archive_client

    try:
        if not client:
            client = get_archive_db_connection()
            own_client = True

        if not client:
            if logger: logger.log("❌ Could not connect to DuckDB storage.")
            return False

        # Clean NaNs and infinite values
        batch_df = df.copy()
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in batch_df.columns:
                batch_df[col] = batch_df[col].replace([np.inf, -np.inf], np.nan)
                batch_df[col] = batch_df[col].where(batch_df[col].notnull(), None)

        if 'timestamp' in batch_df.columns and pd.api.types.is_datetime64_any_dtype(batch_df['timestamp']):
            batch_df['ts_str'] = batch_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            batch_df['ts_str'] = batch_df['timestamp'].astype(str)

        rows_to_insert = []
        for row in batch_df.itertuples(index=False):
            source_val = getattr(row, 'source', 'UNKNOWN').upper()
            rows_to_insert.append((
                row.ts_str,
                row.symbol,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                getattr(row, 'session', 'REG'),
                source_val
            ))

        if logger:
            logger.log(f"   💾 Committing {len(rows_to_insert)} records to DuckDB...")

        return _save_to_client(client, rows_to_insert, logger, "DuckDB")

    except Exception as e:
        if logger: logger.log(f"   ❌ Storage Global Error: {e}")
        return False
    finally:
        if own_client and client:
            try: client.close()
            except Exception: pass

def get_session_row_counts(client, symbols, start_utc: datetime, end_utc: datetime):
    """Returns a dictionary of symbol -> row count for the specified session range in DuckDB."""
    if not client or not symbols:
        return {}

    start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_utc, datetime) else str(start_utc)
    end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_utc, datetime) else str(end_utc)
    placeholders = ",".join(["?"] * len(symbols))

    query = f"""
        SELECT symbol, COUNT(*) 
        FROM market_data 
        WHERE timestamp::TIMESTAMP >= ?::TIMESTAMP AND timestamp::TIMESTAMP < ?::TIMESTAMP AND symbol IN ({placeholders})
        GROUP BY symbol
    """
    params = [start_str, end_str] + list(symbols)

    try:
        res = client.execute(query, params)
        return {row[0]: row[1] for row in res.rows}
    except Exception:
        return {}

def query_candlesticks(symbol: str, start_time=None, end_time=None, timeframe="1m", client=None):
    """
    Queries OHLCV candlesticks resampled dynamically using DuckDB's native time_bucket().
    Supported timeframes: '1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'.
    Returns a pandas DataFrame.
    """
    own_client = False
    if not client:
        client = get_duckdb_connection(read_only=True)
        own_client = True

    if not client:
        return pd.DataFrame()

    try:
        interval_map = {
            "1m": "1 minute",
            "3m": "3 minutes",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "30m": "30 minutes",
            "1h": "1 hour",
            "4h": "4 hours",
            "1d": "1 day",
        }
        interval_str = interval_map.get(timeframe.lower(), "1 minute")

        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start_time:
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime) else str(start_time)
            where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
            params.append(start_str)

        if end_time:
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime) else str(end_time)
            where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
            params.append(end_str)

        where_stmt = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP) AS time,
                symbol,
                first(open ORDER BY timestamp) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close ORDER BY timestamp) AS close,
                sum(volume) AS volume
            FROM market_data
            WHERE {where_stmt}
            GROUP BY time, symbol
            ORDER BY time ASC
        """
        res = client.execute(query, params)
        return res.df()
    finally:
        if own_client and client:
            client.close()


def save_ticks_to_storage(client, ticks, logger=None, label="DuckDB-Ticks") -> bool:
    """
    Saves a batch of raw tick records into streaming.db ticks table.
    Each tick is a tuple: (timestamp, symbol, price, volume, bid, ask, source, session)
    """
    if not client or not ticks:
        return False

    BATCH_SIZE = 1000
    total_ticks = len(ticks)

    try:
        for i in range(0, total_ticks, BATCH_SIZE):
            batch = ticks[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [item for sublist in batch for item in sublist]

            query = f"""
                INSERT INTO ticks 
                (timestamp, symbol, price, volume, bid, ask, source, session) 
                VALUES {placeholders}
            """
            client.execute(query, flat_values)

        if logger:
            logger.log(f"   ✅ {label}: Successfully committed {total_ticks} ticks.")
        return True
    except Exception as e:
        if logger:
            logger.log(f"   ❌ {label} Save Error: {e}")
        return False


def query_ticks(symbol: str, start_time=None, end_time=None, limit=1000, client=None):
    """
    Queries raw tick-by-tick records from streaming.db for a given symbol.
    Returns a pandas DataFrame.
    """
    own_client = False
    if not client:
        from src.database.connection import get_streaming_db_connection
        client = get_streaming_db_connection(read_only=True)
        own_client = True

    if not client:
        return pd.DataFrame()

    try:
        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start_time:
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(start_time, datetime) else str(start_time)
            where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
            params.append(start_str)

        if end_time:
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(end_time, datetime) else str(end_time)
            where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
            params.append(end_str)

        where_stmt = " AND ".join(where_clauses)
        limit_clause = f"LIMIT {int(limit)}" if limit else ""

        query = f"""
            SELECT timestamp, symbol, price, volume, bid, ask, source, session
            FROM ticks
            WHERE {where_stmt}
            ORDER BY timestamp ASC
            {limit_clause}
        """
        res = client.execute(query, params)
        return res.df()
    finally:
        if own_client and client:
            client.close()


def query_candlesticks_from_ticks(symbol: str, timeframe="1m", start_time=None, end_time=None, client=None):
    """
    Dynamically resamples raw tick-by-tick data from streaming.db into OHLCV candlesticks using time_bucket().
    Supported timeframes: '1s', '5s', '15s', '1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'.
    Returns a pandas DataFrame.
    """
    own_client = False
    if not client:
        from src.database.connection import get_streaming_db_connection
        client = get_streaming_db_connection(read_only=True)
        own_client = True

    if not client:
        return pd.DataFrame()

    try:
        interval_map = {
            "1s": "1 second",
            "5s": "5 seconds",
            "15s": "15 seconds",
            "1m": "1 minute",
            "3m": "3 minutes",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "30m": "30 minutes",
            "1h": "1 hour",
            "4h": "4 hours",
            "1d": "1 day",
        }
        interval_str = interval_map.get(timeframe.lower(), "1 minute")

        where_clauses = ["symbol = ?"]
        params = [symbol]

        if start_time:
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(start_time, datetime) else str(start_time)
            where_clauses.append("timestamp::TIMESTAMP >= ?::TIMESTAMP")
            params.append(start_str)

        if end_time:
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(end_time, datetime) else str(end_time)
            where_clauses.append("timestamp::TIMESTAMP <= ?::TIMESTAMP")
            params.append(end_str)

        where_stmt = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                time_bucket(INTERVAL '{interval_str}', timestamp::TIMESTAMP) AS time,
                symbol,
                first(price ORDER BY timestamp) AS open,
                max(price) AS high,
                min(price) AS low,
                last(price ORDER BY timestamp) AS close,
                sum(coalesce(volume, 1.0)) AS volume
            FROM ticks
            WHERE {where_stmt}
            GROUP BY time, symbol
            ORDER BY time ASC
        """
        res = client.execute(query, params)
        return res.df()
    finally:
        if own_client and client:
            client.close()

