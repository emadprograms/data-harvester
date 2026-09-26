"""
Database operations — High-performance DuckDB local storage.
Features Dedicated Dual-DuckDB Architecture:
- Historical DuckDB: Source-tiering 1m candle storage, range cleaning, and time_bucket resampling.
- Streaming DuckDB: High-frequency raw tick ingestion, tick querying, and dynamic tick-to-bar aggregation.
"""
import pandas as pd
import numpy as np
import time
import math
from datetime import datetime
from src.database.connection import (
    get_duckdb_connection,
    DuckDBClient,
    get_archive_db_connection,
    get_historical_db_connection,
    get_streaming_db_connection,
)
from src.config import UTC, US_EASTERN


# --- Symbol Inventory Operations (Historical & Streaming) ---

def get_historical_database_symbols_from_db(client=None) -> dict:
    """Fetches the complete symbol inventory from historical_database_symbols in historical.duckdb."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

    if not client:
        return {}

    try:
        table_name = "historical_database_symbols"
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "historical_database_symbols" not in tables:
            if "historical_symbol_map" in tables:
                table_name = "historical_symbol_map"
            elif "symbol_map" in tables:
                table_name = "symbol_map"

        res = client.execute(f"""
            SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker
            FROM {table_name}
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


def get_historical_symbol_map_from_db(client=None) -> dict:
    """Backward-compatible alias for get_historical_database_symbols_from_db."""
    return get_historical_database_symbols_from_db(client=client)


def get_symbol_map_from_db(client=None) -> dict:
    """Backward-compatible alias for get_historical_database_symbols_from_db."""
    return get_historical_database_symbols_from_db(client=client)


def get_symbol_inventory_list(client=None) -> list[dict]:
    """Fetches historical symbol inventory as a list of dictionaries for APIs and dashboards."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

    if not client:
        return []

    try:
        table_name = "historical_database_symbols"
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "historical_database_symbols" not in tables:
            if "historical_symbol_map" in tables:
                table_name = "historical_symbol_map"
            elif "symbol_map" in tables:
                table_name = "symbol_map"

        res = client.execute(f"""
            SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker
            FROM {table_name}
            ORDER BY display_name
        """)
        return [
            {
                "display_name": row[0],
                "yahoo_ticker": row[1],
                "massive_ticker": row[2],
                "binance_ticker": row[3],
                "capital_ticker": row[4],
            }
            for row in res.rows
        ]
    except Exception:
        return []
    finally:
        if own_client and client:
            client.close()


def add_symbol_to_db(display_name: str, yahoo_ticker=None, massive_ticker=None, binance_ticker=None, capital_ticker=None, client=None) -> bool:
    """Adds or updates a symbol in historical_database_symbols."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

    if not client:
        return False

    try:
        table_name = "historical_database_symbols"
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "historical_database_symbols" not in tables:
            if "historical_symbol_map" in tables:
                table_name = "historical_symbol_map"
            elif "symbol_map" in tables:
                table_name = "symbol_map"

        client.execute(f"DELETE FROM {table_name} WHERE display_name = ?", [display_name])
        client.execute(f"""
            INSERT INTO {table_name} (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker)
            VALUES (?, ?, ?, ?, ?)
        """, [display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker])
        return True
    except Exception as e:
        print(f"❌ Error adding historical symbol {display_name}: {e}")
        return False
    finally:
        if own_client and client:
            client.close()


def remove_symbol_from_db(display_name: str, client=None) -> bool:
    """Deletes a symbol from historical_database_symbols."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

    if not client:
        return False

    try:
        table_name = "historical_database_symbols"
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "historical_database_symbols" not in tables:
            if "historical_symbol_map" in tables:
                table_name = "historical_symbol_map"
            elif "symbol_map" in tables:
                table_name = "symbol_map"

        client.execute(f"DELETE FROM {table_name} WHERE display_name = ?", [display_name])
        return True
    except Exception as e:
        print(f"❌ Error removing historical symbol {display_name}: {e}")
        return False
    finally:
        if own_client and client:
            client.close()


# --- Streaming Symbol Inventory Operations ---

def get_streaming_database_symbols_from_db(client=None) -> dict:
    """Fetches the dedicated streaming symbol inventory from streaming_database_symbols in streaming.duckdb."""
    own_client = False
    if not client:
        client = get_streaming_db_connection(read_only=True)
        own_client = True

    if not client:
        return {}

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "streaming_database_symbols"
        if "streaming_database_symbols" not in tables:
            if "streaming_symbol_map" in tables:
                table_name = "streaming_symbol_map"
            else:
                return {}

        res = client.execute(f"""
            SELECT display_name, capital_ticker, databento_ticker, binance_ticker, is_active
            FROM {table_name}
            ORDER BY display_name
        """)
        inventory = {}
        for row in res.rows:
            inventory[row[0]] = {
                'capital_ticker': row[1],
                'databento_ticker': row[2],
                'binance_ticker': row[3],
                'is_active': bool(row[4])
            }
        return inventory
    except Exception:
        return {}
    finally:
        if own_client and client:
            client.close()


def get_streaming_symbol_map_from_db(client=None) -> dict:
    """Backward-compatible alias for get_streaming_database_symbols_from_db."""
    return get_streaming_database_symbols_from_db(client=client)


def get_streaming_symbol_inventory_list(client=None) -> list[dict]:
    """Fetches streaming symbol inventory as a list of dictionaries."""
    own_client = False
    if not client:
        client = get_streaming_db_connection(read_only=True)
        own_client = True

    if not client:
        return []

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "streaming_database_symbols"
        if "streaming_database_symbols" not in tables:
            if "streaming_symbol_map" in tables:
                table_name = "streaming_symbol_map"
            else:
                return []

        res = client.execute(f"""
            SELECT display_name, capital_ticker, databento_ticker, binance_ticker, is_active
            FROM {table_name}
            ORDER BY display_name
        """)
        return [
            {
                "display_name": row[0],
                "capital_ticker": row[1],
                "databento_ticker": row[2],
                "binance_ticker": row[3],
                "is_active": bool(row[4])
            }
            for row in res.rows
        ]
    except Exception:
        return []
    finally:
        if own_client and client:
            client.close()


def add_streaming_symbol_to_db(display_name: str, capital_ticker=None, databento_ticker=None, binance_ticker=None, is_active=True, client=None) -> bool:
    """Adds or updates a symbol in streaming_database_symbols in streaming.duckdb."""
    own_client = False
    if not client:
        client = get_streaming_db_connection(read_only=False)
        own_client = True

    if not client:
        return False

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "streaming_database_symbols"
        if "streaming_database_symbols" not in tables and "streaming_symbol_map" in tables:
            table_name = "streaming_symbol_map"

        client.execute(f"DELETE FROM {table_name} WHERE display_name = ?", [display_name])
        client.execute(f"""
            INSERT INTO {table_name} (display_name, capital_ticker, databento_ticker, binance_ticker, is_active)
            VALUES (?, ?, ?, ?, ?)
        """, [display_name, capital_ticker, databento_ticker, binance_ticker, is_active])
        return True
    except Exception as e:
        print(f"❌ Error adding streaming symbol {display_name}: {e}")
        return False
    finally:
        if own_client and client:
            client.close()


def remove_streaming_symbol_from_db(display_name: str, client=None) -> bool:
    """Deletes a symbol from streaming_database_symbols in streaming.duckdb."""
    own_client = False
    if not client:
        client = get_streaming_db_connection(read_only=False)
        own_client = True

    if not client:
        return False

    try:
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        table_name = "streaming_database_symbols"
        if "streaming_database_symbols" not in tables and "streaming_symbol_map" in tables:
            table_name = "streaming_symbol_map"

        client.execute(f"DELETE FROM {table_name} WHERE display_name = ?", [display_name])
        return True
    except Exception as e:
        print(f"❌ Error removing streaming symbol {display_name}: {e}")
        return False
    finally:
        if own_client and client:
            client.close()


# --- Historical Market Data Operations ---

def clear_market_data_for_range(client, start_utc: datetime, end_utc: datetime, logger=None, label="DuckDB", symbols=None):
    """
    Deletes records within a specific UTC range from market_data.
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
    Saves historical 1m data with Source-Tiering Protection into DuckDB.
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
    Saves market data DataFrame directly to historical DuckDB with Source-Tiering.
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

        return _save_to_client(client, rows_to_insert, logger, "DuckDB-Historical")

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
    Queries OHLCV candlesticks from historical.duckdb resampled dynamically using DuckDB's native time_bucket().
    Supported timeframes: '1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'.
    Returns a pandas DataFrame.

    NOTE: buckets are aligned to raw UTC storage time. The dashboard's Historical Database page must
    present bars on the NYSE clock, so it uses src.dashboard.analytics.get_historical_candles()
    (exchange-local bucketing + America/New_York labels) instead of this helper.
    """
    own_client = False
    if not client:
        client = get_historical_db_connection(read_only=True)
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


# --- Streaming Raw Tick Operations (Dedicated streaming.duckdb) ---

def save_ticks_to_storage(client_or_ticks, ticks=None, logger=None, label="DuckDB-Ticks", **kwargs) -> bool:
    """
    Saves a batch of raw tick records into streaming.duckdb ticks table.
    Supports two calling signatures:
      1. save_ticks_to_storage(client, ticks, logger=..., label=...)
      2. save_ticks_to_storage(ticks, client=..., logger=...)
    """
    if ticks is not None and not isinstance(client_or_ticks, (list, tuple)):
        actual_client = client_or_ticks
        actual_ticks = ticks
    elif isinstance(client_or_ticks, (list, tuple)):
        actual_ticks = client_or_ticks
        actual_client = kwargs.get("client")
    else:
        actual_client = client_or_ticks
        actual_ticks = ticks or []

    if not actual_ticks:
        return True

    own_client = False
    if not actual_client:
        actual_client = get_streaming_db_connection()
        own_client = True

    if not actual_client:
        if logger:
            msg = "Could not connect to streaming DuckDB."
            logger.error(msg) if hasattr(logger, "error") else logger.log(msg)
        return False

    BATCH_SIZE = 1000
    total_ticks = len(actual_ticks)

    # Normalize rows
    normalized_rows = []
    for item in actual_ticks:
        if isinstance(item, dict):
            ts = item.get("timestamp")
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            sym = str(item.get("symbol") or item.get("epic"))
            price = float(item["price"])
            vol = float(item.get("volume", 1.0)) if item.get("volume") is not None else 1.0
            bid = float(item["bid"]) if item.get("bid") is not None else None
            ask = float(item["ask"]) if item.get("ask") is not None else None
            src = str(item.get("source", "CAPITAL")).upper()
            sess = str(item.get("session", "REG"))
        elif len(item) == 8:
            # (timestamp, symbol, price, volume, bid, ask, source, session)
            ts, sym, price, vol, bid, ask, src, sess = item
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            price = float(price)
            vol = float(vol) if vol is not None else 1.0
            bid = float(bid) if bid is not None else None
            ask = float(ask) if ask is not None else None
            src = str(src).upper() if src else "CAPITAL"
            sess = str(sess) if sess else "REG"
        elif len(item) == 7:
            # (timestamp, symbol, bid, ask, price, volume, source)
            ts, sym, bid, ask, price, vol, src = item
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            price = float(price)
            vol = float(vol) if vol is not None else 1.0
            bid = float(bid) if bid is not None else None
            ask = float(ask) if ask is not None else None
            src = str(src).upper() if src else "CAPITAL"
            sess = "REG"
        else:
            # fallback
            ts_str = str(item[0])
            sym = str(item[1])
            price = float(item[2])
            vol = float(item[3]) if len(item) > 3 and item[3] is not None else 1.0
            bid = float(item[4]) if len(item) > 4 and item[4] is not None else None
            ask = float(item[5]) if len(item) > 5 and item[5] is not None else None
            src = str(item[6]).upper() if len(item) > 6 and item[6] else "CAPITAL"
            sess = "REG"

        normalized_rows.append((ts_str, sym, price, vol, bid, ask, src, sess))

    try:
        for i in range(0, total_ticks, BATCH_SIZE):
            batch = normalized_rows[i : i + BATCH_SIZE]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
            flat_values = [val for row in batch for val in row]

            query = f"""
                INSERT INTO ticks 
                (timestamp, symbol, price, volume, bid, ask, source, session) 
                VALUES {placeholders}
            """
            actual_client.execute(query, flat_values)

        if logger:
            msg = f"   ✅ {label}: Successfully committed {total_ticks} ticks."
            logger.info(msg) if hasattr(logger, "info") else logger.log(msg)
        return True
    except Exception as e:
        if logger:
            msg = f"   ❌ {label} Save Error: {e}"
            logger.error(msg) if hasattr(logger, "error") else logger.log(msg)
        return False
    finally:
        if own_client and actual_client:
            actual_client.close()


save_ticks_to_streaming_db = save_ticks_to_storage


def query_ticks(symbol: str, start_time=None, end_time=None, limit=1000, client=None) -> pd.DataFrame:
    """
    Queries raw tick-by-tick records from streaming.duckdb for a given symbol.
    Returns a pandas DataFrame.
    """
    own_client = False
    if not client:
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


query_streaming_ticks = query_ticks


def query_candlesticks_from_ticks(symbol: str, timeframe="1m", start_time=None, end_time=None, client=None) -> pd.DataFrame:
    """
    Dynamically resamples raw tick-by-tick data from streaming.duckdb into OHLCV candlesticks using time_bucket().
    Supported timeframes: '1s', '5s', '15s', '1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d'.
    Returns a pandas DataFrame.
    """
    own_client = False
    if not client:
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
                sum(coalesce(volume, 1.0)) AS volume,
                count(*) AS tick_count
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


query_streaming_candlesticks = query_candlesticks_from_ticks
