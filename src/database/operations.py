"""
Database CRUD operations for symbol management and market data.
"""
import streamlit as st
import pandas as pd
import time
from libsql_client import Statement
from src.database.connection import get_db_connection


def get_symbol_map_from_db():
    """Fetches the complete symbol inventory from Turso."""
    client = get_db_connection()
    if not client:
        return {}
    try:
        res = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map ORDER BY user_ticker")
        return {row[0]: {'epic': row[1], 'strategy': row[2]} for row in res.rows}
    except Exception as e:
        if st.runtime.exists():
            st.error(f"Error fetching inventory: {e}")
        return {}


def upsert_symbol_mapping(ticker, epic, strategy):
    """Adds or updates a symbol's rules in the database."""
    client = get_db_connection()
    if not client:
        return False
    try:
        client.execute(
            """INSERT INTO symbol_map (user_ticker, capital_epic, source_strategy) 
               VALUES (?, ?, ?) 
               ON CONFLICT(user_ticker) DO UPDATE SET 
                 capital_epic=excluded.capital_epic, 
                 source_strategy=excluded.source_strategy""",
            [ticker, epic, strategy]
        )
        return True
    except Exception as e:
        st.error(f"Error saving symbol: {e}")
        return False


def delete_symbol_mapping(ticker):
    """Deletes a symbol from the inventory."""
    client = get_db_connection()
    if not client:
        return False
    try:
        client.execute("DELETE FROM symbol_map WHERE user_ticker = ?", [ticker])
        return True
    except Exception as e:
        st.error(f"Error deleting symbol: {e}")
        return False


def save_data_to_turso(df: pd.DataFrame, logger=None):
    """Saves a DataFrame of market data to Turso using batched transactions."""
    client = get_db_connection()
    if not client or df.empty:
        return False
    
    try:
        statements = []
        for _, row in df.iterrows():
            ts_str = row['timestamp'].isoformat()
            stmt = Statement(
                """INSERT OR REPLACE INTO market_data 
                   (timestamp, symbol, open, high, low, close, volume, session) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [ts_str, row['symbol'], row['open'], row['high'], row['low'], 
                 row['close'], row['volume'], row['session']]
            )
            statements.append(stmt)
        
        # Chunking Logic
        BATCH_SIZE = 100
        total_batches = (len(statements) + BATCH_SIZE - 1) // BATCH_SIZE
        
        if logger:
            logger.log(f"   💾 Committing {len(statements)} records in {total_batches} batches...")
        
        for i in range(0, len(statements), BATCH_SIZE):
            batch = statements[i : i + BATCH_SIZE]
            client.batch(batch)
            time.sleep(0.05)
            
        return True
    except Exception as e:
        err_msg = f"Batch Commit Failed: {e}"
        if logger:
            logger.log(f"   ❌ {err_msg}")
        elif st.runtime.exists():
            st.error(err_msg)
        return False


def fetch_data_health_matrix(tickers: list, start_date, end_date, session_filter="Total"):
    """Fetches a matrix of candle counts for the data health dashboard."""
    client = get_db_connection()
    if not client:
        return pd.DataFrame()

    start_str = f"{start_date}T00:00:00"
    end_str = f"{end_date}T23:59:59"
    placeholders = ",".join("?" * len(tickers))

    query = f"""
        SELECT 
            symbol, 
            date(timestamp) as day, 
            COUNT(*) as candle_count
        FROM market_data 
        WHERE symbol IN ({placeholders}) 
          AND timestamp >= ? 
          AND timestamp <= ? 
    """
    params = tickers + [start_str, end_str]
    
    if session_filter != "Total":
        query += " AND session = ? "
        params.append(session_filter)
        
    query += " GROUP BY symbol, day ORDER BY symbol, day"
    
    try:
        res = client.execute(query, params)
        if not res.rows:
            return pd.DataFrame()
        cols = ['symbol', 'day', 'candle_count']
        df = pd.DataFrame([list(row) for row in res.rows], columns=cols)
        pivot_df = df.pivot(index='symbol', columns='day', values='candle_count')
        return pivot_df
    except Exception as e:
        st.error(f"Error fetching data health: {e}")
        return pd.DataFrame()
