"""
Database schema initialization and table creation.
Includes strict PRIMARY KEY constraints to prevent duplication.
"""
import streamlit as st
from src.database.connection import get_db_connection


def init_db():
    """Initializes the database, creating tables if they don't exist."""
    client = get_db_connection()
    if not client:
        return
    
    try:
        # --- ⚠️ DANGER ZONE: UNCOMMENT ONCE TO WIPE BAD DATA ---
        # client.execute("DROP TABLE IF EXISTS market_data")
        # -------------------------------------------------------

        # Table for managing symbol rules
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                user_ticker TEXT PRIMARY KEY,
                capital_epic TEXT NOT NULL,
                source_strategy TEXT DEFAULT 'HYBRID' 
            )
        """)
        
        # Table for storing all market data
        # CRITICAL: PRIMARY KEY (symbol, timestamp) forces SQLite to reject duplicates.
        # We store timestamp as a UTC String to ensure strict uniqueness.
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, 
                high REAL, 
                low REAL, 
                close REAL, 
                volume REAL, 
                session TEXT,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        
        # Seed the database if the symbol map is empty
        res = client.execute("SELECT count(*) FROM symbol_map")
        if res.rows and res.rows[0][0] == 0:
            hybrid_tickers = [
                "SPY", "QQQ", "IWM", "DIA", "AMD", "AMZN", "AAPL", "NVDA", "TSLA",
                "BTCUSDT", "ETHUSDT", "CL=F", "GC=F", "VIX"
            ]
            seed_data = [(t, t, "HYBRID") for t in hybrid_tickers]
            for ticker, epic, strategy in seed_data:
                client.execute(
                    "INSERT INTO symbol_map (user_ticker, capital_epic, source_strategy) VALUES (?, ?, ?)", 
                    [ticker, epic, strategy]
                )
            if st.runtime.exists():
                st.toast("Database initialized.", icon="💾")
                
    except Exception as e:
        if st.runtime.exists():
            st.error(f"DB Init Error: {e}")