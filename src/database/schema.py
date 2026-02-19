"""
Database schema initialization and table creation.
Includes strict PRIMARY KEY constraints to prevent duplication.
"""

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

        # Old table (Keep for reference or one-time migration, but we mainly use market_symbols now)
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                user_ticker TEXT PRIMARY KEY,
                capital_epic TEXT NOT NULL,
                source_strategy TEXT DEFAULT 'HYBRID' 
            )
        """)
        
        # --- NEW SCALABLE SCHEMA ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_symbols (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT,
                priority_1 TEXT, -- YAHOO, MASSIVE, BINANCE
                priority_2 TEXT,  -- MASSIVE, YAHOO, NONE
                priority_3 TEXT
            )
        """)

        # Migration: If market_symbols is empty but symbol_map has data, migrate it.
        # This preserves existing user data while moving to the new format.
        res_new = client.execute("SELECT count(*) FROM market_symbols")
        res_old = client.execute("SELECT count(*) FROM symbol_map")
        
        if res_new.rows and res_new.rows[0][0] == 0:
            if res_old.rows and res_old.rows[0][0] > 0:
                # Migrate!
                old_rows = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map").rows
                for row in old_rows:
                    user_ticker = row[0]
                    cap_epic = row[1]
                    strategy = row[2]
                    
                    # Logic to Map Old Strategy to New Priorities
                    p1 = "YAHOO"
                    p2 = "MASSIVE"
                    
                    y_ticker = user_ticker
                    m_ticker = cap_epic # Map old "Epic" to Massive Ticker
                    b_ticker = None
                    
                    if user_ticker.endswith("USDT"):
                         p1 = "BINANCE"
                         p2 = "YAHOO" # As per old logic
                         b_ticker = user_ticker
                    elif user_ticker.endswith("=F"):
                         p1 = "YAHOO"
                         p2 = "MASSIVE"
                         
                    if strategy == "CAPITAL_ONLY":
                        p1 = "MASSIVE"
                        p2 = "NONE"

                    client.execute(
                        """INSERT INTO market_symbols 
                           (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2) 
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [user_ticker, y_ticker, m_ticker, b_ticker, p1, p2]
                    )
                print("📦 Migrated inventory to new schema.")
            else:
                # Fresh Seed (only if BOTH are empty)
                hybrid_tickers = [
                    "SPY", "QQQ", "IWM", "DIA", "AMD", "AMZN", "AAPL", "NVDA", "TSLA",
                    "BTCUSDT", "ETHUSDT", "CL=F", "GC=F", "VIX"
                ]
                for t in hybrid_tickers:
                    p1 = "YAHOO"
                    p2 = "MASSIVE"
                    b_ticker = None
                    y_ticker = t
                    m_ticker = t
                    if t.endswith("USDT"): 
                        p1 = "BINANCE"; p2 = "YAHOO"; b_ticker = t
                        # Heuristic for Crypto fallback on Yahoo
                        y_ticker = t.replace("USDT", "-USD")
                    
                    client.execute(
                        """INSERT INTO market_symbols 
                           (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2) 
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [t, y_ticker, m_ticker, b_ticker, p1, p2]
                    )

        
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
                
    except Exception as e:
        print(f"❌ DB Init Error: {e}")