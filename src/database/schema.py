"""
Database schema initialization and table creation.
Includes strict PRIMARY KEY constraints to prevent duplication.
"""

from src.database.connection import get_db_connection, get_local_db_connection


def init_db(client=None):
    """Initializes the database, creating tables if they don't exist."""
    if client:
        _init_client(client)
    else:
        # Initialize both Turso and Local
        turso_client = get_db_connection()
        try:
            if turso_client:
                _init_client(turso_client)
        finally:
            if turso_client:
                turso_client.close()
        
        local_client = get_local_db_connection()
        try:
            if local_client:
                _init_client(local_client)
        finally:
            if local_client:
                local_client.close()


def _init_client(client):
    """Internal helper to initialize a specific client."""
    if not client:
        return
    
    try:
        # --- COMPATIBILITY TABLE ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT,
                priority_1 TEXT,
                priority_2 TEXT,
                priority_3 TEXT
            )
        """)

        # --- NEW SCALABLE SCHEMA ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_symbols (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT,
                priority_1 TEXT, -- MASSIVE, YAHOO, BINANCE
                priority_2 TEXT, -- YAHOO, MASSIVE, NONE
                priority_3 TEXT
            )
        """)

        # --- MIGRATION: Rename capital_epic to massive_ticker if it exists ---
        try:
            for table in ["symbol_map", "market_symbols"]:
                info = client.execute(f"PRAGMA table_info({table})")
                cols = [row[1] for row in info.rows]
                if "capital_epic" in cols and "massive_ticker" not in cols:
                    print(f"🔄 Migrating {table}: Renaming capital_epic to massive_ticker...")
                    client.execute(f"ALTER TABLE {table} RENAME COLUMN capital_epic TO massive_ticker")
                    print(f"✅ Renamed capital_epic to massive_ticker in {table}.")
        except Exception as e:
            print(f"⚠️ Column migration warning: {e}")

        # --- SEEDING ---
        try:
            res_new = client.execute("SELECT count(*) FROM market_symbols")
            if res_new.rows and res_new.rows[0][0] == 0:
                _seed_default_symbols(client)
        except Exception as e:
            print(f"⚠️ Seeding warning: {e}")

        # --- MARKET DATA TABLE ---
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


def _seed_default_symbols(client):
    """Seeds default symbols into an empty database."""
    print("🌱 Seeding default symbols (Massive/Polygon Optimized)...")
    tickers = [
        "SPY", "QQQ", "IWM", "DIA", "AMD", "AMZN", "AAPL", "NVDA", "TSLA",
        "BTCUSDT", "ETHUSDT", "CL=F", "GC=F", "VIX"
    ]
    for t in tickers:
        p1 = "MASSIVE"
        p2 = "YAHOO"
        b_ticker = None
        y_ticker = t
        m_ticker = t
        
        if t.endswith("USDT"): 
            p1 = "BINANCE"; p2 = "YAHOO"; b_ticker = t
            y_ticker = t.replace("USDT", "-USD")
        elif t in ["CL=F", "GC=F", "VIX"]:
            p1 = "YAHOO"; p2 = "NONE"; m_ticker = None
        
        client.execute(
            """INSERT INTO market_symbols 
               (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [t, y_ticker, m_ticker, b_ticker, p1, p2]
        )
