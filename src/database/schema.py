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
        if turso_client:
            _init_client(turso_client)
        
        local_client = get_local_db_connection()
        if local_client:
            _init_client(local_client)


def _init_client(client):
    """Internal helper to initialize a specific client."""
    if not client:
        return
    
    try:
        # --- NEW SCALABLE SCHEMA ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_symbols (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                capital_epic TEXT,
                binance_ticker TEXT,
                priority_1 TEXT, -- YAHOO, CAPITAL, BINANCE
                priority_2 TEXT,  -- CAPITAL, YAHOO, NONE
                priority_3 TEXT
            )
        """)

        # --- MIGRATION: Rename massive_ticker to capital_epic if it exists ---
        try:
            info = client.execute("PRAGMA table_info(market_symbols)")
            cols = [row[1] for row in info.rows]
            if "massive_ticker" in cols and "capital_epic" not in cols:
                print("🔄 Migrating: Renaming massive_ticker to capital_epic...")
                client.execute("ALTER TABLE market_symbols RENAME COLUMN massive_ticker TO capital_epic")
                print("✅ Renamed massive_ticker to capital_epic.")
        except Exception as e:
            print(f"⚠️ Column migration warning: {e}")

        # --- MIGRATION & SEEDING ---
        try:
            res_new = client.execute("SELECT count(*) FROM market_symbols")
            if res_new.rows and res_new.rows[0][0] == 0:
                # Table is empty, try to migrate from old schema or seed defaults
                try:
                    res_old = client.execute("SELECT count(*) FROM symbol_map")
                    if res_old.rows and res_old.rows[0][0] > 0:
                        _migrate_from_old_schema(client)
                    else:
                        _seed_default_symbols(client)
                except Exception:
                    # symbol_map likely doesn't exist
                    _seed_default_symbols(client)
        except Exception as e:
            print(f"⚠️ Migration/Seeding warning: {e}")

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


def _migrate_from_old_schema(client):
    """Migrates data from the old symbol_map table to market_symbols."""
    print("📦 Migrating inventory to new schema...")
    old_rows = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map").rows
    for row in old_rows:
        user_ticker = row[0]
        cap_epic = row[1]
        strategy = row[2]
        
        p1 = "YAHOO"
        p2 = "CAPITAL"
        y_ticker = user_ticker
        c_ticker = cap_epic
        b_ticker = None
        
        if user_ticker.endswith("USDT"):
             p1 = "BINANCE"
             p2 = "YAHOO"
             b_ticker = user_ticker
        elif user_ticker.endswith("=F"):
             p1 = "YAHOO"
             p2 = "CAPITAL"
             
        if strategy == "CAPITAL_ONLY":
            p1 = "CAPITAL"
            p2 = "NONE"

        client.execute(
            """INSERT INTO market_symbols 
               (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [user_ticker, y_ticker, c_ticker, b_ticker, p1, p2]
        )


def _seed_default_symbols(client):
    """Seeds default symbols into an empty database."""
    print("🌱 Seeding default symbols...")
    hybrid_tickers = [
        "SPY", "QQQ", "IWM", "DIA", "AMD", "AMZN", "AAPL", "NVDA", "TSLA",
        "BTCUSDT", "ETHUSDT", "CL=F", "GC=F", "VIX"
    ]
    for t in hybrid_tickers:
        p1 = "YAHOO"
        p2 = "CAPITAL"
        b_ticker = None
        y_ticker = t
        c_ticker = t
        if t.endswith("USDT"): 
            p1 = "BINANCE"; p2 = "YAHOO"; b_ticker = t
            y_ticker = t.replace("USDT", "-USD")
        
        client.execute(
            """INSERT INTO market_symbols 
               (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [t, y_ticker, c_ticker, b_ticker, p1, p2]
        )