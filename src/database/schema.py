"""
Database schema initialization and table creation.
Includes strict PRIMARY KEY constraints to prevent duplication.
"""

from src.database.connection import get_archive_db_connection, get_mirror_db_connection


def init_db(client=None):
    """Initializes the database, creating tables if they don't exist."""
    if client:
        _init_client(client)
        return

    # Initialize both Turso Archive and Mirror
    for conn_func in [get_archive_db_connection, get_mirror_db_connection]:
        conn = conn_func()
        if conn:
            try:
                _init_client(conn)
            finally:
                conn.close()


def _init_client(client):
    """Internal helper to initialize a specific client."""
    if not client:
        return
    
    try:
        # --- SYMBOL INVENTORY TABLE ---
        # Strictly limited to ticker mapping. Priority is handled in code.
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT,
                capital_ticker TEXT
            )
        """)

        # --- SEEDING ---
        try:
            res = client.execute("SELECT count(*) FROM symbol_map")
            if res.rows and res.rows[0][0] == 0:
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
    print("🌱 Seeding default symbols...")
    tickers = [
        # Equities/ETFs (Fallback changed from Yahoo -> Capital)
        ("SPY", None, "SPY", None, "SPY"),
        ("QQQ", None, "QQQ", None, "QQQ"),
        ("IWM", None, "IWM", None, "IWM"),
        ("DIA", None, "DIA", None, "DIA"),
        ("AMD", None, "AMD", None, "AMD"),
        ("AMZN", None, "AMZN", None, "AMZN"),
        ("AAPL", None, "AAPL", None, "AAPL"),
        ("NVDA", None, "NVDA", None, "NVDA"),
        ("TSLA", None, "TSLA", None, "TSLA"),
        # Crypto
        ("BTCUSDT", "BTC-USD", None, "BTCUSDT", None),
        ("ETHUSDT", "ETH-USD", None, "ETHUSDT", None),
        ("PAXGUSDT", "PAXG-USD", None, "PAXGUSDT", None),
        # Specialized
        ("CL=F", "CL=F", None, None, None),
        ("GC=F", "GC=F", None, None, None),
        ("VIX", "^VIX", None, None, None),
        ("UUP", "UUP", None, None, None)
    ]
    for disp, y, m, b, c in tickers:
        client.execute(
            """INSERT INTO symbol_map 
               (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) 
               VALUES (?, ?, ?, ?, ?)""",
            [disp, y, m, b, c]
        )
