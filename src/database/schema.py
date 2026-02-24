"""
Database schema initialization and table creation.
Includes strict PRIMARY KEY constraints to prevent duplication.
"""

from src.database.connection import get_archive_db_connection, get_mirror_db_connection


def init_db(client=None):
    """Initializes the database, creating tables if they don't exist."""
    if client:
        _init_client(client)
    else:
        # Initialize both Turso Archive and Mirror
        archive_client = get_archive_db_connection()
        try:
            if archive_client:
                _init_client(archive_client)
        finally:
            if archive_client:
                archive_client.close()
        
        mirror_client = get_mirror_db_connection()
        try:
            if mirror_client:
                _init_client(mirror_client)
        finally:
            if mirror_client:
                mirror_client.close()


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
                binance_ticker TEXT
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
        # Equities/ETFs
        ("SPY", "SPY", "SPY", None),
        ("QQQ", "QQQ", "QQQ", None),
        ("IWM", "IWM", "IWM", None),
        ("DIA", "DIA", "DIA", None),
        ("AMD", "AMD", "AMD", None),
        ("AMZN", "AMZN", "AMZN", None),
        ("AAPL", "AAPL", "AAPL", None),
        ("NVDA", "NVDA", "NVDA", None),
        ("TSLA", "TSLA", "TSLA", None),
        # Crypto
        ("BTCUSDT", "BTC-USD", None, "BTCUSDT"),
        ("ETHUSDT", "ETH-USD", None, "ETHUSDT"),
        # Specialized
        ("CL=F", "CL=F", None, None),
        ("GC=F", "GC=F", None, None),
        ("VIX", "^VIX", None, None),
        ("UUP", "UUP", None, None)
    ]
    for disp, y, m, b in tickers:
        client.execute(
            """INSERT INTO symbol_map 
               (display_name, yahoo_ticker, massive_ticker, binance_ticker) 
               VALUES (?, ?, ?, ?)""",
            [disp, y, m, b]
        )
