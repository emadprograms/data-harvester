"""
DuckDB Database schema initialization and table creation.
Enforces PRIMARY KEY constraints (symbol, timestamp) and fast time-series indexes.
"""
from src.database.connection import get_duckdb_connection

get_archive_db_connection = get_duckdb_connection


def init_db(client=None):
    """Initializes the DuckDB database, creating tables and indexes if they don't exist."""
    if client:
        _init_client(client)
        return

    conn = get_duckdb_connection()
    if conn:
        try:
            _init_client(conn)
        finally:
            conn.close()


def _init_client(client):
    """Internal helper to initialize a specific DuckDB client."""
    if not client:
        return

    try:
        # --- SYMBOL INVENTORY TABLE ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                display_name VARCHAR PRIMARY KEY,
                yahoo_ticker VARCHAR,
                massive_ticker VARCHAR,
                binance_ticker VARCHAR,
                capital_ticker VARCHAR
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
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                open DOUBLE, 
                high DOUBLE, 
                low DOUBLE, 
                close DOUBLE, 
                volume DOUBLE, 
                session VARCHAR,
                source VARCHAR,
                PRIMARY KEY (symbol, timestamp)
            )
        """)

        # --- INDEXES FOR FAST TIME-SERIES QUERIES ---
        try:
            client.execute("CREATE INDEX IF NOT EXISTS idx_market_data_ts ON market_data (timestamp)")
            client.execute("CREATE INDEX IF NOT EXISTS idx_market_data_sym_ts ON market_data (symbol, timestamp)")
        except Exception as e:
            # DuckDB automatically indexes primary keys
            pass

    except Exception as e:
        print(f"❌ DuckDB Schema Init Error: {e}")


def _seed_default_symbols(client):
    """Seeds default symbols into an empty database."""
    print("🌱 Seeding default symbols...")
    tickers = [
        # Equities/ETFs
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
        ("PAXGUSDT", "GC=F", None, "PAXGUSDT", None),
        # Specialized
        ("CL=F", "CL=F", None, None, None),
        ("VIX", "^VIX", None, None, None),
        ("UUP", "UUP", None, None, None)
    ]
    for disp, y, m, b, c in tickers:
        client.execute(
            """INSERT OR IGNORE INTO symbol_map 
               (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) 
               VALUES (?, ?, ?, ?, ?)""",
            [disp, y, m, b, c]
        )
