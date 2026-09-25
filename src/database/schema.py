"""
DuckDB Database schema initialization and table creation for Dedicated Dual-DuckDB Architecture.
- historical.duckdb: symbol_map inventory and canonical 1-minute market_data candles.
- streaming.duckdb: high-frequency raw tick quotes (ticks table / streaming_ticks view).
"""
from src.database.connection import (
    get_duckdb_connection,
    get_historical_db_connection,
    get_streaming_db_connection,
)

get_archive_db_connection = get_historical_db_connection


def init_historical_db(client=None):
    """Initializes the historical DuckDB database (symbol_map and market_data tables)."""
    own_client = False
    if not client:
        client = get_archive_db_connection()
        own_client = True

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

        # --- HISTORICAL MARKET DATA TABLE ---
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
        except Exception:
            pass

    except Exception as e:
        print(f"❌ Historical DuckDB Schema Init Error: {e}")
    finally:
        if own_client:
            client.close()


def init_streaming_db(client=None, db_path=None):
    """Initializes the dedicated streaming DuckDB database (ticks table and streaming_ticks view)."""
    own_client = False
    if not client:
        client = get_streaming_db_connection(db_path=db_path)
        own_client = True

    if not client:
        return

    try:
        # --- RAW TICKS TABLE (streaming.duckdb tick-by-tick storage) ---
        client.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR NOT NULL,
                price DOUBLE NOT NULL,
                volume DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                source VARCHAR,
                session VARCHAR DEFAULT 'REG'
            )
        """)

        # --- FAST TIME-SERIES INDEXES ---
        try:
            client.execute("CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks (timestamp)")
            client.execute("CREATE INDEX IF NOT EXISTS idx_ticks_sym_ts ON ticks (symbol, timestamp)")
        except Exception:
            pass

        # --- COMPATIBILITY VIEW ---
        try:
            client.execute("CREATE VIEW IF NOT EXISTS streaming_ticks AS SELECT * FROM ticks")
        except Exception:
            pass

    except Exception as e:
        print(f"❌ Streaming DuckDB Schema Init Error: {e}")
    finally:
        if own_client:
            client.close()


def init_db(client=None, streaming_client=None):
    """
    Initializes database tables.
    If called with a single client, initializes historical schema for backward compatibility.
    If called with no args, initializes both historical and streaming databases.
    """
    if client and not streaming_client:
        # Backward compatibility: single client passed in
        init_historical_db(client)
        return

    init_historical_db(client)
    init_streaming_db(streaming_client)


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
