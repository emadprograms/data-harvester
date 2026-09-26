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
        # --- HISTORICAL SYMBOL INVENTORY TABLE ---
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "historical_symbol_map" in tables and "historical_database_symbols" not in tables:
            # Check if historical_symbol_map is a table and migrate to historical_database_symbols
            client.execute("CREATE TABLE historical_database_symbols AS SELECT * FROM historical_symbol_map")
            client.execute("DROP TABLE historical_symbol_map")
        elif "symbol_map" in tables and "historical_database_symbols" not in tables:
            client.execute("CREATE TABLE historical_database_symbols AS SELECT * FROM symbol_map")
            client.execute("DROP TABLE symbol_map")
        else:
            client.execute("""
                CREATE TABLE IF NOT EXISTS historical_database_symbols (
                    display_name VARCHAR PRIMARY KEY,
                    yahoo_ticker VARCHAR,
                    massive_ticker VARCHAR,
                    binance_ticker VARCHAR,
                    capital_ticker VARCHAR
                )
            """)

        # Backward-compatible views
        try:
            client.execute("CREATE VIEW IF NOT EXISTS historical_symbol_map AS SELECT * FROM historical_database_symbols")
        except Exception:
            pass
        try:
            client.execute("CREATE VIEW IF NOT EXISTS symbol_map AS SELECT * FROM historical_database_symbols")
        except Exception:
            pass

        # --- SEEDING ---
        try:
            res = client.execute("SELECT count(*) FROM historical_database_symbols")
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
        # --- STREAMING SYMBOL INVENTORY TABLE ---
        tables = [t[0] for t in client.execute("SHOW TABLES").fetchall()]
        if "streaming_symbol_map" in tables and "streaming_database_symbols" not in tables:
            client.execute("CREATE TABLE streaming_database_symbols AS SELECT * FROM streaming_symbol_map")
            client.execute("DROP TABLE streaming_symbol_map")
        else:
            client.execute("""
                CREATE TABLE IF NOT EXISTS streaming_database_symbols (
                    display_name VARCHAR PRIMARY KEY,
                    capital_ticker VARCHAR,
                    databento_ticker VARCHAR,
                    binance_ticker VARCHAR,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)

        # Backward-compatible view
        try:
            client.execute("CREATE VIEW IF NOT EXISTS streaming_symbol_map AS SELECT * FROM streaming_database_symbols")
        except Exception:
            pass

        # --- SEED DEFAULT STREAMING SYMBOLS (19 single-stock equities) ---
        try:
            res = client.execute("SELECT count(*) FROM streaming_database_symbols")
            if res.rows and res.rows[0][0] == 0:
                _seed_streaming_symbols(client)
        except Exception as e:
            print(f"⚠️ Streaming seeding warning: {e}")

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


def _seed_streaming_symbols(client):
    """Seeds default streaming symbols (19 pure single stocks) into streaming_symbol_map."""
    print("🌱 Seeding default streaming symbols (19 single-stock equities)...")
    symbols = [
        ("AAPL", "AAPL", "AAPL", None, True),
        ("ADBE", "ADBE", "ADBE", None, True),
        ("AMD", "AMD", "AMD", None, True),
        ("AMZN", "AMZN", "AMZN", None, True),
        ("APP", "APP", "APP", None, True),
        ("AVGO", "AVGO", "AVGO", None, True),
        ("BABA", "BABA", "BABA", None, True),
        ("GOOGL", "GOOGL", "GOOGL", None, True),
        ("META", "META", "META", None, True),
        ("MSFT", "MSFT", "MSFT", None, True),
        ("MU", "MU", "MU", None, True),
        ("NDAQ", "US100", "NDAQ", None, True),
        ("NVDA", "NVDA", "NVDA", None, True),
        ("ORCL", "ORCL", "ORCL", None, True),
        ("PANW", "PANW", "PANW", None, True),
        ("QCOM", "QCOM", "QCOM", None, True),
        ("SHOP", "SHOP", "SHOP", None, True),
        ("TSLA", "TSLA", "TSLA", None, True),
        ("TSM", "TSM", "TSM", None, True),
    ]
    for disp, cap, dbn, binance, active in symbols:
        client.execute(
            """INSERT OR IGNORE INTO streaming_database_symbols 
               (display_name, capital_ticker, databento_ticker, binance_ticker, is_active) 
               VALUES (?, ?, ?, ?, ?)""",
            [disp, cap, dbn, binance, active]
        )


def _seed_default_symbols(client):
    """Seeds default symbols into an empty historical database."""
    print("🌱 Seeding default historical symbols...")
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
            """INSERT OR IGNORE INTO historical_database_symbols 
               (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) 
               VALUES (?, ?, ?, ?, ?)""",
            [disp, y, m, b, c]
        )
