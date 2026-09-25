"""
Database connection management using native DuckDB.
Provides thread-safe local connections to dedicated DuckDB files:
- data/historical.duckdb: 1-minute REST chart candles and symbols
- data/streaming.duckdb: 24/7 live WebSocket raw tick quotes
"""
import os
import duckdb

DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
DEFAULT_HISTORICAL_DB_PATH = os.path.join(DEFAULT_DATA_DIR, "historical.duckdb")
DEFAULT_STREAMING_DB_PATH = os.path.join(DEFAULT_DATA_DIR, "streaming.duckdb")

# Fallback path for v1.0 migration backward compatibility
LEGACY_MARKET_DATA_PATH = os.path.join(DEFAULT_DATA_DIR, "market_data.duckdb")
DEFAULT_DB_PATH = DEFAULT_HISTORICAL_DB_PATH if os.path.exists(DEFAULT_HISTORICAL_DB_PATH) or not os.path.exists(LEGACY_MARKET_DATA_PATH) else LEGACY_MARKET_DATA_PATH


class DuckDBResult:
    """Wrapper around DuckDB cursor to provide seamless compatibility with both .rows and .fetchall()/.fetchone()."""
    def __init__(self, cursor):
        self.cursor = cursor
        self._rows = None
        self._df = None

    @property
    def rows(self):
        if self._rows is None:
            if self._df is not None:
                self._rows = [tuple(x) for x in self._df.to_numpy()]
            else:
                self._rows = self.cursor.fetchall()
        return self._rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def df(self):
        if self._df is None:
            if self._rows is not None:
                import pandas as pd
                cols = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
                self._df = pd.DataFrame(self._rows, columns=cols)
            else:
                self._df = self.cursor.df()
        return self._df

    def arrow(self):
        try:
            return self.cursor.arrow()
        except Exception:
            import pyarrow as pa
            return pa.Table.from_pandas(self.df())


class DuckDBClient:
    """High-performance local DuckDB client wrapper."""
    def __init__(self, db_path=None, read_only=False, max_retries=5, retry_delay=0.05):
        import time
        self.db_path = db_path or DEFAULT_DB_PATH
        self.read_only = read_only
        dirname = os.path.dirname(self.db_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        conn = None
        last_error = None
        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(self.db_path, read_only=self.read_only)
                break
            except Exception as e:
                err_msg = str(e)
                last_error = e
                # 1. Handle in-process configuration conflict: adapt to whichever mode is already open in this process
                if "different configuration" in err_msg:
                    try:
                        self.read_only = not self.read_only
                        conn = duckdb.connect(self.db_path, read_only=self.read_only)
                        break
                    except Exception as inner_e:
                        last_error = inner_e
                # 2. Handle cross-process or concurrent file lock contention: retry with backoff
                if "lock" in err_msg.lower() and attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                break

        if conn is None:
            raise last_error or RuntimeError(f"Could not connect to DuckDB at {self.db_path}")
        self.conn = conn

    def execute(self, query, params=None):
        if params is not None:
            cur = self.conn.execute(query, params)
        else:
            cur = self.conn.execute(query)
        return DuckDBResult(cur)

    def executemany(self, query, seq_of_params):
        cur = self.conn.executemany(query, seq_of_params)
        return DuckDBResult(cur)

    def attach(self, target_db_path: str, alias: str, read_only: bool = True):
        """Attaches another DuckDB database to this connection for cross-database queries."""
        dirname = os.path.dirname(target_db_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        ro_clause = " (READ_ONLY)" if read_only else ""
        escaped_path = target_db_path.replace("'", "''")
        try:
            self.conn.execute(f"ATTACH IF NOT EXISTS '{escaped_path}' AS {alias}{ro_clause}")
        except Exception as e:
            if "already attached" in str(e) or "Unique file handle conflict" in str(e):
                pass
            else:
                raise
        return self

    def detach(self, alias: str):
        """Detaches a previously attached database."""
        self.conn.execute(f"DETACH IF EXISTS {alias}")
        return self

    def commit(self):
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def get_duckdb_connection(db_path=None, read_only=False):
    """Establishes a connection to the local DuckDB database."""
    try:
        return DuckDBClient(db_path=db_path, read_only=read_only)
    except Exception as e:
        print(f"❌ DuckDB Connection Error: {e}")
        return None


def get_historical_db_connection(db_path=None, read_only=False):
    """Establishes a dedicated connection to the historical DuckDB database (data/historical.duckdb)."""
    target_path = db_path or (DEFAULT_HISTORICAL_DB_PATH if os.path.exists(DEFAULT_HISTORICAL_DB_PATH) or not os.path.exists(LEGACY_MARKET_DATA_PATH) else LEGACY_MARKET_DATA_PATH)
    return get_duckdb_connection(db_path=target_path, read_only=read_only)


def get_streaming_db_connection(db_path=None, read_only=False):
    """Establishes a dedicated connection to the streaming DuckDB database (data/streaming.duckdb)."""
    target_path = db_path or DEFAULT_STREAMING_DB_PATH
    return get_duckdb_connection(db_path=target_path, read_only=read_only)


def get_unified_connection(historical_path=None, streaming_path=None, read_only=True):
    """
    Returns an in-memory or read-only DuckDB connection with both historical and streaming
    databases attached as 'hist' and 'live' for cross-database analytical queries.
    """
    hist_p = historical_path or (DEFAULT_HISTORICAL_DB_PATH if os.path.exists(DEFAULT_HISTORICAL_DB_PATH) or not os.path.exists(LEGACY_MARKET_DATA_PATH) else LEGACY_MARKET_DATA_PATH)
    stream_p = streaming_path or DEFAULT_STREAMING_DB_PATH

    client = DuckDBClient(":memory:", read_only=False)
    if os.path.exists(hist_p):
        client.attach(hist_p, "hist", read_only=read_only)
    if os.path.exists(stream_p):
        client.attach(stream_p, "live", read_only=read_only)
    return client


# Backward-compatible alias for existing codebase callers
def get_archive_db_connection(db_path=None):
    """Returns local DuckDB connection (replacing legacy Turso Archive)."""
    return get_historical_db_connection(db_path=db_path)


def create_client_sync(url=None, auth_token=None):
    """Backward-compatible client factory for tests and legacy callers."""
    if url and str(url).startswith("file:"):
        path = str(url).replace("file:", "")
        return DuckDBClient(db_path=path)
    return DuckDBClient()
