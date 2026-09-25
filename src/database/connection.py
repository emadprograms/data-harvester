"""
Database connection management using native DuckDB.
Provides thread-safe local connections to data/market_data.duckdb with zero cloud dependencies.
"""
import os
import duckdb

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "market_data.duckdb")


class DuckDBResult:
    """Wrapper around DuckDB cursor to provide seamless compatibility with both .rows and .fetchall()/.fetchone()."""
    def __init__(self, cursor):
        self.cursor = cursor
        self._rows = None

    @property
    def rows(self):
        if self._rows is None:
            self._rows = self.cursor.fetchall()
        return self._rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def df(self):
        return self.cursor.df()

    def arrow(self):
        return self.cursor.arrow()


class DuckDBClient:
    """High-performance local DuckDB client wrapper."""
    def __init__(self, db_path=None, read_only=False):
        self.db_path = db_path or DEFAULT_DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path, read_only=read_only)

    def execute(self, query, params=None):
        if params is not None:
            cur = self.conn.execute(query, params)
        else:
            cur = self.conn.execute(query)
        return DuckDBResult(cur)

    def executemany(self, query, seq_of_params):
        cur = self.conn.executemany(query, seq_of_params)
        return DuckDBResult(cur)

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


# Backward-compatible alias for existing codebase callers
def get_archive_db_connection(db_path=None):
    """Returns local DuckDB connection (replacing legacy Turso Archive)."""
    return get_duckdb_connection(db_path=db_path)
