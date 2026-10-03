"""Frozen, repeatable DuckDB source fixtures for migration integrity tests."""
from datetime import datetime
from pathlib import Path
import re
from typing import Iterable, Mapping, Sequence

import duckdb


DEFAULT_COLUMNS = (
    "timestamp",
    "symbol",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
    "session",
)


def create_source_db(
    path: Path,
    rows: Iterable[Sequence | Mapping],
    *,
    table: str = "tick_data",
    columns: Sequence[str] = DEFAULT_COLUMNS,
) -> Path:
    """Create and close a source DB; returned file is not modified by the fixture again."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"unsafe test table name: {table!r}")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute(
            f'CREATE TABLE "{table}" ('
            "timestamp TIMESTAMP NOT NULL, "
            "symbol VARCHAR NOT NULL, "
            "price DOUBLE NOT NULL, "
            "volume DOUBLE, bid DOUBLE, ask DOUBLE, "
            "source VARCHAR, session VARCHAR)"
        )
        normalized = []
        for row in rows:
            if isinstance(row, Mapping):
                values = [row.get(column) for column in columns]
            else:
                values = list(row)
            if len(values) != len(columns):
                raise ValueError(f"expected {len(columns)} values, got {len(values)}")
            if values and isinstance(values[0], datetime) and values[0].tzinfo is not None:
                values[0] = values[0].replace(tzinfo=None)
            normalized.append(tuple(values))
        if normalized:
            placeholders = ", ".join("?" for _ in columns)
            con.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', normalized)
    finally:
        con.close()
    return path


def quote_row(
    timestamp: datetime,
    symbol: str,
    price: float,
    *,
    volume=None,
    bid=None,
    ask=None,
    source="CAPITAL",
    session="REG",
) -> tuple:
    return (timestamp, symbol, price, volume, bid, ask, source, session)
