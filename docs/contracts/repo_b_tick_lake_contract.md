# Repo B Tick Lake Read Contract

**Document Version:** 1.0.0  
**Phase / Milestone:** Phase 19 (P4) / Milestone v4.0  
**Target Audience:** Repo B engineers, quantitative research teams, backtesting & simulation consumers.  
**Dependencies on `data-harvester`:** **NONE** (zero library imports required; uses standard `duckdb` and `pyarrow`).

---

## 1. Overview and Operational Boundaries

Historically, downstream consumers (such as Repo B) attached `data/streaming.duckdb` directly. Because DuckDB disk-backed databases permit only a single writer process or exclusive locks, concurrent access by the ingestion streamer and external readers led to `duckdb.IOException: Could not set lock on file` collisions and process crashes.

Under the Partitioned Parquet Tick Lake architecture:
1. **Readers never open or attach `streaming.duckdb`:** The legacy tick database is completely decoupled from live reads.
2. **Readers use private in-memory DuckDB connections (`:memory:`):** Queries execute against finalized, immutable Parquet batch files via DuckDB's vectorized Parquet scanner (`read_parquet`).
3. **Lock-Free Concurrency:** Reading requires zero disk locks. Writing ingestion daemons and arbitrary concurrent reader processes (or threads) operate in parallel with zero contention.
4. **Zero `data-harvester` Code Imports:** Downstream repositories only need standard, publicly available packages (`duckdb >= 1.0.0` or `pyarrow >= 14.0.0`). No modules from `src/` are required.
5. **Atomic Publication Guarantee:** Files in the active `ticks/` partition tree are published via atomic filesystem renames only after their Parquet footers and checksums are verified. Readers will never encounter truncated or partially written files.

---

## 2. Directory Layout & Partitioning Rules

### 2.1 Lake Hierarchy

The tick lake root directory (configured via `TICK_LAKE_ROOT` environment variable or volume mount) conforms to the following layout:

```text
<TICK_LAKE_ROOT>/
├── lake.json                         # Lake metadata & format version
├── ticks/                            # ACTIVE QUERY ROOT (Only query here)
│   ├── symbol=AAPL/
│   │   ├── date=2026-10-02/
│   │   │   ├── batch_w1_000001.parquet
│   │   │   └── batch_w1_000002.parquet
│   │   └── date=2026-10-03/
│   │       └── batch_w1_000003.parquet
│   └── symbol=NVDA/
│       └── date=2026-10-02/
│           └── batch_w1_000001.parquet
├── _staging/                         # IN-FLIGHT WRITES (DO NOT QUERY)
├── _maintenance/                     # MAINTENANCE OPERATIONS
│   └── in_progress.json              # Maintenance guard file (when present)
└── _control/                         # CONTROL PLANE
    ├── symbol_registry.json          # Active & inactive symbols
    ├── writer_status.json            # Streamer heartbeat & statistics
    └── receipts/                     # Publication audit receipts
```

### 2.2 Partitioning Conventions

The lake adheres to Hive two-level partitioning under the `ticks/` directory:
- **Level 1 — Symbol Partition:** `symbol=<ENCODED_SYMBOL>/`
  - Safe symbol encoding: ASCII alphanumeric characters, periods, underscores, and dashes (`[A-Za-z0-9._-]`) remain unescaped (e.g. `symbol=AAPL/`, `symbol=BRK.B/`).
  - Special characters (such as `/`, `:`, `%`, spaces) are uppercase percent-encoded (e.g. `EUR/USD` -> `symbol=EUR%2FUSD/`).
- **Level 2 — Date Partition:** `date=<YYYY-MM-DD>/`
  - Partition date is the **UTC event date** (`CAST(timestamp AS DATE)`), **not** local exchange time and **not** ingestion receive time.
  - A single US regular trading session (09:30 to 16:00 ET) spans a single UTC date during daylight saving time (13:30 to 20:00 UTC) and standard time (14:30 to 21:00 UTC).
  - Late-arriving ticks are placed into their original event-date partition.

### 2.3 Partition Pruning Rules for Readers

To guarantee fast query times and avoid unnecessary filesystem traversal:
- **Never scan the lake root recursively:** Do not execute recursive globs like `**/*.parquet` across `<TICK_LAKE_ROOT>/` because `_staging/` and `_maintenance/` contain incomplete or archived files.
- **Prune before query execution:** Resolve candidate partition directories in Python first (e.g. `ticks/symbol=AAPL/date=2026-10-02/*.parquet`) and pass the resolved file paths directly to `read_parquet([...])`.
- **Empty partitions:** If no files match a symbol or date range, return empty results immediately without querying DuckDB.

---

## 3. Physical Schema v1

Every Parquet file in `ticks/` adheres strictly to **Lake Schema v1**.

### 3.1 Column Specifications

| Column Name | DuckDB Physical Type | Arrow Physical Type | Nullable | Description |
|---|---|---|---|---|
| `timestamp` | `TIMESTAMP` (naive UTC) | `timestamp('us')` | **No** | Microsecond UTC timestamp of the quote event. Zero timezone offset. |
| `symbol` | `VARCHAR` | `string` | **No** | Canonical uppercase display symbol (e.g. `'AAPL'`, `'NVDA'`). |
| `price` | `DOUBLE` | `float64` | **No** | Observed quote/trade price (> 0.0). |
| `volume` | `DOUBLE` | `float64` | Yes | Traded volume or quote depth. **Capital observation semantics:** When null, volume coalesces to `1.0`. |
| `bid` | `DOUBLE` | `float64` | Yes | Current best bid price. |
| `ask` | `DOUBLE` | `float64` | Yes | Current best ask price. |
| `source` | `VARCHAR` | `string` | Yes | Data provider identifier (e.g. `'CAPITAL'`, `'BINANCE'`). |
| `session` | `VARCHAR` | `string` | Yes | Market session tag (e.g. `'REG'`, `'PRE'`, `'POST'`). |
| `ingest_id` | `VARCHAR` | `string` | **No** | Globally unique stable identifier for the tick (e.g. `w1_1727879400000000_0001`). |

### 3.2 Row Ordering Key

Rows within each Parquet file are strictly sorted by the composite key:
$$\text{ORDER BY } \text{timestamp ASC}, \text{ingest_id ASC}$$

### 3.3 Deterministic Resampling Tie-Breaking

When resampling ticks into OHLCV bars (e.g. 1-minute or 5-minute candles), multiple ticks may share the identical microsecond `timestamp`. To ensure 100% deterministic, reproducible candle calculations across independent systems:
- **Open Price:** Price of the tick with `MIN(timestamp, ingest_id)`:
  $$\text{open} = \text{arg\_min}(\text{price}, (\text{timestamp}, \text{ingest\_id}))$$
- **Close Price:** Price of the tick with `MAX(timestamp, ingest_id)`:
  $$\text{close} = \text{arg\_max}(\text{price}, (\text{timestamp}, \text{ingest\_id}))$$
- **High Price:** $\max(\text{price})$
- **Low Price:** $\min(\text{price})$
- **Volume:** $\sum(\text{COALESCE}(\text{volume}, 1.0))$
- **Tick Count:** $\text{COUNT}(*)$

---

## 4. Standalone DuckDB Query Snippets (Zero `data-harvester` Imports)

The following complete Python snippet demonstrates how Repo B can query the tick lake using only standard `duckdb`.

### 4.1 Resampling OHLCV Candles (1-Minute and 5-Minute)

```python
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import duckdb


class RepoBTickReader:
    """
    Zero-dependency reader for Partitioned Parquet Tick Lake.
    Can be copy-pasted directly into Repo B.
    """
    def __init__(self, lake_root: str):
        self.lake_root = Path(lake_root).resolve()
        self.ticks_dir = self.lake_root / "ticks"

    def _resolve_files(self, symbol: str, start_date: date, end_date: date) -> List[str]:
        """Prune partitions at the filesystem level before passing to DuckDB."""
        # Clean symbol for Hive partition directory
        sym_dir = self.ticks_dir / f"symbol={symbol.upper()}"
        if not sym_dir.is_dir():
            return []

        matched_files: List[str] = []
        for date_dir in sym_dir.iterdir():
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            try:
                dir_date = date.fromisoformat(date_dir.name.split("=")[1])
                if start_date <= dir_date <= end_date:
                    for parquet_file in date_dir.glob("*.parquet"):
                        matched_files.append(str(parquet_file))
            except ValueError:
                continue

        return sorted(matched_files)

    def query_candles(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1m",
    ) -> List[Dict[str, Any]]:
        """
        Resample raw ticks into deterministic OHLCV candles using in-memory DuckDB.
        """
        files = self._resolve_files(symbol, start_date, end_date)
        if not files:
            return []

        # Timeframe interval mapping for DuckDB time_bucket
        interval_map = {
            "1s": "1 second",
            "5s": "5 seconds",
            "1m": "1 minute",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "1h": "1 hour",
            "1d": "1 day",
        }
        interval_str = interval_map.get(timeframe.lower(), "1 minute")

        # Isolated in-memory connection
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone = 'UTC'")
            con.execute("SET threads = 4")
            con.execute("SET max_memory = '2GB'")

            # Deterministic OHLCV resampling via arg_min / arg_max
            query = f"""
                SELECT
                    time_bucket(INTERVAL '{interval_str}', timestamp) AS bucket_time,
                    symbol,
                    arg_min(price, (timestamp, ingest_id)) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    arg_max(price, (timestamp, ingest_id)) AS close,
                    sum(coalesce(volume, 1.0)) AS volume,
                    count(*) AS tick_count
                FROM read_parquet(?, hive_partitioning=false)
                GROUP BY bucket_time, symbol
                ORDER BY bucket_time ASC, symbol ASC
            """
            df = con.execute(query, [files]).fetchall()
            
            candles = [
                {
                    "time": row[0],
                    "symbol": row[1],
                    "open": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "close": float(row[5]),
                    "volume": float(row[6]),
                    "tick_count": int(row[7]),
                }
                for row in df
            ]
            return candles
        finally:
            con.close()
```

### 4.2 Querying Reverse-Chronological Stream Tape

```python
def query_tape(lake_root: Path, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch latest ticks in reverse-chronological order with spread calculation.
    """
    sym_dir = lake_root / "ticks" / f"symbol={symbol.upper()}"
    if not sym_dir.is_dir():
        return []

    # Get recent date partitions
    date_dirs = sorted([d for d in sym_dir.iterdir() if d.is_dir() and d.name.startswith("date=")], reverse=True)
    if not date_dirs:
        return []

    # Take files from latest active date
    files = [str(f) for f in date_dirs[0].glob("*.parquet")]
    if not files:
        return []

    con = duckdb.connect(":memory:")
    try:
        query = """
            SELECT
                timestamp,
                symbol,
                price,
                coalesce(volume, 1.0) AS volume,
                bid,
                ask,
                CASE WHEN bid IS NOT NULL AND ask IS NOT NULL THEN (ask - bid) ELSE NULL END AS spread,
                source,
                session,
                ingest_id
            FROM read_parquet(?)
            ORDER BY timestamp DESC, ingest_id DESC
            LIMIT ?
        """
        rows = con.execute(query, [files, limit]).fetchall()
        return [
            {
                "timestamp": r[0],
                "symbol": r[1],
                "price": float(r[2]),
                "volume": float(r[3]),
                "bid": float(r[4]) if r[4] is not None else None,
                "ask": float(r[5]) if r[5] is not None else None,
                "spread": round(float(r[6]), 4) if r[6] is not None else None,
                "source": r[7],
                "session": r[8],
                "ingest_id": r[9],
            }
            for r in rows
        ]
    finally:
        con.close()
```

---

## 5. Standalone PyArrow Query Snippets (Zero `data-harvester` Imports)

If Repo B prefers reading directly into Arrow RecordBatches without DuckDB:

```python
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.compute as pc


def scan_ticks_with_arrow(lake_root: Path, symbol: str, start_dt: str, end_dt: str):
    """
    Direct Arrow dataset scanner leveraging Hive directory partitioning.
    """
    ticks_root = lake_root / "ticks"
    if not ticks_root.is_dir():
        return None

    dataset = ds.dataset(
        str(ticks_root),
        format="parquet",
        partitioning=ds.partitioning(
            schema=None,
            flavor="hive",
        ),
    )

    # Push down filter expression
    expr = (pc.field("symbol") == symbol) & \
           (pc.field("timestamp") >= pc.scalar(start_dt, ds.pa.timestamp("us"))) & \
           (pc.field("timestamp") < pc.scalar(end_dt, ds.pa.timestamp("us")))

    table = dataset.to_table(filter=expr)
    return table
```

---

## 6. Maintenance Guard & Operational Safety Rules

To maintain high availability and prevent reading partially replaced data during off-hours maintenance:

### 6.1 Maintenance In-Progress Guard File

Before starting a compaction, partition rewrite, or symbol purge, the maintenance runner atomically creates:
```text
<TICK_LAKE_ROOT>/_maintenance/in_progress.json
```

**Guard Rule for Repo B:**
Before initiating extensive historical scans, backtests, or batch replays, Repo B should inspect whether this file exists:

```python
def is_lake_maintenance_in_progress(lake_root: Path) -> bool:
    guard_file = lake_root / "_maintenance" / "in_progress.json"
    return guard_file.is_file()
```

- If `in_progress.json` exists, maintenance is active. Readers should pause or retry with exponential backoff (typically 5 to 30 seconds).
- Once the file is removed, maintenance has finished and partition directories are consistent.

### 6.2 Reader Safety Invariants

1. **Read-Only Operation:** Repo B must open all files in read-only mode and must **never** create files inside `ticks/`, `_staging/`, or `_control/`.
2. **Never Query `_staging/`:** The `_staging/` directory contains active `.tmp` Parquet files being constructed by writer threads. Accessing files in `_staging/` will encounter unfinished Parquet footers or `FileNotFoundError` upon atomic rename.
3. **Partition Immutability:** Parquet batch files in `ticks/` are append-only and immutable. A file name will never be overwritten in-place.
4. **Memory & Thread Limits:** Always configure `SET max_memory` and `SET threads` on DuckDB `:memory:` sessions to avoid starvation on shared analytical hosts.
