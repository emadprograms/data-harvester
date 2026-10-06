# Repo B Tick Lake Read Contract

**Document Version:** 1.5.0
**Phase / Milestone:** Originally Phase 19 (P4) / Milestone v4.0 — reviewed and hardened in Milestone v4.1 (Phases 22–27), Milestone v4.3 (Finding C43-07), Milestone v5.0 (Parquet-only storage), and Milestone v6.0 (bid and ask prices)
**Last reviewed:** 2026-10-06
**Target Audience:** Repo B engineers, quantitative research teams, backtesting & simulation consumers.
**Dependencies on `data-harvester`:** **NONE** (zero library imports required; uses standard `duckdb` and `pyarrow`).

**Changes in 1.5.0 (Milestone v6.0):** new rows store `bid_price` and `ask_price`. Do not read `price` or `volume` as the stored quote. The lake already on disk is still schema v1. The rewrite is the last phase and is not to be run yet. Gap fill requests only all-symbol silence.

**Changes in 1.4.0 (Milestone v5.0):** the legacy `streaming.duckdb` store was migrated into the lake and deleted, so §1 now states plainly that no disk database exists; the historical bar archive is gone and candles are always resampled from ticks.

**Changes in 1.3.0 (Milestone v4.3, Finding C43-07):** (1) Retracted §7.3 claim that DuckDB silently ignores removed files; verified and documented that DuckDB raises `duckdb.IOException` when an explicit file list contains a missing file. (2) Updated §7.1 with fail-fast root state validation and structured reader exceptions (`LakeUnavailableError`, `LakeCorruptedMetadataError`, `LakeIncompatibleSchemaError`). (3) Documented support for timezone-aware datetimes and half-open intervals (`inclusive_end`).

**Changes in 1.2.0 (Milestone v4.2, Phase 33):** corrected three defects found by executing these examples against a real lake — (1) the symbol safe set is `[A-Za-z0-9_-]` and the period **is** encoded (`BRK.B` -> `BRK%2EB`), so the example reader now encodes symbols and encoded symbols are actually reachable; (2) `symbol` is physically dictionary-encoded, not plain `string`; (3) the PyArrow example no longer infers Hive partitioning, which collided with the physical `symbol` column and raised `ArrowTypeError`. Added §7.3 (snapshot semantics).

**Changes in 1.1.0:** corrected the control-plane filenames (`_control/registry.json`, plus intents and the reload signal), corrected migrated-chunk filenames, documented lake-root resolution and `lake.json` metadata, and added §7 (v4.1 hardening guarantees for readers).

---

## 1. Overview and Operational Boundaries

Historically, downstream consumers (such as Repo B) attached `data/streaming.duckdb` directly. Because DuckDB disk-backed databases permit only a single writer process or exclusive locks, concurrent access by the ingestion streamer and external readers led to `duckdb.IOException: Could not set lock on file` collisions and process crashes.

Under the Partitioned Parquet Tick Lake architecture:
1. **There is no disk database to open (v5.0):** the legacy `streaming.duckdb` was migrated into the lake and deleted by its owner; `data-harvester` no longer creates a `.duckdb` file on any path. Nothing in this contract depends on one.
2. **Readers use private in-memory DuckDB connections (`:memory:`):** Queries execute against finalized, immutable Parquet batch files via DuckDB's vectorized Parquet scanner (`read_parquet`).
3. **Lock-Free Concurrency:** Reading requires zero disk locks. Writing ingestion daemons and arbitrary concurrent reader processes (or threads) operate in parallel with zero contention.
4. **Zero `data-harvester` Code Imports:** Downstream repositories only need standard, publicly available packages (`duckdb >= 1.0.0` or `pyarrow >= 14.0.0`). No modules from `src/` are required.
5. **Atomic Publication Guarantee:** Files in the active `ticks/` partition tree are published via atomic filesystem renames only after their Parquet footers and checksums are verified. Readers will never encounter truncated or partially written files.

---

## 2. Directory Layout & Partitioning Rules

### 2.1 Lake Hierarchy

The tick lake root directory (configured via `TICK_LAKE_ROOT` environment variable or volume mount) conforms to the following layout:

```text
<TICK_LAKE_ROOT>/                     # Default: <DATA_DIR>/tick_lake, i.e. data/tick_lake
├── lake.json                         # Lake metadata, schema_version (1) & compatible_versions
├── ticks/                            # ACTIVE QUERY ROOT (Only query here)
│   ├── symbol=AAPL/
│   │   ├── date=2026-10-02/
│   │   │   ├── batch_writer_1_000001.parquet   # Live writer micro-batch
│   │   │   └── chunk_000001.parquet            # Migrated historical chunk
│   │   └── date=2026-10-03/
│   │       └── batch_writer_1_000003.parquet
│   └── symbol=NVDA/
│       └── date=2026-10-02/
│           └── batch_writer_1_000001.parquet
├── _staging/                         # IN-FLIGHT WRITES (DO NOT QUERY)
├── _maintenance/                     # MAINTENANCE OPERATIONS
│   └── in_progress.json              # Maintenance guard file (when present)
├── _migration/                       # MIGRATION ARTIFACTS (DO NOT QUERY)
│   ├── plan.json / state.json / verification.json
│   └── staging/ticks/symbol=…/date=…/chunk_NNNNNN.parquet
└── _control/                         # CONTROL PLANE
    ├── registry.json                 # Active & inactive symbols (versioned)
    ├── writer_status.json            # Streamer heartbeat & statistics
    ├── publisher.lock                # Single-writer advisory lock
    ├── intent/                       # In-flight publication intents (crash recovery)
    ├── .stream_reload.signal         # Registry mutation signal
    └── receipts/                     # Publication audit receipts
```

### 2.2 Partitioning Conventions

The lake adheres to Hive two-level partitioning under the `ticks/` directory:
- **Level 1 — Symbol Partition:** `symbol=<ENCODED_SYMBOL>/`
  - **Safe set is `[A-Za-z0-9_-]` only.** Everything else — including the period — is percent-encoded byte-by-byte with uppercase hex, matching `src/storage/config.py::encode_symbol`.
  - Worked examples: `AAPL` -> `symbol=AAPL/`, `BRK.B` -> `symbol=BRK%2EB/`, `EUR/USD` -> `symbol=EUR%2FUSD/`, `BTC/USD` -> `symbol=BTC%2FUSD/`.
  - **Consumers must encode before building a path.** Looking for `symbol=BRK.B/` returns nothing and no error; the directory is `symbol=BRK%2EB/`. The example reader in §4.1 does this via `encode_symbol()`.
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

## 3. Stored quote, schema v1 files, and gap fill

New rows store `bid_price` and `ask_price` only, plus `timestamp`, `symbol`, `source`, `session`, and `ingest_id`. Do not read `price` or `volume` as the stored quote. Capital.com stores its quote-change bid and ask. Databento `tbbo` stores `bid_px_00` as `bid_price` and `ask_px_00` as `ask_price`. Fewer Databento rows than Capital.com rows is accepted.

The existing lake is still schema v1. Those files still contain `price`, `volume`, `bid`, and `ask`. The quote in a schema v1 file is `bid` and `ask`, not `price` or `volume`. The rewrite that copies `bid` to `bid_price` and `ask` to `ask_price` is available as `python -m src.storage.quote_rewrite --lake-root <explicit-lake> --backup-root <explicit-backup>`. Production execution is deferred to the owner. Do not run that rewrite against the production lake from this checkout.

Gap fill names one day and requests Databento `tbbo` only for stretches inside 04:00-20:00 ET where every active registry symbol is silent. A minute where one symbol has a tick is not requested. Pre-market (04:00-09:30 ET) and post-market (16:00-20:00 ET) count at 15 minutes or more. Regular hours (09:30-16:00 ET) count at 2 minutes or more. A stretch that crosses 09:30 or 16:00 is split, and each piece uses its own rule. Weekends, full NYSE holidays, and the time after an official early close (13:00 ET) are not requested. Gap fill does not start while the live writer holds the publisher lock. It appends new rows and does not rewrite existing files. Name exactly one day with `python -m src.data.gap_fill --date YYYY-MM-DD`.

A new-row candle uses `bid_price`. This query does not match files already on disk:

```sql
SELECT
    time_bucket(INTERVAL '1 minute', timestamp) AS bucket_time,
    arg_min(bid_price, (timestamp, ingest_id)) AS open,
    max(bid_price) AS high,
    min(bid_price) AS low,
    arg_max(bid_price, (timestamp, ingest_id)) AS close
FROM read_parquet(?)
GROUP BY bucket_time
```

### 3.1 Physical Schema v1

Files already on disk use this table. `price` and `volume` in it are not the stored quote. New rows do not use this column set. `lake.json` remains schema version 1 until the last-phase rewrite, which must not be run yet.

### 3.2 Column Specifications

| Column Name | DuckDB Physical Type | Arrow Physical Type | Nullable | Description |
|---|---|---|---|---|
| `timestamp` | `TIMESTAMP` (naive UTC) | `timestamp('us')` | **No** | Microsecond UTC timestamp of the quote event. Zero timezone offset. |
| `symbol` | `VARCHAR` | `dictionary<values=string, indices=int32>` | **No** | Canonical uppercase display symbol (e.g. `'AAPL'`, `'NVDA'`). Written dictionary-encoded; it reads back as text, but a schema that asserts plain `string` will not match. |
| `price` | `DOUBLE` | `float64` | **No** | Retired value on schema v1 files. Not the stored quote. |
| `volume` | `DOUBLE` | `float64` | Yes | Present only on schema v1 files. Not the stored quote. A null in these old files coalesces to `1.0` in the historical example below. |
| `bid` | `DOUBLE` | `float64` | Yes | Best bid. This is the quote on a schema v1 file. |
| `ask` | `DOUBLE` | `float64` | Yes | Best ask. This is the quote on a schema v1 file. |
| `source` | `VARCHAR` | `string` | Yes | Data provider identifier (e.g. `'CAPITAL'`, `'BINANCE'`). |
| `session` | `VARCHAR` | `string` | Yes | Market session tag (e.g. `'REG'`, `'PRE'`, `'POST'`). |
| `ingest_id` | `VARCHAR` | `string` | **No** | Globally unique stable identifier for the tick (e.g. `w1_1727879400000000_0001`). |

### 3.3 Row Ordering Key

Rows within each Parquet file are strictly sorted by the composite key:
$$\text{ORDER BY } \text{timestamp ASC}, \text{ingest_id ASC}$$

### 3.4 Deterministic Resampling Tie-Breaking

The formulas in this subsection describe files already on disk. They are not the stored quote. New rows resample from `bid_price`. Do not read `price` or `volume` as the stored quote.

When resampling ticks from a schema v1 file, multiple ticks may share the identical microsecond `timestamp`. To ensure 100% deterministic, reproducible candle calculations across independent systems:
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

### 4.1 Resampling candles from files already on disk

The example below reads files already on disk. Those files are still schema v1. `price` and `volume` in that example are not the stored quote. New rows use `bid_price` and `ask_price`. Do not run the rewrite.

```python
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import duckdb


# Only these characters survive encoding; everything else is percent-encoded
# byte-by-byte with uppercase hex. The period is NOT safe: BRK.B -> BRK%2EB.
SAFE_SYMBOL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)


def encode_symbol(symbol: str) -> str:
    """Encode a display symbol into its partition-directory form.

    Consumers MUST call this before building a path: an unencoded lookup for a
    symbol such as BRK.B or EUR/USD matches nothing and reports no error.
    """
    encoded = []
    for ch in symbol:
        if ch in SAFE_SYMBOL_CHARS:
            encoded.append(ch)
        else:
            encoded.extend(f"%{byte:02X}" for byte in ch.encode("utf-8"))
    return "".join(encoded)


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
        # The directory name holds the ENCODED symbol, not the display symbol.
        sym_dir = self.ticks_dir / f"symbol={encode_symbol(symbol.upper())}"
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

This example also reads files already on disk. `price` and `volume` in it are not the stored quote.

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
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc


def scan_ticks_with_arrow(lake_root: Path, symbol: str, start_dt: str, end_dt: str):
    """
    Direct Arrow dataset scanner over the finalized partition tree.
    """
    ticks_root = lake_root / "ticks"
    if not ticks_root.is_dir():
        return None

    dataset = ds.dataset(
        str(ticks_root),
        format="parquet",
        # Do NOT infer Hive partitioning here. The files already carry physical
        # `symbol` and `timestamp` columns, and inferring a string `symbol`
        # partition key collides with the dictionary-encoded physical column:
        # pyarrow raises ArrowTypeError: Unable to merge: Field symbol has
        # incompatible types. Filter on the physical columns instead.
    )

    # Timestamp bounds must be typed to match the physical timestamp[us] column.
    start = pa.scalar(datetime.fromisoformat(start_dt), type=pa.timestamp("us"))
    end = pa.scalar(datetime.fromisoformat(end_dt), type=pa.timestamp("us"))

    expr = (
        (pc.field("symbol") == symbol)
        & (pc.field("timestamp") >= start)
        & (pc.field("timestamp") < end)
    )

    table = dataset.to_table(filter=expr)
    return table
```

> **Note:** `pc.scalar(value, type)` is not a valid call in current PyArrow
> releases; the two-argument form was removed. Use `pa.scalar(value, type=...)`
> as above.

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

---

## 7. Version 1.1 Addendum — v4.1 Hardening Guarantees for Readers

Milestone v4.1 (Phases 22–27) added 122 adversarial tests over the v4.0 lake implementation without changing the read contract. The following guarantees are now backed by executables in the `data-harvester` repository; downstream consumers can rely on them:

1. **Publication atomicity under collisions:** Concurrent publication attempts, crashed publication intents, and single-writer lock contention are exercised; readers never observe partial files, and previously published data remains readable throughout (`tests/storage/test_storage_edge_cases.py`, `tests/storage/test_crash_recovery.py`).
2. **Zero-loss historical migration under fuzzing:** Two-way `EXCEPT ALL` reconciliation preserves row counts, duplicate multiplicity, and float precision even when the legacy source is corrupt, partially written, or schema-drifted (`tests/storage/test_migration_stress.py`).
3. **Reader scaling:** 30+ concurrent in-memory DuckDB readers and 1,000+ sequential queries complete without memory leaks or file-lock errors (`tests/storage/test_lake_reader_stress.py`).
4. **Deterministic resampling edges:** Sparse partitions, multi-day roll-overs, DST transitions, and leap-year boundaries produce deterministic, gap-consistent bars (`tests/storage/test_lake_reader_stress.py`, `tests/dashboard/test_analytics.py`).
5. **Tape pagination correctness:** Reverse-chronological tape reads remain stable at high offsets and return empty results (rather than errors) for non-existent symbols or partitions (`tests/storage/test_lake_reader_stress.py`).
6. **No hidden writer coupling:** Readers continue to operate while the live writer ingests and while the supervisor restarts children under chaos conditions (`tests/integration/test_supervisor_chaos_soak.py`).

### 7.1 Lake Metadata Contract & Reader Exceptions

`lake.json` declares `format = "tick_lake"`, `schema_version = 1`, and `compatible_versions = [1]`.

The behaviour of the shipped reader (`src/storage/reader.py`) in Milestone v4.3 adheres to strict fail-fast validation:

- **Root State Validation & Structured Exceptions:** Rather than silently returning empty results on missing, misconfigured, or corrupt roots, the reader performs upfront validation (`validate_lake()`):
  - Missing path or non-directory root raises `LakeUnavailableError`.
  - Missing, unreadable, or invalid JSON `lake.json` raises `LakeCorruptedMetadataError`.
  - Incompatible `schema_version` (or unrecognized format) raises `LakeIncompatibleSchemaError`.
  - All lake reader exceptions inherit from `LakeReaderError` (which in turn inherits from `DataHarvesterError` and `StorageError`), allowing callers to catch them selectively or as a group.
- **Distinguishing Legitimate Empty Results from Errors:** When the lake root exists and contains a valid `lake.json` but has no data partitions or ticks for a requested symbol/date range, queries return legitimate empty results (`[]`) without error.
- **Maintenance Guard:** While `_maintenance/in_progress.json` exists, queries fail fast with `LakeMaintenanceInProgressError`.
- **Query Range Flexibility:** The reader normalizes datetime inputs across timezone-aware datetimes (converting to UTC naive), naive datetimes, `date` objects, and ISO strings, and supports half-open `[start, end)` intervals via `inclusive_end=False` (defaulting to `inclusive_end=True`).

### 7.2 Verification Commands

```bash
pytest tests/storage/test_lake_reader_stress.py -v     # concurrent readers, resampling edges, tape pagination
pytest tests/storage/test_migration_stress.py -v       # zero-loss + fuzz reconciliation
pytest tests/storage/test_storage_edge_cases.py -v     # publication/collision/recovery edges
pytest tests/integration/ -v                           # multi-process concurrency, soak, chaos
pytest tests/contract/ -v                              # contract examples, isolation, barrier snapshot race
```

### 7.3 Snapshot Semantics and Stale Resolutions

A read is a snapshot over the **file list resolved at the start of the request**.
Files under `ticks/` are append-only and immutable: a batch file is published by
atomic rename and never rewritten in place. The consequences for consumers are:

1. **Newly finalized files appear on the next request.** A batch published after
   resolution is invisible to the in-flight request and visible to the next one.
   There is no need to invalidate anything: create a new reader or re-resolve.
2. **Never cache a resolved file list.** Resolve immediately before querying.
3. **A file removed between resolve and query raises `duckdb.IOException` (Retraction of v1.2.0 claim).**
   In v1.2.0, it was hypothesized that DuckDB silently ignored files removed after resolution.
   Rigorous barrier-synchronized testing reveals that when DuckDB's `read_parquet` is passed an
   explicit list of resolved file paths, missing files are **never** silently skipped; DuckDB
   raises `duckdb.IOException` (e.g. `No files found that match the pattern "..."`).
   Readers will not silently produce partial results. If compaction or maintenance removes
   a resolved file before query execution, the operation fails fast with `duckdb.IOException`
   (or `LakeReaderError`), signalling the reader to re-resolve partition files and retry.
4. **Staging, migration and retired artifacts are never part of a snapshot.**
   `_staging/`, `_migration/` and `_maintenance/` are outside `ticks/` and are
   excluded by construction, not by filter.
5. **Maintenance guard.** While `_maintenance/in_progress.json` exists, the
   shipped reader refuses to operate (`LakeMaintenanceInProgressError`); the
   standalone examples in this document do not check it, so call
   `is_lake_maintenance_in_progress()` first if partial reads matter to you.

### 7.4 Document History

| Version | Date | Milestone | Summary |
|---|---|---|---|
| 1.0.0 | 2026-10-03 | v4.0 (P4) | Initial read contract for downstream consumers. |
| 1.1.0 | 2026-10-03 | v4.1 | Corrected control-plane filenames and chunk naming; documented lake root resolution and `lake.json`; added hardening guarantees (§7). |
| 1.2.0 | 2026-10-04 | v4.2 (Phase 33) | Fixed symbol encoding rule and example, corrected the physical `symbol` type, fixed the PyArrow example, and documented snapshot semantics (§7.3). |
| 1.3.0 | 2026-10-04 | v4.3 (C43-07) | Retracted §7.3 silent-partial claim (documented duckdb.IOException on missing files), updated §7.1 fail-fast root validation contract, and documented timezone-aware / half-open interval semantics. |
| 1.4.0 | 2026-10-05 | v5.0 | Parquet-only storage: the legacy disk database no longer exists; candles are always resampled from ticks. |
| 1.5.0 | 2026-10-06 | v6.0 | New rows store `bid_price` and `ask_price`. The lake already on disk is still schema v1. The rewrite is the last phase and is not to be run yet. |
