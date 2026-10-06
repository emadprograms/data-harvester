# Partitioned Parquet Tick Lake: Production Operations Guide

**Document Version:** 2.1.0
**Phase / Milestone:** Originally Phase 21 (P8) / Milestone v4.0 — revised for Milestone v5.0 (Parquet-only storage) and Milestone v6.0 (bid and ask prices)
**Last reviewed:** 2026-10-06
**Applicability:** Production Operators, Site Reliability Engineers, Platform Architects, Quant Analytics Teams (Repo B).

**Changes in 2.1.0:** new rows store `bid_price` and `ask_price`. Do not read `price` or `volume` as the stored quote. The lake already on disk is still schema v1. The rewrite is the last phase and is not to be run yet. Gap fill requests only all-symbol silence. Physical schema remains §2.5; backend selection remains §2.6.

**Changes in 2.0.0:** v5.0 deleted the disk-database layer (`src/database/`) and the historical 1-minute bar archive. The lake is the only store; §2.6 documents the rule with no legacy backend, §3 is macOS-only (`launchd`), and §5.4 records the post-migration deletion of the legacy database files.

---

## 1. System Architecture & High-Concurrency Design

### 1.1 Why the Disk Database Went Away
Milestones v1.0–v3.0 wrote every tick into a single disk-backed DuckDB file that every reader also opened, so DuckDB's exclusive file lock turned concurrency into collisions (`duckdb.IOException: Could not set lock on file`). v4.0 replaced that store with immutable Parquet micro-batches; **v5.0 finished the job** — the disk-database layer, the historical bar archive and its harvesters are deleted. No runtime path creates a `.duckdb` file any more.

### 1.2 The Lake Architecture
Ingestion is decoupled from every query path. The writer publishes immutable Parquet micro-batches into a Hive-partitioned directory hierarchy, and each reader runs its own private in-memory DuckDB engine over those files.

```
                      ┌─────────────────────────────────────────┐
                      │    WebSocket Ingestion (Capital.com)    │
                      └────────────────────┬────────────────────┘
                                           │ (async ticks)
                                           ▼
                      ┌─────────────────────────────────────────┐
                      │       TickLakeWriter (Single Owner)     │
                      │   - Bounded Queue & Normalization       │
                      │   - Dedicated Off-Loop PyArrow Worker   │
                      │   - Flush Triggers (runner: 2.0s;       │
                      │     writer default: 5,000 rows / 5.0s)  │
                      └────────────────────┬────────────────────┘
                                           │ Atomic Rename (.tmp -> final)
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        Partitioned Parquet Tick Lake (TICK_LAKE_ROOT)                  │
│                                                                                        │
│   ticks/symbol=AAPL/date=2026-10-03/batch_writer_1_000001.parquet                      │
│   ticks/symbol=MSFT/date=2026-10-03/batch_writer_1_000002.parquet                      │
│   _control/  [registry.json, writer_status.json, publisher.lock, receipts/, intent/]   │
│   _staging/  [in-flight .tmp Parquet files and uncommitted buffers]                    │
│   _maintenance/ [in_progress.json guard, pre-compacted staging, retirement archives]   │
│   _migration/   [plan.json, state.json, verification.json, staging/]                   │
│   _spool/       [optional durable disk spool]                                          │
└───────────────────────┬───────────────────────────────────────┬────────────────────────┘
                        │                                       │
         Vectorized Parquet Read (read_parquet)  Vectorized Parquet Read (read_parquet)
         Isolated in-memory (:memory:) DuckDB    Isolated in-memory (:memory:) DuckDB
                        │                                       │
                        ▼                                       ▼
        ┌───────────────────────────────┐       ┌───────────────────────────────┐
        │  Data Harvester Dashboard API │       │   Repo B Quant Engine         │
        │  (src.dashboard.server)       │       │   (External Reader - ZERO     │
        │  - Port 8420 (REST / UI)      │       │    data-harvester imports)    │
        │  - Candles, Tape, Continuity  │       │   - Replay, Backtest, Signals │
        └───────────────────────────────┘       └───────────────────────────────┘
```

### 1.3 Key Architectural Tenets
1. **Lock-Free Concurrency:** There is no shared database file to lock. Every reader instantiates a private, thread-local in-memory DuckDB instance (`duckdb.connect(":memory:")`) and scans immutable files via `read_parquet(...)`.
2. **Atomic Publication Guarantee:** Ingestion writes to unique `.tmp` files inside `_staging/`. Only after Parquet footer serialisation, row-count validation, and schema checks succeed is the file moved to `ticks/` via an atomic POSIX filesystem rename (`os.replace`). Readers never encounter truncated files or corrupted footers.
3. **Decoupled Control Plane:** Administrative metadata, active symbol registries, writer telemetry, and publication audit receipts reside in `_control/` as atomic, versioned JSON documents. Dynamic configuration changes never require locking data files.
4. **Zero-Dependency Downstream Integration:** Quantitative pipelines (Repo B) require zero code imports from `data-harvester`. Standard DuckDB (`>= 1.0.0`) or PyArrow (`>= 14.0.0`) libraries read the Parquet partitions directly.

---

## 2. Environment Configuration & Directory Setup

### 2.1 Environment Variables Reference

Only the variables below are read by the runtime (verified against `src/storage/config.py`, `src/credentials.py`, and `src/dashboard/server.py`).

| Variable Name | Default Value | Description | Production Guidance |
|---|---|---|---|
| `TICK_LAKE_ROOT` | `<DATA_DIR>/tick_lake` | Root path of the Partitioned Parquet Tick Lake. | **Mandatory in production.** Must point to the high-performance NVMe/SSD mount (e.g. `/Volumes/Crucial X9/data-harvester/data/tick_lake`). |
| `DATA_DIR` | `<repo_root>/data` | Base directory for data harvester assets (`tick_lake/`, logs, run artifacts). | If set, `tick_lake` is resolved beneath it. |
| `DASHBOARD_PORT` (or `PORT`) | `8420` | TCP port for the Dashboard HTTP REST API and UI. | Fallback ports `8421`, `8422`, `8425` are attempted automatically when `8420` is busy. |
| `CAPITAL_COM_X_CAP_API_KEY` / `CAPITAL_COM_IDENTIFIER` / `CAPITAL_COM_PASSWORD` | — | Capital.com WebSocket credentials. | Required for live streaming (loaded from `.env`). |
| `SKIP_DISCORD` | unset | When `true`, suppresses Discord webhook notifications (`src/utils/discord.py`). | Useful for automated/offline runs; a webhook failure never blocks ingestion. |

**Lake-root resolution precedence** (`resolve_tick_lake_root`): explicit argument → `TICK_LAKE_ROOT` → `DATA_DIR/tick_lake` → `/Volumes/Micron-E 0256 A/data-harvester/data/tick_lake` (if mounted) → `<repo_root>/data/tick_lake` → `StorageConfigError`. A broken `data` symlink raises `StorageConfigError` rather than silently falling back to internal storage.

> ⚠️ **Documented-but-unwired knobs.** `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS`, `STREAM_MAX_QUEUE_SIZE`, and `STREAM_COMPRESSION` appear in `.env.example` but are **not read by the runtime** (grep-verified: no `getenv`/`environ` reader exists for them). The effective settings come from constructor defaults and are listed in §2.2.

### 2.2 Effective Runtime Defaults (constructor arguments)

| Setting | Class / Call Site | Default |
|---|---|---|
| Flush interval | `StreamingEngine(flush_interval=…)` → `TickLakeWriter.flush_interval_seconds` | `5.0s` (runner default and writer-class default agree; `DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS`) |
| Max rows per batch | `TickLakeWriter(max_batch_rows=…)` | `5000` |
| Queue capacity | `StreamingEngine(max_queue_size=…)` / `TickLakeWriter(max_queue_size=…)` | `10000` ticks |
| Compression codec | `TickLakeWriter(compression=…)` | `snappy` |
| Writer retry policy | `TickLakeWriter(retry_attempts=…, retry_backoff_base=…)` | `3` attempts, `0.01s` base |
| Reader threads / memory | `TickLakeReader(max_threads=…, max_memory=…)` | `4` threads / `2GB` |
| Registry poll / debounce | `StreamingEngine(registry_poll_interval=…, registry_debounce_interval=…)` | `1.0s` / `0.05s` |
| Writer ID | `StreamingEngine(writer_id=…)` | `writer_1` |

### 2.3 Lake Directory Hierarchy

The tick lake layout strictly segregates queryable data, staging areas, administrative control files, and maintenance artifacts:

```text
<TICK_LAKE_ROOT>/
├── lake.json                           # Format metadata, schema version (v1), created_at
├── ticks/                              # ACTIVE QUERY ROOT (Only query here)
│   ├── symbol=AAPL/
│   │   ├── date=2026-10-02/
│   │   │   ├── batch_writer_1_000001.parquet
│   │   │   └── chunk_000001.parquet          # Migrated historical chunk
│   │   └── date=2026-10-03/
│   │       └── batch_writer_1_000002.parquet
│   ├── symbol=MSFT/
│   │   └── date=2026-10-03/
│   │       └── batch_writer_1_000001.parquet
│   └── symbol=EUR%2FUSD/               # Percent-encoded symbols for special characters
│       └── date=2026-10-03/
│           └── batch_writer_1_000001.parquet
├── _staging/                           # IN-FLIGHT WRITES (DO NOT QUERY)
│   └── tmp_writer_1_000003_a9b8c7.parquet.tmp
├── _control/                           # CONTROL PLANE & AUDIT RECEIPTS
│   ├── registry.json                   # Versioned JSON symbol inventory
│   ├── writer_status.json              # Streamer PID, throughput, heartbeat, metrics
│   ├── publisher.lock                  # Advisory lock file for single writer daemon
│   ├── .stream_reload.signal           # Touched on registry mutation (dynamic reload)
│   ├── intent/                         # In-flight publication intents (crash recovery)
│   └── receipts/                       # Immutable publication audit receipts
│       └── batch_writer_1_000001.json
├── _maintenance/                       # OFFLINE MAINTENANCE ARTIFACTS
│   ├── in_progress.json                # Maintenance guard file (created during compaction)
│   ├── staging/                        # Compacted Parquet candidate batches
│   └── retired/                        # Retired raw batches pending purge
├── _migration/                         # HISTORICAL MIGRATION ARTIFACTS
│   ├── plan.json                       # Chunk migration plan
│   ├── state.json                      # Progress checkpoints for zero-loss export
│   ├── verification.json               # Two-way EXCEPT ALL audit report
│   └── staging/ticks/symbol=…/date=…/chunk_NNNNNN.parquet   # Staged export chunks
└── _spool/                             # OPTIONAL DURABLE DISK SPOOL (reserved)
```

### 2.4 Partition Pruning Rules for Operators and Query Engines
- **Never scan the lake root recursively:** Recursive scans (`**/*.parquet`) over `<TICK_LAKE_ROOT>/` will erroneously traverse `_staging/`, `_maintenance/`, and `_migration/`, encountering incomplete or staged files.
- **Always prune by symbol and UTC date:** Resolving candidate directory paths in Python (e.g. `ticks/symbol=AAPL/date=2026-10-03/*.parquet`) before dispatching to DuckDB `read_parquet([...])` eliminates unnecessary directory traversal and speeds up query response times by over 95%.
- **Safe Symbol Encoding:** Symbols containing special characters (e.g., `/`, `:`, `%`, space) are uppercase percent-encoded (e.g., `EUR/USD` $\to$ `symbol=EUR%2FUSD`). Standard ASCII alphanumeric characters, periods, underscores, and hyphens (`[A-Za-z0-9._-]`) remain unescaped.
- **Partition date is UTC event date:** `date=<YYYY-MM-DD>` is the UTC event date (`CAST(timestamp AS DATE)`), not local exchange time. Late-arriving ticks land in their original event-date partition.

### 2.4.1 Stored quote and gap fill

New rows store `bid_price` and `ask_price` only, plus `timestamp`, `symbol`, `source`, `session`, and `ingest_id`. Do not read `price` or `volume` as the stored quote. Capital.com stores its quote-change bid and ask. Databento `tbbo` stores `bid_px_00` as `bid_price` and `ask_px_00` as `ask_price`. Fewer Databento rows than Capital.com rows is accepted.

The existing lake is still schema v1. Those files still contain `price`, `volume`, `bid`, and `ask`. The quote in a schema v1 file is `bid` and `ask`, not `price` or `volume`. The rewrite that copies `bid` to `bid_price` and `ask` to `ask_price` is the last phase. Do not run that rewrite. It is not available from this checkout, and it is not to be run yet.

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

### 2.5 Physical Schema v1 (9 columns)

Files already on disk use this table. `price` and `volume` in it are not the stored quote.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `timestamp` | `TIMESTAMP` (naive UTC, µs) | No | Parquet `timestamp('us')` |
| `symbol` | `VARCHAR` | No | Canonical uppercase display symbol. Physically written dictionary-encoded (`dictionary<string, int32>`); reads back as text. |
| `price` | `DOUBLE` | No | Retired value on schema v1 files. Not the stored quote. |
| `volume` | `DOUBLE` | Yes | Present only on schema v1 files. Not the stored quote. |
| `bid` | `DOUBLE` | Yes | Best bid. This is the quote on a schema v1 file. |
| `ask` | `DOUBLE` | Yes | Best ask. This is the quote on a schema v1 file. |
| `source` | `VARCHAR` | Yes | Provider identifier (`CAPITAL`, `BINANCE`, …) |
| `session` | `VARCHAR` | Yes | Session tag (`REG`, `PRE`, `POST`, …) |
| `ingest_id` | `VARCHAR` | No | Stable unique ingestion identity (writer-generated string; migrated rows use `mig_<symbol>_<YYYYMMDD>_<index:08d>`) |

Rows are stored ordered by `(timestamp ASC, ingest_id ASC)`.

### 2.6 Backend Selection and Fail-Closed Behaviour

The lake is the only store, so there is nothing to fall back to:

| Selection | Condition | Behaviour on failure |
|---|---|---|
| **Explicit lake** | `TICK_LAKE_ROOT` or `DATA_DIR` is set | **Fails closed.** Any fault — unresolvable root, missing or corrupt `lake.json`, maintenance active, publisher lock conflict — raises. |
| **Autodetected lake** | Neither variable is set, and the lake looks populated (`ticks/`, `_control/writer_status.json`, or `lake.json` exists) | Uses the lake |
| **No lake** | Neither variable is set and no lake can be resolved | `LakeUnavailableError`; the dashboard reports `tick lake unavailable` and the streamer refuses to start. No data is invented. |

Implemented by `_get_lake_reader()` in `src/dashboard/analytics.py`, which loads
and validates `lake.json` **before** choosing the reader, so an empty or damaged
explicitly-selected lake cannot masquerade as "no data".

**Operational consequence:** a dashboard read error is a genuine configuration or
storage fault. Fix the lake, do not look for another backend — v5.0 has none.

> ℹ️ **API label.** The dashboard JSON payloads still carry `"database": "streaming"`.
> That is the historical source label the tick lake has emitted since v4.0 — it is
> not a selector and there is no second store behind it. Renaming it would break
> Repo B consumers for cosmetic gain.

---

## 3. Production Service Management

Data Harvester provides a dual-layer production management architecture on macOS: a multi-threaded service supervisor for active monitoring and auto-healing, combined with OS-native `launchd` agents for start-at-login.

### 3.1 Service Supervisor (`tools/service_supervisor.py`)
The supervisor process coordinates background execution of the streamer and dashboard:
- **Process Supervision:** Monitors child processes; captures stdout/stderr into rotating log files under `logs/` (rotated at 20 MB).
- **Code & Git Auto-Reload:** Watches `src/` and `.git/HEAD`. When new code is pulled or committed, child processes are automatically restarted cleanly.
- **Crash Auto-Healing:** Detects process crashes and automatically restarts the child with exponential backoff (default factor `3.0`, capped at 30 seconds) to prevent CPU thrashing. The crash counter resets after a `--stability-threshold` (default 30s) of healthy uptime.
- **Graceful Shutdown:** Catches `SIGINT` and `SIGTERM`. Grants children a drain period so the streamer's bounded queue can flush before issuing `SIGKILL`.
- **Key CLI flags:** `--name`, `--module`, `--watch` (default `src`), `--git-sync`, `--poll-interval` (2.0s), `--backoff-factor` (3.0), `--max-backoff` (30s), `--stability-threshold` (30s), `--max-restarts`, `--log-dir`, `--args`.

### 3.2 macOS Production Scripts (`tools/mac/`)

| Script | Purpose | Command |
|---|---|---|
| `start_services.sh` | Starts both Streamer and Dashboard under supervised background execution. | `./tools/mac/start_services.sh` |
| `stop_services.sh` | Gracefully stops all supervisors, streamers, and dashboard processes. | `./tools/mac/stop_services.sh` |
| `status_services.sh` | Displays live PIDs, supervisor states, port bindings, and recent log outputs. | `./tools/mac/status_services.sh` |
| `install_startup.sh` | Generates and loads `launchd` agents (`~/Library/LaunchAgents/com.dataharvester.*.plist`). | `./tools/mac/install_startup.sh` |
| `uninstall_startup.sh`| Unloads and removes `launchd` agents for clean teardown. | `./tools/mac/uninstall_startup.sh` |
| `start_streamer.sh` | Starts the ingestion runner as a standalone background process. | `./tools/mac/start_streamer.sh` |
| `start_dashboard.sh` | Starts the dashboard REST server on port 8420. | `./tools/mac/start_dashboard.sh` |

Repository-root convenience wrappers (`START_SERVICES.sh`, `VIEW_STATUS.sh`, `STOP_SERVICES.sh`) delegate to the matching `tools/mac/` scripts. All Mac scripts resolve `./.venv/bin/python` first and fall back to `python3`.

### 3.3 Dynamic Symbol Management & Dynamic Reload
1. **Adding a Symbol:**
   ```bash
   curl -X POST http://localhost:8420/api/streaming/symbols \
     -H "Content-Type: application/json" \
     -d '{"display_name": "TSLA", "epic": "TSLA", "source": "CAPITAL"}'
   ```
   - Persisted atomically to `<TICK_LAKE_ROOT>/_control/registry.json`.
   - Increments registry `version`.
   - Touches `<TICK_LAKE_ROOT>/.stream_reload.signal` (and `_control/.stream_reload.signal` when present).
   - Streamer runner polls version changes (default 1.0s) and dynamically subscribes to the Capital.com WebSocket feed without process restart.

2. **Deactivating / Toggling a Symbol:**
   ```bash
   curl -X PATCH http://localhost:8420/api/streaming/symbols \
     -H "Content-Type: application/json" \
     -d '{"display_name": "TSLA", "status": "INACTIVE"}'
   ```
   - Streamer unhooks live WebSocket ticks immediately.

3. **Deleting a Symbol (Pending Purge Lifecycle):**
   ```bash
   curl -X DELETE http://localhost:8420/api/streaming/symbols \
     -H "Content-Type: application/json" \
     -d '{"display_name": "TSLA"}'
   ```
   - Symbol state transitions to `PENDING_PURGE`.
   - Active WebSocket callbacks drop incoming ticks immediately (subscription fencing).
   - Re-adding the symbol is strictly blocked (`SymbolPendingPurgeError`) to prevent resurrected historical state.
   - Physical deletion of data files occurs during scheduled offline maintenance.

---

## 4. Offline Maintenance & Compaction Rules

### 4.1 Append-Only Policy
The production tick lake is strictly append-only during live market capture:
- Live batches written by `TickLakeWriter` are immutable once published.
- In-place mutation, overwriting, or deletion of active Parquet files is prohibited while ingestion is running.
- Accumulation budget: live micro-batching produces multiple small Parquet files per active symbol per day. Over a 30-day period, a 20-symbol universe accumulates thousands of small files — schedule compaction before this becomes a scan-efficiency problem.

### 4.2 Maintenance In-Progress Guard (`in_progress.json`)
Before executing any partition maintenance, file compaction, or physical symbol purge, operators must establish the maintenance guard:

```bash
# Example atomic creation of maintenance guard file
cat <<EOF > "<TICK_LAKE_ROOT>/_maintenance/in_progress.json"
{
  "operation": "compaction",
  "started_at": "2026-10-03T23:00:00Z",
  "symbols": ["AAPL", "MSFT", "NVDA"],
  "operator_pid": $$
}
EOF
```

**Guard Invariant:**
- Synthetic/offline maintenance tooling and `TickLakeReader`-based readers inspect `_maintenance/in_progress.json`; `load_lake_metadata` raises `LakeMaintenanceInProgressError` while it exists.
- While present, extensive scans should be paused and background readers should back off.
- Remove the guard only after compaction/purge completes and the partition tree is consistent.

> ⚠️ Do not run compaction while the live writer owns `_control/publisher.lock`. The runner takes `_maintenance/in_progress.json` and fences new publishers; off-hours scheduling (weekdays 20:00–04:00 ET) is handled by the supervisor (Phase 48).

### 4.3 Off-Hours Compaction Procedure (P7a Protocol)
1. **Pre-requisite:** Live market capture is closed or paused; readers are notified.
2. **Select Candidates:** Identify target symbol and UTC date partition with multiple micro-batches (e.g. `ticks/symbol=AAPL/date=2026-10-03/*.parquet`).
3. **Pre-build Compacted Batch:** Read all candidate batches into an in-memory DuckDB table, sort by `(timestamp ASC, ingest_id ASC)`, and write a single consolidated Parquet file in `_maintenance/staging/`:
   ```sql
   COPY (
       SELECT * FROM read_parquet('ticks/symbol=AAPL/date=2026-10-03/*.parquet')
       ORDER BY timestamp ASC, ingest_id ASC
   ) TO '_maintenance/staging/AAPL_2026-10-03_compacted_001.parquet' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
   ```
4. **Reconcile Multiplicity:** Run two-way `EXCEPT ALL` between the raw source micro-batches and the candidate compacted file. The difference must be 0 in both directions.
5. **Establish Guard:** Atomically create `_maintenance/in_progress.json`.
6. **Retire & Promote:**
   - Move raw micro-batches to `_maintenance/retired/`.
   - Atomically move `_maintenance/staging/AAPL_2026-10-03_compacted_001.parquet` to `ticks/symbol=AAPL/date=2026-10-03/2026-10-03_compacted_001.parquet`.
7. **Release Guard:** Unlink `_maintenance/in_progress.json`.

### 4.4 Physical Symbol Purge
The purge is automated; do not move directories by hand. A symbol becomes purgeable when
it is fenced out of the registry (`remove_symbol` → `PENDING_PURGE`, `active=False`),
which is what the dashboard's remove-symbol action does. Then:

```bash
python3 -m src.storage.compaction --purge SYMBOL
```

`purge_symbol_physical` holds the publisher lock, deletes only
`ticks/symbol=<ENCODED_SYMBOL>/`, archives the generation via `complete_purge`, and
refuses to touch an active symbol. It is crash-safe: the registry keeps `PENDING_PURGE`
until the files are gone, so an interrupted purge is simply re-run. Live writers for
other symbols are unaffected.

---

## 5. Legacy Migration (One-Time, Owner-Run) and Store Deletion

The historical migration utility (`tools/migrate_streaming_to_parquet.py`) provides zero-loss migration of legacy `streaming.duckdb` data into the Partitioned Parquet Lake. It is the **only** tool permitted to open a disk database (enforced by `tests/test_disk_database_layer_removed.py`).

### 5.1 The 4-Stage Lifecycle

```text
 ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
 │ 1. PLAN      │ ──> │ 2. EXPORT    │ ──> │ 3. VERIFY    │ ──> │ 4. PUBLISH   │
 │ Schema/Count │     │ Chunked Copy │     │ 2-Way EXCEPT │     │ Atomic Move  │
 │ Inspection   │     │ & Stable IDs │     │ ALL Check    │     │ into ticks/  │
 └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

1. **Stage 1: PLAN (`--mode plan`)**
   - Connects to the source DuckDB in read-only mode (`read_only=True`).
   - Discovers distinct symbols and UTC date partitions.
   - Computes row counts, null profiles, and chunk boundaries (default chunk size: 100,000 rows).
   - Generates `<TICK_LAKE_ROOT>/_migration/plan.json`.
   - **Zero mutation:** Performs no writes against any database or filesystem.

2. **Stage 2: EXPORT (`--mode export`)**
   - Iterates through planned chunks using memory-bounded `fetchmany`.
   - Synthesizes deterministic, globally unique ingest IDs:
     $$\text{mig\_}\{symbol\}\_\{YYYYMMDD\}\_\{index:08d\}$$
   - Encodes batches into Lake Schema v1 Parquet files under `_migration/staging/ticks/symbol=…/date=…/chunk_NNNNNN.parquet`.
   - Persists partition progress to `_migration/state.json`. Fully resumable across interruptions (`--resume`).

3. **Stage 3: VERIFY (`--mode verify`)**
   - Rigorously tests data integrity using mathematical set difference with duplicate multiplicity:
     - **Direction 1:** $\text{Legacy Source} \text{ EXCEPT ALL } \text{Parquet} = \emptyset$
     - **Direction 2:** $\text{Parquet} \text{ EXCEPT ALL } \text{Legacy Source} = \emptyset$
   - Reconciles the 8 source columns (`timestamp`, `symbol`, `price`, `volume`, `bid`, `ask`, `source`, `session`); `ingest_id` is synthesized during export and therefore is not part of the comparison.
   - Produces `<TICK_LAKE_ROOT>/_migration/verification.json`.
   - Any discrepancy (missing row, altered float, lost null) immediately aborts the migration pipeline.

4. **Stage 4: PUBLISH (`--mode publish`)**
   - Promotes verified Parquet chunks from `_migration/staging/ticks/` into production `ticks/` using atomic directory moves and filesystem renames (chunk filenames such as `chunk_000001.parquet` are preserved).
   - Issues immutable publication receipts in `_control/receipts/` (`batch_migrated_<symbol>_<YYYYMMDD>.json`).

### 5.2 Command Line Execution

```bash
# 1. Execute end-to-end migration (Plan -> Export -> Verify -> Publish)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Crucial\ X9/data-harvester/data/tick_lake \
  --mode all

# 2. Dry run (verify planning and chunking without disk writes)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Crucial\ X9/data-harvester/data/tick_lake \
  --dry-run

# 3. Targeted partition migration (specific symbols or dates)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Crucial\ X9/data-harvester/data/tick_lake \
  --symbols AAPL,MSFT \
  --date-start 2026-10-01 \
  --mode all

# 4. Resume interrupted export
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Crucial\ X9/data-harvester/data/tick_lake \
  --resume
```

**Full flag set:** `--source-db`, `--source-table`, `--lake-root`, `--mode {plan,export,verify,verify-published,publish,all,audit-lake,audit}`, `--chunk-size` (default 100,000), `--symbols`, `--date-start`, `--date-end`, `--dry-run`, `--resume`, `--force`, `--compression` (default `snappy`), `--migration-id`.

`--mode all` covers plan → export → verify → publish; `verify-published` and `audit-lake`
are run afterwards as the published-inventory and whole-lake checks.

### 5.3 Rollback Protocol
- **Before Publish Stage:** If export or verification encounters errors, simply delete the staging artifacts:
  ```bash
  rm -rf "<TICK_LAKE_ROOT>/_migration/staging"
  rm -f "<TICK_LAKE_ROOT>/_migration/state.json" "<TICK_LAKE_ROOT>/_migration/verification.json"
  ```
  The source database was opened read-only and remains unaltered.
- **After Publish Stage:** To roll back published historical chunks without affecting live streaming batches:
  ```bash
  find "<TICK_LAKE_ROOT>/ticks" -name "chunk_*.parquet" -delete
  rm -f "<TICK_LAKE_ROOT>/_control/receipts"/batch_migrated_*.json
  ```
  Live streaming batches (`batch_*.parquet`) remain active and unaffected.

### 5.4 Deleting the Legacy Store

Once verification passes, v5.0 keeps **no** second copy. The owner deletes the
legacy files on the Mac as the last step of the milestone:

```bash
rm -f data/streaming.duckdb data/historical.duckdb
```

Do **not** export, convert or archive the historical 1-minute bars: that store is
deliberately dropped so the project has one model (ticks) and one format (Parquet).

`tools/mac/run_phase49_migration.sh` wraps the whole sequence (including the duplicate-safety
re-run and the report table) in one command and refuses to run while the streamer is alive;
the steps below remain the by-hand equivalent.

The gate is the full sequence, not verification alone: `--mode all` must finish with a
`PASSED` verification, then `--mode verify-published` and `--mode audit-lake` must both
exit `0`. Only then are the files deleted, and immediately — there is no retention
window. The complete, rehearsal-tested procedure (including the duplicate-safe re-run and
the abort branches) is **[phase49_migration_runbook.md](phase49_migration_runbook.md)**.

---

## 6. Troubleshooting & Disaster Recovery Protocol

### 6.1 Orphaned `.tmp` Staging Files
- **Symptom:** Files named `tmp_writer_*.parquet.tmp` accumulate in `<TICK_LAKE_ROOT>/_staging/`.
- **Root Cause:** Ingestion process was killed forcefully (`kill -9`, power outage, or OS reboot) during active PyArrow file serialization before the atomic rename step.
- **Resolution:**
  Data Harvester includes an automated cleanup utility. Run the following Python command or incorporate it into daily maintenance:
```python
from src.storage.publication import cleanup_orphaned_staging_files
# Deletes uncommitted staging files older than 1 hour (3600s, the default)
# Files referenced by an active publication intent in _control/intent/ are always preserved.
cleaned = cleanup_orphaned_staging_files("/Volumes/Crucial X9/data-harvester/data/tick_lake", max_age_seconds=3600)
print(f"Cleaned {cleaned} orphaned staging files.")
```

### 6.2 Crash Recovery & Uncommitted Publication Intents
- **Mechanism:** Before renaming any `.tmp` file into production `ticks/`, `LakePublisher` writes an atomic publication intent (`_control/intent/<batch_id>.json`).
- **Automatic Recovery:** When `TickLakeWriter` initializes, it automatically invokes `recover_pending_publications(lake_root)`.
  - If the target file exists in `ticks/` and matches the intent checksum, the publication receipt is committed.
  - If the target file is missing, any lingering staging file is removed so the batch can be retried safely.
- **Coverage:** Crash-intent recovery paths are exercised by `tests/storage/test_crash_recovery.py` and `tests/storage/test_storage_edge_cases.py` (Phase 22).

### 6.3 Stale Publisher Lock (`publisher.lock`)
- **Symptom:** `TickLakeWriter` raises `LakeOwnershipError: Another writer (PID ...) currently owns lake root`.
- **Root Cause:** A previous writer process crashed abruptly without executing its shutdown handler.
- **Verification & Resolution:**
  1. Inspect the PID recorded in `<TICK_LAKE_ROOT>/_control/publisher.lock`.
  2. Verify if the process is actually running:
     ```bash
     ps -p <PID>
     ```
  3. If the process does not exist, the advisory lock is stale. Remove the lock file:
     ```bash
     rm "<TICK_LAKE_ROOT>/_control/publisher.lock"
     ```
  4. Restart the service supervisor (`./tools/mac/start_services.sh`).

### 6.4 External Volume Handling (SSD Unmount / Disconnection)
- **Symptom:** Streamer logs report `StorageConfigError: Storage mount missing: data symlink is broken at …` or `Lake root … does not exist or is not a directory`.
- **Protective Behavior:**
  - `resolve_tick_lake_root()` validates the configured root and a broken `data` symlink before every critical write.
  - If an external NVMe/SSD drive (e.g. Micron / Crucial) is disconnected, the writer **never silently falls back to an internal root** (which would risk filling the OS boot drive or splitting data).
  - The writer logs an error, retains uncommitted ticks in its bounded buffer, and enters a retry loop with backoff.
- **Operator Action:**
  1. Reconnect or remount the external volume.
  2. Verify directory accessibility:
     ```bash
     ls -la "/Volumes/Crucial X9/data-harvester/data/tick_lake"
     ```
  3. The writer resumes flushing automatically once the filesystem path becomes available.
  4. If the drive cannot be remounted within 5 minutes, gracefully stop the service to avoid WebSocket connection drops.

### 6.5 Production Health Verification Checklist
Before handing off or ending maintenance, verify the following health endpoints:

```bash
# 1. Check HTTP server status and lake metrics
curl -s http://localhost:8420/api/status | jq .

# 2. Check Streamer heartbeat and total rows written
curl -s http://localhost:8420/api/stream/status | jq .

# 3. Check Live Tape for active quotes
curl -s "http://localhost:8420/api/stream/tape?symbol=AAPL&limit=5" | jq .

# 4. Check Data Continuity
curl -s "http://localhost:8420/api/streaming/continuity?symbol=AAPL&days=1" | jq .
```

---

## 7. Verification & Hardening (Milestone v5.0)

The v5.0 rewrite deleted the disk-database layer, the historical bar archive and
the replay subsystem. Two structural guards keep them deleted:

| Guard | What it enforces |
|---|---|
| `tests/test_disk_database_layer_removed.py` | No module under `src/` or `tools/` may name the removed database API (`DuckDBClient`, `get_streaming_db_*`, `DEFAULT_*DB_PATH`, …). The sole exemption is `tools/migrate_streaming_to_parquet.py`. The `duckdb` package itself must survive — it is the in-memory query engine. |
| `tests/test_bar_era_removal.py` | The bar pipeline (harvesters, `minute_data`, replay, `/api/harvester`) is gone, including frontend tokens and requirement entries. |

Operational guarantees carried over from the lake hardening work (v4.1/v4.3 and
re-verified under the lake-only fixtures in v5.0): zero-loss atomic publication,
crash-intent recovery, malformed-tick quarantine, disk-full backoff, registry
purge fencing, and multi-process reader concurrency with zero file locks.

> **Deferred to the owner (Phase 49):** the legacy migration itself and the
> subsequent deletion of `data/streaming.duckdb` / `data/historical.duckdb` run
> on the Mac against the real files. The agent never copies, converts or deletes
> the owner's data. The 16.42 s/M-legacy rows-per-second reference baseline is
> archived in `.planning/milestones/v4.3-MILESTONE-AUDIT.md`.

## 8. Document History

| Version | Date | Milestone | Summary |
|---|---|---|---|
| 1.0.0 | 2026-10-03 | v4.0 (P8) | Initial production operations guide for the Partitioned Parquet Tick Lake. |
| 1.1.0 | 2026-10-03 | v4.1 | Corrected defaults/paths/filenames to match shipped code; documented unwired `STREAM_*` knobs; added §7 verification & hardening and §8 history; documented compaction/migration status honestly. |
| 2.0.0 | 2026-10-05 | v5.0 | Removed the legacy backend from §2.6, dropped Windows service scripts, rewrote §5 as a one-time owner-run migration plus store deletion, replaced §7 with the v5.0 structural guards. |
| 2.1.0 | 2026-10-06 | v6.0 | New rows store `bid_price` and `ask_price`. The lake already on disk is still schema v1. The rewrite is the last phase and is not to be run yet. Physical schema remains §2.5; backend selection remains §2.6. |
