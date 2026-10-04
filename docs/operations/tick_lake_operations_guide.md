# Partitioned Parquet Tick Lake: Production Operations Guide

**Document Version:** 1.1.0
**Phase / Milestone:** Originally Phase 21 (P8) / Milestone v4.0 — revised for Milestone v4.1 (Phases 22–27, Deep Testing & Hardening)
**Last reviewed:** 2026-10-03
**Applicability:** Production Operators, Site Reliability Engineers, Platform Architects, Quant Analytics Teams (Repo B).

**Changes in 1.1.0:** corrected the lake-root default (`<DATA_DIR>/tick_lake`, i.e. `data/tick_lake`), registry filename (`_control/registry.json`), migration state/artifact filenames (`_migration/plan.json`, `state.json`, `verification.json`, `_migration/staging/…/chunk_NNNNNN.parquet`), rollback patterns, and the true state of the environment-variable knobs; added the v4.1 hardening and verification section (§7).

---

## 1. System Architecture & High-Concurrency Design

### 1.1 The Legacy Concurrency Bottleneck
In legacy architectures (Milestones v1.0–v3.0), both the ingestion engine (`StreamingEngine`) and all concurrent analytical consumers (the dashboard HTTP server, background integrity monitors, and downstream quantitative backtesting engines such as Repo B) connected directly to a single shared, disk-backed DuckDB database (`data/streaming.duckdb`).

Because DuckDB enforces strict single-writer or exclusive process file locking on disk-backed `.duckdb` files, concurrent read-write access produced severe operational collisions:
- Ingestion crashed with `duckdb.IOException: Could not set lock on file` whenever a secondary process attempted to write or checkpoint.
- Dashboard queries suffered intermittent latency spikes and connection failures during high-throughput tick flushes.
- Downstream quantitative backtesters (Repo B) could not read real-time market data without shutting down the live capture engine.

### 1.2 The Decoupled Lake Architecture
Milestone v4.0 eliminated this fundamental limitation by decoupling the ingestion writer from all query execution paths. The live tick database is completely replaced by immutable Parquet micro-batches organized in a Hive-partitioned directory hierarchy.

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
1. **Lock-Free Concurrency:** Readers never open or attach `streaming.duckdb` and never acquire POSIX locks against active storage. Every reader instantiates a private, thread-local in-memory DuckDB instance (`duckdb.connect(":memory:")`) and scans immutable files via `read_parquet(...)`.
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
| `DATA_DIR` | `<repo_root>/data` | Base directory for data harvester assets (`historical.duckdb`, `tick_lake/`, logs). | If set, `tick_lake` is resolved beneath it. |
| `DASHBOARD_PORT` (or `PORT`) | `8420` | TCP port for the Dashboard HTTP REST API and UI. | Fallback ports `8421`, `8422`, `8425` are attempted automatically when `8420` is busy. |
| `CAPITAL_COM_X_CAP_API_KEY` / `CAPITAL_COM_IDENTIFIER` / `CAPITAL_COM_PASSWORD` | — | Capital.com WebSocket credentials. | Required for live streaming (loaded from `.env`). |
| `SKIP_DISCORD` | unset | When `true`, suppresses Discord webhook notifications in `main.py`. | Useful for automated/offline runs. |

**Lake-root resolution precedence** (`resolve_tick_lake_root`): explicit argument → `TICK_LAKE_ROOT` → `DATA_DIR/tick_lake` → `/Volumes/Micron-E 0256 A/data-harvester/data/tick_lake` (if mounted) → `<repo_root>/data/tick_lake` → `StorageConfigError`. A broken `data` symlink raises `StorageConfigError` rather than silently falling back to internal storage.

> ⚠️ **Documented-but-unwired knobs.** `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS`, `STREAM_MAX_QUEUE_SIZE`, and `STREAM_COMPRESSION` appear in `.env.example` but are **not read by the runtime as of v4.1** (grep-verified: no `getenv`/`environ` reader exists for them). The effective settings come from constructor defaults and are listed in §2.2. Wiring these variables is a tracked backlog item in `.planning/ROADMAP.md`.

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

### 2.5 Physical Schema v1 (9 columns)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `timestamp` | `TIMESTAMP` (naive UTC, µs) | No | Parquet `timestamp('us')` |
| `symbol` | `VARCHAR` | No | Canonical uppercase display symbol. Physically written dictionary-encoded (`dictionary<string, int32>`); reads back as text. |
| `price` | `DOUBLE` | No | Observed quote/trade price |
| `volume` | `DOUBLE` | Yes | Coalesces to `1.0` during resampling when null |
| `bid` | `DOUBLE` | Yes | Best bid |
| `ask` | `DOUBLE` | Yes | Best ask |
| `source` | `VARCHAR` | Yes | Provider identifier (`CAPITAL`, `BINANCE`, …) |
| `session` | `VARCHAR` | Yes | Session tag (`REG`, `PRE`, `POST`, …) |
| `ingest_id` | `VARCHAR` | No | Stable unique ingestion identity (writer-generated string; migrated rows use `mig_<symbol>_<YYYYMMDD>_<index:08d>`) |

Rows are stored ordered by `(timestamp ASC, ingest_id ASC)`.

### 2.6 Backend Selection and Fail-Closed Behaviour

Read paths choose between the tick lake and the legacy `streaming.duckdb`
database. The rule is asymmetric on purpose, and operators need to know which
mode they are in:

| Selection | Condition | Behaviour on failure |
|---|---|---|
| **Explicit lake** | `TICK_LAKE_ROOT` or `DATA_DIR` is set | **Fails closed.** Any fault — unresolvable root, missing or corrupt `lake.json`, maintenance active, publisher lock conflict — raises. It never falls back to `streaming.duckdb`. |
| **Autodetected lake** | Neither variable is set, and the lake looks populated (`ticks/`, `_control/writer_status.json`, or `lake.json` exists) | Uses the lake |
| **Legacy** | Neither variable is set and the lake is absent or empty | Uses `streaming.duckdb`; explicitly supported historical and legacy paths remain available |

Implemented by `_get_lake_reader()` in `src/dashboard/analytics.py`, which loads
and validates `lake.json` **before** choosing the reader, so an empty or damaged
explicitly-selected lake cannot masquerade as "no data" and silently reopen the
legacy tick database.

**Operational consequence:** once `TICK_LAKE_ROOT` is set, a dashboard read error
is a genuine configuration or storage fault. Do not "fix" it by unsetting the
variable — that switches to autodetection, which may quietly serve historical
data from the legacy database instead.

---

## 3. Production Service Management

Data Harvester provides a dual-layer production management architecture: a multi-threaded service supervisor for active monitoring and auto-healing, combined with OS-native service scripts for macOS (`launchd`) and Windows (`Task Scheduler`).

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

### 3.3 Windows Production Scripts (`tools/windows/`)

| Script | Purpose | Command |
|---|---|---|
| `install_services.ps1` / `INSTALL_STARTUP.bat` | Installs Windows Scheduled Tasks running the supervisor under the configured Python executable with startup triggers. | `powershell -ExecutionPolicy Bypass -File tools/windows/install_services.ps1` |
| `stop_services.ps1` / `STOP_SERVICES.bat` | Safely terminates all supervised Python processes and tasks. | `tools\windows\STOP_SERVICES.bat` |
| `status_services.ps1` / `VIEW_STATUS.bat` | Displays Windows task status, active PIDs, and tail logs. | `tools\windows\VIEW_STATUS.bat` |
| `uninstall_services.ps1` / `UNINSTALL_STARTUP.bat` | Unregisters all Data Harvester Scheduled Tasks. | `tools\windows\UNINSTALL_STARTUP.bat` |
| `enable_git_autoupdate.ps1` | Documents/enables the supervisor's `.git/HEAD` auto-reload behaviour. | `powershell -File tools/windows/enable_git_autoupdate.ps1` |

> [!IMPORTANT]
> **Windows Python Executable Requirement:** Prefer `python.exe` when configuring services (the PowerShell installer does this automatically) rather than `pythonw.exe`, which suppresses console handles and can cause silent termination when modules expect standard I/O pipes. The legacy `INSTALL_STARTUP.bat` path launches the supervisor via WScript with `pythonw.exe`; avoid it if you need live supervisor logs.
>
> The supervisor-launched processes inherit the shell environment; set `TICK_LAKE_ROOT` system-wide (or in `.env`) so both the streamer and dashboard resolve the same lake.

### 3.4 Dynamic Symbol Management & Dynamic Reload
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

> ⚠️ **As of v4.1 the compaction runner itself is not implemented** — the guard and the procedure below are the documented operator protocol (P7a), tracked as backlog. Do not run compaction while the live writer owns `_control/publisher.lock`.

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
When a symbol is marked `PENDING_PURGE`:
1. Ensure the symbol has been deactivated from active streaming for at least one maintenance cycle.
2. Create `_maintenance/in_progress.json`.
3. Move `ticks/symbol=<ENCODED_SYMBOL>/` to `_maintenance/retired/symbol=<ENCODED_SYMBOL>/`.
4. Update `<TICK_LAKE_ROOT>/_control/registry.json` using `SymbolRegistry.purge_symbol(symbol)` to permanently remove the registry entry.
5. Remove `_maintenance/in_progress.json`.

---

## 5. Historical Data Migration Procedure

The historical migration utility (`tools/migrate_streaming_to_parquet.py`) provides zero-loss migration of legacy `streaming.duckdb` data into the Partitioned Parquet Lake.

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

**Full flag set:** `--source-db`, `--source-table`, `--lake-root`, `--mode {plan,export,verify,publish,all}`, `--chunk-size` (default 100,000), `--symbols`, `--date-start`, `--date-end`, `--dry-run`, `--resume`, `--force`, `--compression` (default `snappy`).

### 5.3 Rollback Protocol
- **Before Publish Stage:** If export or verification encounters errors, simply delete the staging artifacts:
  ```bash
  rm -rf "<TICK_LAKE_ROOT>/_migration/staging"
  rm -f "<TICK_LAKE_ROOT>/_migration/state.json" "<TICK_LAKE_ROOT>/_migration/verification.json"
  ```
  The source `streaming.duckdb` was opened read-only and remains 100% unaltered.
- **After Publish Stage:** To roll back published historical chunks without affecting live streaming batches:
  ```bash
  find "<TICK_LAKE_ROOT>/ticks" -name "chunk_*.parquet" -delete
  rm -f "<TICK_LAKE_ROOT>/_control/receipts"/batch_migrated_*.json
  ```
  Live streaming batches (`batch_*.parquet`) remain active and unaffected.

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
  4. Restart the service supervisor (`./START_SERVICES.sh` on macOS, `tools\windows\STOP_SERVICES.bat` then `tools\windows\INSTALL_STARTUP.bat` on Windows).

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

## 7. Verification & Hardening (Milestone v4.1)

Milestone v4.1 added 122 adversarial tests over the v4.0 architecture. Operators should know what is now guaranteed by executable tests and how to re-run the relevant suites.

> ⚠️ **Audit context.** The independent v4.1 milestone audit ([.planning/v4.1-MILESTONE-AUDIT.md](../../.planning/v4.1-MILESTONE-AUDIT.md)) confirms all 688 offline tests pass with **no data-corrupting defects**, but records `gaps_found` because no per-phase `VERIFICATION.md` artifacts were produced and because two suites cover production paths only partially: TEST-P23-02 never drives a real SIGINT/SIGTERM through `src/stream/runner.py`'s signal handlers, and TEST-P27-02's streamer chaos runs against `tools/synthetic_streamer.py` rather than the real `StreamingEngine`. Treat streamer crash-recovery behaviour as *inferred, not observed* until those gaps close (tracked in `.planning/ROADMAP.md`).

### 7.1 Coverage Map

| Area | Test file | Focus |
|---|---|---|
| Storage foundation & publication | `tests/storage/test_storage_edge_cases.py` (36) | Path traversal, unicode/special symbols, corrupted `lake.json`, schema coercion, float extremes, null bitmasks, publication collisions, crashed intents |
| Writer & runner lifecycle | `tests/stream/test_lake_runner_stress.py` (14) | 100k+ tick micro-batching, bounded-queue backpressure, shutdown mid-flush, drain timeout, disk-full/I-O backoff, malformed-tick quarantine |
| Registry & dynamic reload | `tests/storage/test_registry_stress.py` (13) | Cross-process CRUD serialization, monotonic versioning, `PENDING_PURGE` fences, 500-touch signal storms, reload latency under polling |
| Reader & analytics edges | `tests/storage/test_lake_reader_stress.py` (17) | 30+ concurrent in-memory readers, 1,000+ sequential queries without leaks, sparse partitions, DST/leap-year resampling, tape pagination |
| Migration tooling | `tests/storage/test_migration_stress.py` (32) | Corrupt/partial sources, schema drift, crash interruption in all modes, `EXCEPT ALL` fuzzing with precision-mismatch detection |
| Multi-process soak & chaos | `tests/integration/test_supervisor_chaos_soak.py` (10) | Sustained writer+reader soak, chaos-monkey termination of streamer/dashboard/supervisor, supervisor self-healing |

### 7.2 Commands

```bash
# Full offline suite (688 tests as of v4.1; 8 live/performance tests deselected)
pytest tests/ -m "not live and not performance"

# Concurrency / multi-process contract validation (writer + dashboard + Repo B simulation)
python tools/validate_concurrency.py                 # 6,000 synthetic ticks, 160 dashboard requests, 60 Repo B iterations
python tools/validate_concurrency.py --output-json reports/concurrency.json

# Integrity audit of lake + historical database
python tools/audit_database_integrity.py --lake-only
```

### 7.3 Operator Notes on Timing-Sensitive Gates
- Supervisor soak tests assert dashboard p95 latency under 100 ms and reader/debounce timing behaviour. On slow or heavily loaded hosts these can exceed the threshold; run them on the named reference machine described in `docs/plans/tick-lake-test-first-remediation.md` before treating a failure as a regression.
- Do not tune thresholds or performance gates to make a run pass; record the environment (packages, hardware, dataset) with every run.
- Multi-process chaos tests terminate child processes deliberately; they are safe with respect to production data because all fixtures operate inside temporary lake roots.

---

## 8. Document History

| Version | Date | Milestone | Summary |
|---|---|---|---|
| 1.0.0 | 2026-10-03 | v4.0 (P8) | Initial production operations guide for the Partitioned Parquet Tick Lake. |
| 1.1.0 | 2026-10-03 | v4.1 | Corrected defaults/paths/filenames to match shipped code; documented unwired `STREAM_*` knobs; added §7 verification & hardening and §8 history; documented compaction/migration status honestly. |
