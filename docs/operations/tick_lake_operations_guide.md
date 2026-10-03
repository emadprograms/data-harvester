# Partitioned Parquet Tick Lake: Production Operations Guide

**Document Version:** 1.0.0  
**Phase / Milestone:** Phase 21 (P8: Production Cutover, Concurrency Validation & Handoff) / Milestone v4.0  
**Applicability:** Production Operators, Site Reliability Engineers, Platform Architects, Quant Analytics Teams (Repo B).

---

## 1. System Architecture & High-Concurrency Design

### 1.1 The Legacy Concurrency Bottleneck
In legacy architectures (Milestones v1.0–v3.0), both the ingestion engine (`StreamingEngine`) and all concurrent analytical consumers (the dashboard HTTP server, background integrity monitors, and downstream quantitative backtesting engines such as Repo B) connected directly to a single shared, disk-backed DuckDB database (`data/streaming.duckdb`).

Because DuckDB enforces strict single-writer or exclusive process file locking on disk-backed `.duckdb` files, concurrent read-write access produced severe operational collisions:
- Ingestion crashed with `duckdb.IOException: Could not set lock on file` whenever a secondary process attempted to write or checkpoint.
- Dashboard queries suffered intermittent latency spikes and connection failures during high-throughput tick flushes.
- Downstream quantitative backtesters (Repo B) could not read real-time market data without shutting down the live capture engine.

### 1.2 The Decoupled Lake Architecture
Milestone v4.0 eliminates this fundamental limitation by decoupling the ingestion writer from all query execution paths. The live tick database is completely replaced by immutable Parquet micro-batches organized in a Hive-partitioned directory hierarchy.

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
                      │   - Flush Triggers: 5.0s or 5,000 rows  │
                      └────────────────────┬────────────────────┘
                                           │ Atomic Rename (.tmp -> final)
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        Partitioned Parquet Tick Lake (TICK_LAKE_ROOT)                  │
│                                                                                        │
│   ticks/symbol=AAPL/date=2026-10-03/batch_w1_000001.parquet                            │
│   ticks/symbol=MSFT/date=2026-10-03/batch_w1_000002.parquet                            │
│   _control/  [symbol_registry.json, writer_status.json, publisher.lock, receipts/]     │
│   _staging/  [in-flight .tmp Parquet files and uncommitted buffers]                    │
│   _maintenance/ [in_progress.json guard, pre-compacted staging, retirement archives]   │
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

| Variable Name | Default Value | Description | Production Guidance |
|---|---|---|---|
| `TICK_LAKE_ROOT` | `<DATA_DIR>/market_data` | Root path of the Partitioned Parquet Tick Lake. | **Mandatory in production.** Must point to the high-performance NVMe/SSD mount (e.g. `/Volumes/Crucial X9/market_data` or `/Volumes/Micron/data/market_data`). |
| `DATA_DIR` | `<repo_root>/data` | Base directory for data harvester assets (historical candles, signals, logs). | Kept for legacy compatibility and `historical.duckdb` storage. |
| `STREAM_FLUSH_INTERVAL` | `5.0` | Max elapsed seconds before in-memory ticks are flushed to Parquet. | Lowering to `1.0s` provides near real-time tape at the expense of higher file count. Range: `1.0` to `10.0`. |
| `STREAM_MAX_BATCH_ROWS` | `5000` | Max buffered ticks per batch before triggering immediate off-loop flush. | Set to `500`–`5000` depending on symbol universe velocity. |
| `STREAM_MAX_QUEUE_SIZE` | `10000` | Bounded memory buffer capacity. Backpressure fence prevents memory exhaustion. | Default `10000` accommodates bursts up to 2,000 ticks/sec across 20+ symbols. |
| `STREAM_COMPRESSION` | `snappy` | Compression codec for Parquet micro-batches (`snappy` or `zstd`). | Use `snappy` for minimum CPU utilization and event-loop responsiveness. |
| `DASHBOARD_PORT` | `8420` | TCP port for the Dashboard HTTP REST API and UI. | Fallback ports: `8421`, `8422`, `8425`. |
| `PYTHONUNBUFFERED` | `1` | Forces unbuffered stdout/stderr streams. | Required for live log capture by supervisor. |

### 2.2 Lake Directory Hierarchy

The tick lake layout strictly segregates queryable data, staging areas, administrative control files, and maintenance artifacts:

```text
<TICK_LAKE_ROOT>/
├── lake.json                           # Format metadata, schema version (v1), created_at
├── ticks/                              # ACTIVE QUERY ROOT (Only query here)
│   ├── symbol=AAPL/
│   │   ├── date=2026-10-02/
│   │   │   ├── batch_w1_000001.parquet
│   │   │   └── historical_m1_000001.parquet
│   │   └── date=2026-10-03/
│   │       └── batch_w1_000002.parquet
│   ├── symbol=MSFT/
│   │   └── date=2026-10-03/
│   │       └── batch_w1_000001.parquet
│   └── symbol=EUR%2FUSD/               # Percent-encoded symbols for special characters
│       └── date=2026-10-03/
│           └── batch_w1_000001.parquet
├── _staging/                           # IN-FLIGHT WRITES (DO NOT QUERY)
│   └── tmp_w1_000003_a9b8c7.parquet.tmp
├── _control/                           # CONTROL PLANE & AUDIT RECEIPTS
│   ├── symbol_registry.json            # Versioned JSON symbol inventory
│   ├── writer_status.json              # Streamer PID, throughput, heartbeat, metrics
│   ├── publisher.lock                  # Advisory lock file for single writer daemon
│   └── receipts/                       # Immutable publication audit receipts
│       └── receipt_batch_w1_000001.json
├── _maintenance/                       # OFFLINE MAINTENANCE ARTIFACTS
│   ├── in_progress.json                # Maintenance guard file (created during compaction)
│   ├── staging/                        # Compacted Parquet candidate batches
│   └── retired/                        # Retired raw batches pending purge
├── _migration/                         # HISTORICAL MIGRATION CHECKPOINTS
│   ├── plan.json                       # Chunk migration plan
│   ├── migration_state.json            # Progress checkpoints for zero-loss export
│   └── reconciliation_report.json      # Mathematical EXCEPT ALL audit report
└── _spool/                             # OPTIONAL DURABLE DISK SPOOL
```

### 2.3 Partition Pruning Rules for Operators and Query Engines
- **Never scan the lake root recursively:** Recursive scans (`**/*.parquet`) over `<TICK_LAKE_ROOT>/` will erroneously traverse `_staging/` and `_maintenance/`, encountering incomplete files.
- **Always prune by symbol and UTC date:** Resolving candidate directory paths in Python (e.g. `ticks/symbol=AAPL/date=2026-10-03/*.parquet`) before dispatching to DuckDB `read_parquet([...])` eliminates unnecessary directory traversal and speeds up query response times by over 95%.
- **Safe Symbol Encoding:** Symbols containing special characters (e.g., `/`, `:`, `%`, space) are uppercase percent-encoded (e.g., `EUR/USD` $\to$ `symbol=EUR%2FUSD`). Standard ASCII alphanumeric characters, periods, underscores, and hyphens (`[A-Za-z0-9._-]`) remain unescaped.

---

## 3. Production Service Management

Data Harvester provides a dual-layer production management architecture: a multi-threaded service supervisor for active monitoring and auto-healing, combined with OS-native service scripts for macOS (`launchd`) and Windows (`Task Scheduler` / Services).

### 3.1 Service Supervisor (`tools/service_supervisor.py`)
The supervisor process coordinates background execution of the streamer and dashboard:
- **Process Supervision:** Monitors child processes; captures stdout/stderr into rotating log files under `logs/` (rotated at 20MB).
- **Code & Git Auto-Reload:** Watches `src/` and `.git/HEAD`. When new code is pulled or committed, child processes are automatically restarted cleanly.
- **Crash Auto-Healing:** Detects process crashes and automatically restarts the child with exponential backoff (up to 30 seconds) to prevent CPU thrashing.
- **Graceful Shutdown:** Catches `SIGINT` and `SIGTERM`. Grants children a 15-second drain period to allow the streamer's 10-second bounded queue drain to finish cleanly before issuing `SIGKILL`.

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

### 3.3 Windows Production Scripts (`tools/windows/`)

| Script | Purpose | Command |
|---|---|---|
| `install_services.ps1` / `INSTALL_STARTUP.bat` | Installs Windows Scheduled Tasks running under `python.exe` with startup triggers. | `INSTALL_STARTUP.bat` |
| `stop_services.ps1` / `STOP_SERVICES.bat` | Safely terminates all supervised Python processes and tasks. | `STOP_SERVICES.bat` |
| `status_services.ps1` / `VIEW_STATUS.bat` | Displays Windows task status, active PIDs, and tail logs. | `VIEW_STATUS.bat` |
| `uninstall_services.ps1` / `UNINSTALL_STARTUP.bat` | Unregisters all Data Harvester Scheduled Tasks. | `UNINSTALL_STARTUP.bat` |

> [!IMPORTANT]
> **Windows Python Executable Requirement:** Always configure services with `python.exe` rather than `pythonw.exe`. `pythonw.exe` suppresses console handles and causes immediate silent termination when modules expect standard I/O pipes.

### 3.4 Dynamic Symbol Management & Dynamic Reload
1. **Adding a Symbol:**
   ```bash
   curl -X POST http://localhost:8420/api/streaming/symbols \
     -H "Content-Type: application/json" \
     -d '{"display_name": "TSLA", "epic": "TSLA", "source": "CAPITAL"}'
   ```
   - Persisted atomically to `<TICK_LAKE_ROOT>/_control/symbol_registry.json`.
   - Increments registry `version`.
   - Touches `<TICK_LAKE_ROOT>/_control/.stream_reload.signal`.
   - Streamer runner polls version changes every 1.0s and dynamically subscribes to Capital.com WebSocket feed without process restart.

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
- Accumulation budget: Ingestion produces ~12 to 20 small micro-batch Parquet files per active symbol per day. Over a 30-day period, a 20-symbol universe accumulates ~7,200 to 12,000 small files.

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
- External readers (including Repo B and Dashboard) inspect `_maintenance/in_progress.json`.
- When present, readers pause extensive scans and back off.
- The service supervisor and ingestion runners refuse to start replacement tasks while the guard is active.

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
3. Move `ticks/symbol=<SYMBOL>/` to `_maintenance/retired/symbol=<SYMBOL>/`.
4. Update `<TICK_LAKE_ROOT>/_control/symbol_registry.json` using `SymbolRegistry.purge_symbol(symbol)` to permanently remove the registry entry.
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
   - Connects to source `streaming.duckdb` in read-only mode (`read_only=True`).
   - Discovers distinct symbols and date partitions.
   - Computes row counts, null profiles, and chunk boundaries (default chunk size: 100,000 rows).
   - Generates `<TICK_LAKE_ROOT>/_migration/plan.json`.
   - **Zero mutation:** Performs no writes against any database or filesystem.

2. **Stage 2: EXPORT (`--mode export`)**
   - Iterates through planned chunks using memory-bounded `fetchmany`.
   - Synthesizes deterministic, globally unique ingest IDs:
     $$\text{mig\_}\{symbol\}\_\{date\}\_\{index:08d\}$$
   - Encodes batches into Lake Schema v1 Parquet files under `_staging/ticks/`.
   - Persists partition progress to `_migration/migration_state.json`. Fully resumable across interruptions.

3. **Stage 3: VERIFY (`--mode verify`)**
   - Rigorously tests data integrity using mathematical set difference with duplicate multiplicity:
     - **Direction 1:** $\text{Legacy Source} \text{ EXCEPT ALL } \text{Parquet} = \emptyset$
     - **Direction 2:** $\text{Parquet} \text{ EXCEPT ALL } \text{Legacy Source} = \emptyset$
   - Checks all 8 physical columns (`timestamp`, `symbol`, `price`, `volume`, `bid`, `ask`, `source`, `session`).
   - Produces `<TICK_LAKE_ROOT>/_migration/reconciliation_report.json`.
   - Any discrepancy (missing row, altered float, lost null) immediately aborts the migration pipeline.

4. **Stage 4: PUBLISH (`--mode publish`)**
   - Promotes verified Parquet files from `_staging/ticks/` into production `ticks/` using atomic directory moves and filesystem renames.
   - Issues immutable `FilePublicationReceipt` objects in `_control/receipts/`.

### 5.2 Command Line Execution

```bash
# 1. Execute end-to-end migration (Plan -> Export -> Verify -> Publish)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Data/market_data \
  --mode all

# 2. Dry run (verify planning and chunking without disk writes)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Data/market_data \
  --dry-run

# 3. Targeted partition migration (specific symbols or dates)
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Data/market_data \
  --symbols AAPL,MSFT \
  --date-start 2026-10-01 \
  --mode all

# 4. Resume interrupted export
python tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb \
  --lake-root /Volumes/Data/market_data \
  --resume
```

### 5.3 Rollback Protocol
- **Before Publish Stage:** If export or verification encounters errors, simply delete the staging artifacts:
  ```bash
  rm -rf "<TICK_LAKE_ROOT>/_staging/ticks"
  rm -f "<TICK_LAKE_ROOT>/_migration/migration_state.json"
  ```
  The source `streaming.duckdb` was opened read-only and remains 100% unaltered.
- **After Publish Stage:** To roll back published historical files without affecting live streaming batches:
  ```bash
  find "<TICK_LAKE_ROOT>/ticks" -name "historical_*.parquet" -delete
  ```
  Live streaming batches (`batch_*.parquet`) remain active and unaffected.

---

## 6. Troubleshooting & Disaster Recovery Protocol

### 6.1 Orphaned `.tmp` Staging Files
- **Symptom:** Files named `tmp_*.parquet.tmp` accumulate in `<TICK_LAKE_ROOT>/_staging/`.
- **Root Cause:** Ingestion process was killed forcefully (`kill -9`, power outage, or OS reboot) during active PyArrow file serialization before the atomic rename step.
- **Resolution:**
  Data Harvester includes an automated cleanup utility. Run the following Python command or incorporate it into daily maintenance:
  ```python
  from src.storage.publication import cleanup_orphaned_staging_files
  # Deletes uncommitted staging files older than 1 hour (3600s)
  cleaned = cleanup_orphaned_staging_files(lake_root="/Volumes/Data/market_data", max_age_seconds=3600)
  print(f"Cleaned {cleaned} orphaned staging files.")
  ```

### 6.2 Crash Recovery & Uncommitted Publication Intents
- **Mechanism:** Before renaming any `.tmp` file into production `ticks/`, `LakePublisher` writes an atomic publication intent.
- **Automatic Recovery:** When `TickLakeWriter` initializes, it automatically invokes `recover_pending_publications(lake_root)`.
  - If the target file exists in `ticks/` and matches the intent checksum, the publication receipt is committed.
  - If the target file is missing, any lingering staging file is removed so the batch can be retried safely.

### 6.3 Stale Publisher Lock (`publisher.lock`)
- **Symptom:** `TickLakeWriter` raises `LakeOwnershipError: Another writer (PID ...) currently owns lake root`.
- **Root Cause:** A previous writer process crashed abruptly without executing its shutdown handler.
- **Verification & Resolution:**
  1. Inspect the PID in `<TICK_LAKE_ROOT>/_control/publisher.lock`.
  2. Verify if the process is actually running:
     ```bash
     ps -p <PID>
     ```
  3. If the process does not exist, the lock is stale. Remove the lock file:
     ```bash
     rm "<TICK_LAKE_ROOT>/_control/publisher.lock"
     ```
  4. Restart the service supervisor (`./tools/mac/start_services.sh`).

### 6.4 External Volume Handling (SSD Unmount / Disconnection)
- **Symptom:** Streamer logs report `StorageConfigError: Lake root /Volumes/... does not exist or is not a directory`.
- **Protective Behavior:**
  - `resolve_tick_lake_root()` validates volume mount presence before every critical write.
  - If an external NVMe/SSD drive (e.g. Micron / Crucial) is disconnected, the writer **never silently falls back to an internal root** (which would risk filling the OS boot drive or splitting data).
  - The writer logs an error, retains uncommitted ticks in its bounded buffer, and enters a retry loop with backoff.
- **Operator Action:**
  1. Reconnect or remount the external volume.
  2. Verify directory accessibility:
     ```bash
     ls -la "/Volumes/Crucial X9/market_data"
     ```
  3. The writer resumes flushing automatically once the filesystem path becomes available.
  4. If the drive cannot be remounted within 5 minutes, gracefully stop the service to avoid WebSocket connection drops.

### 6.5 Production Health Verification Checklist
Before handing off or ending maintenance, verify the following health endpoints:

```bash
# 1. Check HTTP server status and DuckDB lake metrics
curl -s http://localhost:8420/api/status | jq .

# 2. Check Streamer heartbeat and total rows written
curl -s http://localhost:8420/api/stream/status | jq .

# 3. Check Live Tape for active quotes
curl -s "http://localhost:8420/api/stream/tape?symbol=AAPL&limit=5" | jq .

# 4. Check Data Continuity
curl -s "http://localhost:8420/api/streaming/continuity?symbol=AAPL&days=1" | jq .
```
All endpoints should respond with HTTP 200 within $< 50\text{ms}$.
