# Partitioned Parquet tick lake: implementation and verification plan

Status: proposed implementation plan; no implementation performed.
Prepared: 2026-10-03. Reviewed and revised: 2026-10-03. Code inspected at commit `d8e40c4c`.
Scope: this ingestion repository, its dashboard, migration tooling, and a reader contract for Repo B. Repo B was not inspected; its file-level changes require a separate code inventory.

## 1. Outcome and boundaries

Replace the live tick database with immutable Parquet batches so the ingestion process and independent analytics/replay processes never need to open the same writable DuckDB tick database. Keep `historical.duckdb` and its canonical minute candles intact. Historical tick migration means migrating `streaming.duckdb`, not converting those separate historical candles into fabricated ticks.

Acceptance outcomes:

- Live tick persistence does not open a disk-backed DuckDB database.
- Readers use private in-memory DuckDB connections over finalized Parquet files.
- Files are published only after they are complete; readers cannot encounter partially written Parquet footers.
- All rows in the frozen legacy tick database, including identical duplicates and null fields, survive migration.
- A failed write cannot advance saved counters, acknowledge persistence, or discard the batch.
- Streaming callbacks remain responsive while encoding and disk writes run outside their event loop.
- Replay has deterministic ordering and bounded memory, including when timestamps repeat.
- Existing charts, sessions, integrity checks, symbol management, and historical-data separation retain explicit tested behavior.

The design removes shared tick-database lock contention. It does not eliminate SSD bandwidth competition or guarantee millisecond scans of arbitrary millions of rows. Performance is an acceptance benchmark, not a consequence of choosing Parquet.

Durability boundary: the requested RAM-buffered mode can lose accepted-but-unpublished ticks on process/power failure. Historical migration has a zero-loss gate. Live zero-loss from a durable acceptance point requires the optional spool described below; uninterrupted lossless exchange capture also requires provider replay, which this code does not implement. Do not claim that a five-second RAM buffer provides crash-proof ingestion; increasing the flush interval also increases the normal unpublished-data window, which can grow further during an I/O outage.

## 2. Findings grounded in this repository

| Code / tests | Observed behavior | Required plan response |
|---|---|---|
| `src/stream/runner.py`, `StreamingEngine` | Unbounded `asyncio.Queue`; flush at 100 ticks or 2 seconds; calls synchronous storage on event loop | Bounded queue/batch memory, off-loop encoding and I/O, configurable thresholds |
| Runner `_duckdb_writer_worker` | Calls `task_done()` before persistence; ignores Boolean save result; increments counters and clears buffer | Explicit publication receipt and acknowledgment only after success |
| Runner `start()` | `db_conn` normally remains `None`; schema connection closes; saves open/close their own connection | Current lock exposure is intermittent in this path, not necessarily one permanent lock; still unsafe across processes |
| `src/database/operations.py`, `save_ticks_to_storage` | Normalizes ticks and executes multi-row INSERT chunks of 1,000; may return `False` after partial progress | This is already batched SQL, not one INSERT per tick; replace storage, preserve normalization deliberately, make retries idempotent |
| `src/database/schema.py`, `init_streaming_db` | Eight columns; UTC-naive `TIMESTAMP`; no tick primary key; `ticks` and `streaming_ticks` compatibility views; symbol registry in same DB | Preserve duplicates, add stable ingestion identities, migrate registry separately |
| `src/database/connection.py` | Retries/adapts connection mode; `DuckDBResult.rows` fetches everything; `fetchone()` uses `.rows` | Retries cannot solve process ownership; do not use materializing wrapper for replay |
| `src/database/operations.py`, symbol operations | Removing a symbol deletes its tick rows; seed initialization can restore defaults | Explicit purge workflow and subscription fence; no automatic reseeding of an intentionally empty registry |
| Runner / dashboard server | Server touches `.stream_reload.signal`; runner waits on an in-process event and does nothing on timeout | Wire cross-process registry-version polling; current signal file is not consumed by runner |
| Runner `main()` / `tools/service_supervisor.py` | Signal handler calls `sys.exit(0)`; supervisor force-kills after six seconds | Await producer stop and writer drain; portable supervisor shutdown contract |
| `src/dashboard/analytics.py` | Streaming candles, tape, raw ticks, status, weeks, continuity open streaming DB; raw ticks use LIMIT/OFFSET | Introduce lake reader once, retain API contracts, add dedicated replay iterator |
| `src/utils/integrity.py` | Drift attaches both databases; health assumes DB files | Read live side through Parquet, preserve historical side and report its separate availability |
| `tests/database/test_concurrency.py` | Primarily same-process/thread tests; intermittent writer test reads after each write | Add actual simultaneous independent processes |
| `tests/test_streaming_symbols_management.py` and other tests | Some tests open real default paths and perform mutations | Isolate every automated test before broad test execution |
| Data path | Repository `data` is a symlink to an external volume; connection code also chooses that volume automatically | Require explicit resolved lake path; detect missing mount; never silently create an alternate empty lake |
| `requirements.txt` | DuckDB unpinned; PyArrow not listed | Add and pin a tested DuckDB/PyArrow combination during implementation |

This planning pass inspected source/tests and official documentation. It did not open, modify, checkpoint, or audit the production database, run live services, or execute the existing potentially mutating tests. Production row counts, installed package behavior, filesystem durability, and CPU/latency numbers remain to be measured.

## 3. Storage and data contracts

### 3.1 Layout

Use an explicit `TICK_LAKE_ROOT`, preferably under the resolved data volume:

```text
market_data/
  lake.json                         # format version and lake identity
  ticks/
    symbol=AAPL/date=2026-10-03/
      batch_<writer-id>_<sequence>.parquet
      historical_<migration-id>_<chunk>.parquet
    symbol=NVDA/date=2026-10-03/
      batch_<writer-id>_<sequence>.parquet
  _staging/                         # incomplete writes, never queried
  _maintenance/                     # replacement staging, recovery journal, retired inputs
  _migration/                       # export checkpoints and reconciliation reports
  _control/                         # atomic registry, status, serialized admin commands
  _spool/                           # optional durable ingestion journal
```

Add UTC date below symbol to bound file discovery and replay windows. A NYSE day may cover two UTC partitions. Use event UTC date, not receive date; late ticks belong in their original event-date partition. Safe symbol encoding must be reversible and reject traversal/glob injection; verify directory symbol equals the physical symbol column. Keep symbol inside each file for standalone portability, with explicit consistent Hive types.

**First release: append-only files and simple globs.** Consumers may query `ticks/symbol=AAPL/date=*/*.parquet`; this repository should expand only relevant symbol/date globs once per request and pass the resulting file list to DuckDB. That is a request-local snapshot, not a persistent catalog. Never scan the entire lake root recursively: staging, backups and retired inputs are outside `ticks/` and must stay excluded.

Compaction and purge run only in a coordinated maintenance window with all affected readers stopped/drained and writer publication fenced. Market close is a scheduling hint, not proof that replay, extended-hours trading, crypto, or late-arrival writes have stopped. If Repo B cannot be paused, defer replacement. Retain raw batches and monitor the file-count/capacity budget.

**Later, only if required:** introduce `_catalog/` with immutable manifests and generation pointers to support maintenance while readers remain active. All consumers must adopt catalog-selected file lists before enabling this mode. Do not build manifests or leases into the first release merely to support a possible future need.

### 3.2 Schema v1

| Field | Physical contract |
|---|---|
| `timestamp` | Microsecond UTC timestamp, mapped consistently to DuckDB UTC-naive TIMESTAMP for current SQL compatibility; normalize aware inputs to UTC before removing timezone |
| `symbol` | Non-null string; canonical display symbol |
| `price` | Non-null float64, no rounding |
| `volume`, `bid`, `ask` | Nullable float64, preserving historical nulls |
| `source`, `session` | Nullable string; preserve historical values exactly |
| `ingest_id` | Non-null unique stable string, distinct for every accepted row; persisted across retries |

Store schema version in file metadata and `lake.json`; later catalogs must carry the same version. Live IDs can be writer UUID plus monotonic sequence. Historical IDs use a frozen snapshot identity and stable source-row mapping. Ties sort by `(timestamp, ingest_id)`; for multiple symbols, the globally unique ID still breaks ties. This defines a reproducible tie order, not an inferred exchange order.

Do not use `(timestamp, symbol)` or a hash of the eight fields as an event ID: both collapse legitimate duplicates. Provider trade IDs may later support explicitly scoped deduplication, but Capital quote tuples currently provide no equivalent identifier. Keep quote volume `1.0` semantics for current Capital capture; it counts observations and is not exchange traded volume. Historical migration must bypass live defaulting/uppercasing/session rewriting.

Malformed new ticks go to a bounded, observable quarantine or fail ingestion explicitly; never silently skip them. Migration must report unusual historical values and preserve representable values; unsupported conversion blocks publication instead of coercing data away. Reject incompatible schema versions; do not use `union_by_name` to conceal a schema bug.

### 3.3 Writer/publication protocol

Use PyArrow to construct typed column batches and write Parquet in one dedicated worker. Start with Snappy as a low-CPU candidate; benchmark against low-level Zstandard. Use an initial default of **5 seconds or 5,000 ticks, whichever comes first**, with a byte-size limit that can flush sooner, a queue byte limit, and a maximum number of in-flight batches. Measure age from the oldest pending tick with a monotonic clock. Start with a global batch split by symbol/date and never write empty files. A hot symbol can trigger early flushes of other symbols; benchmark this before assuming each symbol produces exactly one file per interval. Allow 10 seconds only as a configurable, measured tradeoff.

For continuously active, timer-limited batches, moving from 2 seconds to 5 or 10 seconds reduces timer-triggered file opportunities by 60% or 80%. Actual file counts depend on tick rates, row/byte triggers and symbol distribution. Historical/finalized candle values do not change, but live candles, tape and status can be up to the configured interval behind plus publication time. Do not promise zero noticeable latency impact; show publication age and tune against the live display requirement.

1. Assign IDs when accepting normalized ticks; use a monotonic clock for flush age.
2. Drain into a bounded batch; split by symbol and event UTC date; sort each output by the ordering key.
3. Send immutable batch ownership to the worker; only one publication worker owns a lake root. Prevent duplicate ingestion processes with a control-plane ownership lock. Readers never acquire it.
4. Write to a unique `.tmp` staging path on the same filesystem as destination. Finish the footer, close, validate row count/schema, and flush the file using the durability level selected.
5. Atomically rename into its unique final destination. Never overwrite an existing final batch; verify a matching existing batch receipt on retry or raise a collision error.
6. In the first release, final rename is the reader-visibility point; sync directory/metadata where supported. A batch split into multiple files is visible file by file, not atomically across symbols or dates. Persist per-file intent/receipts outside the query glob so recovery can identify the completed subset.
7. Return `PublishReceipt(batch_id, row_count, paths)` after all files in that receipt are published. Only then advance published counters, complete queue acknowledgments, and release that portion of memory/spool. During a partial batch failure, some finalized files may already be readable; retry only missing outputs with their original IDs/names.
8. Recover a crash between rename and receipt by validating the existing final file against persisted intent, then completing its receipt. Never append those rows again under a new filename or replace a valid finalized file. In a future catalog mode, catalog publication becomes the visibility point and recovery also handles finalized-but-unlisted files.

No queue eviction. At capacity, callbacks await capacity and expose backlog/overload status. Network backpressure can eventually disconnect a provider; report this limitation. Track received, accepted, published, quarantined, retrying, queue bytes/age, last successful publish, and errors separately. A disk-full or unplugged-volume error leaves data pending, keeps counters honest, retries with bounded backoff, and surfaces unhealthy status. Unlimited outage tolerance with finite storage is impossible.

Shutdown sequence: stop accepting new provider messages, await producers, drain queue, await current worker and publication, write status, close. Replace immediate `sys.exit` and cancellation-based success. Supervisor grace period must exceed the configured drain deadline; forced termination is reported as such. Windows termination requires a cooperative stop mechanism, not assuming `terminate()` behaves like Unix SIGTERM.

Optional strict live durability mode: append framed/checksummed records with stable IDs to a sequential spool off the event loop; durable acknowledgment happens only after group fsync. Replay surviving frames after restart, reconcile publication receipts, and reclaim a segment only after all its rows are published. Test truncated last frame and disk-full behavior. Group commit still exposes records before the durable acknowledgment; never advertise them as durable. This mode adds I/O and belongs in the CPU benchmark.

### 3.4 Metadata and maintenance concurrency

Move streaming symbol inventory into a versioned atomic JSON registry managed by one control writer (the existing dashboard service can own administration). Other processes only read snapshots; CLI edits use that owner or an explicit exclusive offline command. Preserve all five existing registry fields and inactive entries. Poll version changes from runner on a short configurable interval; the signal file can remain a wake hint. An empty registry means no subscriptions, not fallback default tickers.

For the first release, keep metadata limited to the registry, writer status, publication/migration receipts, and a maintenance recovery journal. No live file-selection catalog or reader leases are required. The ingestion publisher owns batch publication; maintenance acquires that ownership only after publication is fenced. Migration uses unique provenance/file IDs and the same serialized publication path. Never allow separate processes to replace the same final filename.

Symbol deletion needs an explicit first-release policy because raw glob consumers do not understand tombstones. Deactivate/unsubscribe immediately using a subscription-generation fence; return a truthful pending-purge state. Physical removal from the active `ticks/` tree happens in a coordinated maintenance window after affected readers stop. Until then, raw readers may still see historical rows; do not claim logical deletion has hidden them. Retire files outside the query tree and keep recovery records. Do not re-enable the symbol until its pending purge finishes, or old history would reappear. Update API/UI tests and wording for this deliberate change from synchronous SQL deletion. Never silently delete buffered ticks except as part of the explicitly requested purge. Keep archived mappings for historical symbols absent from the active registry.

Before any maintenance replacement, stop/drain affected dashboard and Repo B queries and disable their automatic restarts. Fence writes to affected partitions, including late arrivals. Inbound capture may continue only within a tested bounded buffer or durable spool; if that cannot cover the window, defer maintenance or plan a capture pause and record its gap. A maintenance lock alone does not stop a raw-glob consumer: verified shutdown of all configured consumers is an operational prerequisite. Use a durable in-progress journal and make managed services refuse restart while recovery is unfinished. Recovery must complete or roll back the replacement before those services resume.

For future online maintenance only: introduce a single catalog commit owner and immutable per-symbol/day manifests containing file IDs/paths, schema, row counts, min/max ordering keys, checksums and active generation. Readers capture explicit versioned file lists. Cross-partition capture is a vector of versions, not a global transaction. Retain superseded files until no supported snapshot references them; defer GC to offline maintenance unless both repositories implement tested reader pins/leases. A fixed delay alone is not evidence that a long replay has finished. Do not rewrite a global history manifest on each flush.

## 4. Reader, candles, and replay

Introduce a `TickLakeReader` that accepts an explicit root/configuration and owns a private `duckdb.connect(':memory:')` per worker/request. Set UTC, reader thread limit, memory budget, and a dedicated spill directory; close in `finally`. Keep total parallel query concurrency bounded so several readers do not each consume all machine cores. No process-global mutable connection.

Public contracts to implement:

```text
TickLakeWriter.append_batch(records, batch_id) -> PublishReceipt
TickLakeReader.snapshot(symbols, start_utc, end_utc) -> Snapshot
TickLakeReader.query_ticks(snapshot, filters, limit) -> bounded result
TickLakeReader.query_candles(snapshot, timeframe, session_policy) -> result
TickLakeReader.iter_ticks(snapshot, cursor, batch_rows) -> iterator  # P5 follow-up
SymbolRegistry.read() -> versioned inventory
MigrationTool.plan/export/verify/publish -> persisted reports
```

Resolve symbol/date partitions before file discovery. Pass a list of active files into DuckDB with explicit Hive configuration. Apply native timestamp range predicates early (`>= start`, `< end`) and select only needed columns. Avoid casting the stored column in pruning predicates. Empty snapshot returns a typed empty relation/result; unmatched globs must not become API errors. Each refresh captures new files; a long-running replay retains its original snapshot.

Adapt legacy `tick_data`, `ticks`, and `streaming_ticks` names as temporary read-only views where needed during the transition. Do not route INSERT or DELETE through those views or reuse the SQL-rewriting compatibility adapter as the new storage API. Test view predicates actually push down; final reader interfaces should receive the range before creating relations.

Candles:

- Preserve current dashboard response keys, ascending display order, limits, `time` UTC epochs, exchange-local `time_str`, regular/extended hours, and day/session counts.
- Compute first/last by `(timestamp, ingest_id)` for deterministic open/close; high/low and current volume/count semantics remain explicit.
- Keep lower-level UTC bucket behavior and dashboard NYSE-aligned behavior separately documented until a deliberate API version unifies them. A one-day NYSE candle is not blindly a UTC date group.
- Use UTC range filters for partition pruning, then exchange-local bucketing. Retain DST and session-boundary tests. Keep missing intervals as gaps unless a current documented endpoint explicitly fills them.
- Existing endpoints using inclusive `end` retain compatibility at their boundary; new reader/replay contracts are half-open. Do not silently change old API inclusivity.
- In the first release, key cached results by symbol/range/timeframe/session policy/schema version and the discovered immutable file identities; refresh discovery on each request or apply an explicitly bounded freshness TTL. Invalidate cached file lists after maintenance. A late file must invalidate its historical candle, not only the current day. Use writer status for publication freshness and Parquet metadata where sufficient for counts; do not add a persistent catalog solely for caching. Future catalog mode may use partition versions instead.

Replay:

- New iterator; existing paginated inspector is not a replay transport. Avoid pandas, `.rows`, `.fetchall()`, and large OFFSET pagination.
- Save the explicit immutable file list for a replay, iterate disjoint UTC date/time windows chronologically, query with total ordering, and consume Arrow record batches or bounded fetches. A cursor includes snapshot identity, window, timestamp, and ingest ID; resume uses a lexicographic `>` predicate. Offline maintenance must wait for active replay and invalidate retired snapshots explicitly; persistent cross-maintenance resume requires retaining/remapping their files or the later catalog/pinning mode.
- `ORDER BY` may sort/spill before producing a batch. Bounded output alone is not bounded query memory or fast first output. Configure spill and measure time-to-first-batch; if needed subdivide windows or merge sorted file streams with bounded fan-in.
- Files may overlap in event time; never concatenate files by filename. Snapshot replay excludes later arrivals. A future live-follow mode needs a separate late-event/watermark policy.
- Validate cursor ownership/schema and fail clearly if maintenance has intentionally retired its snapshot; never silently resume against a newly discovered file set. If uninterrupted resumability through maintenance becomes a requirement, implement durable pins before enabling that maintenance behavior.

Repo B handoff: supply a minimal schema/glob contract, in-memory connection example, safe symbol/date selection, empty-lake behavior, maintenance pause/restart requirements, and a tiny golden fixture in P4. Repo B must stop attaching the legacy tick DB to resolve the cross-repository lock issue. Full replay iterator/cursor integration follows in P5; catalog snapshot selection is required only before future online maintenance. Repo B was not inspected, so its implementation and UI performance cannot be declared complete from this repository alone.

## 5. Ordered implementation packages

Implement each package with its tests, reviewable as a separate commit/PR. Package numbers identify work areas, not an obligation to finish every package before releasing the lock fix.

**First release order:** `P0 → P1 → P2 → P3 → P4 → P6 rehearsal → P8 first cutover`. P6 tooling can be developed after P1 and before P4, but run production migration/cutover only once the new reader and registry paths pass their gates. P3 is essential: leaving the symbol registry in the old writable DB retains an ingestion/dashboard dependency and possible lock contention. P4 includes the minimal Repo B read contract; full P5 playback is not a prerequisite.

**Follow-up order:** P7a off-hours compaction before the measured file-count/capacity budget is exceeded; P5 full replay as a separate feature; P7b online catalog maintenance only if offline windows are insufficient. Replay and compaction can be scheduled in either order once their own prerequisites pass. Until P7a is ready, the lake stays append-only, purges stay pending, and no file replacement runs. The first release must expose that limitation and have a capacity-triggered follow-up date.

### P0 — Safe baseline and characterization

Files: `tests/conftest.py`, test configuration, new benchmark tool, storage contract documentation.

1. Route all default data/database paths to `tmp_path` before imports or use injectable runtime config; cover modules that imported constants by value. Block production volume access and network in normal tests.
2. Mark live-provider/real-database tests opt-in; remove real writes from ordinary test cases. Use ephemeral server ports and proper server shutdown.
3. Establish current isolated test baseline, recording pre-existing failures separately.
4. Build deterministic quote fixtures with repeats, late arrivals, multiple sources, nulls, UTC day crossings, NYSE session edges and DST. Record golden endpoint responses and expected candles independently of production SQL.
5. Benchmark current SQL writer and queries on a copied/synthetic database, never the active one. Record hardware, versions, volume filesystem, rates, CPU seconds per million ticks, event-loop lag, RSS, bytes written, query percentiles.

Gate: ordinary tests cannot reach production data; baseline and fixture contract saved.

### P1 — Schema, config, atomic files and recovery

New suggested files: `src/storage/{__init__,config,schema,publication}.py`; add pinned dependencies in `requirements.txt`.

Implement explicit path resolution, format version, typed schema, symbol encoding, stable IDs, publication state machine, empty snapshots, publisher ownership, per-file recovery receipts, and a maintenance-in-progress startup guard. Defer `catalog.py` and reader leases to P7b. Support read-only lake inspection without initialization side effects. Test on supported macOS/Windows filesystems; validate rename/fsync behavior on the actual external volume before durability claims.

Gate: crash-stage tests prove no partial visibility or duplicate retry publication.

### P2 — Streaming writer and lifecycle

Files: new `src/storage/parquet_writer.py`; `src/stream/runner.py`; callback adapters in `capital_stream.py`/`binance_stream.py`; `tools/service_supervisor.py` and service launch/stop scripts as necessary.

Implement queue bounds, age/size flushing, one off-loop worker, receipts, retry state, honest counters and cooperative shutdown. Keep Capital as default and Binance disabled. Keep legacy tuple compatibility only through tested explicit adapters; do not label an OHLCV bar as a true trade tick silently. Ensure a custom root controls every write (the existing custom `db_path` can be bypassed when `db_conn` is unset).

Gate: slow-disk fault injection does not stall callback scheduling; normal drain publishes every accepted fixture row exactly once. Failure behavior remains observable.

### P3 — Registry and administrative compatibility

Files: new `src/storage/registry.py`; streaming symbol functions in `src/database/operations.py`; `src/dashboard/server.py`; runner reload; symbol UI.

Implement migration/import of inventory, version polling, serialized add/remove, purge fencing and inactive/empty semantics. Keep public route names where practical; return truthful pending/error state for asynchronous purge. No registry path should reopen streaming DuckDB after cutover.

Gate: separate dashboard and runner processes demonstrate add, deactivate, pending purge and re-add refusal while purge is pending; unrelated symbols remain intact. P7a tests completed purge and safe re-add. First-release UI must not report a pending purge as completed deletion.

### P4 — Lake queries and dashboard integration

Files: new `src/storage/reader.py`; streaming portions of `src/database/operations.py`; `src/dashboard/analytics.py`; `src/utils/integrity.py`; `tools/audit_database_integrity.py`; `src/database/__init__.py` exports; relevant static labels.

Replace all streaming connection calls including tape/status/weeks/continuity, inventory counts, unified connection use, price drift and health reports. Keep historical behavior. Treat a locked historical DB as an explicit separate limitation in mixed integrity requests; this migration does not solve its writer concurrency. Replace misleading UI/README claims about DB files. Report active files and separately retained backup/maintenance bytes in health. Ship the minimal Repo B read contract and confirm its actual read path works before claiming the second-repository lock issue is resolved.

Gate: golden API equivalence and timezone regression suite pass; intercept disk `duckdb.connect`/ATTACH and prove streaming routes cannot touch either legacy tick DB or historical DB accidentally.

### P5 — Replay contract and Repo B integration artifact

Files: new `src/storage/replay.py`, `docs/tick-lake-reader-contract.md`, fixtures and examples.

Implement bounded snapshot replay and resumption. Deliver shared contract tests usable in Repo B; inventory Repo B separately before selecting its adapter files. Do not assume this repo already contains a rewind engine.

Gate: concatenated replay equals an independently sorted fixture exactly, with no skip/duplicate at equal timestamps or restart boundaries; memory does not scale with archive size.

### P6 — Migration tooling and rehearsal

Files: new `tools/migrate_streaming_to_parquet.py`, migration modules/tests, operator runbook.

Implement `plan`, `export`, `verify`, `publish`, `status` modes with explicit source/destination and dry-run output. Follow section 6. Keep legacy schema initialization available only for migration fixtures/legacy mode until transition ends; never call it to initialize a lake.

Gate: interrupted/resumed rehearsal on synthetic and copied databases passes exact reconciliation; unresolved discrepancy blocks publish. Building the tool does not authorize or start production migration; P8 first-release prerequisites must pass before the actual handoff.

### P7a — Off-hours compaction and performance tuning

Files: new `src/storage/compaction.py`, maintenance CLI, benchmark suite.

For timer-only flushing over a full 24 hours, the old two-second setting gives 43,200 batches/day; five seconds gives 17,280 and ten seconds gives 8,640. Multiply by continuously represented symbols, and account for early row/byte triggers. These are illustrative counts, not bounds. Prevent empty files and establish actual file-count thresholds from P0/P4 query benchmarks.

Default to a scheduled off-hours maintenance window, never an assumed automatic “safe after close” window. Produce one date-named file per symbol/day when suitably sized, using a unique generation suffix such as `date=2026-10-03/2026-10-03_compacted_<id>.parquet` so stale file references cannot silently read different contents; split large days into numbered files instead of requiring one arbitrarily large file. Preserve UTC date partitioning; market-session close does not seal a UTC date or rule out late data.

1. Capture the candidate input file set; immutable files allow prebuilding sorted replacement outputs outside `ticks/` while normal appends continue. This prebuild does not change reader visibility.
2. Verify exact original-column multiplicity and ingest IDs against that captured set. Never deduplicate legitimate ticks. Initial benchmark candidates are 64–256 MiB compressed files and roughly 100k–250k rows/group; tune to the actual workload.
3. Stop/drain all affected readers, prevent their restart, and fence affected writer publication. Persist a recovery journal listing input/output paths and checksums before moving anything. Revalidate the captured inputs; preserve every new/late file outside that captured set.
4. Retire captured inputs to `_maintenance/` outside all reader globs and publish verified replacement files into the partition. This is a journaled multi-file operation, not an atomic directory transaction. On any interruption, keep maintenance active until recovery completes or rolls back; never restart readers while both copies or neither copy are visible. On re-compaction, treat any existing daily file as an input and publish its replacement under a new unique name. Never reuse a retired path: a stale replay cursor must fail explicitly rather than read changed contents.
5. Compare the resulting active partition to the pre-maintenance expected union of captured and uncaptured files. Resume services only after success; retain retired inputs for a defined recovery period and reclaim them separately. Late ticks arriving after maintenance become ordinary new batch files, read alongside the daily file and included next time.

Use the same stopped-reader procedure for pending symbol purges, with subscription fencing and controlled queue handling. Refuse maintenance when any required consumer cannot be stopped. Add crash/restart tests at every retire/publish step, reader-restart guards, late-file preservation, and double-run idempotence.

Gate: before and after maintenance produce identical tick IDs and values (except an explicitly requested purge); ordinary appends remain concurrent with reads; no reader runs during replacement. File-count and capacity benchmarks establish when another pass is needed.

If raw queries miss the agreed chart SLO, add a derived minute-candle cache keyed by input file identities; combine rollups only with disjoint raw ranges and invalidate on late data. Raw ticks remain authoritative. This is conditional optimization, not a migration prerequisite.

### P7b — Optional online catalog maintenance

Implement only if P7a windows cannot meet operational needs. Add the catalog design from section 3.4 and migrate every consumer before enabling it. Compact captured inputs into separate immutable outputs; the single catalog owner atomically replaces only still-active input IDs while preserving concurrent appends. Retain files for existing snapshots; add durable pins/leases only when automatic online GC or uninterrupted cross-maintenance replay is needed. Never enable this mode for consumers still scanning raw globs.

Gate: separate writer/readers and compactor run concurrently with exact per-snapshot equivalence, no duplicate/missing rows, and tested catalog crash recovery. This gate does not block the append-only first release.

### P8 — Production cutover and completion

**First cutover:** require P0, P1, P2, P3, P4 and the P6 rehearsal gates, including the minimal Repo B read integration. Execute the section 6 handoff, run sustained concurrent ingestion/queries, retain legacy snapshot and reports, then remove normal runtime legacy tick connection paths. Full P5 replay and P7 compaction are not first-cutover blockers if append-only capacity/freshness targets hold and pending purge is communicated accurately.

Update README, `.env.example`, service documentation and planning state to the implemented architecture. No automatic old-data deletion. First-release acceptance includes zero-loss historical reconciliation, isolated tests, publication recovery, separate-process queries and honest durability/freshness reporting.

**Follow-up completion:** enable P7a only after its maintenance gates pass; deliver P5 against the established lake; use P7b only for a demonstrated need. Apply section 8 gates to the features being released, not to deferred features as though they were already implemented.

## 6. Historical migration: zero-loss procedure

1. **Inventory, without mutation.** Resolve the actual source path (`streaming.duckdb` in this repository; accept explicit `streaming.db` if the operator has one). Identify base tables/views/types, count rows per symbol/date/source, nulls, duplicate multiplicity, timestamp extrema, and registry records. Do not export both a base table and its compatibility view. If distinct legacy tick tables coexist, stop and define which are authoritative; do not silently union/deduplicate.
2. **Plan capacity.** Estimate source backup + exported Parquet + validation spill + new live accumulation + compaction headroom. Verify destination mount identity and same-filesystem staging. Record source schema, version and immutable migration ID.
3. **Establish a consistent frozen source.** Stop all legacy writers cooperatively, drain them using the tested lifecycle, close connections, perform a controlled checkpoint if required, then take and verify a closed database backup. Never copy only a live `.duckdb` while ignoring its WAL. If recovering an unclean shutdown, recover/checkpoint under exclusive ownership first. The current immediate-exit handler is not sufficient evidence of a drained queue.
4. **Switch new capture to the lake promptly.** Import and verify the frozen symbol registry before starting the new writer; then make the new registry authoritative. Disable purge/compaction until historical migration finishes so reconciliation sees a stable historical set. Start the tested Parquet writer with a distinct provenance namespace and stop any old supervisor from restarting the DB writer. Export the frozen legacy copy while new lake ingestion continues. Use provenance, not event timestamp, to separate old/new records: delayed events may have old timestamps. Record the capture interruption interval; no provider replay means ticks emitted during it cannot be promised recoverable.
5. **Export bounded chunks.** Iterate symbol/UTC-day, subdividing large partitions. Use native COPY or Arrow batches without full-table pandas. Preserve all eight original columns with no rounding/defaulting. Derive deterministic IDs from the frozen source mapping. Prefer a verified stable base-table row identifier for this exact backup; if unavailable, materialize a persistent numbered table in a separate migration working database once and resume from it; never mutate the immutable source backup. Timestamp-only pagination and rerunning an unordered `row_number()` are invalid. Persist mapping/snapshot checksum before resuming chunks.
6. **Checkpoint every chunk.** Store input range/IDs, row counts, target file IDs, schema, checksums, ordering statistics and verification state. Write files in `_migration/` staging outside all reader globs. Re-execution verifies/reuses completed chunks; damaged chunks are rewritten under controlled staging. Changed source identity invalidates resume.
7. **Verify exact equivalence.** Compare total and per-partition counts, all-column null counts, extrema and useful sums as diagnostics. Perform both directions of `EXCEPT ALL` on the original eight columns, partition by partition, between frozen source and export. Both differences must be empty; `EXCEPT` alone loses duplicate multiplicity. Verify unique generated IDs separately and every file footer/checksum. Check precision and special floating-point values explicitly; hashes/sums alone are insufficient proof. Verify the imported initial registry against its frozen source, including inactive entries; record untracked historical symbols in an archive mapping without activating subscriptions. Preserve later legitimate registry edits instead of overwriting them when the long-running tick export finishes.
8. **Publish only verified chunks.** Publisher atomically renames verified historical files, with deterministic unique names, into their existing symbol/date partitions without replacing newly streamed files. Serialize publication/retry ownership and preserve new live files. In the future catalog mode, publish their catalog membership instead. Publication receipts make a restart after a partial multi-partition publish idempotent. Mark migration complete only after every source chunk is visible and a final source-only provenance reconciliation passes. Dashboard can label historical coverage incomplete until then.
9. **Preserve evidence and backup.** Retain immutable source backup, mapping, reports, migration checkpoints and final file inventory. Do not remove the legacy DB as part of cutover. Zero loss refers to all rows present in this frozen source, including rows for symbols no longer subscribed.

Rollback before new lake capture: keep/resume the verified legacy source. Rollback after new lake capture: keep every lake file/spool and snapshot; roll back compatible reader/application releases while continuing supported lake capture. Do not just restart the old DB-only system and call it current. A true DB fallback requires a separately verified import of post-cutover lake records with an ID ledger into a replacement database, then another drained writer handoff. Rehearse this before any operational promise of one-command rollback.

## 7. Test implementation map

| Existing suite | Planned action |
|---|---|
| `tests/stream/test_live_engine.py`, `test_dynamic_reload.py` | Replace writer persistence expectations with temporary Parquet and receipt assertions; await completion instead of sleeps followed by cancel; preserve parser/subscription behavior |
| `tests/database/test_storage.py`, `test_crud.py`, `test_resampling.py`, `test_dual_storage.py` | Keep historical cases; move streaming cases to lake fixtures; preserve duplicates and precision; separate backend-independent result tests from legacy-only adapter tests |
| `tests/database/test_concurrency.py` | Keep relevant historical tests; replace claimed lake coverage with subprocess readers/writer and explicit synchronization barriers |
| `tests/test_database_exclusivity.py`, `test_dashboard_segregation.py`, `test_symbol_maps_separation.py` | Assert historical DB versus tick lake/control metadata separation, not existence of two writable DuckDB files |
| `tests/test_streaming_symbols_management.py` | Eliminate real DB writes; validate pending purge, fencing, P7a maintenance completion, blocked re-add until completion, empty inventory and cross-process reload |
| `tests/test_streaming_*` chart/gap/continuity/week suites | Retain behavioral assertions; create fixture lake via test writer, inject reader; keep SQL-only fixtures only for backend-neutral timestamp unit tests |
| `tests/dashboard/test_chart_timezone*.py`, other dashboard API suites | Run lake-backed results under multiple host timezones; preserve UTC epoch and NYSE labels; isolate cache by root/snapshot |
| `tests/utils/test_integrity.py` | Lake quiet intervals, price drift and health; historical-side errors reported independently |
| `tests/e2e/test_milestone_v2.py`, `test_cli_smoke.py` | Update architecture assertions; run independent writer/dashboard/reader processes using temporary roots |
| API/config tests | Keep scope unchanged unless dependency/config injection changes affect imports; external-provider tests opt-in |

New test groups, with concrete assertions:

- **Schema:** roundtrip every field, microseconds, aware timestamp offsets, null bid/ask/volume, duplicates, mixed sources, unsafe symbol paths, schema mismatch, empty lake, unusual historical numeric values.
- **Publication:** pause writer before footer close, before rename and after rename/before receipt; in optional P7b also pause before/after catalog publication. Readers see only valid committed files; recover same IDs exactly once. Inject permission error, full disk, unplugged root, collision, truncated file, corrupt publication receipt (and, for P7b, corrupt catalog); report errors instead of pretending an empty lake.
- **Queue/lifecycle:** flush age/row/byte triggers with controllable clocks; low traffic, multi-symbol split, late-day partition, max memory, retry conservation, no concurrent writes, slow worker with responsive event-loop heartbeat. SIGTERM/drain and forced kill tested in real subprocesses; optional spool recovery separately.
- **Concurrency:** use spawned processes, one writer and at least two continuously querying readers. Coordinate interleavings with events/barriers, not lucky sleeps. Readers check footer validity and unique IDs; final union equals all successful publish receipts. Keep a legacy tick DB write lock open in a separate process to prove lake reads never need it.
- **Query correctness:** independent Python oracle for candles, deterministic equal-time open/close, DST transitions, regular/extended edges, UTC midnight, inclusive legacy API versus half-open new API, no data, tick-count volume, source isolation, cache invalidation on late ticks.
- **Pruning:** seed many irrelevant symbol/date partitions; inspect query profile/files scanned and snapshot-selected paths. Assert unrelated files are not selected/scanned. Timing alone is not proof of pruning.
- **Replay:** duplicate timestamps across files, out-of-order arrival, multi-symbol order, page size one, cursor restart, cancellation/close, invalid cursor, new writes during pinned replay, compacted input retention. Compare complete result and ordering to independent expected IDs; measure RSS/time-to-first-batch at larger scale.
- **Migration:** synthetic legacy DB with identical duplicate rows, null fields, inactive/orphan symbols, legacy aliases, multiple days. Test lock refusal, wrong snapshot identity, interruption at each checkpoint, double rerun, new live files during historical publish, verification corruption and insufficient space. Any failed exact comparison blocks final publish.
- **Maintenance:** for P7a, prebuild while new/late files arrive, then stop readers/fence publication and inject crashes at each replacement step; restart is blocked until recovery, and all uncaptured late files survive. Reject maintenance when a consumer cannot stop. Purge/re-add cannot resurrect removed history. For P7b only, test concurrent old/new snapshots and online catalog replacement. GC refuses unsafe active-reader mode; Windows open-file failures remain retryable.
- **End to end:** mocked WebSocket -> actual runner process -> finalized lake -> separate DuckDB reader -> candles (plus replay at P5); graceful restart; dashboard symbol edit -> runner observes new registry version.

Suggested new locations: `tests/storage/`, `tests/migration/`, `tests/replay/`, `tests/performance/`, `tests/e2e/test_tick_lake.py`. Register markers `integration`, `performance`, `live`. Normal CI runs offline deterministic tests; performance runs on a named reference machine; live tests require explicit opt-in.

Proposed commands after implementation (not executed during planning):

```sh
python -m pytest tests/storage tests/migration tests/replay tests/stream -q
python -m pytest tests/database tests/dashboard tests/utils tests/e2e -m 'not live and not performance' -q
python -m pytest tests -m 'not live and not performance' -q
python -m pytest tests/performance -m performance -q
```

Run existing top-level streaming regression files through the full isolated suite. Capture failures rather than weakening assertions to accept the new implementation.

## 8. Performance and release gates

Benchmark with deterministic 1M/10M-row datasets, at least the current 19-symbol distribution plus a skewed hot symbol, one day/month ranges, duplicates/late ticks, and projected small-file counts. Use both cold-ish and warm runs and label OS cache conditions honestly. First release benchmarks concurrent ingestion plus chart queries; add concurrent replay at P5 and online compaction only at P7b. Compare identical workloads to the P0 baseline. P7a measures compaction duration and before/after query cost in its stopped-reader window.

Initial proposed acceptance targets, to calibrate on the actual Mac/SSD before treating as contractual:

| Metric | Proposed gate |
|---|---|
| Correctness | Zero mismatches against accepted/published IDs and migration `EXCEPT ALL`; zero invalid published files |
| Process concurrency | No tick-database lock errors across sustained independent writer/readers |
| CPU | At least 50% lower writer CPU seconds per million ticks than measured baseline, or documented tuning blocker before release |
| Freshness | Healthy-load p99 visibility within configured flush interval plus 1 second |
| Callback responsiveness | p99 event-loop scheduling lag under 20 ms at measured peak input; no sustained queue growth |
| Chart/symbol switch | Warm single-symbol single-session 1m/5m query p95 under 100 ms; broader cold history measured separately |
| Daily candles | Warm one-symbol month query p95 under 250 ms; use conditional rollups if target fails |
| Replay (P5 gate) | Warm time-to-first-batch under 250 ms for a requested session; RSS within configured budget plus measured engine overhead, independent of total archive size |
| Mixed load | No dropped acknowledged records; read targets hold at peak recorded ingestion rate; maintenance prebuild throttled if it causes backlog; replacement only within its tested publication-pause budget |
| Endurance | At least 24 hours of synthetic sustained load plus restart/fault scenarios; repeat real-volume benchmark on supported hosts |

Measure absolute CPU/RSS as well as relative improvements. A DuckDB memory setting is not a hard total-process RSS cap, so enforce aggregate service concurrency and observe real RSS. Parquet compression is still work; “ultra-low CPU” must be established experimentally.

Release checklist:

- [ ] Every runtime streaming reader/writer is detached from legacy tick DB.
- [ ] All tests are isolated and pass, with pre-existing issues documented.
- [ ] Failure/recovery and true multi-process tests pass.
- [ ] Migration exact reconciliation and registry mapping are complete.
- [ ] Repo B uses the minimal lake read contract and passes shared fixtures; full replay is tracked separately under P5.
- [ ] First release is append-only with file-count/capacity alerts and a scheduled P7a follow-up; pending purge is clearly reported.
- [ ] P7a replacement is enabled only after every affected reader can be stopped and recovery/restart guards pass.
- [ ] P7b online maintenance, if later required, is enabled only after all readers use the catalog contract.
- [ ] Performance report meets agreed gates; deviations are explicit.
- [ ] Graceful and forced shutdown behavior is documented and exercised on target OS.
- [ ] Backups and post-cutover recovery are verified; no legacy data is deleted automatically.

## 9. Documentation supporting the design

These references support engine capabilities; the publication and maintenance protocols above are this plan's proposed application design. Online catalog machinery is an optional later extension.

- [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency): native database process ownership explains why short write windows/retries do not establish reliable independent-process access.
- [Hive partitioning](https://duckdb.org/docs/current/data/partitioning/hive_partitioning): partition predicates support excluding irrelevant symbol/date paths.
- [Parquet reading](https://duckdb.org/docs/current/data/parquet/overview): DuckDB accepts globs or explicit file lists and supports filter/projection pushdown.
- [Parquet tuning](https://duckdb.org/docs/current/data/parquet/tips): file layout, sorting and row-group sizes affect scan efficiency; micro-batches require measurement and maintenance.
- [Workload tuning](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads): memory, thread settings and blocking operations matter for concurrent queries and replay.
- [PyArrow Parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html): explicit schema conversion, compression and row-group configuration should be pinned and tested.
