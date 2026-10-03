# Roadmap: Data Harvester

## Milestones

- ✅ **v1.0 Local DuckDB & 24/7 Live Streaming Engine** — Phases 1–4 (shipped 2026-09-25)
- ✅ **v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard** — Phases 5–9 (shipped 2026-09-25)
- ✅ **v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard** — Phases 10–14 (shipped 2026-09-26)
- ✅ **v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)** — Phases 15–21 (shipped 2026-10-03)
- 🟡 **v4.1 Partitioned Parquet Lake Deep Testing & Hardening** — Phases 22–27 (in progress)

## Phases

### Completed Milestones

<details>
<summary>✅ v1.0 Local DuckDB & 24/7 Live Streaming Engine (Phases 1–4) — SHIPPED 2026-09-25</summary>

- [x] Phase 1: Turso Data Migration & Legacy Purge (1/1 plan) — completed 2026-09-25
- [x] Phase 2: DuckDB Storage Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 3: 24/7 Live WebSocket Streaming (1/1 plan) — completed 2026-09-25
- [x] Phase 4: Test Suite Migration & Verification (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md)

</details>

<details>
<summary>✅ v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard (Phases 5–9) — SHIPPED 2026-09-25</summary>

- [x] Phase 5: Dedicated Dual-DuckDB Storage Layer (1/1 plan) — completed 2026-09-25
- [x] Phase 6: Capital.com Exclusive WebSocket Streamer & Dynamic Reload (1/1 plan) — completed 2026-09-25
- [x] Phase 7: Data Integrity & Health Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 8: Interactive JavaScript Web Dashboard & Symbol Management (1/1 plan) — completed 2026-09-25
- [x] Phase 9: End-to-End Verification & Full Test Suite (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v2.0-ROADMAP.md](milestones/v2.0-ROADMAP.md)

</details>

<details>
<summary>✅ v3.0 Observability Command Center (Phases 10–14) — SHIPPED 2026-09-26</summary>

- [x] Phase 10: Backend Analytics & High-Performance Data APIs (1/1 plan) — completed 2026-09-25
- [x] Phase 11: Interactive Financial Charting & Data Explorer UI (1/1 plan) — completed 2026-09-25
- [x] Phase 12: Live Stream Tape, Telemetry & Market Operations UI (1/1 plan) — completed 2026-09-25
- [x] Phase 13: Enhanced Symbol Data Matrix & Context-Aware Integrity Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 14: Comprehensive Verification, Testing & Polish (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v3.0-ROADMAP.md](milestones/v3.0-ROADMAP.md)

</details>

<details>
<summary>✅ v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage) (Phases 15–21) — SHIPPED 2026-10-03</summary>

- [x] Phase 15: Safe Test Isolation & Baseline Characterization (P0) (1/1 plan) — completed 2026-10-03
- [x] Phase 16: Lake Schema, Configuration, Atomic Files & Recovery (P1) (1/1 plan) — completed 2026-10-03
- [x] Phase 17: Streaming Parquet Writer & Runner Lifecycle Integration (P2) (1/1 plan) — completed 2026-10-03
- [x] Phase 18: Versioned Symbol Registry & Administrative Compatibility (P3) (1/1 plan) — completed 2026-10-03
- [x] Phase 19: In-Memory DuckDB Lake Reader & Dashboard Integration (P4) (1/1 plan) — completed 2026-10-03
- [x] Phase 20: Zero-Loss Migration Tooling & Rehearsal (P6) (1/1 plan) — completed 2026-10-03
- [x] Phase 21: Production Cutover, Concurrency Validation & Handoff (P8) (1/1 plan) — completed 2026-10-03

See: [.planning/milestones/v4.0-ROADMAP.md](milestones/v4.0-ROADMAP.md)

</details>

### Active Milestone: v4.1 Partitioned Parquet Lake Deep Testing & Hardening

- [x] **Phase 22: Storage Foundation & Publication Edge Case Tests** (`src/storage/config.py`, `schema.py`, `publication.py`) — completed 2026-10-03 (commit 04544d56, 36/36 tests passing)
  - Storage layout path traversal, unicode/special symbol encoding, and corrupted metadata handling.
  - PyArrow schema type coercion, extreme numeric limits (float min/max, subnormal), null bitmasks.
  - Atomic publication concurrency collisions, crashed intent recovery, and file lock serialization.
- [ ] **Phase 23: Streaming Writer & Runner Stress & Lifecycle Tests** (`src/storage/parquet_writer.py`, `src/stream/runner.py`)
  - High-throughput micro-batching under memory pressure (100k+ ticks) and bounded queue backpressure.
  - Runner sudden shutdown mid-flush, graceful drain timeouts, and honest queue acknowledgments.
  - Transient disk full / I/O error exponential backoff and quarantine handling.
- [ ] **Phase 24: Versioned Symbol Registry & Dynamic Reload Stress Tests** (`src/storage/registry.py`, `src/dashboard/server.py`)
  - Cross-process concurrent symbol CRUD lock serialization and monotonic versioning integrity.
  - Rapid symbol toggle/delete flapping and `PENDING_PURGE` generation fences.
  - File signal debouncing and dynamic reload latency under heavy polling.
- [ ] **Phase 25: In-Memory DuckDB Lake Reader & Analytics Edge Tests** (`src/storage/reader.py`, `src/dashboard/analytics.py`)
  - Multi-threaded in-memory DuckDB connection scaling (30+ concurrent readers) without memory leaks.
  - Vectorized resampling edge cases: sparse partitions, multi-day roll-overs, DST shifts, leap years.
  - Reverse-chronological tape pagination with high offsets and non-existent symbol pruning.
- [ ] **Phase 26: Migration Tooling Rehearsal & Fuzz Tests** (`tools/migrate_streaming_to_parquet.py`)
  - Migration of corrupt / partial legacy DuckDB tables and schema drift.
  - Simulated crash interruption across all migration modes (`plan`, `export`, `verify`, `publish`).
  - Two-way `EXCEPT ALL` fuzz testing with synthetic data corruption and precision mismatch detection.
- [ ] **Phase 27: Multi-Process Long-Running Soak & Chaos Tests** (`tools/service_supervisor.py`, `tools/validate_concurrency.py`)
  - Multi-process soak testing under continuous ingestion and continuous analytical reading.
  - Chaos monkey process termination (streamer, dashboard, supervisor) and automatic self-healing.
