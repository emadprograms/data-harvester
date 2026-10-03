# Roadmap: Data Harvester

## Milestones

- ✅ **v1.0 Local DuckDB & 24/7 Live Streaming Engine** — Phases 1–4 (shipped 2026-09-25)
- ✅ **v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard** — Phases 5–9 (shipped 2026-09-25)
- ✅ **v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard** — Phases 10–14 (shipped 2026-09-26)
- 🟡 **v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)** — Phases 15–21 (in progress)

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

### Active Milestone: v4.0 Partitioned Parquet Tick Lake

- [ ] **Phase 15: Safe Test Isolation & Baseline Characterization (P0)**
  - Isolate all test suites to `tmp_path`, block production volume mutations.
  - Build deterministic quote fixtures (repeats, late arrivals, nulls, session boundaries).
  - Benchmark baseline DuckDB write/query CPU seconds and event-loop lag.
- [ ] **Phase 16: Lake Schema, Configuration, Atomic Files & Recovery (P1)**
  - Implement `src/storage/{config, schema, publication}.py`.
  - Schema v1 with microsecond UTC timestamps and stable unique `ingest_id`.
  - Atomic `.tmp` staging, final rename, and idempotent publication receipts.
- [ ] **Phase 17: Streaming Parquet Writer & Runner Lifecycle Integration (P2)**
  - Implement `TickLakeWriter` with 5s / 5,000 tick thresholds and bounded queue.
  - Dedicated off-loop PyArrow worker thread preserving <20ms event loop lag.
  - Wire into `src/stream/runner.py` with graceful shutdown drain.
- [ ] **Phase 18: Versioned Symbol Registry & Administrative Compatibility (P3)**
  - Implement `src/storage/registry.py` (atomic JSON registry, version polling).
  - Dynamic reload without DuckDB file locks; pending-purge state handling.
- [ ] **Phase 19: In-Memory DuckDB Lake Reader & Dashboard Integration (P4)**
  - Implement `src/storage/reader.py` (`TickLakeReader` with in-memory DuckDB).
  - Deterministic OHLCV resampling via `arg_min(price, (timestamp, ingest_id))`.
  - Update dashboard queries; deliver minimal read contract for Repo B.
- [ ] **Phase 20: Zero-Loss Migration Tooling & Rehearsal (P6)**
  - Implement `tools/migrate_streaming_to_parquet.py`.
  - Chunked export and two-way `EXCEPT ALL` verification against frozen legacy DB.
- [ ] **Phase 21: Production Cutover, Concurrency Validation & Handoff (P8)**
  - Freeze legacy writer, switch runner to live Parquet lake, publish historical partitions.
  - Multi-process concurrency validation (Writer + Dashboard + Repo B simulation).

