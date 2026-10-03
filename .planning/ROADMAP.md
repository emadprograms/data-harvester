# Roadmap: Data Harvester

## Milestones

- ✅ **v1.0 Local DuckDB & 24/7 Live Streaming Engine** — Phases 1–4 (shipped 2026-09-25)
- ✅ **v2.0 Dedicated Dual-DuckDB Storage, Capital.com Tick Streamer & Data Integrity Web Dashboard** — Phases 5–9 (shipped 2026-09-25)
- ✅ **v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard** — Phases 10–14 (shipped 2026-09-26)
- ✅ **v4.0 Partitioned Parquet Tick Lake (Decoupled High-Concurrency Storage)** — Phases 15–21 (shipped 2026-10-03)
- ✅ **v4.1 Partitioned Parquet Lake Deep Testing & Hardening** — Phases 22–27 (shipped 2026-10-03)
- ⏭️ **Next milestone:** not yet planned (see "Milestone Backlog" below)

## Phases

### Completed Milestones

<details>
<summary>✅ v4.1 Partitioned Parquet Lake Deep Testing & Hardening (Phases 22–27) — SHIPPED 2026-10-03</summary>

- [x] Phase 22: Storage Foundation & Publication Edge Case Tests (1/1 plan) — completed 2026-10-03 (36/36 tests)
- [x] Phase 23: Streaming Writer & Runner Stress & Lifecycle Tests (1/1 plan) — completed 2026-10-03 (14/14 tests)
- [x] Phase 24: Versioned Symbol Registry & Dynamic Reload Stress Tests (1/1 plan) — completed 2026-10-03 (13/13 tests)
- [x] Phase 25: In-Memory DuckDB Lake Reader & Analytics Edge Tests (1/1 plan) — completed 2026-10-03 (17/17 tests)
- [x] Phase 26: Migration Tooling Rehearsal & Fuzz Tests (1/1 plan) — completed 2026-10-03 (32/32 tests)
- [x] Phase 27: Multi-Process Long-Running Soak & Chaos Tests (1/1 plan) — completed 2026-10-03 (10/10 tests)

Result: 122 new tests, offline suite at **688 passed** (0 failures, 0 errors at closeout). Audit verdict: ⚠️ **`gaps_found`** (verification artifacts missing; 2 coverage gaps; 3 integration findings — no data defects).

See: [.planning/milestones/v4.1-ROADMAP.md](milestones/v4.1-ROADMAP.md) · [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md) · [.planning/v4.1-MILESTONE-AUDIT.md](v4.1-MILESTONE-AUDIT.md)

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

<details>
<summary>✅ v3.0 Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard (Phases 10–14) — SHIPPED 2026-09-26</summary>

- [x] Phase 10: Backend Analytics & High-Performance Data APIs (1/1 plan) — completed 2026-09-25
- [x] Phase 11: Interactive Financial Charting & Data Explorer UI (1/1 plan) — completed 2026-09-25
- [x] Phase 12: Live Stream Tape, Telemetry & Market Operations UI (1/1 plan) — completed 2026-09-25
- [x] Phase 13: Enhanced Symbol Data Matrix & Context-Aware Integrity Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 14: Comprehensive Verification, Testing & Polish (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v3.0-ROADMAP.md](milestones/v3.0-ROADMAP.md)

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
<summary>✅ v1.0 Local DuckDB & 24/7 Live Streaming Engine (Phases 1–4) — SHIPPED 2026-09-25</summary>

- [x] Phase 1: Turso Data Migration & Legacy Purge (1/1 plan) — completed 2026-09-25
- [x] Phase 2: DuckDB Storage Engine (1/1 plan) — completed 2026-09-25
- [x] Phase 3: 24/7 Live WebSocket Streaming (1/1 plan) — completed 2026-09-25
- [x] Phase 4: Test Suite Migration & Verification (1/1 plan) — completed 2026-09-25

See: [.planning/milestones/v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md)

</details>

### Active Milestone

**None.** Milestone v4.1 shipped on 2026-10-03; the v4.0/v4.1 architecture is feature-complete and hardened. There is no in-flight phase.

### Milestone Backlog (candidates, unplanned)

These are surfaced from the [v4.1 milestone audit](v4.1-MILESTONE-AUDIT.md), the v4.0/v4.1 retrospectives, and code/docs review; none are committed scope yet.

**Audit recommendations (from `v4.1-MILESTONE-AUDIT.md` §6):**

- **Process gap:** produce per-phase `VERIFICATION.md` artifacts for Phases 22–27 (or formally accept v4.1 as a test-only milestone with verification debt) — the FAIL gate behind the `gaps_found` verdict.
- **INT-1 (medium):** resolve the `cleanup_orphaned_staging_files` name collision — rename the registry variant (e.g. `cleanup_orphaned_registry_staging_files`) and keep the publication cleaner exported at package level.
- **INT-2 (medium):** remove `_is_mocked_db_connection` from `src/dashboard/analytics.py`; have legacy tests set `TICK_LAKE_ROOT` / patch `_get_lake_reader` instead of relying on `isinstance(..., Mock)` in production routing.
- **INT-3 (low):** surface `TickLakeWriter.total_dropped` / `StreamingEngine.ticks_dropped` in `get_stream_status` (+ dashboard/health), and rate-limit the per-drop `writer_status.json` write.
- **Coverage gaps:** drive a real SIGINT/SIGTERM through `src/stream/runner.py`'s signal handlers in a subprocess (TEST-P23-02), and run supervisor chaos against the real `StreamingEngine` rather than `tools/synthetic_streamer.py` (TEST-P27-02). The registry cleaner also has no production runtime caller.

**Architecture / operations follow-ups:**

- Wire the documented `STREAM_FLUSH_INTERVAL`, `STREAM_MAX_BATCH_ROWS`, `STREAM_MAX_QUEUE_SIZE`, and `STREAM_COMPRESSION` environment variables into `StreamingEngine` / `TickLakeWriter` (currently only the constructor defaults apply — see `docs/operations/tick_lake_operations_guide.md` §2.1).
- Off-hours compaction / partition consolidation for the micro-batch file count (P7a protocol described in the operations guide; not implemented).
- Physical `PENDING_PURGE` symbol data retirement automation.
- Durable disk spool (`_spool/`) for zero-loss capture across process/power failure.
- Repo B end-to-end integration rehearsal against the published contract.
- Repo B-facing replay iterator (P5 gate).
