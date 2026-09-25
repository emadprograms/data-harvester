# Phase 9 Plan: End-to-End Verification & Full Test Suite

**Goal:** Implement rigorous end-to-end integration and concurrency stress tests across all Milestone v2.0 modules: dual-file DuckDB isolation, exclusive Capital.com streaming, dynamic symbol reload, data integrity audits, and dashboard REST APIs. Verify 100% test pass rate with zero regressions.

**Requirements:** QUAL-02, QUAL-03

## Tasks

### Task 1: End-to-End Workflow Integration Test (`tests/test_milestone_v2_end_to_end_verification.py`)
- Full lifecycle flow:
  1. Add symbol via Dashboard REST API (`POST /api/symbols`).
  2. Verify symbol is stored in `historical.duckdb` `symbol_map`.
  3. Verify reload signal is received and streamer updates subscriptions.
  4. Stream raw quotes for the new symbol into `streaming.duckdb`.
  5. Run on-demand integrity audit via Dashboard API (`GET /api/integrity`).
  6. Resample streaming ticks to 1-minute OHLCV candlesticks.
  7. Delete symbol via Dashboard API (`DELETE /api/symbols/{symbol}`).

### Task 2: Multi-Process / Concurrent Concurrency Stress Test
- Launch concurrent threads:
  - Thread 1: High-throughput streaming tick ingestion to `streaming.duckdb`.
  - Thread 2: Batch 1m historical candle harvesting to `historical.duckdb`.
  - Thread 3: Dashboard health & integrity API queries.
- Verify 100% completion with zero file-locking collisions or `BinderException`s.

### Task 3: Complete Test Suite Run & Milestone Verification
- Execute full test suite across the entire repository.
- Verify 100% tests pass cleanly.
- Produce `09-SUMMARY.md` and `v2.0-MILESTONE-AUDIT.md`.
