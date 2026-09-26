# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v3.0 — Observability Command Center, Interactive Financial Charts & Live Telemetry Dashboard

**Shipped:** 2026-09-26
**Phases:** 5 | **Plans:** 5 | **Sessions:** 1

### What Was Built
- Interactive financial charts powered by TradingView Lightweight Charts (v4.1.3) with sub-25ms dynamic rendering across 7.99M candles.
- High-performance analytical query engine in DuckDB with native `time_bucket()` resampling (`1m` to `1D`).
- Live Ticker Wall and quote tape with visual flash animations, bid/ask spreads, and streamer process telemetry.
- Real-time market session clock with countdowns to regular trading hours and 8:00 PM ET cutoff.
- Background harvester job runner with real-time log streaming in a web drawer.
- Searchable, actionable Symbol Coverage Matrix and context-aware data integrity auditor.

### What Worked
- Vanilla ES6 + Tailwind CSS CDN + TradingView Lightweight Charts CDN eliminated all build-step overhead while providing native-grade performance.
- DuckDB's in-engine `time_bucket()` execution allowed instant multi-timeframe aggregation without transferring raw bars into Python memory.
- Symbol-specific date range discovery avoided false gap alerts during historical audits.
- Dedicated tests for each feature domain enabled fast feedback loops.

### What Was Inefficient
- Re-architecting symbol maps late in the cycle (historical vs streaming) required quick tasks to resolve schema overlaps. Defining dual symbol tables earlier would have avoided migrations.

### Patterns Established
- Separation of concerns between static historical analytical storage and live tick streaming buffers.
- In-process adaptive configuration matching for concurrent DuckDB connections.
- NYSE Eastern time normalization for financial UI display.

### Key Lessons
1. Native database aggregation outperforms application-layer resampling by orders of magnitude for large datasets (>1M rows).
2. Clean separation of analytical queries and live write loops prevents lock contention completely in embedded databases.

### Cost Observations
- Model mix: Claude Opus / Gemini Flash
- Sessions: 1
- Notable: Comprehensive automated testing (182 tests) enabled zero-regression iteration.

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | 1 | 4 | Migration from Turso SQLite to local DuckDB and 24/7 WebSockets |
| v2.0 | 1 | 5 | Decoupled dual DuckDB files (`historical.duckdb` & `streaming.duckdb`) and web dashboard |
| v3.0 | 1 | 5 | Observability Command Center with TradingView charts, live telemetry, and automated job drawer |

### Cumulative Quality

| Milestone | Tests | Pass Rate | Total Records |
|-----------|-------|-----------|---------------|
| v1.0 | 127 | 100% | 3.9M bars |
| v2.0 | 163 | 100% | 7.99M bars |
| v3.0 | 182 | 100% | 7.99M bars + live tick stream |

### Top Lessons (Verified Across Milestones)

1. Zero-dependency single-file frontend delivery (vanilla JS + CDN libraries) provides high velocity and zero build drift for local developer tools.
2. DuckDB read-only concurrency guarantees clean operational dashboards alongside continuous streaming ingestion when file-locking is minimized.
