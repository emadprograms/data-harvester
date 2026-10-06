---
gsd_state_version: "1.0"
milestone: v6.0
milestone_name: Bid and Ask Prices (Checkout complete)
current_phase: 53
status: idle
last_updated: "2026-10-06T12:47:15.576Z"
last_activity: 2026-10-06
last_activity_desc: Completed quick task 261006-hfu (v6.0 gap-fill remediation: distinct batch filenames, durable pending intervals, receipt reconciliation, recoverable empty named batches). Production rewrite remains owner-run.
state_head: f46720bfce3a4775aa2e3fc562c0e036ea34d1f1
progress:
  total_phases: 4
  completed_phases: 4
  total_plans: 4
  completed_plans: 4
  percent: 100
---

# Project State: Data Harvester

## Project Reference

See: [.planning/PROJECT.md](PROJECT.md) (updated 2026-10-05)

**Core value:** Zero-cloud, zero-quota persistent market data ingestion and storage.
**Current focus:** v6.0 checkout complete. Production lake rewrite remains owner-run.

## Current Position

Phase: 53 (complete)
Plan: 53-01
Status: Milestone v6.0 tools and tests are in this checkout. The production lake was not rewritten.
Last activity: 2026-10-06 - Completed quick task 261006-hfu: v6.0 gap-fill remediation: distinct batch filenames for same-day intervals, durable pending intervals with receipt reconciliation, recoverable empty named batches. Production rewrite remains owner-run.

## Accumulated Context

### Decisions

**Working method (owner directive, 2026-10-05):** every phase is test-driven — research, write failing tests, implement, verify, re-implement and re-verify on failure. A phase is not complete until its tests pass.

**Milestone v5.0 (active) — Parquet-only storage:**

- **DuckDB is removed as storage, not as an engine** (owner decision, 2026-10-05). No `.duckdb` files may exist; DuckDB stays in `requirements.txt` and continues to read Parquet in memory.
- **Rejected alternative:** deleting the DuckDB library and reimplementing resampling in PyArrow. Owner chose the smaller option. `reader.py` and `compaction.py` are **out of scope for modification**; `replay.py` was deleted later in Phase 47 (see below).
- **Phases 46–49, numbered in execution order (renumbered 2026-10-05).** The former Phase 46 (migration gate) is now Phase 49; the former Phase 47 (rewiring) is now Phase 46. Code phases 46-48 run in this checkout; the owner-machine gate runs last.
- **Sequencing:** code phases 46-48 need no `data/` directory. The legacy migration verify and the deletion both happen in Phase 49, on the owner's machine, at the end.
- **Owner actions only:** the agent never copies, exports, archives or deletes the owner's `.duckdb` files.
- **Data sources:** Capital.com for live ticks; Databento for gap repair, writing into the lake. Massive/Polygon, Yahoo and Binance are removed with the bar subsystem; Discord is **retained** for streamer notifications (see below).
- **Symbols:** exactly 19 approved equities — AAPL, ADBE, AMD, AMZN, APP, AVGO, BABA, GOOGL, META, MSFT, MU, NDAQ, NVDA, ORCL, PANW, QCOM, SHOP, TSLA, TSM. `_control/registry.json` is the single authority.
- **Schedule:** ingestion 04:00–20:00 ET (Mon–Fri); compaction unattended in the closed interval.
- **Remove the unused replay subsystem (owner decision, 2026-10-05):** `src/storage/replay.py` (819 lines), its 693 lines of tests, its exports, and the two `create_replay_*` methods on `TickLakeReader`. It was built in v4.3 Phase 42 against `PROJECT.md`'s explicit out-of-scope placement of market-rewind, has no UI, no API endpoint and no runner integration, and was never requested by the owner.
- **Discord notifications are RETAINED (owner, 2026-10-05):** reversing the earlier removal. `src/utils/discord.py` is rewritten for streamer operations - session start/stop, **session failed to start**, supervisor restart, drain/compaction failure - while bar-era alert builders are deleted. `DISCORD_WEBHOOK_URL` stays in `.env.example`. Rationale: an unattended 16-hour window needs a way to report a failed 04:00 start.
- **Platform: macOS only (owner, 2026-10-05):** the streamer runs on macOS. Build the launchd scheduler; remove or mark the Windows scripts unsupported.
- **Schedule: weekdays only (owner, 2026-10-05):** Mon-Fri, 04:00-20:00 ET. No exchange calendar - holidays simply produce no ticks.
- **Governing principle - maximum declutter (owner, 2026-10-05):** reduce repo clutter as much as possible. Where a choice exists between keeping something "just in case" and removing it, remove it. The dashboard becomes a single Parquet-only view; the ~2 years of bar history is permanently dropped rather than retained; unused subsystems are deleted rather than left dormant.
- **No retention period for the legacy databases:** `historical.duckdb` and `streaming.duckdb` are deleted immediately after Phase 49 verification. The owner accepted permanent, irrecoverable loss of the bar history.
- **Stop rule lifted (owner, 2026-10-06):** v5.0 is no longer terminal. v6.0 is the active milestone.

**Milestone v6.0 (active) — Bid and Ask Prices:**

- A row stores `bid_price` and `ask_price` only, plus `timestamp`, `symbol`, `source`, `session`, and `ingest_id`. No midpoint, no `price`, no `volume`, no sizes.
- Capital.com maps `bid` and `ofr` on every quote change. Databento `tbbo` maps `bid_px_00` and `ask_px_00` when a trade happens. Fewer Databento rows is accepted.
- The inspection chart uses `bid_price`. The volume histogram is removed.
- The existing lake is rewritten, not shimmed. `bid` becomes `bid_price`. `ask` becomes `ask_price`. The old `price` column is discarded. A missing bid or ask is quarantined. The tool needs a second copy of disk, holds the writer lock, rewrites receipts, and sets schema version 2 only after every file passes.
- Gap fill is one target day. A hole is all registry symbols silent together: 15 minutes pre/post, 2 minutes regular hours, inside 04:00–20:00 ET. Weekends, full holidays, and post-early-close time are skipped.
- The existing-lake rewrite is the last phase (Phase 53). Owner, 2026-10-06: it cannot be done in this checkout. Gap fill and docs come first. The rewrite still requires a backup before any file is changed, and the owner runs it locally.
- Research was skipped. The owner locked this design in conversation before the milestone was opened.

**v4.3 outcomes (closed 2026-10-05, archived):**

- Phases 37–43 delivered: fail-closed release validator; migration coverage ledger; provenance-scoped verification plus whole-lake audit; reader fail-fast with no legacy fallback; durability barrier crash matrix; capacity monitoring and offline compaction; bounded replay iterator; corrected benchmark harness.
- Phases 44–45 **waived by owner** — no capability added; both require external infrastructure.
- **Honest limitation:** the Phase 43 writer-CPU gate (`>= 50%` vs legacy) was **not met** (measured `−14.4%`); `reports/benchmarks/pass2_qualification_report.json` records `"overall_passed": false`. Recorded as a measured characterization, not a pass. The 1M/10M scale claim was not re-run.

### Pending Todos

- None. v6.0 requirements are in `.planning/REQUIREMENTS.md`.

### Blockers/Concerns

- None.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 261006-hfu | v6.0 gap-fill remediation: distinct batch filenames for same-day intervals, durable pending intervals with receipt reconciliation, recoverable empty named batches | 2026-10-06 | f46720b | Verified | [261006-hfu-v6-0-gap-fill-remediation-distinct-batch](./quick/261006-hfu-v6-0-gap-fill-remediation-distinct-batch/) |

## Session Continuity

Completed: 2026-10-06
Status: v6.0 checkout complete. v5.0 remains shipped: 105,894,626 ticks in 7,367 Parquet files. Those files are still schema v1. No production rewrite has been run from this checkout, and the gap fill was not run against that lake.

## Operator Next Steps

- Do not rewrite the existing lake in this checkout. That work is Phase 53, the last phase, and the owner runs it on their own machine after a backup.
- New Capital.com and Databento rows are schema v2. The reader still opens the existing v1 files. A partition that contains both is left alone by compaction until the rewrite.
