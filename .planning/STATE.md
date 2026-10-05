---
gsd_state_version: "1.0"
milestone: v5.0
milestone_name: DuckDB-Free Tick-Only Parquet
status: planning
last_updated: "2026-10-05T07:44:47.827Z"
last_activity: 2026-10-05
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State: Data Harvester

## Project Reference

See: [.planning/PROJECT.md](PROJECT.md) (updated 2026-10-05)

**Core value:** Zero-cloud, zero-quota persistent market data ingestion and storage.
**Current focus:** Phase 46: Rewire Off the Disk Databases (test-driven)

## Current Position

Phase: 46 — Rewire Off the Disk Databases
Plan: — (planning)
Status: Phase 46 in progress — DASH-01..03, STOR-01/02/04, GAP-01/02 and BASE-01 done; STOR-03/05 remain
Last activity: 2026-10-05 — Milestone v5.0 started; v4.3 closed with a documented waiver of phases 44–45.

Progress: [░░░░░░░░░░] 0%

## Accumulated Context

### Decisions

**Working method (owner directive, 2026-10-05):** every phase is test-driven — research, write failing tests, implement, verify, re-implement and re-verify on failure. A phase is not complete until its tests pass.

**Milestone v5.0 (active) — Parquet-only storage:**
- **DuckDB is removed as storage, not as an engine** (owner decision, 2026-10-05). No `.duckdb` files may exist; DuckDB stays in `requirements.txt` and continues to read Parquet in memory.
- **Rejected alternative:** deleting the DuckDB library and reimplementing resampling in PyArrow. Owner chose the smaller option. `reader.py`, `compaction.py`, `replay.py` are **out of scope for modification**.
- **Phases 46–49, numbered in execution order (renumbered 2026-10-05).** The former Phase 46 (migration gate) is now Phase 49; the former Phase 47 (rewiring) is now Phase 46. Code phases 46-48 run in this checkout; the owner-machine gate runs last.
- **Sequencing:** code phases 46-48 need no `data/` directory. The legacy migration verify and the deletion both happen in Phase 49, on the owner's machine, at the end.
- **Owner actions only:** the agent never copies, exports, archives or deletes the owner's `.duckdb` files.
- **Data sources:** Capital.com for live ticks; Databento for gap repair, writing into the lake. Massive/Polygon, Yahoo, Binance and Discord are removed with the bar subsystem.
- **Symbols:** exactly 19 approved equities — AAPL, ADBE, AMD, AMZN, APP, AVGO, BABA, GOOGL, META, MSFT, MU, NDAQ, NVDA, ORCL, PANW, QCOM, SHOP, TSLA, TSM. `_control/registry.json` is the single authority.
- **Schedule:** ingestion 04:00–20:00 ET (Mon–Fri); compaction unattended in the closed interval.
- **Remove the unused replay subsystem (owner decision, 2026-10-05):** `src/storage/replay.py` (819 lines), its 693 lines of tests, its exports, and the two `create_replay_*` methods on `TickLakeReader`. It was built in v4.3 Phase 42 against `PROJECT.md`'s explicit out-of-scope placement of market-rewind, has no UI, no API endpoint and no runner integration, and was never requested by the owner.
- **Discord notifications are RETAINED (owner, 2026-10-05):** reversing the earlier removal. `src/utils/discord.py` is rewritten for streamer operations - session start/stop, **session failed to start**, supervisor restart, drain/compaction failure - while bar-era alert builders are deleted. `DISCORD_WEBHOOK_URL` stays in `.env.example`. Rationale: an unattended 16-hour window needs a way to report a failed 04:00 start.
- **Platform: macOS only (owner, 2026-10-05):** the streamer runs on macOS. Build the launchd scheduler; remove or mark the Windows scripts unsupported.
- **Schedule: weekdays only (owner, 2026-10-05):** Mon-Fri, 04:00-20:00 ET. No exchange calendar - holidays simply produce no ticks.
- **Governing principle - maximum declutter (owner, 2026-10-05):** reduce repo clutter as much as possible. Where a choice exists between keeping something "just in case" and removing it, remove it. The dashboard becomes a single Parquet-only view; the ~2 years of bar history is permanently dropped rather than retained; unused subsystems are deleted rather than left dormant.
- **No retention period for the legacy databases:** `historical.duckdb` and `streaming.duckdb` are deleted immediately after Phase 49 verification. The owner accepted permanent, irrecoverable loss of the bar history.
- **Stop rule:** one completion report, then the milestone stops. No new phases or follow-up programme.

**v4.3 outcomes (closed 2026-10-05, archived):**
- Phases 37–43 delivered: fail-closed release validator; migration coverage ledger; provenance-scoped verification plus whole-lake audit; reader fail-fast with no legacy fallback; durability barrier crash matrix; capacity monitoring and offline compaction; bounded replay iterator; corrected benchmark harness.
- Phases 44–45 **waived by owner** — no capability added; both require external infrastructure.
- **Honest limitation:** the Phase 43 writer-CPU gate (`>= 50%` vs legacy) was **not met** (measured `−14.4%`); `reports/benchmarks/pass2_qualification_report.json` records `"overall_passed": false`. Recorded as a measured characterization, not a pass. The 1M/10M scale claim was not re-run.

### Pending Todos

- Execute Phase 46 test-driven: research, write failing tests, implement, verify.

### Blockers/Concerns

- **Legacy file access:** only Phase 49 needs the owner's machine; Phases 46-48 run entirely in this checkout.
- **Repo B contract:** resolved 2026-10-05 — consumers read the Parquet `ticks/` tree with their own duckdb or pyarrow. Only a consumer of the legacy `.duckdb` files themselves would be affected, and none is known.

## Session Continuity

Last session: 2026-10-05
Stopped at: GAP-01/02 closed — Databento publishes into the lake through the product writer, reads its scope from `_control/registry.json`, checks backfill state against the lake, and `databento==0.86.0` is declared. The dashboard renders one Parquet-only view and `src/dashboard/server.py` imports no `src.database` module. `tests/dashboard` 86 passed; spectrum+dashboard+planning+docs 168 passed.
Resume file: PLAN-MILESTONE-5.0.md

## Operator Next Steps

- Then STOR-03/05: move the last legacy in-memory DuckDB tests onto the lake, drop the `client=` seam in `analytics.py`, delete `src/database` (part 2 falls out of Phase 47's file deletions).
- Phase 49 (final gate + deletion) runs later, on the owner's machine.
