---
gsd_state_version: "1.0"
milestone: v5.0
milestone_name: DuckDB-Free Tick-Only Parquet
status: handoff
last_updated: "2026-10-05T15:40:00.000Z"
last_activity: 2026-10-05
progress:
  total_phases: 4
  completed_phases: 3
  total_plans: 4
  completed_plans: 3
  percent: 75
---

# Project State: Data Harvester

## Project Reference

See: [.planning/PROJECT.md](PROJECT.md) (updated 2026-10-05)

**Core value:** Zero-cloud, zero-quota persistent market data ingestion and storage.
**Current focus:** Phase 49 handoff — owner-run migration, deletion and milestone close (code phases 46–48 are complete)

## Current Position

Phase: 49 — Final Gate, Deletion & Closure (owner's machine)
Plan: — (handoff prepared; the agent cannot run MIG-01..03, RMV-05 or CO-03)
Status: Phases 46, 47 and 48 complete. Full offline suite 988 passed. Remaining: owner runs the migration gate (MIG-01/02/03), deletes the two `.duckdb` files (RMV-05), and writes the single completion report (CO-03) — then v5.0 stops. The owner handoff is `docs/operations/phase49_migration_runbook.md`, rehearsal-tested end to end in the sandbox (all six stages, re-run idempotence, tamper refusal, symbol purge).
Last activity: 2026-10-05 — Phase 48 closed and Phase 49 handed off: SYMB-01 made `_control/registry.json` the single symbol authority at both the callback boundary and the add-symbol UI, CO-01 recorded the disposition of every removed/retargeted test, CO-02 proved the candle engine unchanged, and the owner runbook was rehearsed end to end. Full offline suite **988 passed** (no failures, no xfails).

Progress: [███████░░░] 75% (code phases 46–48 of 46–49; Phase 49 is owner-run)

## Accumulated Context

### Decisions

**Working method (owner directive, 2026-10-05):** every phase is test-driven — research, write failing tests, implement, verify, re-implement and re-verify on failure. A phase is not complete until its tests pass.

**Milestone v5.0 (active) — Parquet-only storage:**
- **DuckDB is removed as storage, not as an engine** (owner decision, 2026-10-05). No `.duckdb` files may exist; DuckDB stays in `requirements.txt` and continues to read Parquet in memory.
- **Rejected alternative:** deleting the DuckDB library and reimplementing resampling in PyArrow. Owner chose the smaller option. `reader.py`, `compaction.py`, `replay.py` are **out of scope for modification**.
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
- **Stop rule:** one completion report, then the milestone stops. No new phases or follow-up programme.

**v4.3 outcomes (closed 2026-10-05, archived):**
- Phases 37–43 delivered: fail-closed release validator; migration coverage ledger; provenance-scoped verification plus whole-lake audit; reader fail-fast with no legacy fallback; durability barrier crash matrix; capacity monitoring and offline compaction; bounded replay iterator; corrected benchmark harness.
- Phases 44–45 **waived by owner** — no capability added; both require external infrastructure.
- **Honest limitation:** the Phase 43 writer-CPU gate (`>= 50%` vs legacy) was **not met** (measured `−14.4%`); `reports/benchmarks/pass2_qualification_report.json` records `"overall_passed": false`. Recorded as a measured characterization, not a pass. The 1M/10M scale claim was not re-run.

### Pending Todos

- Nothing on the agent side. Phase 49 is owner-run and its procedure is
  `docs/operations/phase49_migration_runbook.md`: run the six-stage migration gate,
  delete `data/historical.duckdb` and `data/streaming.duckdb` (no retention), write the
  single completion report, stop.

### Blockers/Concerns

- **Owner-machine access:** only Phase 49 needs the owner's Mac; everything the agent can
  do in this checkout (Phases 46–48) is complete and pushed.
- **Partially verified by design:** the runbook's commands were rehearsed against a
  synthetic legacy database, not the owner's real 8.9M-row bar store; the acceptance table
  in runbook §1 is what confirms the real migration.

## Session Continuity

Last session: 2026-10-05
Stopped at: v5.0 code phases complete at `a706406` (PR #9, branch
`arena/01a10aa6-data-harvester`, clean tree). The lake is the only store, the bar era and
the disk-database layer are gone (guarded by `tests/test_bar_era_removal.py` and
`tests/test_disk_database_layer_removed.py`), the streamer runs Mon–Fri 04:00–20:00 ET with
close-time drain and off-hours compaction, `_control/registry.json` is the symbol
authority, and Discord reports session start/stop/failure. Full offline suite: 988 passed.
Resume file: PLAN-MILESTONE-5.0.md (historical); live procedure in
`docs/operations/phase49_migration_runbook.md`.

## Operator Next Steps

1. Stop the services (`./STOP_SERVICES.sh`), then work through
   `docs/operations/phase49_migration_runbook.md` §1–§2: `--dry-run`, the six migration
   stages, the acceptance table, then the duplicate-safe re-run.
2. Only after the gate passes: `rm -f data/streaming.duckdb data/historical.duckdb`, start
   the services and confirm `./VIEW_STATUS.sh` is healthy (runbook §3).
3. Write the one completion report (runbook §5) and stop — v5.0 adds no further phases.
