# Requirements: Data Harvester

**Active milestone:** None — Milestone **v4.1 (Partitioned Parquet Lake Deep Testing & Hardening)** shipped on 2026-10-03.
**Status:** All requirements for every shipped milestone have passing tests. Milestone v4.1 carries a documented verification gap (no per-phase `VERIFICATION.md` artifacts), so its audit verdict is `gaps_found` — see the note below.

- Milestone v4.1 requirements (Phases 22–27, 17/17 with passing tests): archived at [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md)
- Milestone v4.1 audit (verdict `gaps_found`): [.planning/v4.1-MILESTONE-AUDIT.md](v4.1-MILESTONE-AUDIT.md)
- Milestone v4.0 requirements (Phases 15–21, 19/19 verified): archived at [.planning/milestones/v4.0-REQUIREMENTS.md](milestones/v4.0-REQUIREMENTS.md)
- Earlier milestones: archived under [.planning/milestones/](milestones/)

## Verification Summary — Milestone v4.1

| Requirement | Phase | Result | Evidence |
|---|---|---|---|
| TEST-P22-01 … TEST-P22-03 | 22 | ✅ Tests green · ⚠️ no VERIFICATION.md | `tests/storage/test_storage_edge_cases.py` — 36 tests |
| TEST-P23-01 … TEST-P23-03 | 23 | ✅ Tests green · ⚠️ no VERIFICATION.md (P23-02 partial: real signal path undriven) | `tests/stream/test_lake_runner_stress.py` — 14 tests |
| TEST-P24-01 … TEST-P24-03 | 24 | ✅ Tests green · ⚠️ no VERIFICATION.md (INT-1 export collision) | `tests/storage/test_registry_stress.py` — 13 tests |
| TEST-P25-01 … TEST-P25-03 | 25 | ✅ Tests green · ⚠️ no VERIFICATION.md (INT-2 test hook in prod code) | `tests/storage/test_lake_reader_stress.py` — 17 tests |
| TEST-P26-01 … TEST-P26-03 | 26 | ✅ Tests green · ⚠️ no VERIFICATION.md | `tests/storage/test_migration_stress.py` — 32 tests |
| TEST-P27-01 … TEST-P27-02 | 27 | ✅ Tests green · ⚠️ no VERIFICATION.md (P27-02 partial: chaos streamer is a stand-in) | `tests/integration/test_supervisor_chaos_soak.py` — 10 tests |

**Suite at closeout:** `pytest tests/ -m "not live and not performance"` → **688 passed** (696 collected; 8 `live`/`performance` tests deselected). Verified by the auditor on 2026-10-03 (86.5s) and reproduced in this repository.

**Why `gaps_found`:** the audit's FAIL gate triggers because the milestone produced no `VERIFICATION.md` per phase (the 2-stage researcher→implementer cycle used for v4.1 did not emit verification artifacts). All 17 requirements do have passing tests, and the audit found **no failing tests and no data-corrupting defects**. The remaining partial-coverage and integration items are recorded as backlog in [.planning/ROADMAP.md](ROADMAP.md).

## Next Milestone

No scope committed. Candidate follow-ups — including the five audit-recommended fixes — are listed in [.planning/ROADMAP.md](ROADMAP.md) under "Milestone Backlog (candidates, unplanned)".
