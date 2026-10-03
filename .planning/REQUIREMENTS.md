# Requirements: Data Harvester

**Active milestone:** None — Milestone **v4.1 (Partitioned Parquet Lake Deep Testing & Hardening)** shipped on 2026-10-03.
**Status:** All requirements for every shipped milestone are validated and closed. There are no active or pending requirements.

- Milestone v4.1 requirements (Phases 22–27, 20/20 verified): archived at [.planning/milestones/v4.1-REQUIREMENTS.md](milestones/v4.1-REQUIREMENTS.md)
- Milestone v4.0 requirements (Phases 15–21, 19/19 verified): archived at [.planning/milestones/v4.0-REQUIREMENTS.md](milestones/v4.0-REQUIREMENTS.md)
- Earlier milestones: archived under [.planning/milestones/](milestones/)

## Verification Summary — Milestone v4.1

| Requirement | Phase | Result | Evidence |
|---|---|---|---|
| TEST-P22-01 … TEST-P22-03 | 22 | ✅ Verified | `tests/storage/test_storage_edge_cases.py` — 36 tests |
| TEST-P23-01 … TEST-P23-03 | 23 | ✅ Verified | `tests/stream/test_lake_runner_stress.py` — 14 tests |
| TEST-P24-01 … TEST-P24-03 | 24 | ✅ Verified | `tests/storage/test_registry_stress.py` — 13 tests |
| TEST-P25-01 … TEST-P25-03 | 25 | ✅ Verified | `tests/storage/test_lake_reader_stress.py` — 17 tests |
| TEST-P26-01 … TEST-P26-03 | 26 | ✅ Verified | `tests/storage/test_migration_stress.py` — 32 tests |
| TEST-P27-01 … TEST-P27-02 | 27 | ✅ Verified | `tests/integration/test_supervisor_chaos_soak.py` — 10 tests |

**Suite at closeout:** `pytest tests/ -m "not live and not performance"` → **688 passed** (696 collected; 8 `live`/`performance` tests deselected).

## Next Milestone

No scope committed. Candidate follow-ups are listed in [.planning/ROADMAP.md](ROADMAP.md) under "Milestone Backlog (candidates, unplanned)".
