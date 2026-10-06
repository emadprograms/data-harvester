---
quick_id: 261006-hfu
verified: 2026-10-06T13:10:00Z
status: passed
score: 7/7 must-haves verified
covered_files:
  - .planning/quick/261006-hfu-v6-0-gap-fill-remediation-distinct-batch/261006-hfu-PLAN.md
  - src/data/databento_backfill.py
  - src/data/gap_fill.py
  - src/storage/publication.py
  - tests/data/test_databento.py
  - tests/data/test_phase51_gap_fill.py
covered_digest: "v3:sha256:169f1a0e36f89bb952763497f94cf5c42ce6cdf2d1966e87a20b34e3fdd1a575"
behavior_unverified: 0
---

# Quick task 261006-hfu verification report

**Goal:** Close P1 (same-day intervals collided on one filename) and P2 (an interruption between publication and coverage commit bought the same data twice), with tests for both.
**Verified:** 2026-10-06
**Status:** passed

## Observable truths

| # | Truth (from must_haves) | Status | Evidence |
|---|---|---|---|
| 1 | Two nonempty intervals for one symbol and UTC day publish two distinct files and two receipts, and a second fill makes zero requests | ✓ VERIFIED | `test_qfill_04_two_intervals_publish_distinct_files` (two files, distinct names, 2 rows, 2 distinct batch ids, `requests == 0` on rerun); baseline raised `BatchCollisionError` on `batch_gap_fill_000000.parquet` |
| 2 | A durable publication followed by a failed coverage write does not cause another paid request; the original bounds are completed from the receipt | ✓ VERIFIED | `test_qfill_04_sparse_receipt_recovery_avoids_second_download` (one `get_range` call after restart, `10:01–10:06` in coverage, 1 stored quote, v1 file byte-identical); baseline re-requested `14:02–14:06` |
| 3 | A deleted ledger is rebuilt from receipts carrying the original request scope | ✓ VERIFIED | `test_qfill_04_deleted_ledger_is_recovered_from_receipt_request_scope` and the strengthened `test_qfill_04_crash_between_publish_and_coverage_does_not_duplicate` (zero extra requests, no duplicate rows) |
| 4 | A successful empty named response is a recoverable zero-row batch | ✓ VERIFIED | `test_qfill_04_empty_success_publishes_recoverable_zero_row_batch` (receipt `row_count == 0`, `file_paths == []`, no Parquet files, recovery after ledger deletion); `test_named_empty_batch_publishes_a_zero_row_receipt` |
| 5 | A failed request stays retryable on its original bounds, never shortened | ✓ VERIFIED | `test_qfill_04_failed_request_stays_retryable_on_original_bounds` (both requests `14:01–14:06`; third run makes zero requests) |
| 6 | Corrupt evidence refuses before cost estimate or download, state retained | ✓ VERIFIED | `test_qfill_04_invalid_recovery_evidence_refuses_before_cost_or_download` (`GapFillRecoveryError`, `cost_calls == []`, no new downloads, ledger bytes unchanged) |
| 7 | Legacy coverage, reordered symbols, and scope isolation keep working; v1 files unchanged; pending publication recovers | ✓ VERIFIED | `test_qfill_04_legacy_coverage_forms_still_cover` (2 params), `test_qfill_04_coverage_is_isolated_by_day_dataset_schema_and_symbols` (4 params), `test_qfill_04_pending_publication_recovers_after_receipt_durability_interrupt` (intent cleared, 1 receipt, no extra download), `test_legacy_unnamespaced_receipt_still_replays` |

## Commands run

```bash
# focused suites (plan gate)
DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest \
  tests/data/test_phase51_gap_fill.py tests/data/test_databento.py \
  tests/storage/test_atomic_publication.py tests/storage/test_v6_recovery_and_mixed_snapshot.py -q
# -> 55 passed

# complete offline suite (plan gate)
DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest -m 'not live and not performance' -q
# -> 1045 passed, 14 deselected in 288.43s
```

The commands above are the plan's literal commands. They were executed in this environment with the identical pinned dependencies (`pyarrow==22.0.0`, `duckdb==1.5.5`, `pandas`, `pytest`, `pytz`, `psutil`, `websockets`, `requests`, `beautifulsoup4`); the interpreter path differed because this sandbox has no `.venv` directory.

`gsd-tools check verify-command-paths` over the plan: 4 commands, 0 blocker, 0 warning.

## Prior-finding closure

| ID | Prior status on `3c35a577` | Now |
|---|---|---|
| P1 filename collision | reproduced (`BatchCollisionError`), unlisted in the milestone audit's gaps | closed; namespace `db_<sha256(batch_id)[:40]>`; helper and integration tests |
| P2 repeated paid requests after interruption | reproduced (second request `14:02–14:06`), audit claimed F3/QFILL-04 verified | closed; pending intervals + receipt reconciliation; the audit's own deleted-ledger test now asserts zero extra requests |
| Milestone-audit QFILL-04 evidence | overstated: the cited crash test passed while issuing a second paid request | corrected; that test now fails on the old code and passes on the new |

## Notes and limits

- No production lake was opened, copied, or rewritten. No paid market-data API was called; all exports go to `tmp_path` lakes with injected clients.
- The rewrite-execution requirements from the milestone audit remain owner-run and deferred, unchanged by this task.
- Sequential execution (no subagent harness in this session) is recorded as a deviation in the SUMMARY. Verifier status was read through `gsd-tools query verification.status`.
