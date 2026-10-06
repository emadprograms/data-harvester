---
quick_id: 261006-hfu
slug: v6-0-gap-fill-remediation-distinct-batch
subsystem: data
tags: [gap-fill, databento, publication, crash-recovery, durability]
provides:
  - Distinct physical filenames for named gap-fill batches at sequence zero
  - Version-2 coverage ledger with pending intervals and durable writes
  - Receipt-scope recovery so a lost ledger never buys the same interval twice
  - Recoverable zero-row named batches for successful empty responses
affects: [gap-fill, publication, databento-backfill]
actuals:
  tasks: 3
  commits: 4
tech-stack:
  patterns: [write-ahead pending state, receipt-driven reconciliation, deterministic file namespaces]
key-files:
  modified:
    - src/data/gap_fill.py
    - src/data/databento_backfill.py
    - src/storage/publication.py
    - tests/data/test_phase51_gap_fill.py
    - tests/data/test_databento.py
key-decisions:
  - "Namespace is db_ + sha256(batch_id)[:40]: deterministic, 43 chars, safe alphabet."
  - "Receipts carry the request scope so bounds never come from a hashed batch id."
  - "Reconcile under the fill lock before occupancy scanning, estimation, or download."
  - "Refuse on unverifiable evidence instead of resetting a malformed ledger to empty."
duration: ~55min
completed: 2026-10-06
status: complete
---

# Quick Task 261006-hfu Summary: v6.0 gap-fill remediation

**Both audit findings are closed with reproductions that fail on the old code and pass on the new code; the offline suite is green at 1,045 tests.**

## Performance

- **Tasks:** 3 completed (plus one verification-gap test commit)
- **Files modified:** 5
- **Commits:** 4 (test → P1 fix → P2 fix → verification test)

## What changed

1. **P1 — distinct physical filenames.** `publish_ticks_to_lake` publishes a named batch through `LakePublisher(file_namespace="db_" + sha256(batch_id)[:40])`. Two intervals on one symbol/UTC day now own two files and two receipts. Writer id, sequence, borrowed lock, and receipt replay are unchanged; the unnamed `TickLakeWriter` path is untouched.
2. **P2 — durable pending intervals and receipt reconciliation.** `gap_fill_coverage.json` is now `{"version": 2, "intervals": [...], "pending": [...]}` written with flush + fsync + atomic replace + parent-directory fsync. Each interval is persisted as pending **before** its download. `fill_named_day` reconciles under the fill lock **before** occupancy scanning, cost estimation, or any download:
   - verified receipt, or an intent recovered with the borrowed lock → the interval moves to completed coverage on its **original** bounds (zero requests, zero rows, zero cost);
   - no receipt and no intent → the interval stays retryable on its original bounds, and those bounds are excluded from fresh selection, so a shortened duplicate is never requested;
   - unverifiable receipt, unrecoverable intent, or malformed ledger → `GapFillRecoveryError` before any external request, with the state left untouched for diagnosis.
3. **Recoverable empty responses.** Successful empty normalized responses are published as zero-row named batches (the helper's empty-frame shortcut moved into the unnamed path), so "empty means covered" has durable evidence.
4. **Complete-queue estimation.** The pre-download estimate covers unresolved retries plus new gaps.

## Deviations from the plan

1. **Kept and strengthened the deleted-ledger crash test** instead of replacing it. Row count alone passed on the old code while the restart still issued a second paid request; it now also asserts request counts, rebuilt bounds, and the byte-identical v1 file.
2. **Request-scope receipts (review addendum).** The plan's assumption — "recovery prevents another download once a durable publication receipt exists" — is false when the *ledger itself* is deleted, because pending bounds live in that ledger. `LakePublisher.publish_batch` therefore accepts an optional `request_scope`, stored in the intent and receipt; gap-fill reconciliation rebuilds coverage from orphan receipts. This is what makes the audit's deleted-ledger scenario recoverable without inferring bounds from a hashed batch id. Omitting the scope keeps existing payload shapes byte-identical.
3. **Sequential execution (ISOLATION=none).** This session has no subagent harness, so the workflow's sequential path was used: the planner/checker/executor/verifier roles were executed in the main session on the current branch, with the plan-checker's deterministic probe and the verifier's fingerprint run through `gsd-tools`. `query worktree.base-check` reported `shouldDegrade: false`; the degrade is harness-based, not base-based.

## Test inventory added (19 items)

`tests/data/test_phase51_gap_fill.py`: `test_qfill_04_two_intervals_publish_distinct_files`, `test_qfill_04_receipts_carry_the_original_request_scope`, `test_qfill_04_sparse_receipt_recovery_avoids_second_download`, `test_qfill_04_deleted_ledger_is_recovered_from_receipt_request_scope`, `test_qfill_04_empty_success_publishes_recoverable_zero_row_batch`, `test_qfill_04_pending_publication_recovers_after_receipt_durability_interrupt`, `test_qfill_04_failed_request_stays_retryable_on_original_bounds`, `test_qfill_04_invalid_recovery_evidence_refuses_before_cost_or_download`, `test_qfill_04_estimate_covers_retries_and_fresh_gaps`, `test_qfill_04_legacy_coverage_forms_still_cover` (2 params), `test_qfill_04_coverage_is_isolated_by_day_dataset_schema_and_symbols` (4 params), plus the strengthened `test_qfill_04_crash_between_publish_and_coverage_does_not_duplicate`.

`tests/data/test_databento.py`: `test_named_batches_at_sequence_zero_get_distinct_files`, `test_replaying_named_batch_writes_no_additional_file`, `test_named_batch_receipt_records_the_request_scope`, `test_named_empty_batch_publishes_a_zero_row_receipt`, `test_legacy_unnamespaced_receipt_still_replays`.

## Baseline and after

| Gate | Red baseline (`3c35a577` + tests) | After fixes |
|---|---|---|
| `tests/data/test_phase51_gap_fill.py` | 7 failed | 35 passed |
| `tests/data/test_databento.py` | 3 failed | 11 passed |
| Four focused suites | 10 failed / 44 passed | 55 passed |
| Full offline suite (`-m 'not live and not performance'`) | not re-measured | **1,045 passed, 14 deselected** |

Baseline failure signatures retained: P1 → `BatchCollisionError: Destination file ticks/symbol=NVDA/date=2026-10-02/batch_gap_fill_000000.parquet already exists with differing content`; P2 → `a second paid request happened: [('2026-10-02T14:01:00','2026-10-02T14:06:00'), ('2026-10-02T14:02:00','2026-10-02T14:06:00')]`.

## CI unblock (folded in)

The repository's `Offline tests` workflow had failed on **every** run, `main` included (12/12 at the time of writing): six dashboard/streaming test modules import `bs4`, but `requirements.txt` never declared `beautifulsoup4`, so the suite aborted with 6 collection errors before running anything. Added the one missing declaration and verified in a clean venv created from the edited `requirements.txt`: **1,046 passed, 14 deselected**. The workflow now completes successfully on this branch. Nothing else was missing — every other test import (`duckdb`, `pandas`, `pyarrow`, `pytest`, `pytz`, `requests`, `psutil`, `websockets`, `databento`, `tzdata`, `python-dotenv`) was already declared.

## Observations and deferred items

- **`.gitignore` traps `git add` on these paths.** The bare `data` entry matches the `tests/data` and `src/data` directories, so `git add tests/data/...` prints "paths are ignored" and exits 1 even though the explicitly named files are tracked and staged. Tracked files commit normally; an untracked new file under `src/data/` or `tests/data/` would need `-f`. Not fixed here (out of scope for this task) — worth `/gsd-quick` later.
- The milestones audit's documentation-availability wording ("the rewrite is not available from this checkout") is untouched; the audit md itself excludes document changes.
- No production lake was opened. No paid API was called. All fixtures are temporary lakes with injected clients.
