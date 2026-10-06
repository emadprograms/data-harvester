---
quick_id: 261006-hfu
slug: v6-0-gap-fill-remediation-distinct-batch
mode: quick-full
date: 2026-10-06
status: planned
source_findings: .planning/milestones/v6.0-gap-fill-remediation-plan.md (P1, P2)
must_haves:
  truths:
    - "Two nonempty silence intervals for the same symbol and UTC day publish two distinct Parquet files, with two receipts, and a second fill makes zero requests."
    - "A durable publication followed by a failed coverage write does not cause another paid request on restart; the original interval bounds are completed from the receipt."
    - "A coverage ledger that is entirely deleted is reconstructed from publication receipts that carry the original request scope, so the interval is not re-requested with shortened bounds."
    - "A successful empty named response is published as a zero-row named batch and is recoverable from its receipt without creating Parquet files."
    - "A request that failed before publication remains retryable with its original bounds and is never shortened."
    - "Corrupt recovery evidence raises before any cost estimate or download, and the recovery state is retained."
    - "Legacy coverage records, reordered symbol sets, and scope isolation by day, dataset, schema, and symbol set keep working; existing v1 files stay byte-for-byte unchanged."
  artifacts:
    - src/data/gap_fill.py
    - src/data/databento_backfill.py
    - src/storage/publication.py
    - tests/data/test_phase51_gap_fill.py
    - tests/data/test_databento.py
  key_links:
    - "publish_ticks_to_lake -> LakePublisher(file_namespace=db_<sha256(batch_id)[:40]>)"
    - "fill_named_day -> reconcile pending/receipt evidence strictly before occupancy scan, cost estimate, and download"
    - "LakePublisher.publish_batch(request_scope=...) -> intent + receipt request block -> gap-fill reconciliation"
---

# Quick Task 261006-hfu: v6.0 gap-fill remediation (distinct batch filenames, durable pending intervals)

**Goal:** Close findings P1 and P2 of the v6.0 gap-fill remediation plan so a day with more than one nonempty silence interval for the same symbol publishes cleanly, and an interruption between publication and ledger commit never buys the same data twice.

**Requirements:** QFILL-04, QFILL-05 (v6.0 milestone). The milestones-audit documentation-availability wording is left as recorded debt: the audit md's own scope excludes document changes.

## Locked decisions

- The audit md is the source of the findings. Both were reproduced against `3c35a577` before this plan was written (P1: `BatchCollisionError ... batch_gap_fill_000000.parquet already exists with differing content`; P2: restart re-requested `14:02–14:06` after the `14:01–14:06` request was published but its coverage write failed).
- Tests use temporary lakes and injected clients only. No paid API, no production lake.
- Public function signatures and result fields stay unchanged.
- Two extensions beyond the audit md's literal text, both recorded as deviations in the SUMMARY:
  1. `LakePublisher.publish_batch` accepts an optional `request_scope`, stored in the intent and the receipt. This is what lets a *deleted* coverage ledger be reconstructed without inferring bounds from hashed batch IDs.
  2. The existing crash test that deletes the whole coverage file is kept and strengthened (`no additional paid request`) instead of being replaced, so both interruption shapes stay covered.
- The production lake rewrite stays deferred and out of scope. No CLI redesign, no calendar changes, no unrelated storage refactoring.

## Tasks

### Task 1 — Failing regression tests for P1 and P2

**files:** `tests/data/test_phase51_gap_fill.py`, `tests/data/test_databento.py`

**action:** Add the audit reproductions and the supporting compatibility tests, all on temporary lakes with an injected historical client that records every request and filters a fixed quote set by the requested symbols and half-open UTC window.

- Two nonempty intervals (10:01–10:06 and 10:11–10:16 ET, 2026-10-02) each returning one NVDA quote: two requests, two rows, two receipts, two distinct physical files, and a second fill makes zero requests.
- Sparse receipt recovery: one quote at 10:01, an `OSError` injected at the coverage commit, then a restart. One download total, coverage carries the original bounds, no request for 10:02–10:06, one stored quote.
- Deleted ledger recovery: publish, delete the whole coverage file, restart. Zero additional downloads (bounds come from the receipt's request scope).
- Empty receipt recovery: an empty successful response is published as a zero-row named batch; restart makes zero additional downloads and creates no Parquet files.
- Pending publication recovery: interrupt a nonempty publish at the `receipt_durability` barrier, restart, recover the receipt and coverage, clear the intent, no additional download.
- Failed request stays retryable: first download raises, the next invocation uses the original bounds and then becomes covered.
- Invalid recovery evidence: a corrupt pending record or receipt checksum refuses before any cost estimate or download and retains the state.
- Compatibility: legacy coverage records (bare list and dict-without-version), reordered symbol sets, and isolation by day, dataset, schema, and symbol set.
- Helper-level tests in `tests/data/test_databento.py`: two batches at sequence zero produce distinct files; replaying one batch creates no additional file; a legacy unnamespaced receipt still replays.
- Every integration fixture asserts the pre-existing v1 file is byte-for-byte unchanged.

**verify:**
```
<automated>DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest tests/data/test_phase51_gap_fill.py tests/data/test_databento.py -q</automated>
```
This verify is **expected to fail at this task** — it is the red half of the project's test-driven method (STATE.md: "research, write failing tests, implement, verify"). The pass condition for Task 1 is the failure *text*: the multi-interval test must fail with `BatchCollisionError` on `batch_gap_fill_000000.parquet`, the coverage-write test must fail with a second paid request (`14:02–14:06`), and the helper tests must fail on namespace/zero-row assertions. Every pre-existing test must still pass. Tasks 2 and 3 turn this same command green.

**done:** The new tests exist, the pre-existing tests still pass, and the failures name P1 (`BatchCollisionError`) and P2 (a second paid request) rather than fixture construction errors.

### Task 2 — Give named batches distinct physical filenames (P1)

**files:** `src/data/databento_backfill.py`

**action:** When `batch_id` is supplied, publish through `LakePublisher` with a deterministic namespace `"db_" + sha256(batch_id.encode("utf-8")).hexdigest()[:40]`. Keep the batch ID, writer ID, sequence, borrowed lock, and receipt replay behavior exactly as they are. Leave the unnamed `TickLakeWriter` path unchanged. The namespace is 43 characters from the allowed alphabet, valid for `LakePublisher`'s 1–64 character rule and for all currently valid batch IDs.

**verify:**
```
<automated>DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest tests/data/test_databento.py "tests/data/test_phase51_gap_fill.py::test_qfill_04_two_intervals_publish_distinct_files" -q</automated>
```

**done:** Two nonempty intervals at sequence zero produce two files and two receipts; replaying one batch adds no file; a legacy unnamespaced receipt still replays.

### Task 3 — Persist original intervals and reconcile receipts before scanning (P2)

**files:** `src/storage/publication.py`, `src/data/databento_backfill.py`, `src/data/gap_fill.py`

**action:**

1. `LakePublisher.publish_batch(records_or_table, batch_id, sequence, request_scope=None)` stores the optional request scope in the intent payload, in the published receipt, and in the recovered receipt. A receipt whose stored request scope disagrees with a re-supplied scope is a collision, not a silent overwrite. Omitted scope keeps today's payload byte-shape.
2. `gap_fill_coverage.json` becomes `version: 2` with `intervals` (unchanged records) and `pending` (scope, original bounds, batch ID, writer ID, sequence). `_load_coverage` keeps reading the legacy bare-list and dict-without-version shapes. Writes flush and fsync the temporary file, atomically replace the destination, then fsync the parent directory.
3. Under the existing fill lock, reconcile before occupancy scanning, cost estimates, or downloads:
   - a matching pending record whose receipt verifies (identity, writer, sequence, row count, files, payload) moves atomically to completed coverage;
   - a pending record with an intent is recovered through `recover_pending_publications` with the borrowed lock, then moved to completed coverage;
   - a pending record with neither stays retryable on its original bounds and those bounds are excluded from fresh gap selection;
   - a receipt whose `request` scope names this day/dataset/schema/symbol set but is absent from the coverage ledger is added to coverage (orphan-receipt reconstruction);
   - an unresolved intent or a failed verification raises before any external request.
4. Persist each new interval as pending before its download; after publication commits coverage and removes the pending record. Coverage is written before the pending record is cleared, so a crash between the two is idempotent.
5. Estimate the complete queue (unresolved retries plus newly selected gaps) before downloading. Recovered intervals count as zero requests, zero rows, and zero estimated cost.
6. Publish successful empty normalized responses as zero-row named batches, so they receive recoverable receipts. Move the helper's empty-frame shortcut into the unnamed path.

**verify:**
```
<automated>DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest tests/data/test_phase51_gap_fill.py tests/data/test_databento.py tests/storage/test_atomic_publication.py tests/storage/test_v6_recovery_and_mixed_snapshot.py -q</automated>
<automated>DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest -m 'not live and not performance' -q</automated>
```

**done:** All new tests pass, the four focused suites pass, and the complete offline suite passes with no production lake or paid API touched.
