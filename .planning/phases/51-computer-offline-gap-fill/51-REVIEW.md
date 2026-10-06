---
phase: 51-computer-offline-gap-fill
reviewed: 2026-10-06T14:46:28Z
depth: deep
candidate_sha: 1d51a22cdfd1e5118d79dd3c2a436cfa3f2d1161
diff_base: 3c35a577004286efb24b35b463d69454da4a4989
files_reviewed: 12
files_reviewed_list:
  - src/data/gap_fill.py
  - src/data/databento_backfill.py
  - src/storage/publication.py
  - tests/data/test_phase51_gap_fill.py
  - tests/data/test_databento.py
  - requirements.txt
  - reports/v6.0_completion_report.md
  - .planning/milestones/v6.0-MILESTONE-AUDIT.md
  - .planning/milestones/v6.0-gap-fill-remediation-plan.md
  - .planning/quick/261006-hfu-v6-0-gap-fill-remediation-distinct-batch/261006-hfu-PLAN.md
  - .planning/quick/261006-hfu-v6-0-gap-fill-remediation-distinct-batch/261006-hfu-SUMMARY.md
  - .planning/quick/261006-hfu-v6-0-gap-fill-remediation-distinct-batch/261006-hfu-VERIFICATION.md
findings:
  critical: 4
  warning: 1
  info: 0
  total: 5
status: issues_found
---

# Phase 51: Gap-fill remediation review

**Reviewed:** 2026-10-06T14:46:28Z  
**Depth:** deep  
**Candidate:** `1d51a22cdfd1e5118d79dd3c2a436cfa3f2d1161` (PR #12)  
**Status:** issues_found

## Narrative Findings (AI reviewer)

The original two-interval collision and ordinary coverage-write interruption reproductions now pass. The implementation also preserves the deleted-ledger test and reconstructs coverage from intact scoped receipts. Four independently reproduced edge cases remain: one silently duplicates a historical quote, two make avoidable repeat downloads, and one acknowledges persistence despite an explicit filesystem durability error. The audit metadata also mixes revisions and incorrectly labels a reachable commit absent.

No source or tracked test files were modified. The production lake, paid APIs, and production rewrite were not used. No root `AGENTS.md`, project `.agents/skills`, or project `.Codex/skills` was present. The referenced global agent-skills bootstrap path was absent. Explicitly scoped source/test files are tracked; generic `data` ignore rules did not hide them from review.

## Critical Issues

### CR-01: A stale pending interval can append identical quotes under a different namespace

**Classification:** BLOCKER — P1, silent duplicate data.  
**File:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/src/data/databento_backfill.py:229-241`  
**Related:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/src/data/gap_fill.py:584-603`, `:779-786`.

**Issue:** Different request scopes have different batch IDs, and the new namespace makes them different physical files even when their returned payloads are identical. There is no check for an already stored `ingest_id`. An unresolved pending interval retains its original bounds even after another scope has filled them, so this is reachable through ordinary retries and symbol registry changes.

**Reproduction:** Seed 2026-10-02 with only 10:01–10:06 ET silent. The fixed historical dataset contains one NVDA quote at 10:01. Fail the first download for `[AAPL, NVDA]`, leaving its interval pending. Fill successfully with `[AAPL, NVDA, MSFT]`, then resume `[AAPL, NVDA]`. Both successful responses contain that same quote. The final lake contains **two physical files, two rows, one distinct ingest_id**. Both requests use the original `14:01–14:06` UTC interval. No client returns an out-of-range quote.

**Regression control:** Repeating the same scenario with only `batch_file_namespace` patched to return `None` produces **one file, one row, one distinct ingest_id**: the old shared destination recognizes its identical checksum. Ordinary successful scope changes alone do not establish this finding; the reproduction requires the original stale pending interval.

**Fix:** Preserve distinct filenames for distinct data, but add idempotency across batch IDs before appending named Databento records, under the existing ownership lock. Verify/replay an existing batch before applying new-batch deduplication so a partially deduplicated payload does not break later replay. Compare stable quote identities against already published rows, append only new identities, and still persist recoverable request coverage for a completely deduplicated response. Add the scope-change reproduction and a partial-overlap case asserting both total row count and distinct quote identities. Do not simply remove the namespace, which would restore P1.

### CR-02: Losing the ledger hides recoverable publication intents

**Classification:** BLOCKER — P2, repeats paid requests despite surviving original bounds.  
**File:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/src/data/gap_fill.py:584-592`  
**Related:** `:611-617`.

**Issue:** Intent recovery only runs for IDs listed in ledger `pending`. When the ledger is absent, reconciliation scans receipts but never discovers intents, even though the newly extended intents carry the original request scope. A promoted quote can therefore change occupancy and produce the same shortened re-download that P2 was intended to prevent.

**Reproduction:** Use the same sparse interval and interrupt at `receipt_durability`, leaving one promoted quote, a valid scoped intent, and no receipt. Delete the coverage ledger, then restart. Observed requests are **14:01–14:06 followed by 14:02–14:06**. The second response is empty; the original recoverable intent is still present after the fill returns. This combines the separately tested publication interruption and deleted-ledger cases.

**Fix:** Discover gap-fill intents independently of ledger entries, validate their request scope and identity, and recover matching intents under the borrowed lock before occupancy scanning or estimates. Reconstruct completed coverage from the recovered receipt. If surviving intent evidence is unverifiable, refuse before spending. Add a combined `receipt_durability` interruption plus deleted-ledger test requiring one lifetime download, original coverage bounds, and no remaining intent.

### CR-03: A malformed orphan gap-fill receipt is silently treated as no evidence

**Classification:** BLOCKER — P2, violates refusal-before-spend on corrupt evidence.  
**File:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/src/data/gap_fill.py:521-527`.

**Issue:** `_scope_receipts` catches unreadable/invalid JSON and skips it before invoking the strict verifier. Once the ledger is missing, an authentic `gfill_<hash>.json` receipt that became unreadable is indistinguishable from no prior request. This bypasses the documented refusal behavior. The safe-filename filter should not imply that malformed files with valid gap-fill identities can be ignored.

**Reproduction:** Successfully fill the sparse interval, replace its real gap-fill receipt contents with `{`, delete the ledger, and restart with a budget. The fill performs **one new estimate and one new download for 14:02–14:06**, returns success, and leaves the corrupt receipt untouched. It should fail before both external calls. The submitted invalid-evidence test only corrupts a checksum while pending state still names the receipt, so it does not cover this branch.

**Fix:** Distinguish unrelated or nonconforming directory entries from recognizable gap-fill evidence. Raise `GapFillRecoveryError` on unreadable or malformed gap-fill receipts whose scope cannot be established, retaining them for diagnosis; alternatively use a durable scope index that allows safe attribution. Keep genuinely unrelated receipt handling intact. Test both invalid JSON and invalid request-scope structure after ledger deletion, requiring zero estimates/downloads.

### CR-04: Directory fsync errors are swallowed before paid work starts

**Classification:** BLOCKER — P2, durability failure is reported as success.  
**File:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/src/data/gap_fill.py:313-328`.

**Issue:** `_fsync_directory` suppresses every `OSError` from opening or fsyncing the parent directory, including `EIO` and `ENOSPC`. Consequently `_record_pending` can acknowledge durability and proceed to download after the filesystem explicitly failed to persist the rename. Completed coverage also reports success after the same error. The state machine relies on these writes surviving a crash.

**Reproduction:** Patch `os.fsync` to raise `OSError(errno.EIO, ...)` only for directory descriptors; regular-file fsync continues to succeed. Fill an interval with an empty successful response. Observed result: **one download, two failed directory fsyncs, successful returned result**. Failure of the first pending-state directory fsync must prevent the download.

**Fix:** Propagate real persistence failures from directory open/fsync. Suppress only explicitly supported platform/filesystem cases where directory fsync is unavailable, consistent with the publisher's handling of `EIO`/`ENOSPC`. Add a fault test expecting an exception and zero downloads when pending-state durability fails; also cover completed-state fsync failure followed by receipt recovery.

## Warnings

### WR-01: The audit's revision metadata cannot describe the reported verification consistently

**Classification:** WARNING — inaccurate verification provenance.  
**File:** `/Users/emadarshadalam/Documents/GitHub/data-harvester/.planning/milestones/v6.0-MILESTONE-AUDIT.md:4-22`  
**Related:** `:35`.

**Issue:** `candidate_sha` remains `7a8f557ed9ee92e9d0e7f5e7d008c2379527fffb`, while the body incorporates later remediation and says its implementation is verified at that candidate. The new note claims this SHA is absent. In this checkout, `git cat-file -t` returns `commit`, `git show` resolves it, and `git merge-base 7a8f557e HEAD` returns that exact SHA: it is a real ancestor. The `remediation.sha: 1e54137` also predates subsequent hardening whose tests are cited as final gates.

**Fix:** Record the actual audited implementation revision and associate each test result with its revision. Retain earlier candidate/reproduction SHAs as historical metadata, remove the incorrect absence claim, and reopen applicable gap-fill findings/status until the blockers above are fixed. A snapshot of historical passing evidence should not imply the present stronger recovery contract was established at the old candidate.

## Original Findings and Implemented Corrections

These observations concern the specific tested scenarios, not a general signoff:

| Prior concern | Current evidence |
|---|---|
| Two ordinary nonempty same-day intervals collide | Fixed in the original reproduction: distinct files and receipts, no request on rerun. |
| Coverage write fails after durable sparse publication | Fixed with intact pending state: receipt reconciliation preserves original bounds and avoids another download. |
| Entire ledger deleted after completed publication | Fixed when its scoped receipt is intact, including zero-row responses. |
| Deleted-ledger test would be removed | Preserved and strengthened with request-count/bounds assertions. |
| Pending publication interrupted at `receipt_durability` | Recovers with ledger intact; CR-02 covers the missing combination. |
| Empty successful response lacked recoverable evidence | Named empty batches now have zero-row receipts; interruption and ledger-deletion tests pass. |
| Failed download must retry original bounds | Covered; the interaction with another scope still needs CR-01. |
| Legacy shapes and reordered symbol sets | Covered by submitted compatibility tests; malformed JSON ledger refuses. |
| Namespace-induced duplicate risk | Still present, reproduced in CR-01. |
| Audit candidate provenance correction | Incomplete/inaccurate; WR-01. |

## Executed Verification

From the repository root:

```sh
DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest \
  tests/data/test_phase51_gap_fill.py tests/data/test_databento.py \
  tests/storage/test_atomic_publication.py tests/storage/test_v6_recovery_and_mixed_snapshot.py -q
# 71 passed in 1.55s

DISCORD_WEBHOOK_URL='' .venv/bin/python /private/tmp/v6_pr12_reaudit_repros.py
# Exit 0: all assertions confirming the four defects and the namespace control hold.

DISCORD_WEBHOOK_URL='' .venv/bin/python -m pytest tests -m 'not live and not performance' -q -rf
# 1061 passed, 14 deselected in 176.66s (0:02:56)
```

The independent reproduction driver is `/private/tmp/v6_pr12_reaudit_repros.py`. It reuses the submitted occupied-day fixture and range-honoring historical client, but exercises new combinations and explicit filesystem failures. It creates only temporary lakes under `/private/tmp`. Its assertions characterize current faulty behavior, so a zero exit code is **not** a product pass. After fixes, turn the desired opposite outcomes into tracked regression tests.

Observed output summary:

| Probe | Result |
|---|---|
| CR01 namespaced | 2 files, 2 rows, 1 unique ingest_id |
| CR01 unnamespaced control | 1 file, 1 row, 1 unique ingest_id |
| CR02 orphan intent | Repeat request 14:02–14:06; 1 intent left |
| CR03 corrupt orphan receipt | 1 new cost call and repeat request 14:02–14:06 |
| CR04 directory fsync | 2 EIO failures suppressed; 1 download; successful return |

The orchestrator independently reran all four probes and the namespace control; all reproduced exactly as recorded above. The full offline suite also passed at candidate `1d51a22cdfd1e5118d79dd3c2a436cfa3f2d1161`: **1,061 passed, 14 deselected**. Its output is saved at `/private/tmp/v6-pr12-full-offline-unrestricted.log`; independent reproduction output is at `/private/tmp/v6-pr12-independent-repros.log`.

An initial full-suite attempt was stopped after sandbox restrictions prevented local socket binding. The successful complete run used the required localhost/subprocess permissions, with the suite's production-path and external-network guards retained and Discord notifications disabled. Some standalone PyArrow probes emit sandbox `sysctlbyname` capability warnings; their file operations, assertions, and exit codes complete successfully. Passing the existing suite does not cover the independently reproduced defects above. No conclusion about production rewrite readiness follows from this review.

_Reviewer: Codex (gsd-code-reviewer)_
