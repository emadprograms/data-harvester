# Milestone v4.2 — Audit Report

**Milestone:** v4.2 Tick Lake Qualification & Scoped Signoff
**Branch:** `arena/01a1054c-data-harvester`
**Date:** 2026-10-04
**Status:** Complete for the signed scope below; Phase 35 and Phase 31 are out of it.

This is the consolidated audit artifact. It states what was verified, what was
found, and — as explicitly — what was **not** verified and is therefore excluded
from the signoff. Every claim here points at an executable artifact; nothing is
asserted on the basis of inspection alone.

---

## 1. Scope

**In scope (qualified by this milestone):** CI evidence and traceability,
test isolation and independent oracles, production-scale benchmarks, the
durability boundary, the Repo B read contract, and migration/backup/restore
rehearsal — plus the documentation and contract repair that closes them out.

**Explicitly out of scope, and not covered by any statement in this report:**

| Excluded | Requirement IDs | Reason |
|---|---|---|
| Replay / rewind | RPLY-01..05 (Q10a) | Excluded by instruction; no provider replay protocol |
| Offline compaction / purge | COMP-01..05 (Q10b) | Excluded by instruction; deferred to v4.3 |
| 24-hour endurance | ENDR-01..05 (Phase 31) | Deferred by instruction |
| Append-only capacity & maintenance safety | CAPA-01..05 (Phase 35) | Phase 35 not executed in this milestone |
| Real historical migration source | — | No access to production historical data; sources are synthetic |
| Provider disconnect / capture-gap replay | DURB-04 | Needs provider retention guarantees |
| Production-host performance verdict | PERF-04 | Measured on a 2-core/3 GB container only |

The deferred items above are **not** signed off. They are not claimed as passing,
and no reader should infer otherwise from the absence of a failure.

---

## 2. Method

1. **Everything runs.** Each gate is a test, not a review note. Where a property
   could not be tested in the sandbox, it is listed as deferred rather than
   asserted.
2. **Oracles are shown to fail.** An oracle that has never detected a defect
   proves nothing, so the reference oracles were mutation-tested: 6 injected
   defects into the documented resampling SQL (all 6 caught), and 7 injected
   drifts into the documentation (all 7 caught).
3. **Documentation is executed.** The published Repo B examples are extracted
   from the markdown and run, so the document cannot drift from what is
   verified. Documentation claims about schema, defaults and precedence are
   compared against the code by test.
4. **Processes are real.** Crash, migration and durability tests run in fresh
   interpreters, so a "resume" genuinely loses in-memory state.

---

## 3. Requirement matrix

45 tracked requirements across Phases 28–36. Status is taken from
`.planning/REQUIREMENTS.md`; the machine-readable mapping is in
`.planning/artifacts/traceability.json`.

| Phase | Area | Requirements | Complete | Not complete |
|---|---|---|---|---|
| 28 | CI evidence & traceability | 3 | 3 | — |
| 29 | Isolation & oracles | 5 | 5 | — |
| 30 | Production-scale benchmarks | 8 | 4 | 4 (see §5) |
| 31 | Endurance | 5 | 0 | 5 — **deferred** |
| 32 | Durability boundary | 5 | 4 | 1 (DURB-04 deferred) |
| 33 | Repo B contract | 5 | 5 | — |
| 34 | Migration, backup & restore | 5 | 5 | — |
| 35 | Capacity & maintenance | 5 | 0 | 5 — **not executed** |
| 36 | Contract repair & docs | 4 | 4 | — |

**Total: 30 of 45 complete.** The 15 incomplete are 5 deferred (Phase 31), 5 not
executed (Phase 35), 1 deferred (DURB-04), and 4 benchmark items (PERF-02 blocked
for want of a pre-existing baseline, PERF-03 not measurable offline, PERF-04 not a
production verdict, PERF-07 needs production data — see §5).

---

## 4. Test evidence

| Measurement | Result |
|---|---|
| Local offline suite (`pytest tests -m 'not live and not performance'`) | 918 passed, 16 deselected, 2 xfailed |
| Hosted CI, branch `arena/01a1054c-data-harvester` | 8 of 9 runs successful |
| CI run `37182035819` (commit `3132e20`) | **Unexplained failure** — see §6 |
| Performance suite (excluded from the CI job) | 5 benchmarks × 3 scales, all passing |
| Oracle mutation coverage | 6/6 SQL mutations detected; 7/7 documentation drifts detected |

The two `xfail(strict=True)` tests are deliberate: they pin known gaps (F10, F11)
so they fail loudly if the behaviour ever changes, rather than rotting.

---

## 5. Benchmark artifacts

Raw per-scale measurements are in `.planning/artifacts/`
(`lake-scale-{write,query,freshness}-{200000,1000000,10000000}.json`), rolled up
in `lake-scale-summary.json`.

| Rows | Wall s | CPU s/M ticks | rows/s | Files | Peak RSS MB | 1m p95 | 5m p95 | 1d p95 | Flush→visible ms |
|---|---|---|---|---|---|---|---|---|---|
| 200,000 | 7.806 | 37.0 | 25,621.8 | 400 | 180.2 | 51.8 ms | 53.0 ms | 67.9 ms | 80.9 |
| 1,000,000 | 42.876 | 40.59 | 23,323.0 | 2,000 | 181.9 | 184.0 ms | 170.9 ms | 151.4 ms | 86.6 |
| 10,000,000 | 427.511 | 40.566 | 23,391.2 | 20,000 | 185.3 | 1288.2 ms | 1297.4 ms | 1384.7 ms | 133.5 |

**Finding F2:** the write path scales (CPU per million ticks is flat at 37–41 s;
peak RSS is flat at 180–185 MB). **Query latency tracks files per symbol, not
rows** — roughly 0.6–1.3 ms per file even after partition pruning. The retained
latency gates are met at 200k and breached at 1M/10M **on this 2-core/3 GB
container**; this is deliberately not presented as a production-host verdict.
The host-independent *shape* is the input to the deferred Q10b compaction
decision.

**PERF-02 remains open:** the requirement asks for a comparison against a
pre-existing baseline, and no baseline test existed before this milestone. The
absolute figures above are recorded so a future run has something to compare
against.

---

## 6. Findings

| # | Area | Finding | Disposition |
|---|---|---|---|
| D1 | Phase 28 | A port-conflict chaos test raced a fixed sleep | Fixed |
| F2 | Phase 30 | Query latency tracks files per symbol, not rows | Recorded; input to deferred Q10b |
| F3 | Phase 33 | Contract claimed periods stay unescaped; they are encoded (`BRK.B` → `BRK%2EB`) | Doc fixed |
| F4 | Phase 33 | Contract listed `symbol` as Arrow `string`; it is dictionary-encoded | Doc fixed |
| F5 | Phase 33 | The published reader built paths from the raw symbol, so percent-encoded symbols **silently returned zero candles** | Doc fixed |
| F6 | Phase 33 | The published PyArrow example inferred Hive partitioning → `ArrowTypeError` | Doc fixed |
| F7 | Phase 33 | The published PyArrow example called a removed `pc.scalar` signature | Doc fixed |
| F8 | Phase 33 | Contract claimed the shipped reader fails fast on a bad root; it returns empty results | Doc corrected; behaviour left unchanged |
| F9 | Phase 33 | A file removed between resolve and query is skipped **without error**, yielding partial results | Documented (§7.3 of the contract); behaviour pinned by test |
| F10 | Phase 34 | Re-running a migration with a different date filter duplicates partitions (51 → 102 rows) | Pinned `xfail`; needs a design decision |
| F11 | Phase 34 | `verify` reconciles **staging**, not the published lake — which is why it cannot see F10 | Pinned `xfail`; suite reconciles against `ticks/` independently |
| F12 | Phase 36 | Operations guide documented the runner flush interval as `2.0s`; it is `5.0s` | Doc fixed |
| F13 | Phase 36 | Archived v4.0 text described an "8-column schema" while also naming `ingest_id`; there are nine | Doc fixed; test prevents recurrence |
| F14 | Phase 36 | Phase 29 was reported complete while ISOL-01/ISOL-02 were still Pending in the matrix | Closed: tests written this phase |

**F14 deserves emphasis.** It is a process finding, not a product one: a phase
was marked complete in `.planning/STATE.md` and `.planning/MILESTONES.md` while two of its five
requirements had never been verified. The matrix, not the phase status, is the
authority — which is why this report counts requirements rather than phases.

**Unexplained CI failure.** Run `37182035819` on `3132e20` failed in the
"Run isolated offline suite" step. Log and artifact egress from the sandbox is
blocked, so **no cause was established** — it is recorded as an unexplained
transient, not as flakiness, timing, or OOM. Every subsequent run on the same
branch succeeded, including a re-run of the identical suite.

---

## 7. Artifacts

| Artifact | Path |
|---|---|
| Execution report (per-phase detail) | `docs/plans/milestone-4.2-execution.md` |
| Requirement traceability (machine-readable) | `.planning/artifacts/traceability.json` |
| Requirement matrix (authoritative) | `.planning/REQUIREMENTS.md` |
| Benchmark roll-up | `.planning/artifacts/lake-scale-summary.json` |
| Benchmark raw, per scale | `.planning/artifacts/lake-scale-{write,query,freshness}-*.json` |
| Repo B read contract (v1.2.0) | `docs/contracts/repo_b_tick_lake_contract.md` |
| Operations runbook | `docs/operations/tick_lake_operations_guide.md` |
| Signoff & verification plan | `docs/plans/milestone-4.2-signoff-and-verification.md` |
| Durability contract evidence | `tests/storage/test_durability_boundary.py`, `tests/storage/test_durability_faults.py` |
| Repo B contract evidence | `tests/contract/test_repo_b_contract.py` |
| Migration & restore evidence | `tests/storage/test_migration_rehearsal.py`, `tests/storage/test_cutover_rehearsal.py` |
| Documentation-contract evidence | `tests/docs/test_documentation_contract.py` |
| Tooling isolation evidence | `tests/support/test_tooling_isolation.py` |

---

## 8. Signoff statement

For the scope in §1, the following are established by executable evidence:

1. Durability is bounded and documented: acknowledged publications survive a
   SIGKILL; unflushed in-memory rows do not; the RAM-only window is stated as
   lossy rather than implied durable.
2. The Repo B read contract is complete and executable for a fresh third-party
   consumer with no `src` imports, and its published examples now run correctly —
   five documentation defects were found by executing them.
3. Migration reaches the final published inventory with exact multiplicity for
   inactive symbols, duplicates, ties, nulls, float edges and late events, and
   survives crashes at export, promotion and receipt.
4. A retained backup restores to a scratch destination, is queryable and
   reconciles; rollback preserves live data written after migration.
5. Documentation for schema, runtime defaults and backend selection now matches
   the code, and is held there by tests.

For the scope in §1, the following are **not** established, and this report does
not claim them: replay/rewind, offline compaction and purge, 24-hour endurance,
append-only capacity and maintenance safety, provider-level capture-gap
accounting, and any performance verdict about production hardware.
