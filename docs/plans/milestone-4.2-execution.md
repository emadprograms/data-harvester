# Milestone v4.2 — Execution Report (Working Document)

**Milestone:** v4.2 Tick Lake Qualification & Scoped Signoff (Phases 28–36)
**Started:** 2026-10-04
**Status:** In progress — Phase 28 complete (hosted CI green), Phase 29 complete; Phases 30–36 in progress
**Release candidate:** `6706e5f` · **PR:** [#8](https://github.com/emadprograms/data-harvester/pull/8)
**Source plan:** [`milestone-4.2-signoff-and-verification.md`](milestone-4.2-signoff-and-verification.md)
**Traceability matrix:** [`milestone-4.2-traceability.md`](milestone-4.2-traceability.md)

This report records only what was actually executed and observed. Every gate is
marked `PASS`, `FAIL`, `BLOCKED`, or `DEFERRED`. A gate with no evidence stays
`BLOCKED` — it is never carried forward as a pass from a previous milestone.

---

## 1. Environment manifest

| Item | Value |
|---|---|
| Executing host | Linux `6.1.158+` x86_64 (sandbox container) |
| CPU | 2 cores |
| Memory | 3 GB (available ~3 GB) |
| Python | 3.11.2 |
| Dependencies | `requirements.txt` installed into `.venv` (pytest 9.1.1, duckdb 1.5.5, pyarrow 22.0.0) |
| Production data | **Absent** — no `data/` directory, no tick lake, no `historical.duckdb` |

**Consequence, stated up front:** this host is *not* the project's performance
reference host (a Mac Mini / Windows machine). Latency and CPU numbers measured
here describe this container only and are **not** claimed against the production
SLA. Gates that need the production host or production data are recorded as
`DEFERRED` with the reason, per the user's instruction to defer what cannot be
tested here.

## 2. Baseline: current suite on Linux (Phase 28)

Command:

```sh
python -m pytest tests -m 'not live and not performance' -q
```

Result: **745 passed, 1 failed, 11 deselected in 281.37s** on commit `feb08e3`.

The archived v4.1 claim was 746 passed on the documented host. On this Linux
host one test fails. It was investigated rather than re-run until green.

## 3. Defect D1 — port-conflict chaos test raced a fixed sleep

**Gate:** Q02 / Q04 (test determinism) · **Requirement:** ISOL-05, ENDR-05
**Node:** `tests/integration/test_supervisor_chaos_soak.py::test_chaos_port_conflict_backoff_and_recovery`

**Observed (before fix):** failed 3 of 3 runs.
`AssertionError: Supervisor did not detect port conflict crash — assert 0 >= 1`.

**Diagnosis:** the supervisor is correct; the *test* was not. It slept a fixed
`0.8s` and then asserted the supervisor had observed the child's crash. Measured
directly, the supervised child needs **0.615s** to import `src.dashboard.server`
and fail with `EADDRINUSE` on this host, so crash detection lands right at the
0.8s boundary and loses the race whenever the host is slower or loaded. The
supervisor polls every 0.2s, so it would detect the crash — just not inside the
hardcoded window. This is the exact "arbitrary sleep instead of a deterministic
barrier" anti-pattern the milestone plan calls out for Q04.

**Fix:** added `wait_until` / `wait_until_or_fail` to
`tests/support/process_harness.py` and polled for the observable condition
(`supervisor.consecutive_crashes >= 1`, timeout 15s) instead of sleeping. The
assertion is unchanged; no threshold was widened and the recovery SLA is still
`<10s`. The recovery wait was converted to the same helper.

**Observed (after fix):** passed 3 of 3 runs (~1.55s each).

**Regression coverage:** `tests/support/test_process_harness.py` — 6 unit tests
pinning the helper's contract (immediate return, polling until true, no early
return past timeout, tolerance of transient predicate exceptions, and the error
message carrying the timing evidence).

## 4. Requirement traceability matrix (Phase 28, EVID-02)

Tool: `tools/requirement_traceability.py` · Artifact:
[`milestone-4.2-traceability.md`](milestone-4.2-traceability.md) · Raw:
`.planning/artifacts/traceability.json`

| Metric | Value |
|---|---|
| Requirements tracked | 51 |
| Mapped to executable nodes | 48 |
| Declared coverage gaps | 3 |
| Collected pytest nodes | 771 |

Mapping granularity is recorded per requirement: `test` (a named test function),
`module` (covered somewhere in that module — a weaker claim, labelled as such),
or `none` (a declared gap).

**Declared gaps (not passing evidence):**

| Requirement | Gap | Planned |
|---|---|---|
| LAKE-P0-03 | Baseline characterization of legacy DuckDB CPU/latency has **no automated node**; only the manual script `tools/benchmark_baseline.py` exists. This is why the v4.0 "≥50% CPU reduction" claim has no reproducible baseline. | Deferred (needs production host) |
| LAKE-P4-04 | External consumer (Repo B) contract is documentation only, with no executable test. | Phase 33 (REPB-01) |
| LAKE-P8-03 | Documentation/service-config requirement has no automated node. | Phase 36 (DOCS-01..03) |

**Note on granularity:** F01–F11 are mapped to named test functions. The v4.0
`LAKE-*` and v4.1 `TEST-P2x-*` requirements are mapped at module granularity,
which is a real limitation — a module-level mapping proves the file has tests,
not that a specific assertion covers the requirement. Tightening those to named
tests is tracked as follow-up work rather than claimed as done.

## 5. Release report validator (Phase 28, EVID-03)

Tool: `tools/validate_release_report.py` · Tests:
`tests/planning/test_release_report_validator.py` (21 tests)

Rejects a gate for: missing artifact, missing artifact path, artifact checksum
mismatch, code SHA mismatch, missing `code_sha`, missing metrics, a metric whose
value is `null` (must fail rather than read as zero), skipped required tests,
failed tests, non-passing status on a required gate, an empty test selection, a
test count with no named nodes, duplicate gate IDs, an invalid status value, and
inconsistent pass/fail/skip counts. Non-required gates may carry `DEFERRED`.

## 6. Independent oracle proof (Phase 29, ISOL-04)

Tests: `tests/support/test_lake_assertions_oracle.py` (7 tests)

The oracle (`tests/support/lake_assertions.py`) was already a Counter-based
full-row multiset reading Parquet through PyArrow, never through the application
reader — but nothing proved it fails when it should. It now does, on all three
corruption classes:

| Injected defect | Detected | Why a weaker oracle would miss it |
|---|---|---|
| One changed price value | Yes | A row count still matches |
| One of two identical rows removed | Yes | A set comparison still matches; only multiplicity catches it |
| One extra phantom row added | Yes | A row count would differ, but a "contains" check would pass |

A static test also asserts the oracle module never imports `src.storage.reader`,
keeping it independent of the code it verifies.

## 7. Deterministic generator contract (Phase 29, ISOL-03)

Tests: `tests/fixtures/test_deterministic_quotes.py` (28 tests)

`tests/fixtures/deterministic_quotes.py` was verified rather than assumed. Pinned
per scenario generator: reproducibility across calls, stable unique `ingest_id`
values, schema v1 validity, and UTC-naive timestamps. Per-scenario content
assertions confirm the dataset really contains what the lake must preserve —
duplicate pairs and one triplet, microsecond-distinct burst timestamps,
out-of-order late arrivals, null volume and null bid observations, a UTC date
rollover spanning two dates, and PRE/REG/POST session boundaries.

**Finding (not a defect):** no single generator combines all edge cases; each
lives in its own scenario function, and `generate_multisymbol_distribution`
produces no duplicates, nulls, or non-regular sessions. A combined, seeded,
scalable generator is therefore a prerequisite for the Q03 large-dataset work.

## 8. Hosted CI evidence (Phase 28, EVID-01)

The workflow only triggers on `pull_request` and pushes to `main`, so PR #8 was
opened to produce candidate-specific evidence.

| Run | Candidate SHA | Conclusion | URL |
|---|---|---|---|
| 37181864550 | `15f98b0` | success | https://github.com/emadprograms/data-harvester/actions/runs/37181864550 |
| 37182035819 | `3132e20` | **failure** | https://github.com/emadprograms/data-harvester/actions/runs/37182035819 |
| 37182614963 | `6706e5f` | success | https://github.com/emadprograms/data-harvester/actions/runs/37182614963 |

Local suite on Linux for the same commit set: **823 passed, 11 deselected in
298.17s**.

**Unexplained transient, recorded not dismissed.** Run 37182035819 failed while
the identical tests pass locally (823 passed) and passed on the subsequent run
37182614963, whose only workflow change was adding log capture. The CI log and
artifact could not be retrieved from this environment — log and artifact egress
is blocked — so the cause is **not** established. It is recorded as an
unexplained transient rather than written off, and re-appearing failures must be
investigated against the artifact before any signoff. This is also why the
workflow now uploads `pytest.log` and JUnit XML on every run.

## 8b. Production-scale benchmarks (Phase 30, PERF-01/04/05/06/08)

Tool: `tests/performance/test_lake_scale_benchmarks.py` (marked `performance`, so
it is excluded from the 20-minute offline job). Input:
`tests/support/deterministic_dataset.py` — a new combined, seeded, batched
generator, because no existing generator produced a large dataset containing all
the edge cases at once (§7).

Rows are controlled by `GSD_LAKE_BENCH_ROWS`. Artifacts are per scale in
`.planning/artifacts/`, summarised in `lake-scale-summary.json`.

| Rows | Wall s | CPU s/M ticks | rows/s | Files | Files (hot symbol) | Peak RSS MB | 1m p95 | 5m p95 | 1d p95 | Flush→visible ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 200,000 | 7.806 | 37.0 | 25,621.8 | 400 | 40 | 180.2 | 51.787 ms | 52.95 ms | 67.93 ms | 80.93 |
| 1,000,000 | 42.876 | 40.59 | 23,323.0 | 2,000 | 200 | 181.9 | 184.046 ms | 170.905 ms | 151.373 ms | 86.617 |
| 10,000,000 | 427.511 | 40.566 | 23,391.2 | 20,000 | 2,000 | 185.3 | 1288.248 ms | 1297.38 ms | 1384.718 ms | 133.463 |

**Finding F2 — the write path scales; the query path does not.** Writer CPU cost
per million ticks is flat (37.0 → 40.6 s) and peak RSS is flat (180 → 185 MB)
from 200k to 10M rows, so ingestion and memory are bounded and linear. Query
latency, however, tracks **files per symbol**, not rows: 40 files → 52 ms,
200 files → 184 ms, 2,000 files → 1,288 ms, roughly 0.6–1.3 ms per file even
after pruning selects only the hot symbol's files.

Against the retained gates (`1m`/`5m` p95 <100 ms, `1d` p95 <250 ms): met at
200k rows; **breached at 1M and 10M rows on this host**. Pruning works and is
proven (10× file reduction, `PERF-06`), but pruning alone does not keep latency
bounded as the append-only design accumulates one file per micro-batch per
symbol per day.

**Interpretation, stated carefully.** This is a 2-core/3 GB container, not the
project's performance reference host, so these numbers are **not** a production
verdict and are not compared against the production SLA. What *is*
host-independent is the shape of the curve: latency grows linearly with file
count, and the design has no compaction (Q10b, deferred to v4.3). Even a
production host several times faster would breach the `<100 ms` gate at a few
thousand files per symbol. This is therefore a capacity/operations finding for
Q08 and a direct input to the Q10b compaction decision — recorded as evidence,
not as a fixed defect, since no gate is claimed to have been broken on the
reference host.

**Not measured here (recorded as unmeasured, not passed):**
- **PERF-02** (≥50% CPU reduction vs legacy DuckDB): no reproducible baseline
  exists — traceability gap LAKE-P0-03. The absolute figure (≈40 CPU s per
  million ticks) is recorded and can be compared once a baseline is produced.
- **PERF-03** (event-loop lag p99 <20 ms): needs the live streaming runner and a
  provider, neither available here.
- **PERF-07** (historical resampling benchmarks): needs the production
  historical database.

## 9. Gate status

| Gate | Phase | Status | Evidence |
|---|---|---|---|
| EVID-01 (hosted CI for candidate SHA) | 28 | PASS | Runs 37181864550 / 37182614963 green; run 37182035819 failed, cause unknown (see §8) |
| EVID-02 (traceability matrix) | 28 | PASS | `milestone-4.2-traceability.md`, 8 tests in `tests/planning/test_requirement_traceability.py` |
| EVID-03 (release report validator) | 28 | PASS | 21 tests, one per rejection rule |
| ISOL-03 (deterministic generator) | 29 | PASS | 28 contract tests |
| ISOL-04 (independent oracle) | 29 | PASS | 7 tests proving detection of all three corruption classes |
| ISOL-05 / ENDR-05 (deterministic barriers) | 29/31 | PASS (partial) | Defect D1 fixed and covered by 6 unit tests |
| ISOL-01/02 (isolation of new tools/processes) | 29 | Not started | — |
| PERF-01 (1M/10M reproducible datasets) | 30 | PASS | §8b, `lake-scale-summary.json` |
| PERF-02 (≥50% CPU reduction) | 30 | BLOCKED | No reproducible baseline (gap LAKE-P0-03); absolute cost recorded |
| PERF-03 (event-loop lag p99 <20 ms) | 30 | NOT MEASURED | Needs the live runner and a provider |
| PERF-04 (1m/5m p95 <100 ms, 1d p95 <250 ms) | 30 | FAIL on this host | Met at 200k; 184 ms / 1,288 ms at 1M / 10M — see finding F2 |
| PERF-05 (visibility freshness) | 30 | PASS | 81–133 ms flush→visible across scales |
| PERF-06 (partition pruning) | 30 | PASS | 10× file reduction proven by file inventory |
| PERF-07 (historical benchmarks) | 30 | DEFERRED | Requires production historical database |
| PERF-08 (bounded memory & backpressure) | 30 | PASS | Peak RSS 180→185 MB from 200k to 10M rows |
| ENDR-01..04 (24h endurance) | 31 | DEFERRED | User instruction: defer the 24-hour run |
| PERF-07 (historical benchmarks) | 30 | DEFERRED | Requires production historical database, absent here |
| MIGR-01/02/04 (operational migration) | 34 | DEFERRED | Requires production inventory, absent here |
| CAPA-01 (real capacity model) | 35 | DEFERRED | Requires production lake, absent here |
| REPB-03 (named Repo B repo) | 33 | DEFERRED | No external consumer repository is named; contract is treated as "any third-party consumer" per user instruction |
| RPLY-01..05, COMP-01..05 (Q10a/Q10b) | — | EXCLUDED | Out of v4.2 scope by user instruction |

---

*Updated: 2026-10-04 — Phase 28 execution, defect D1, traceability matrix, validator.*
