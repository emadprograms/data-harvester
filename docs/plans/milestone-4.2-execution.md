# Milestone v4.2 — Execution Report (Working Document)

**Milestone:** v4.2 Tick Lake Qualification & Scoped Signoff (Phases 28–36)
**Started:** 2026-10-04
**Status:** In progress — Phase 28 complete pending hosted CI; Phases 29+ in progress
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

## 6. Gate status

| Gate | Phase | Status | Evidence |
|---|---|---|---|
| EVID-02 (traceability matrix) | 28 | PASS | `milestone-4.2-traceability.md`, 8 tests in `tests/planning/test_requirement_traceability.py` |
| EVID-03 (release report validator) | 28 | PASS | 21 tests, one per rejection rule |
| EVID-01 (hosted CI for candidate SHA) | 28 | Pending | Requires a PR to trigger `.github/workflows/offline-tests.yml`; recorded when the run completes |
| ISOL-05 / ENDR-05 (deterministic barriers) | 29/31 | PASS (partial) | Defect D1 fixed and covered by 6 unit tests |
| ENDR-01..04 (24h endurance) | 31 | DEFERRED | User instruction: defer the 24-hour run |
| PERF-07 (historical benchmarks) | 30 | DEFERRED | Requires production historical database, absent here |
| MIGR-01/02/04 (operational migration) | 34 | DEFERRED | Requires production inventory, absent here |
| CAPA-01 (real capacity model) | 35 | DEFERRED | Requires production lake, absent here |
| REPB-03 (named Repo B repo) | 33 | DEFERRED | No external consumer repository is named; contract is treated as "any third-party consumer" per user instruction |
| RPLY-01..05, COMP-01..05 (Q10a/Q10b) | — | EXCLUDED | Out of v4.2 scope by user instruction |

---

*Updated: 2026-10-04 — Phase 28 execution, defect D1, traceability matrix, validator.*
