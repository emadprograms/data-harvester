# INCIDENT-2026-10-06 — Total ingestion loss with every status light green

**Symptom.** Tuesday produced no data. The streamer process was alive and the supervisor
reported `INGESTING`. No crash, no error, no alert.

**Root cause (one sentence).** The lake registry — the single authority for which symbols
are subscribed — was left empty by the Monday-evening migration, and nothing in the system
treated "the authority lists nothing" as a fault: the engine subscribed to nothing, stayed
alive, and every monitor that reported health was reading artefacts that cannot change when
ingestion is zero.

**Scope of this document.** §1 verifies each claim of the original diagnosis against the
running code and marks it confirmed / corrected / refined. §2 lists what the diagnosis
missed. §3 traces the causal chain. §4 lists the fixes applied with the tests that pin
them. §5 lists what is deliberately left to the operator.

Evidence was produced by `tools/repro_incident.py`, which drives the real supervisor, the
real stream engine and the real dashboard against a lake shaped like the production one and
interrogates the same endpoints an operator would. Claims marked *(executed)* were observed
live, not inferred.

---

## 1. Verdict on the reported findings

### 1.1 "Four conflicting definitions of symbols" — **CONFIRMED, and there are five**

| # | Definition | Location | Role |
|---|---|---|---|
| 1 | Lake registry entry | `src/storage/registry.py:422` `get_active_symbols()` | the documented authority (`SYMB-01`) |
| 2 | `MONITORED_19_SYMBOLS` | `src/storage/reader.py:142` (+ a duplicate copy in `src/dashboard/analytics.py:215`) | drives the continuity spectrogram, bypassing the registry |
| 3 | `APPROVED_EQUITY_SYMBOLS` | `src/config.py:17` | allowlist gate for adds (`server.py:33`) and Databento scope (`databento_backfill.py:44`) |
| 4 | Directory names on disk | `src/storage/reader.py:1615` | `get_lake_health_report().active_symbols` |
| 5 | **Provider-side defaults** | `src/stream/fake_provider.py:37` — *not in the original diagnosis* | substitutes `["AAPL","MSFT"]` when handed no epics |

Notably, `src/config.py` claims "the lake registry holds exactly these symbols", and
`tests/test_symbol_authority.py` asserts the registry is the single authority — while
nothing in the product ever *writes* config symbols into the registry. The only production
writer is the dashboard's one-at-a-time POST (`server.py:440`). The claim that the
codebase has no single source of truth is correct; the fix in §4 keeps the registry as the
authority and makes the absence of a seed visible.

### 1.2 "The Phase 49 migration gate missed registry initialization" — **CONFIRMED in effect, CORRECTED in mechanism**

Confirmed: no migration component references the registry at all
(`grep -n "registry" tools/migrate_streaming_to_parquet.py` → nothing; likewise
`tools/validate_concurrency.py`, `tools/synthetic_streamer.py`, `tools/preflight.py`).
The tool migrates data and never symbol knowledge.

Corrected: the migration does **not** create the blank file. `init_registry()` writes
`{"symbols": {}}` (`registry.py:584`) and is called from exactly three places — the engine's
fresh-lake bootstrap (`runner.py:208`), a benchmark helper, and the runbook's manual purge
snippet. So the empty registry came from the engine's bootstrap (or a manual call), or the
migration was pointed at a lake root whose registry was initialised separately. The
mechanism matters because "the migration forgot to seed" and "the engine silently accepts
an empty authority" are two different defects: fixing only the first leaves the second
live. Both are fixed.

> **The sharpest version of this finding:** a *missing* registry is a hard failure — the
> engine raises `RegistryError` and the supervisor crash-loops *(executed, scenario B)*.
> A *blank* registry is not. `init_registry()` therefore converts a loud failure into a
> silent one. That inversion is the incident.

### 1.3 "Silent streamer abort and idle process deadlock" — **CONFIRMED** *(executed, scenario A/E)*

`capital_stream.py:141-145` returns without raising when `self.epics` is empty;
`runner.py:648-663` builds that empty list from the registry; `_lake_writer_worker` and
`_symbol_watcher_worker` keep the process alive forever. Observed: lifecycle `INGESTING`,
`writer_status.json` = `RUNNING` with `total_rows_written: 0`, log line
`🎯 Target Capital.com symbols for live quotes (0): []...`.

Refinement: in **live** mode the idle is total, but the mock provider additionally
substitutes `["AAPL","MSFT"]`, emits phantom ticks, has them rejected as
`Rejected out-of-scope symbol ... the lake registry is the only symbol authority`, and then
**crashes** with `ZeroDivisionError` on the first subscription reload that clears the list
(`fake_provider.py:99` divides by `len(self.epics)`), triggering a supervisor restart.
Two of the five defect classes in this incident live in the dev provider.

### 1.4 "Supervisor reports false INGESTING" — **CONFIRMED** *(executed)*

`_windowed_tick()` published `INGESTING` on `process.poll() is None` alone. Untrue in both
directions: a healthy child writing nothing reads green, and — observed in scenario B — a
child that dies one second later still logs `INGESTING — restarted after a crash` first.

### 1.5 "Uncontrolled supervisor log flooding" — **CONFIRMED**

Poll loop is 2 s (`--poll-interval 2.0`) and `_record()` logged unconditionally, so
identical `Lifecycle: INGESTING` lines were appended every poll: ~43,000 lines/day. The
20 MB rotation cap is reached in about a week and keeps only one `.old.log`. Observed at
1 s polling: 9 identical lines in 10 s; after the fix, 1 *(executed)*.

### 1.6 "Dashboard telemetry masks outages" — **CONFIRMED, and worse than reported**

- `get_lake_health_report()` returns `"healthy": True` on every non-exceptional path
  (`reader.py:1634`), so `/api/status` could only ever be `HEALTHY` or `DEGRADED`-for-crash.
- **Correction:** the diagnosis says stream status was green "because the PID is active".
  No PID was checked anywhere. `get_stream_status()` derived `is_alive` purely from the
  `status` **string** in a file that persists after death. In scenario B the writer died at
  PID 1742, and `/api/status` still reported `stream.status = RUNNING`; the PID was
  demonstrably dead (`os.kill(pid, 0)` → `No such process`) *(executed)*. Even the
  crash-looping lake read green.
- `symbols_audited: []` on `/api/integrity` is right, and so is `overall_passed: false` —
  but an empty audit list is easy to skim past; it now carries an explicit
  `registry_empty` flag.

### 1.7 "Downstream backfill and gap-fill blocked" — **CONFIRMED by code path** (not re-executed end-to-end)

`get_target_stock_symbols()` reads the registry and returns `[]` on an empty one
(`databento_backfill.py:56-76`); callers `databento_backfill.py:293` and `gap_fill.py:875`
then no-op. Both entry points swallow every exception into `[]`, so an empty registry and
an unreadable registry are indistinguishable to the caller.

---

## 2. What the original diagnosis missed

1. **`init_registry()` turns a loud failure into a silent one.** A missing registry file
   crash-loops the streamer into a visible `ERROR`; a blank one idles invisibly. Whichever
   path created it, the file *masked* the fault. (§1.2)
2. **The mock provider is a fifth symbol source and a crash site** — phantom
   `["AAPL","MSFT"]` defaults plus a `len()` division by zero on reload-to-empty. (§1.3)
3. **Nothing in the product can seed the registry.** No tool, script, or runbook step
   populates it from `APPROVED_EQUITY_SYMBOLS`; the only writers are the dashboard POST and
   test fixtures. The migration gate — the intended last step before services start —
   ends by telling the operator to delete the legacy store and start services, with no
   symbol step at all. This is the missing capability, not merely a missing call.
4. **No heartbeat is ever refreshed while idle.** `_update_status_file()` runs on init,
   flush, publish and close — never on a timer — so a quiet writer's file ages silently
   while still claiming `RUNNING`.
5. **Tests locked the bug in.** `create_lake()` always seeds a registry, so no test ever
   ran a migrated-but-unseeded lake; and two fixtures asserted that a fabricated PID
   (98765, 54321) with `status: RUNNING` reports `is_alive: True`, pinning the
   string-only liveness check as intended behavior.
6. **`Reader.active_symbols` is a historical count**, computed from directory names. It
   reported 19 during a day of zero ingestion; the honest names are now also reported
   (`historical_symbols`, `seconds_since_newest_write`).

---

## 3. Causal chain

```
Phase 49 migration        moves tick data                          (no symbol knowledge)
        │
        ▼
_control/registry.json    exists but {"symbols": {}}               (blank ≠ missing)
        │
        ▼
runner.start()            capital_symbols = []                     (no fault raised)
        │
        ▼
CapitalStreamer.start()   "no active subscriptions" → return       (silent no-op)
        │
        ├──► process alive forever (writer worker + symbol watcher)
        │
        ▼
supervisor                process.poll() is None → INGESTING       (green)
        │
        ▼
dashboard /api/status     lake.healthy=True, stream string=RUNNING (green)
        │
        ▼
/api/integrity            symbols_audited=[]                       (nothing audited)
        │
        ▼
gap fill / backfill       get_target_stock_symbols() = []          (no-op)
```

Every one of the six monitors along that path is reading something that is *incapable of
changing* when ingestion is zero. The failure was not that a monitor broke; it is that
none of them measured ingestion.

---

## 4. Fixes applied

Each fix targets a link in the chain above; the tests named are the ones that fail if the
link reopens.

| # | Fix | Files | Pinned by |
|---|---|---|---|
| 1 | **A live engine with zero active symbols refuses to start** (`RegistryError`, exit non-zero → supervisor `ERROR` → Discord `SESSION_START_FAILED`). Mock/offline runs stay lenient. | `src/stream/runner.py` | `tests/stream/test_registry_authority_fail_loud.py` |
| 2 | **A live reload that empties the subscription set logs an error and emits `INGESTION_STALLED`** instead of going quiet. | `src/stream/runner.py`, `src/utils/notifications.py` | same |
| 3 | **Writer liveness requires a real PID**; the status file can no longer report `LIVE` for a dead process. New fields: `pid_alive`, `heartbeat_age_seconds`, `stale_status_file`. | `src/storage/reader.py` | `test_registry_authority_fail_loud.py`, updated `tests/storage/test_lake_reader.py` |
| 4 | **`/api/status` is `HEALTHY` only when the writer is alive *and* publishing**, with a `reasons[]` list; alive-but-silent (no writes for `NO_PROGRESS_SECONDS`, default 900) degrades. | `src/dashboard/server.py` | `tools/repro_incident.py` scenarios A/C/D |
| 5 | **`/api/integrity` reports `registry_empty`** so an unauditable lake cannot read as a pass by omission. | `src/dashboard/server.py` | `tools/repro_incident.py` scenario E |
| 6 | **New `STALLED` lifecycle state**: the child is up inside the window but no rows have been written for `--stall-seconds` (default 900, `0` disables). One Discord `INGESTION_STALLED` per stall, a recovery notice when it clears. Scoped to ingestion services (`--watch-ingestion-progress`, implied by `--enforce-window`) so the dashboard supervisor can never inherit the streamer's progress file. | `src/utils/lifecycle.py`, `tools/service_supervisor.py`, launch scripts | `tests/support/test_supervisor_lifecycle.py`; manual trace: `INGESTING → STALLED (no rows written for 6s)` |
| 7 | **Lifecycle logging de-duplicated** — transitions log immediately, unchanged states every `--heartbeat-log-interval` (default 900 s) marked `(still)`; the state file keeps refreshing every poll. | `tools/service_supervisor.py` | `tools/repro_incident.py` (9 lines → 1) |
| 8 | **Registry seeding exists and runs by itself**: `python -m src.storage.registry --root <lake> --seed approved\|A,B,C` (idempotent, never deactivates); `--mode all`/`--mode publish` seed from the migration plan (`--no-seed-registry` opts out); the one-command gate verifies the registry and fails the gate if it would hand over an empty authority. | `src/storage/registry.py`, `tools/migrate_streaming_to_parquet.py`, `tools/mac/run_phase49_migration.sh`, runbook + ops guide | `tests/storage/test_migration_seeds_registry.py`, `test_registry_authority_fail_loud.py` |
| 9 | **Provider no longer fabricates symbols or crashes**: `FakeProvider` keeps an empty epic list empty and idles instead of dividing by zero. | `src/stream/fake_provider.py` | full suite (mock suites unchanged) |
| 10 | **One fewer duplicate symbol list**: the unused copy in `analytics.py` is now a import of the single definition; the spectrogram's presentation ticket is documented as such. | `src/dashboard/analytics.py` | full suite |
| 11 | **`VIEW_STATUS.sh` tells the truth**: prints each supervisor's lifecycle state (flagging `STALLED`), the registry's active-symbol count with the seed command when empty, and marks a `RUNNING` writer whose PID is gone. | `tools/mac/status_services.sh` | manual run |

**Regression harness.** `python3 tools/repro_incident.py` (~60 s) rebuilds the incident and
asserts all eight expectations, including the two negative controls (a seeded lake must
still ingest normally; mock runs must stay lenient). It runs offline and is safe to re-run:
all output goes to `/tmp/incident`.

**Test status.** 1087 passed, 1 failed. The single failure,
`tests/storage/test_parquet_writer.py::test_off_loop_event_loop_responsiveness`, asserts
event-loop scheduling lag < 20 ms; it fails identically on unmodified `HEAD` in this
2-vCPU sandbox (24–30 ms) and is unrelated to these changes.

---

## 5. Left to the operator (deliberately not changed)

1. **The spectrogram still renders 19 fixed symbols** (`reader.py:1186`), and
   `tests/test_streaming_continuity.py:186` requires `monitored_symbols_count == 19` from a
   lake whose registry holds 11. Making the view registry-driven is the correct end state —
   the registry is the authority — but it changes product-visible behaviour and several
   spec tests; it should be a deliberate, reviewed change, not a by-product of an incident
   fix. Until then the empty-registry condition is surfaced through `registry_empty`,
   the `STALLED` state and honest `/api/status`.
2. **Frontend remnants**: `index.html:154` hardcodes an `AAPL` option and
   `continuity.js:259` labels the spectrogram "All 19 symbols". Cosmetic, but they will
   read as truth to an operator.
3. **`gap_fill` / `databento_backfill` still return `[]` silently** on an empty or
   unreadable registry. They should raise or log loudly; that is a behaviour change to two
   CLI tools with their own test suites.
4. **The three legacy `.duckdb` files** are still on disk, per the runbook. Nothing in
   v5.0 reads them; delete them only after re-running the gate, which now also proves the
   registry is populated before services start.

---

## 6. Lessons worth keeping

- **"Process alive" is not "work happening."** Every monitor in this stack measured the
  first and reported the second.
- **Absence of data is not absence of failure.** An empty registry, an empty audit list
  and a zero-row heartbeat are all *valid-looking* states that require explicit rejection.
- **A fail-closed check on a missing file is worthless if something creates an empty one.**
  `init_registry()`'s blank schema is what turned a visible crash into a silent day.
- **Test fixtures encode policy.** `create_lake()` seeding a registry, and fake PIDs
  asserted alive, made the outage unreachable by test.
