# Phase 49 Runbook — Migration Gate, Store Deletion & Milestone Close

**Owner-run, one time.** This is the only page that needs to be open while finishing v5.0.
Phases 46–48 are complete in code: nothing is written to a disk database any more, the
dashboard reads Parquet only, and the streamer runs 04:00–20:00 ET on weekdays.

Two things only the owner can do remain:

1. **MIG-01/02/03** — move the existing `data/streaming.duckdb` ticks into the Parquet lake
   and verify the move before anything is deleted.
2. **RMV-05 + CO-03** — delete the two legacy `.duckdb` files and close the milestone.

The agent never opens, copies, exports, archives or deletes the owner's database files.
If any check below does not pass, **stop and keep both `.duckdb` files** — they are the
safety net until this runbook says otherwise.

Supported path used everywhere below: `python3` from the repo root, with the same
environment the streamer runs in (`duckdb` and `pyarrow` must be importable).
`--lake-root` is optional: it defaults to `$TICK_LAKE_ROOT`, then `$DATA_DIR/tick_lake`,
then `<repo>/data/tick_lake`. Keep the migrations and the live streamer pointed at the
same lake root.

---

## 0. Preconditions (5 minutes)

```bash
cd "/path/to/data-harvester"

# 1. The live writer must be stopped: migration asserts ownership of the lake.
./STOP_SERVICES.sh
./tools/mac/status_services.sh          # expect: not running

# 2. No writer heart-beating in the lake control directory.
cat "$TICK_LAKE_ROOT/_control/writer_status.json" 2>/dev/null || \
  cat data/tick_lake/_control/writer_status.json

# 3. Free disk: staging is a full copy of the source until publish.
du -sh data/streaming.duckdb data/historical.duckdb
df -h "$(dirname "$TICK_LAKE_ROOT")"

# 4. Dry run first — plans and chunks the source, writes nothing (silent, exit 0,
#    and it must not create the lake directory).
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --dry-run
```

`data/historical.duckdb` is **not migrated**. Its 1-minute bar history is deliberately
dropped (owner decision, 2026-10-05): v5.0 keeps one model — ticks — in one format —
Parquet. Nothing in the toolkit reads that file.

---

## 1. MIG-01 — the migration gate

Run the stages one at a time the first time. Each command is read-only against the
source database; the source snapshot's SHA-256 is captured at plan time so that a
changed source can never be silently migrated twice.

```bash
# Stage 1 — PLAN: discovers symbols, UTC dates, row counts, chunks. No writes to the DB.
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode plan

# Stage 2 — EXPORT: chunked copy into _migration/staging with deterministic ingest ids.
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode export

# Stage 3 — VERIFY: two-way EXCEPT ALL reconciliation, per (symbol, date).
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode verify

# Stage 4 — PUBLISH: atomic promotion into ticks/ with immutable receipts.
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode publish

# Stage 5 — VERIFY-PUBLISHED: re-proves the published inventory against the source.
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode verify-published

# Stage 6 — AUDIT-LAKE: whole-lake integrity sweep (live + migrated files).
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode audit-lake
```

`--mode all` runs stages 1–4 in one process and is equivalent; stages 5 and 6 still have
to be run afterwards.

### Acceptance for MIG-01

| Check | Where to look | Pass condition |
|---|---|---|
| Plan covered everything | `"$TICK_LAKE_ROOT"/_migration/plan.json` | `total_rows` equals your own source count; `partitions[]` lists every symbol × UTC date with its `row_count` |
| Per-symbol, per-date reconciliation | `"$TICK_LAKE_ROOT"/_migration/verification.json` | `status` is `PASSED`, `discrepancies` is `[]`, `total_source_rows == total_parquet_rows`, and `files[]` has one entry per partition with the same `row_count` |
| Two-way set difference | the same file | both directions of `EXCEPT ALL` are empty — nothing missing, nothing extra, duplicates preserved |
| Published files are immutable | `ls -l "$TICK_LAKE_ROOT"/ticks/symbol=*/date=*/*.parquet` | mode `-r--r--r--` |
| Whole-lake sweep | stage 6 output | `Whole-lake integrity audit passed`, exit code `0` |
| Exit codes | every command | `0`; any non-zero means the pipeline aborted **before** publishing |

Sanity-check the numbers yourself if you want an independent count — this reads only the
production lake and is safe to run at any time:

```bash
python3 - <<'END'
import glob, duckdb
files = glob.glob("data/tick_lake/ticks/symbol=*/date=*/*.parquet")
print("files:", len(files))
print("rows:", duckdb.sql(f"SELECT count(*) FROM read_parquet({files!r})").fetchone()[0])
END
```

### MIG-02 — the ordering rule

**Migrate, verify, publish and audit first; delete second.** Concretely: do not run any
command from section 3 until section 1's acceptance table is fully satisfied. The
migration tool refuses to publish without a passing verification bound to the same plan
fingerprint, so this is enforced — but the rule also applies to eyeballing the results.

---

## 2. MIG-03 — a re-run must not duplicate

After a successful migration, run the migration again. Nothing should change:

```bash
python3 tools/migrate_streaming_to_parquet.py \
  --source-db data/streaming.duckdb --mode all

# Same count as before the re-run:
python3 - <<'END'
import glob, duckdb
files = glob.glob("data/tick_lake/ticks/symbol=*/date=*/*.parquet")
print(duckdb.sql(f"SELECT count(*) FROM read_parquet({files!r})").fetchone()[0])
END
```

Expected: exit code `0`, identical row count, and `_migration/coverage.json` still marking
every partition `COVERED` with its source fingerprint and per-file SHA-256. Covered
partitions are skipped, so a re-run — including one with a broader date range or more
symbols — cannot publish duplicates. If the source changed since the first run, or a
published file was altered, the tool stops with a `MigrationError` naming the partition
instead of silently copying: reconcile, then re-run.

*Rehearsed in the development sandbox on 2026-10-05 with 360 synthetic rows across 3
symbols × 3 dates: all six stages passed, the re-run left 9 files / 360 rows untouched,
and appending one byte to a published file was refused with
`Published migration file size mismatch`.*

---

## 3. RMV-05 — delete the legacy stores (owner action, immediately after section 1/2)

```bash
rm -f data/streaming.duckdb data/historical.duckdb
ls data/                       # no .duckdb files remain
```

No retention period, per the owner's decision. The bar history inside
`historical.duckdb` is permanently gone once this runs — that is the intent, so do not
copy it, convert it or archive it.

Then confirm the toolkit is healthy without them:

```bash
./START_SERVICES.sh
./VIEW_STATUS.sh                # supervisor + dashboard status, writer RUNNING/DEGRADED
```

The dashboard must load with ticks only; if it reports no symbols, check that the lake
root in `.env` matches the one migrated into.

---

## 4. Optional — retiring out-of-scope symbols already in the source

The tool migrates whatever the legacy database contains; only the 19 approved equities
are streamed from now on. If `plan.json` shows symbols you no longer want (they are
historical only), fence and physically retire each one:

```bash
SYMBOL=EXAMPLE

# 1. Register, then fence — this is exactly the UI's remove-symbol flow.
python3 - <<END
from src.storage.registry import get_symbol_registry, init_registry
root = "data/tick_lake"                    # or $TICK_LAKE_ROOT
init_registry(root, force=False)
registry = get_symbol_registry(root=root)
registry.add_symbol("$SYMBOL", display_name="$SYMBOL")
registry.remove_symbol("$SYMBOL")          # -> PENDING_PURGE, active=False
END

# 2. Delete the partition directory and archive the generation.
python3 -m src.storage.compaction --purge "$SYMBOL"
```

The purge refuses to run while the symbol is active and never touches anything outside
`ticks/symbol=<ENCODED_SYMBOL>/`; the registry keeps `PENDING_PURGE` until the files are
gone, so an interrupted purge can simply be re-run.

---

## 5. CO-03 — close the milestone

Copy this table into the completion report, fill in what actually happened, and stop —
no new phases, no follow-up programme.

| Item | Recorded value |
|---|---|
| `plan.json` `total_rows` / partitions | |
| `verification.json` `status` / `discrepancies` / `total_parquet_rows` | |
| `verify-published` exit code | |
| `audit-lake` result | |
| MIG-03 re-run: rows before / after | |
| Deletion command run (date, file list) | |
| Post-deletion `./VIEW_STATUS.sh` result | |

### If something fails

| Symptom | Action |
|---|---|
| `verification.json` `status: FAILED` | Do **not** delete anything. Keep `_migration/` and attach the `discrepancies` entries (each names `symbol`, `date`, `type`). Re-export with `--force` after fixing, then verify again. |
| `Publish aborted: verification must pass…` | The staging directory no longer matches the verified plan. Re-run `--mode verify`, then `--mode publish`. |
| `MigrationError: Source partition … rows but coverage ledger records …` | The source changed since the first run. Reconcile the delta, then re-run; the ledger protects you from duplicates. |
| Export interrupted (laptop slept, disk full) | Re-run with `--resume`; completed partitions are skipped. |
| Anything unexplained | Stop. Keep both `.duckdb` files. The migration is repeatable and idempotent — nothing is lost by waiting. |

Rollback before publish: delete `_migration/staging`, `state.json` and `verification.json`
and start over; the source database was only ever opened read-only.
