#!/usr/bin/env bash
#
# Phase 49 migration gate — the owner's one command.
#
# Runs the whole MIG-01/02/03 sequence against the legacy streaming database,
# proves the result three ways, re-runs once to prove it cannot duplicate, and
# prints the CO-03 completion-report table.
#
# It NEVER deletes anything. The .duckdb files are yours to delete, by hand,
# only after this says PASSED. See docs/operations/phase49_migration_runbook.md.
#
# Usage:
#   tools/mac/run_phase49_migration.sh                 # migrate data/streaming.duckdb
#   tools/mac/run_phase49_migration.sh --dry-run       # plan only, writes nothing
#   tools/mac/run_phase49_migration.sh --source-db /path/to/streaming.duckdb
#   tools/mac/run_phase49_migration.sh --lake-root "/Volumes/X9/data-harvester/data/tick_lake"
#
# Exit codes: 0 gate passed · 1 migration or verification failed
#             2 source database missing · 3 services still running
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SOURCE_DB="${SOURCE_DB:-data/streaming.duckdb}"
LAKE_ROOT_FLAG=""
DRY_RUN=0
ALLOW_LIVE_WRITER=0

while [ $# -gt 0 ]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --lake-root) LAKE_ROOT_FLAG="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --allow-live-writer) ALLOW_LIVE_WRITER=1; shift ;;
    -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

say()  { printf '%s\n' "$*"; }
fail() { code="${2:-1}"; printf '\nFAILED: %s\n' "$1" >&2; exit "$code"; }

# ---------------------------------------------------------------- environment

command -v "$PYTHON_BIN" >/dev/null 2>&1 || fail "python not found: $PYTHON_BIN"
"$PYTHON_BIN" -c 'import duckdb, pyarrow' 2>/dev/null || \
  fail "$PYTHON_BIN cannot import duckdb and pyarrow — run the streamer's own interpreter (PYTHON_BIN=/path/to/python3)"

# ---------------------------------------------------------------- lake root
# Same precedence the toolkit uses: flag > TICK_LAKE_ROOT > DATA_DIR > MICRON_DATA_DIR > repo/data.

LAKE_ROOT="$LAKE_ROOT_FLAG"
if [ -z "$LAKE_ROOT" ]; then
  for candidate in "${TICK_LAKE_ROOT:-}" "${DATA_DIR:+$DATA_DIR/tick_lake}" "${MICRON_DATA_DIR:+$MICRON_DATA_DIR/tick_lake}" "$REPO_ROOT/data/tick_lake"; do
    if [ -n "$candidate" ]; then LAKE_ROOT="$candidate"; break; fi
  done
fi
[ -n "$LAKE_ROOT" ] || fail "could not resolve the lake root; pass --lake-root"
export TICK_LAKE_ROOT="$LAKE_ROOT"

say "repo:       $REPO_ROOT"
say "python:     $("$PYTHON_BIN" -c 'import sys; print(sys.executable)')"
say "source:     $SOURCE_DB"
say "lake root:  $LAKE_ROOT"

[ -f "$SOURCE_DB" ] || fail "legacy database not found at $SOURCE_DB — nothing was migrated, nothing was deleted" 2
[ -s "$SOURCE_DB" ] || fail "legacy database $SOURCE_DB is empty" 2

# ------------------------------------------------------- services must be stopped

LIVE_CHECK="$("$PYTHON_BIN" - "$LAKE_ROOT" <<'PY'
import json, os, sys, time
from pathlib import Path
path = Path(sys.argv[1]) / "_control" / "writer_status.json"
if not path.is_file():
    print("none"); raise SystemExit
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("none"); raise SystemExit
status = str(payload.get("status") or "").upper()
live = status in {"RUNNING", "DEGRADED", "CLOSING", "DRAIN_FAILED"}
if live and isinstance(payload.get("pid"), int):
    try:
        os.kill(payload["pid"], 0)
    except OSError:
        heartbeat = payload.get("heartbeat_monotonic")
        if isinstance(heartbeat, (int, float)) and (time.monotonic() - heartbeat) >= 120.0:
            live = False
print(f"{status or 'unknown'}" if live else "none")
PY
)"

if [ "$LIVE_CHECK" != "none" ] && [ "$ALLOW_LIVE_WRITER" -eq 0 ]; then
  say ""
  fail "the streamer looks alive (writer_status.json = $LIVE_CHECK).
Stop it first:  ./STOP_SERVICES.sh   (then re-run)
Override only if you know better:  --allow-live-writer" 3
fi
[ "$LIVE_CHECK" = "none" ] || say "warning: proceeding with a live writer ($LIVE_CHECK) as requested"

# ---------------------------------------------------------------- dry run

if [ "$DRY_RUN" -eq 1 ]; then
  say ""
  say "== dry run: planning the migration, writing nothing =="
  "$PYTHON_BIN" tools/migrate_streaming_to_parquet.py --source-db "$SOURCE_DB" --dry-run
  say ""
  say "Dry run OK — nothing was written. Re-run without --dry-run to migrate."
  exit 0
fi

# ---------------------------------------------------------------- the gate

run_stage() {
  say ""
  say "== $1 =="
  "$PYTHON_BIN" tools/migrate_streaming_to_parquet.py --source-db "$SOURCE_DB" --mode "$2" || \
    fail "$1 failed — nothing was deleted.
See docs/operations/phase49_migration_runbook.md ('If something fails').
Keep $SOURCE_DB and the lake's _migration/ directory for diagnosis." 1
}

rows_now() {
  "$PYTHON_BIN" - "$LAKE_ROOT" <<'PY'
import glob, sys
from pathlib import Path
root = Path(sys.argv[1])
files = sorted(glob.glob(str(root / "ticks" / "symbol=*" / "date=*" / "*.parquet")))
if not files:
    print("0 0")
else:
    import duckdb
    count = duckdb.sql("SELECT count(*) FROM read_parquet(?)", params=[files]).fetchone()[0]
    print(f"{count} {len(files)}")
PY
}

say ""
say "Phase 49 migration gate — stages 1-4 (plan, export, verify, publish)"
run_stage "plan -> export -> verify -> publish" all

run_stage "verify-published" verify-published

run_stage "audit-lake" audit-lake

READ_BEFORE="$(rows_now)"
say ""
say "published rows / files: $READ_BEFORE"

say ""
say "MIG-03 — re-running the migration to prove it cannot duplicate"
run_stage "re-run (plan -> export -> verify -> publish)" all
READ_AFTER="$(rows_now)"
say ""
say "published rows / files after the re-run: $READ_AFTER"
[ "$READ_BEFORE" = "$READ_AFTER" ] || fail "the re-run changed the published lake ($READ_BEFORE -> $READ_AFTER). Nothing was deleted."

# ---------------------------------------------------------------- report

say ""
"$PYTHON_BIN" - "$LAKE_ROOT" "$SOURCE_DB" "$READ_BEFORE" "$READ_AFTER" <<'PY'
import json, sys
from pathlib import Path

lake, source_db, before, after = sys.argv[1:5]
migration = Path(lake) / "_migration"

def load(name):
    try:
        return json.loads((migration / name).read_text(encoding="utf-8"))
    except Exception:
        return {}

plan = load("plan.json")
verification = load("verification.json")
coverage = load("coverage.json")
partitions = plan.get("partitions", [])
covered = [k for k, v in coverage.get("partitions", coverage).items()
           if isinstance(v, dict) and v.get("status") == "COVERED"]

print("================================================================")
print("CO-03 completion report — fill these values in")
print("================================================================")
print(f"| Source database                          | {source_db}")
print(f"| plan.json total_rows / partitions        | {plan.get('total_rows', '?')} / {len(partitions)}")
print(f"| verification status                      | {verification.get('status', 'MISSING')}")
print(f"| discrepancies                            | {len(verification.get('discrepancies', []))}")
print(f"| total_source_rows == total_parquet_rows  | {verification.get('total_source_rows', '?')} == {verification.get('total_parquet_rows', '?')}")
print(f"| verification files reconciled            | {len(verification.get('files', []))}")
print(f"| coverage ledger partitions COVERED       | {len(covered)}")
print(f"| verify-published exit code               | 0")
print(f"| audit-lake exit code                     | 0")
print(f"| MIG-03 re-run rows before / after        | {before} / {after}")
print("================================================================")
passed = (
    verification.get("status") == "PASSED"
    and not verification.get("discrepancies")
    and verification.get("total_source_rows") == verification.get("total_parquet_rows")
)
raise SystemExit(0 if passed else 1)
PY

# ---------------------------------------------------------- symbol registry
# The streamer subscribes to the lake registry and nothing else. A migrated lake
# with an empty registry is a lake that will ingest nothing, silently
# (INCIDENT-2026-10-06). The migration seeds it from the plan; this verifies it,
# and backfills from the published partitions if the seed could not be derived.

say ""
say "== symbol registry (the streamer's single authority) =="

REG_CHECK="$("$PYTHON_BIN" - "$LAKE_ROOT" <<'PY'
import json, sys
from pathlib import Path
root = Path(sys.argv[1])
reg = root / "_control" / "registry.json"
if not reg.is_file():
    print("MISSING 0")
    raise SystemExit
try:
    data = json.loads(reg.read_text(encoding="utf-8"))
except Exception:
    print("UNREADABLE 0")
    raise SystemExit
symbols = data.get("symbols") or {}
active = [k for k, v in symbols.items()
          if isinstance(v, dict) and v.get("active") and v.get("status") == "ACTIVE"]
print(f"{'OK' if active else 'EMPTY'} {len(active)}")
PY
)"

REG_STATE="${REG_CHECK%% *}"
REG_COUNT="${REG_CHECK##* }"
say "   registry state: $REG_STATE ($REG_COUNT active symbols)"

if [ "$REG_STATE" != "OK" ]; then
  LAKE_SYMBOLS="$("$PYTHON_BIN" - "$LAKE_ROOT" <<'PY'
import sys
from pathlib import Path
ticks = Path(sys.argv[1]) / "ticks"
found = sorted({p.name.split("=", 1)[1] for p in ticks.glob("symbol=*") if p.is_dir()})
print(",".join(found))
PY
)"
  if [ -z "$LAKE_SYMBOLS" ]; then
    fail "the lake registry has no active symbols and no symbol partitions exist.
The streamer would subscribe to nothing and ingest nothing while every status
light stayed green. Nothing was deleted; fix the migration or seed by hand:
  $PYTHON_BIN -m src.storage.registry --root \"$LAKE_ROOT\" --seed approved"
  fi
  say "   seeding the registry from the published partitions: $LAKE_SYMBOLS"
  "$PYTHON_BIN" -m src.storage.registry --root "$LAKE_ROOT" --seed "$LAKE_SYMBOLS" || \
    fail "registry seeding failed — the streamer will refuse to start until it is seeded."

  REG_CHECK="$("$PYTHON_BIN" - "$LAKE_ROOT" <<'PY'
import json, sys
from pathlib import Path
data = json.loads((Path(sys.argv[1]) / "_control" / "registry.json").read_text(encoding="utf-8"))
symbols = data.get("symbols") or {}
active = [k for k, v in symbols.items()
          if isinstance(v, dict) and v.get("active") and v.get("status") == "ACTIVE"]
print(f"{'OK' if active else 'EMPTY'} {len(active)}")
PY
)"
  [ "${REG_CHECK%% *}" = "OK" ] || fail "the lake registry is still empty after seeding."
  say "   registry now: $REG_CHECK"
fi

say ""
say "GATE PASSED. Nothing has been deleted yet."
say ""
say "Now, and only now:"
say "    rm -f data/streaming.duckdb data/historical.duckdb"
say "    ./START_SERVICES.sh && ./VIEW_STATUS.sh"
say ""
say "Produce the CO-03 report from the table above, then stop — v5.0 adds no further phases."
