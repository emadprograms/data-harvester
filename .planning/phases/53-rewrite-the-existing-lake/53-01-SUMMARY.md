---
phase: 53
plan: 01
title: Offline rewrite of a fixture lake
status: complete
completed_at: 2026-10-06T12:00:00Z
requirements-completed: [REWRITE-01, REWRITE-02, REWRITE-03, REWRITE-04, REWRITE-05]
---

# Phase 53 Summary: Rewrite the existing lake

## Accomplishments

1. **REWRITE-01 / REWRITE-02**: Fixture rewrite keeps old `bid`/`ask` as `bid_price`/`ask_price` and does not copy `price`.
2. **REWRITE-03**: Null or NaN bid/ask rows are quarantined. Resume after a journal-before-swap interrupt reconstructs quarantine totals.
3. **REWRITE-04**: Backup runs under exclusive ownership. The tool refuses on insufficient disk, copy failure, checksum mismatch, a held publisher lock, or a foreign maintenance guard. Unused `_probe_lock` was removed; the run holds the lock before copy.
4. **REWRITE-05**: Receipts match swapped bytes. `lake.json` becomes schema version 2 only after every file passes. Retired v1 bytes are then deleted.
5. CLI requires both `--lake-root` and `--backup-root`. The module does not call `resolve_tick_lake_root()`.

## Verification

- `tests/storage/test_phase53_quote_rewrite.py`: 16 passed.

## Not done here

The production lake was not opened and was not rewritten. The owner runs the tool locally with explicit roots.
