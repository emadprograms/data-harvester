# Phase 53 verification

**Date:** 2026-10-06

## Evidence

`tests/storage/test_phase53_quote_rewrite.py`: 16 passed.

- REWRITE-01 / REWRITE-02: kept rows store the old `bid` and `ask`. A differing `price` is not copied into `bid_price`.
- REWRITE-03: null bid and NaN ask are quarantined; resume after a journal-before-swap interrupt keeps quarantine totals.
- REWRITE-04: refuses when `free_bytes` cannot cover a second copy, when copy fails, when backup checksums do not match, when the publisher lock is held, and when a foreign maintenance guard exists. Backup runs under exclusive ownership. A stopped run resumes and skips finished files. `_probe_lock` is not in the module.
- REWRITE-05: `_verify_receipt_files` passes after the swap. `lake.json` is schema version 2 only after every file passes. Retired v1 bytes are gone. Backup marker is written.
- CLI requires both `--lake-root` and `--backup-root`. The source does not call `resolve_tick_lake_root()`.

## Not done here

The production lake was not opened and was not rewritten. `rewrite_quote_lake` requires an explicit `lake_root` and does not call `resolve_tick_lake_root()`.
