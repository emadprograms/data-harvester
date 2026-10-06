---
phase: 52
plan: 01
title: Contract and operator docs
status: complete
completed_at: 2026-10-06T12:00:00Z
requirements-completed: [DOCS-01]
---

# Phase 52 Summary: Contract and operator docs

## Accomplishments

1. **DOCS-01**: README, the operations guide, and the Repo B contract name `bid_price` and `ask_price`, forbid reading `price` or `volume` as the stored quote, and describe the named-day all-symbol gap-fill rule.
2. The three documents still say the existing lake is schema v1. They name the rewrite tool (`python -m src.storage.quote_rewrite --lake-root <explicit-lake> --backup-root <explicit-backup>`) and defer production execution to the owner. They do not tell the operator to run that rewrite from this checkout.
3. Published schema v1 examples still execute against a schema v1 fixture.

## Verification

- `tests/docs/test_phase52_docs.py`, `tests/docs/test_documentation_contract.py`, and `tests/contract/test_repo_b_contract.py`: 48 passed.

## Not done here

The production lake was not opened and was not rewritten.
