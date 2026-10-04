#!/usr/bin/env python3
"""
Requirement-to-evidence traceability for Milestone v4.2 (Q01 / EVID-02).

Maps every archived requirement ID (`LAKE-*` from v4.0, `TEST-P22-*` through
`TEST-P27-*` from v4.1, and audit findings `F01`-`F11`) to the pytest nodes that
assert it, then verifies each node actually exists in the collected suite.

Design notes:
- `granularity: test` means the map names a specific test function.
- `granularity: module` means the requirement is covered somewhere inside that
  module; it is a weaker claim than a named test and is labelled as such.
- An empty `nodes` list is a declared coverage gap, not an oversight. The tool
  reports gaps explicitly so they cannot be mistaken for passing evidence.

Usage:
    python tools/requirement_traceability.py --check
    python tools/requirement_traceability.py --markdown docs/plans/milestone-4.2-traceability.md
    python tools/requirement_traceability.py --json .planning/artifacts/traceability.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# (id, description, nodes, granularity)
REQUIREMENTS: List[Dict[str, object]] = [
    # ---------------------------------------------------------------- v4.0 LAKE
    {
        "id": "LAKE-P0-01",
        "description": "Test isolation routes default paths to tmp_path and blocks production mutations",
        "nodes": ["tests/test_isolation_guard.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P0-02",
        "description": "Deterministic quote fixtures with repeats, ties, late arrivals, nulls, session boundaries",
        "nodes": ["tests/test_quote_fixtures.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P0-03",
        "description": "Baseline characterization of legacy DuckDB CPU seconds, latency percentiles, event-loop lag",
        "nodes": [],
        "granularity": "none",
        "gap": "No automated node. Only the manual script tools/benchmark_baseline.py exists; no test asserts baseline CPU/latency numbers.",
    },
    {
        "id": "LAKE-P1-01",
        "description": "Lake layout, TICK_LAKE_ROOT resolution, format versioning and safe symbol encoding",
        "nodes": ["tests/storage/test_storage_config.py", "tests/storage/test_symbol_encoding.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P1-02",
        "description": "Typed Schema v1 with microsecond timestamp and stable unique ingest_id",
        "nodes": ["tests/storage/test_schema_v1.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P1-03",
        "description": "Atomic file staging via .tmp files and atomic rename",
        "nodes": ["tests/storage/test_atomic_publication.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P1-04",
        "description": "Publication state machine and idempotent receipts",
        "nodes": ["tests/storage/test_atomic_publication.py", "tests/storage/test_crash_recovery.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P2-01",
        "description": "Micro-batch writer with configurable flush thresholds and bounded queue",
        "nodes": ["tests/storage/test_parquet_writer.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P2-02",
        "description": "Off-loop PyArrow worker keeping event-loop scheduling lag bounded",
        "nodes": ["tests/storage/test_parquet_writer.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P2-03",
        "description": "Runner lifecycle integration with honest counters and cooperative drain",
        "nodes": ["tests/stream/test_lake_runner_stress.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P3-01",
        "description": "Atomic JSON symbol registry managed by a single control owner",
        "nodes": ["tests/storage/test_registry.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P3-02",
        "description": "Cross-process registry polling and dynamic reload without DB locks",
        "nodes": ["tests/storage/test_registry_stress.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P3-03",
        "description": "Pending purge semantics and subscription fencing",
        "nodes": ["tests/storage/test_registry_stress.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P4-01",
        "description": "TickLakeReader engine with private in-memory DuckDB and partition pruning",
        "nodes": ["tests/storage/test_lake_reader.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P4-02",
        "description": "Deterministic OHLCV resampling via time_bucket/arg_min/arg_max",
        "nodes": ["tests/storage/test_lake_reader.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P4-03",
        "description": "Dashboard analytics migrated off legacy DuckDB files",
        "nodes": ["tests/dashboard/test_lake_dashboard_integration.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P4-04",
        "description": "External consumer (Repo B) reader contract: schema docs, examples, connection patterns",
        "nodes": [],
        "granularity": "none",
        "gap": "No executable node. The contract is documentation only; Phase 33 (REPB-01) adds executable contract tests.",
        "planned_phase": 33,
    },
    {
        "id": "LAKE-P6-01",
        "description": "Migration CLI with plan/export/verify/publish modes",
        "nodes": ["tests/storage/test_migration_tool.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P6-02",
        "description": "Chunked export and checkpointed progress",
        "nodes": ["tests/storage/test_migration_tool.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P6-03",
        "description": "Two-way EXCEPT ALL reconciliation with zero row loss",
        "nodes": ["tests/storage/test_migration_tool.py", "tests/storage/test_migration_stress.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P8-01",
        "description": "Coordinated writer cutover from legacy writer to live Parquet lake",
        "nodes": ["tests/storage/test_tick_lake_audit_migration_regressions.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P8-02",
        "description": "Multi-process concurrency without lock errors",
        "nodes": ["tests/integration/test_lake_multi_process_concurrency.py"],
        "granularity": "module",
    },
    {
        "id": "LAKE-P8-03",
        "description": "Documentation and service configuration updates",
        "nodes": [],
        "granularity": "none",
        "gap": "Documentation-only requirement; no automated node. Phase 36 (DOCS-01..03) verifies docs against code.",
        "planned_phase": 36,
    },
    # ------------------------------------------------------- v4.1 phase reqs
    {
        "id": "TEST-P22-01",
        "description": "Storage foundation edge cases: paths, encoding, corrupted metadata",
        "nodes": ["tests/storage/test_storage_edge_cases.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P22-02",
        "description": "Schema coercion and extreme numeric handling",
        "nodes": ["tests/storage/test_storage_edge_cases.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P22-03",
        "description": "Atomic publication collisions, intent recovery, lock serialization",
        "nodes": ["tests/storage/test_storage_edge_cases.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P23-01",
        "description": "High-throughput micro-batching under memory pressure",
        "nodes": ["tests/stream/test_lake_runner_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P23-02",
        "description": "Bounded queue backpressure and honest shedding",
        "nodes": ["tests/stream/test_lake_runner_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P23-03",
        "description": "Runner lifecycle: shutdown mid-flush, drain, acknowledgment after durability",
        "nodes": ["tests/stream/test_lake_runner_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P24-01",
        "description": "Cross-process concurrent registry CRUD serialization",
        "nodes": ["tests/storage/test_registry_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P24-02",
        "description": "Monotonic versioning, torn-write rejection, pending purge fencing",
        "nodes": ["tests/storage/test_registry_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P24-03",
        "description": "Reload signal debouncing and reload latency under polling",
        "nodes": ["tests/storage/test_registry_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P25-01",
        "description": "Concurrent in-memory reader connections without leaks",
        "nodes": ["tests/storage/test_lake_reader_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P25-02",
        "description": "Resampling edge cases: sparse partitions, roll-overs, DST, leap years",
        "nodes": ["tests/storage/test_lake_reader_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P25-03",
        "description": "Reverse-chronological tape pagination and symbol pruning",
        "nodes": ["tests/storage/test_lake_reader_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P26-01",
        "description": "Migration of corrupt/partial legacy sources and schema drift",
        "nodes": ["tests/storage/test_migration_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P26-02",
        "description": "Crash interruption across migration modes and resumable checkpoints",
        "nodes": ["tests/storage/test_migration_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P26-03",
        "description": "Two-way EXCEPT ALL fuzz reconciliation preserving multiplicity and precision",
        "nodes": ["tests/storage/test_migration_stress.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P27-01",
        "description": "Sustained multi-process soak with concurrent analytical readers",
        "nodes": ["tests/integration/test_supervisor_chaos_soak.py"],
        "granularity": "module",
    },
    {
        "id": "TEST-P27-02",
        "description": "Chaos monkey termination and supervisor self-healing to steady state",
        "nodes": ["tests/integration/test_supervisor_chaos_soak.py"],
        "granularity": "module",
    },
    # ------------------------------------------------------------ findings F01-F11
    {
        "id": "F01",
        "description": "Restart defaults; payload replay and corrupt receipt target",
        "nodes": [
            "tests/storage/test_tick_lake_audit_writer_regressions.py::test_default_writer_restart_publishes_new_observation_once",
            "tests/storage/test_tick_lake_audit_writer_regressions.py::test_receipt_identity_replay_rejects_changed_payload_and_corrupt_target",
        ],
        "granularity": "test",
    },
    {
        "id": "F02",
        "description": "Callback capacity/cancellation; retry retention; failed drain; receipt counts",
        "nodes": [
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_real_callback_waits_for_queue_capacity_without_loss",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_callback_waiting_for_capacity_can_be_cancelled_without_false_drop",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_exhausted_storage_retries_retain_batch_until_recovery",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_shutdown_reports_unsaved_accepted_batch_as_failed_drain",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_committed_count_uses_verified_receipt_rows",
        ],
        "granularity": "test",
    },
    {
        "id": "F03",
        "description": "Receipt failure after rename; retry publishes exactly once",
        "nodes": [
            "tests/storage/test_tick_lake_audit_writer_regressions.py::test_receipt_failure_retry_reuses_prepared_batch_without_duplicate_rows",
        ],
        "granularity": "test",
    },
    {
        "id": "F04",
        "description": "Staged file replacement, wrong schema/extra file, source/scope mismatch at publication",
        "nodes": [
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_rejects_staging_mutated_after_successful_verification",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_verification_is_bound_to_source_scope_and_migration",
        ],
        "granularity": "test",
    },
    {
        "id": "F05",
        "description": "Distinct-source migrations, corrupt checkpoint, source mismatch on resume",
        "nodes": [
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_distinct_migrations_append_immutably_and_same_migration_is_idempotent",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_resume_does_not_trust_corrupt_completed_chunk_checkpoint",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_resume_rejects_different_source_or_scope",
        ],
        "granularity": "test",
    },
    {
        "id": "F06",
        "description": "Maintenance marker fences writer, recovery and migration; shared ownership gate",
        "nodes": [
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_writer_startup_and_direct_publication_are_fenced_by_maintenance",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_pending_publication_recovery_is_fenced_by_maintenance",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_migration_publication_is_fenced_by_maintenance",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_maintenance_lock_serializes_with_publisher_and_fences_new_publishers",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_maintenance_and_publisher_ownership_cross_process_barriers",
        ],
        "granularity": "test",
    },
    {
        "id": "F07",
        "description": "Configured lake initialization and dashboard errors must not fall back to legacy DuckDB",
        "nodes": [
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_environment_selected_lake_failure_never_falls_back_to_streaming_duckdb",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_dashboard_lake_error_does_not_fall_back_to_streaming_duckdb",
            "tests/storage/test_tick_lake_audit_storage_regressions.py::test_explicit_legacy_backend_remains_available",
        ],
        "granularity": "test",
    },
    {
        "id": "F08",
        "description": "Empty/inactive registry and missing/corrupt registry startup",
        "nodes": [
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_empty_registry_start_does_not_subscribe_or_authenticate",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_established_lake_registry_errors_fail_closed_before_provider_start",
        ],
        "granularity": "test",
    },
    {
        "id": "F09",
        "description": "Invalid values, effective defaults/env/CLI precedence, row trigger, child config",
        "nodes": [
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_invalid_stream_flush_settings_are_rejected",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_invalid_stream_batch_settings_are_rejected",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_defaults_and_environment_overrides_reach_real_writer",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_runner_cli_overrides_env_and_reaches_engine",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_configured_batch_row_trigger_flushes_before_timer",
            "tests/stream/test_tick_lake_audit_runner_regressions.py::test_supervisor_passes_effective_stream_settings_to_child",
        ],
        "granularity": "test",
    },
    {
        "id": "F10",
        "description": "All dry-run modes are non-mutating, including an absent destination",
        "nodes": [
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_each_dry_run_mode_preserves_existing_tree_byte_for_byte",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_dry_run_does_not_create_a_nonexistent_destination",
        ],
        "granularity": "test",
    },
    {
        "id": "F11",
        "description": "Real runner child, competing owner, supervised stop/publish/restart handoff",
        "nodes": [
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_ownership_blocks_competing_live_writer",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_publish_holds_migration_ownership_during_promotion",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_writer_handoff_contract_preserves_pre_and_post_cutover_rows",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_real_supervised_runner_cutover_drains_publishes_and_restarts",
            "tests/storage/test_tick_lake_audit_migration_regressions.py::test_supervised_handoff_persists_publish_failure_and_restarts_capture",
        ],
        "granularity": "test",
    },
]


def collect_nodes(project_root: Path = PROJECT_ROOT, nodes_file: Optional[Path] = None) -> List[str]:
    """Return the collected pytest node IDs, optionally from a cached file."""
    if nodes_file is not None:
        return [line.strip() for line in Path(nodes_file).read_text(encoding="utf-8").splitlines() if line.strip()]

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "--collect-only", "-q", "-p", "no:cacheprovider"],
        cwd=str(project_root),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 and not proc.stdout:
        raise RuntimeError(f"pytest collection failed: {proc.stderr[-2000:]}")
    return [line.strip() for line in proc.stdout.splitlines() if line.strip().startswith("tests/")]


def _node_exists(node: str, nodes: Sequence[str]) -> bool:
    """A node exists if it is collected exactly, or is a module with collected tests.

    Parametrized tests are collected as ``node[param]``, so a bare node id is a
    valid mapping for all of its variants.
    """
    if any(n == node or n.startswith(node + "[") for n in nodes):
        return True
    if "::" not in node:
        return any(n.split("::")[0] == node for n in nodes)
    return False


def validate(nodes: Sequence[str]) -> Dict[str, object]:
    """Check every mapped node exists and report declared gaps."""
    missing: Dict[str, List[str]] = {}
    gaps: List[Dict[str, object]] = []
    by_granularity: Dict[str, int] = {}

    for req in REQUIREMENTS:
        rid = str(req["id"])
        gran = str(req.get("granularity", "module"))
        by_granularity[gran] = by_granularity.get(gran, 0) + 1
        mapped = list(req.get("nodes") or [])
        absent = [n for n in mapped if not _node_exists(n, nodes)]
        if absent:
            missing[rid] = absent
        if not mapped:
            gaps.append({"id": rid, "gap": req.get("gap", ""), "planned_phase": req.get("planned_phase")})

    return {
        "total_requirements": len(REQUIREMENTS),
        "mapped_requirements": len(REQUIREMENTS) - len(gaps),
        "declared_gaps": gaps,
        "missing_nodes": missing,
        "by_granularity": by_granularity,
        "collected_nodes": len(nodes),
        "ok": not missing,
    }


def render_markdown(report: Dict[str, object], nodes: Sequence[str]) -> str:
    lines = [
        "# Milestone v4.2 — Requirement Traceability Matrix",
        "",
        f"- Requirements tracked: **{report['total_requirements']}**",
        f"- Mapped to executable nodes: **{report['mapped_requirements']}**",
        f"- Declared coverage gaps: **{len(report['declared_gaps'])}**",
        f"- Collected pytest nodes: **{report['collected_nodes']}**",
        "",
        "Mapping granularity: `test` = a named test function; `module` = covered somewhere in that module",
        "(a weaker claim, labelled as such); `none` = declared gap, not passing evidence.",
        "",
        "| Requirement | Description | Nodes | Granularity | Status |",
        "|---|---|---|---|---|",
    ]
    for req in REQUIREMENTS:
        rid = str(req["id"])
        nodes_list = list(req.get("nodes") or [])
        if not nodes_list:
            status = "GAP (declared)"
            node_cell = "—"
        elif rid in report["missing_nodes"]:  # type: ignore[operator]
            status = "MISSING NODE"
            node_cell = ", ".join(f"`{n}`" for n in nodes_list)
        else:
            status = "Mapped"
            node_cell = ", ".join(f"`{n}`" for n in nodes_list)
        lines.append(
            f"| {rid} | {req['description']} | {node_cell} | {req.get('granularity')} | {status} |"
        )

    if report["declared_gaps"]:  # type: index
        lines += ["", "## Declared coverage gaps", ""]
        for gap in report["declared_gaps"]:  # type: index
            phase = gap.get("planned_phase")
            suffix = f" Planned: Phase {phase}." if phase else ""
            lines.append(f"- **{gap['id']}**: {gap['gap']}{suffix}")

    lines += ["", "---", "", "*Generated by `tools/requirement_traceability.py`.*"]
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="exit non-zero if any mapped node is missing")
    parser.add_argument("--markdown", metavar="PATH", help="write the matrix as markdown")
    parser.add_argument("--json", metavar="PATH", dest="json_path", help="write the raw report as JSON")
    parser.add_argument("--nodes-file", metavar="PATH", help="read collected node IDs from a file instead of running pytest")
    args = parser.parse_args(argv)

    nodes = collect_nodes(nodes_file=Path(args.nodes_file) if args.nodes_file else None)
    report = validate(nodes)

    if args.markdown:
        out = Path(args.markdown)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(render_markdown(report, nodes), encoding="utf-8")
        print(f"wrote {out}")
    if args.json_path:
        out = Path(args.json_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"wrote {out}")

    if not args.markdown and not args.json_path:
        print(json.dumps(report, indent=2))

    if args.check and not report["ok"]:
        print("ERROR: mapped nodes are missing from the collected suite", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
