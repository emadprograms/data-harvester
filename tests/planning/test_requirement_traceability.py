"""
Tests for the milestone v4.2 requirement traceability map (Q01 / EVID-02).

These tests pin the *map itself*, not just the tool: every archived requirement
must either name an executable node that really exists in the collected suite,
or declare a documented gap. That prevents the two failure modes this milestone
exists to catch — a requirement silently unmapped, and a mapping that points at
a test which no longer exists.
"""

from pathlib import Path

import pytest

from tools.requirement_traceability import REQUIREMENTS, collect_nodes, validate

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def collected_nodes():
    return collect_nodes(PROJECT_ROOT)


@pytest.fixture(scope="module")
def report(collected_nodes):
    return validate(collected_nodes)


def test_every_mapped_node_exists_in_collected_suite(report):
    assert report["missing_nodes"] == {}, f"mapped nodes not found: {report['missing_nodes']}"


def test_requirement_ids_are_unique():
    ids = [str(r["id"]) for r in REQUIREMENTS]
    assert len(ids) == len(set(ids)), "duplicate requirement IDs in the traceability map"


def test_all_audit_findings_are_tracked():
    expected = {f"F{n:02d}" for n in range(1, 12)}
    tracked = {str(r["id"]) for r in REQUIREMENTS}
    assert expected <= tracked, f"untracked audit findings: {sorted(expected - tracked)}"


def test_all_v41_phase_requirements_are_tracked():
    """TEST-P22-01..03 through TEST-P27-01..02 (P27 was specified with two items)."""
    expected = {f"TEST-P{phase}-{n:02d}" for phase in range(22, 27) for n in (1, 2, 3)}
    expected |= {"TEST-P27-01", "TEST-P27-02"}
    tracked = {str(r["id"]) for r in REQUIREMENTS}
    assert expected <= tracked, f"untracked v4.1 phase requirements: {sorted(expected - tracked)}"


def test_all_v40_lake_requirements_are_tracked():
    lake_ids = {str(r["id"]) for r in REQUIREMENTS if str(r["id"]).startswith("LAKE-")}
    assert len(lake_ids) == 23, f"expected 23 v4.0 LAKE requirements, found {len(lake_ids)}"


def test_declared_gaps_documented_and_not_empty(report):
    """A gap may exist, but it must be explained — never a silent omission."""
    for gap in report["declared_gaps"]:
        assert gap["gap"], f"gap {gap['id']} has no explanation"


def test_module_level_mappings_reference_existing_files():
    for req in REQUIREMENTS:
        for node in req.get("nodes") or []:
            path = PROJECT_ROOT / node.split("::")[0]
            assert path.exists(), f"{req['id']} maps to missing file {node}"


def test_test_level_mappings_use_a_file_and_function():
    for req in REQUIREMENTS:
        if req.get("granularity") != "test":
            continue
        for node in req.get("nodes") or []:
            assert "::" in node, f"{req['id']} claims test granularity but names no test: {node}"
