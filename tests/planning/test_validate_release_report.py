"""
Comprehensive mutation and regression tests for tools/validate_release_report.py.
Directly reproduces and resolves Finding C43-06 and Requirements VALD-02, VALD-03.
"""

import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

from tools.validate_release_report import (
    DEFAULT_REQUIRED_GATES,
    Finding,
    GateSpec,
    validate_report,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_SHA = "d11d967"
EXISTING_FILE = "tools/requirement_traceability.py"
EXISTING_NODE = "tests/planning/test_requirement_traceability.py::test_requirement_ids_are_unique"


def make_gate(**overrides) -> Dict[str, Any]:
    gate = {
        "id": "Q01",
        "title": "CI evidence and traceability",
        "status": "PASS",
        "required": True,
        "code_sha": CANDIDATE_SHA,
        "artifacts": [{"path": EXISTING_FILE}],
        "metrics": [{"name": "collected_nodes", "value": 771, "unit": "nodes"}],
        "tests": {
            "nodes": [EXISTING_NODE],
            "total": 8,
            "passed": 8,
            "failed": 0,
            "skipped": 0,
            "xfailed": 0,
            "deselected": 0,
        },
    }
    gate.update(overrides)
    return gate


def make_all_required_gates() -> List[Dict[str, Any]]:
    gates = []
    for gid in ("Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"):
        metric_name = "p95_ms" if gid == "Q03" else "collected_nodes"
        metric_val = 50.0 if gid == "Q03" else 771
        metric_unit = "ms" if gid == "Q03" else "nodes"
        gates.append(
            make_gate(
                id=gid,
                title=f"Gate {gid}",
                metrics=[{"name": metric_name, "value": metric_val, "unit": metric_unit}],
            )
        )
    return gates


def make_valid_report(**overrides) -> Dict[str, Any]:
    report = {
        "candidate_sha": CANDIDATE_SHA,
        "gates": make_all_required_gates(),
    }
    report.update(overrides)
    return report


def codes(findings: List[Finding]) -> List[str]:
    return [f.code for f in findings]


# ============================================================================
# C43-06 DIRECT REPRODUCTION REGRESSION
# ============================================================================

def test_reproduce_c43_06_direct_regression():
    """
    Direct reproduction of C43-06:
    A gate reported as PASS with empty artifacts, latency 0, 10 total tests,
    and 0 passed MUST NOT return an empty list of findings.
    It MUST be rejected with specific defect codes:
    - missing_artifact
    - nonpositive_latency
    - zero_tests_passed (and inconsistent_counts / incomplete_passed_count)
    """
    report = {
        "candidate_sha": CANDIDATE_SHA,
        "gates": [
            {
                "id": "Q01",
                "title": "C43-06 Vulnerability Reproduction Gate",
                "status": "PASS",
                "required": True,
                "code_sha": CANDIDATE_SHA,
                "artifacts": [],
                "metrics": [{"name": "p95_latency_ms", "value": 0, "unit": "ms"}],
                "tests": {
                    "nodes": [EXISTING_NODE],
                    "total": 10,
                    "passed": 0,
                    "failed": 0,
                    "skipped": 0,
                    "deselected": 0,
                },
            }
        ],
    }

    findings = validate_report(report, PROJECT_ROOT, required_inventory=["Q01"])
    finding_codes = codes(findings)

    assert "missing_artifact" in finding_codes, f"Allowed empty artifacts! Codes: {finding_codes}"
    assert "nonpositive_latency" in finding_codes, f"Allowed 0 latency! Codes: {finding_codes}"
    assert "zero_tests_passed" in finding_codes, f"Allowed 0 passed tests out of 10! Codes: {finding_codes}"
    assert "inconsistent_counts" in finding_codes, f"Allowed inconsistent test counts! Codes: {finding_codes}"


# ============================================================================
# TABLE-DRIVEN MUTATION TESTS (VALD-02 & VALD-03)
# ============================================================================

MUTATION_CASES = [
    # (case_name, mutation_func, expected_error_code)
    (
        "deleted_required_gate",
        lambda r: r.update({"gates": [g for g in r["gates"] if g["id"] != "Q02"]}),
        "missing_required_gate",
    ),
    (
        "empty_artifacts",
        lambda r: r["gates"][0].update({"artifacts": []}),
        "missing_artifact",
    ),
    (
        "directory_instead_of_artifact",
        lambda r: r["gates"][0].update({"artifacts": [{"path": "tools"}]}),
        "artifact_is_directory",
    ),
    (
        "wrong_checksum",
        lambda r: r["gates"][0].update({"artifacts": [{"path": EXISTING_FILE, "sha256": "0" * 64}]}),
        "artifact_checksum_mismatch",
    ),
    (
        "stale_candidate_sha",
        lambda r: r["gates"][0].update({"code_sha": "a1b2c3d4e5"}),
        "sha_mismatch",
    ),
    (
        "invalid_test_node_format",
        lambda r: r["gates"][0]["tests"].update({"nodes": ["invalid_node_string_no_separator"]}),
        "invalid_test_node",
    ),
    (
        "zero_latency",
        lambda r: r["gates"][2].update({"metrics": [{"name": "p95_ms", "value": 0.0, "unit": "ms"}]}),
        "nonpositive_latency",
    ),
    (
        "negative_latency",
        lambda r: r["gates"][2].update({"metrics": [{"name": "p95_ms", "value": -12.5, "unit": "ms"}]}),
        "nonpositive_latency",
    ),
    (
        "nonfinite_latency_infinity",
        lambda r: r["gates"][2].update({"metrics": [{"name": "p95_ms", "value": float("inf"), "unit": "ms"}]}),
        "nonfinite_metric",
    ),
    (
        "nonfinite_latency_nan",
        lambda r: r["gates"][2].update({"metrics": [{"name": "p95_ms", "value": float("nan"), "unit": "ms"}]}),
        "nonfinite_metric",
    ),
    (
        "zero_sample_count",
        lambda r: r["gates"][0].update({"metrics": [{"name": "sample_count", "value": 0}]}),
        "nonpositive_samples",
    ),
    (
        "incomplete_counts",
        lambda r: r["gates"][0]["tests"].update({"total": 10, "passed": 8, "failed": 0, "skipped": 0, "xfailed": 0}),
        "inconsistent_counts",
    ),
    (
        "required_xfail",
        lambda r: r["gates"][0]["tests"].update({"total": 8, "passed": 7, "xfailed": 1, "failed": 0, "skipped": 0}),
        "xfailed_required_test",
    ),
    (
        "required_skip",
        lambda r: r["gates"][0]["tests"].update({"total": 8, "passed": 7, "skipped": 1, "failed": 0, "xfailed": 0}),
        "skipped_required_test",
    ),
    (
        "missing_metrics",
        lambda r: r["gates"][0].update({"metrics": []}),
        "missing_metrics",
    ),
    (
        "missing_required_metric",
        lambda r: r["gates"][0].update({"metrics": [{"name": "unrelated_metric", "value": 123}]}),
        "missing_required_metric",
    ),
    (
        "exceeded_threshold",
        lambda r: r["gates"][2].update({"metrics": [{"name": "p95_ms", "value": 999.0, "unit": "ms"}]}),
        "threshold_exceeded",
    ),
    (
        "malformed_gate_shape",
        lambda r: r["gates"].append("not_a_valid_gate_dictionary"),
        "malformed_gate_shape",
    ),
    (
        "duplicate_gate_id",
        lambda r: r["gates"].append(make_gate(id="Q01")),
        "duplicate_gate",
    ),
    (
        "mark_failed_gate_optional",
        lambda r: r["gates"][0].update({"required": False, "status": "FAIL"}),
        "unauthorized_optional_gate",
    ),
]


@pytest.mark.parametrize("case_name,mutator,expected_error_code", MUTATION_CASES)
def test_table_driven_report_mutations(case_name, mutator, expected_error_code):
    """VALD-03: Every defect mutation must fail closed and report the specific finding code."""
    report = make_valid_report()
    mutator(report)

    findings = validate_report(report, PROJECT_ROOT)
    finding_codes = codes(findings)

    assert expected_error_code in finding_codes, (
        f"Mutation '{case_name}' did not produce expected code '{expected_error_code}'. Produced: {finding_codes}"
    )


# ============================================================================
# CLI REJECTION & WAIVER TESTS
# ============================================================================

def test_cli_waiver_on_failed_gate_rejected(tmp_path):
    """VALD-02: --allow-deferred MUST NOT waive FAIL gates or produce release approval."""
    report = make_valid_report()
    report["gates"][0].update({"status": "FAIL"})

    path = tmp_path / "failed_report.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path), "--allow-deferred"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1, f"Expected returncode 1, got {proc.returncode}. Output:\n{proc.stdout}\n{proc.stderr}"
    assert "cannot be waived with --allow-deferred" in proc.stderr
    assert "failed_gate" in proc.stderr
    assert "ACCEPTED" not in proc.stdout


def test_cli_waiver_on_blocked_gate_rejected(tmp_path):
    """VALD-02: --allow-deferred MUST NOT waive BLOCKED gates or produce release approval."""
    report = make_valid_report()
    report["gates"][0].update({"status": "BLOCKED"})

    path = tmp_path / "blocked_report.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path), "--allow-deferred"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1
    assert "cannot be waived with --allow-deferred" in proc.stderr
    assert "blocked_gate" in proc.stderr
    assert "ACCEPTED" not in proc.stdout


def test_cli_waiver_labels_output_as_draft_and_never_emits_approval(tmp_path):
    """VALD-02: Draft reports under --allow-deferred must be explicitly labelled draft/incomplete."""
    report = make_valid_report()
    # Mark gate Q04 as DEFERRED
    for g in report["gates"]:
        if g["id"] == "Q04":
            g["status"] = "DEFERRED"

    path = tmp_path / "draft_report.json"
    out_json = tmp_path / "draft_out.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path), "--allow-deferred", "--json", str(out_json)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    assert "DRAFT / INCOMPLETE" in proc.stderr
    assert "Release approval is NOT granted" in proc.stderr
    assert "ACCEPTED" not in proc.stdout

    # Verify JSON structure
    out_data = json.loads(out_json.read_text(encoding="utf-8"))
    assert out_data["draft"] is True
    assert out_data["release_approved"] is False
    assert out_data["ok"] is False
    assert "Q04" in out_data["deferred_gates"]


def test_positive_report_acceptance(tmp_path):
    """VALD-02: Valid report with all required gates, positive latencies, and valid artifacts passes unconditionally."""
    report = make_valid_report()
    path = tmp_path / "valid_report.json"
    out_json = tmp_path / "out.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path), "--json", str(out_json)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"Validator failed on valid report:\n{proc.stderr}"
    assert "ACCEPTED — 9 gate(s) carry complete evidence" in proc.stdout

    out_data = json.loads(out_json.read_text(encoding="utf-8"))
    assert out_data["ok"] is True
    assert out_data["release_approved"] is True
    assert out_data["draft"] is False
    assert out_data["findings"] == []
