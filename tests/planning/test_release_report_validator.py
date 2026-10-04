"""
Unit tests for tools/validate_release_report.py (Q01 / EVID-03).

Each rejection rule has its own test so a future change cannot quietly drop one.
The point of the validator is that a gate cannot be reported PASS on hope: no
artifact, a stale SHA, an empty metric list, a null measurement, a skipped
required test, or a test selection that collected nothing must all be refused.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools.validate_release_report import validate_report

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_SHA = "d11d967"
EXISTING_FILE = "tools/requirement_traceability.py"
EXISTING_NODE = "tests/planning/test_requirement_traceability.py::test_requirement_ids_are_unique"


def make_gate(**overrides):
    gate = {
        "id": "Q01",
        "title": "CI evidence and traceability",
        "status": "PASS",
        "required": True,
        "code_sha": CANDIDATE_SHA,
        "artifacts": [{"path": EXISTING_FILE}],
        "metrics": [{"name": "collected_nodes", "value": 771}],
        "tests": {
            "nodes": [EXISTING_NODE],
            "total": 8,
            "passed": 8,
            "failed": 0,
            "skipped": 0,
            "deselected": 0,
        },
    }
    gate.update(overrides)
    return gate


def make_report(**overrides):
    report = {"candidate_sha": CANDIDATE_SHA, "gates": [make_gate()]}
    report.update(overrides)
    return report


def codes(findings):
    return [f.code for f in findings]


def test_valid_report_is_accepted():
    assert validate_report(make_report(), PROJECT_ROOT) == []


def test_rejects_missing_artifact():
    report = make_report(gates=[make_gate(artifacts=[{"path": "docs/plans/does-not-exist.md"}])])
    assert "missing_artifact" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_artifact_without_path():
    report = make_report(gates=[make_gate(artifacts=[{}])])
    assert "missing_artifact" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_artifact_checksum_mismatch(tmp_path):
    target = tmp_path / "artifact.json"
    target.write_text('{"ok": true}', encoding="utf-8")
    report = make_report(gates=[make_gate(artifacts=[{"path": str(target), "sha256": "0" * 64}])])
    assert "artifact_checksum_mismatch" in codes(validate_report(report, PROJECT_ROOT))


def test_accepts_artifact_with_matching_checksum(tmp_path):
    import hashlib

    target = tmp_path / "artifact.json"
    target.write_text('{"ok": true}', encoding="utf-8")
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    report = make_report(gates=[make_gate(artifacts=[{"path": str(target), "sha256": digest}])])
    assert validate_report(report, PROJECT_ROOT) == []


def test_rejects_code_sha_mismatch():
    report = make_report(gates=[make_gate(code_sha="0000000")])
    assert "sha_mismatch" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_gate_without_code_sha():
    gate = make_gate()
    gate.pop("code_sha")
    report = make_report(gates=[gate])
    assert "sha_mismatch" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_missing_metrics():
    report = make_report(gates=[make_gate(metrics=[])])
    assert "missing_metrics" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_null_measurement_rather_than_treating_it_as_zero():
    report = make_report(gates=[make_gate(metrics=[{"name": "p95_ms", "value": None}])])
    assert "zero_measurements" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_skipped_required_test():
    tests = make_gate()["tests"] | {"skipped": 2, "total": 10, "passed": 8}
    report = make_report(gates=[make_gate(tests=tests)])
    assert "skipped_required_test" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_failed_tests():
    tests = make_gate()["tests"] | {"failed": 1, "total": 9, "passed": 8}
    report = make_report(gates=[make_gate(tests=tests)])
    assert "failed_tests" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_nonpassing_status():
    report = make_report(gates=[make_gate(status="FAIL")])
    assert "nonpassing_status" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_deferred_required_gate():
    report = make_report(gates=[make_gate(status="DEFERRED")])
    assert "nonpassing_status" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_empty_test_selection():
    report = make_report(gates=[make_gate(tests={"nodes": [], "total": 0, "passed": 0})])
    assert "empty_test_selection" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_empty_gate_selection():
    report = make_report(gates=[])
    assert "empty_gate_selection" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_invalid_status_value():
    report = make_report(gates=[make_gate(status="PASSED")])
    assert "invalid_status" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_duplicate_gate_ids():
    report = make_report(gates=[make_gate(), make_gate()])
    assert "duplicate_gate" in codes(validate_report(report, PROJECT_ROOT))


def test_rejects_inconsistent_counts():
    tests = make_gate()["tests"] | {"total": 5, "passed": 8}
    report = make_report(gates=[make_gate(tests=tests)])
    assert "inconsistent_counts" in codes(validate_report(report, PROJECT_ROOT))


def test_non_required_gate_may_be_deferred():
    report = make_report(gates=[make_gate(status="DEFERRED", required=False)])
    assert validate_report(report, PROJECT_ROOT) == []


def test_cli_accepts_valid_report(tmp_path):
    path = tmp_path / "report.json"
    path.write_text(json.dumps(make_report()), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr


def test_cli_rejects_report_with_gaps(tmp_path):
    path = tmp_path / "report.json"
    path.write_text(json.dumps(make_report(gates=[make_gate(metrics=[])])), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, "tools/validate_release_report.py", str(path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1
    assert "missing_metrics" in proc.stderr
