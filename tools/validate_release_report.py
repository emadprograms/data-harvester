#!/usr/bin/env python3
"""
Release report validator for Milestone v4.2 (Q01 / EVID-03).

A gate may only be reported as PASS when it carries real evidence. This tool
enforces that mechanically, so a signoff cannot be assembled from missing
artifacts, stale code, empty metric lists, skipped required tests, or a green
label on a run that never selected a test.

Report schema (JSON):
{
  "candidate_sha": "<git sha the gates were measured on>",
  "gates": [
    {
      "id": "Q01",
      "title": "...",
      "status": "PASS|FAIL|BLOCKED|DEFERRED",
      "required": true,
      "code_sha": "<git sha>",
      "artifacts": [{"path": "docs/...", "sha256": "<optional hex digest>"}],
      "metrics": [{"name": "p95_ms", "value": 12.3, "unit": "ms"}],
      "tests": {
        "nodes": ["tests/x.py::test_y"],
        "total": 10, "passed": 10, "failed": 0,
        "skipped": 0, "deselected": 0
      }
    }
  ]
}

Usage:
    python tools/validate_release_report.py report.json
    python tools/validate_release_report.py report.json --json out.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

VALID_STATUSES = {"PASS", "FAIL", "BLOCKED", "DEFERRED"}


@dataclass
class Finding:
    gate: str
    code: str
    message: str


def _sha256(path: Path) -> Optional[str]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def validate_report(report: Dict[str, Any], project_root: Path) -> List[Finding]:
    """Return every evidence defect found in `report`. Empty list == acceptable."""
    findings: List[Finding] = []

    if not isinstance(report, dict):
        return [Finding("<report>", "invalid_report", "report must be a JSON object")]

    candidate_sha = report.get("candidate_sha")
    if not candidate_sha:
        findings.append(Finding("<report>", "missing_candidate_sha", "report has no candidate_sha"))

    gates = report.get("gates")
    if not isinstance(gates, list) or not gates:
        findings.append(Finding("<report>", "empty_gate_selection", "report contains no gates"))
        return findings

    seen_ids: set[str] = set()
    for gate in gates:
        gid = str(gate.get("id", "<unnamed>"))
        if gid in seen_ids:
            findings.append(Finding(gid, "duplicate_gate", "gate id appears more than once"))
        seen_ids.add(gid)

        status = str(gate.get("status", "")).upper()
        required = bool(gate.get("required", True))

        if status not in VALID_STATUSES:
            findings.append(
                Finding(gid, "invalid_status", f"status {status!r} is not one of {sorted(VALID_STATUSES)}")
            )
        elif required and status != "PASS":
            findings.append(
                Finding(gid, "nonpassing_status", f"required gate reported {status}, not PASS")
            )

        # Code identity: the measurement must come from the candidate under test.
        gate_sha = gate.get("code_sha")
        if candidate_sha and gate_sha and gate_sha != candidate_sha:
            findings.append(
                Finding(gid, "sha_mismatch", f"gate measured on {gate_sha}, candidate is {candidate_sha}")
            )
        elif candidate_sha and not gate_sha:
            findings.append(Finding(gid, "sha_mismatch", "gate does not record the code_sha it was measured on"))

        # Artifacts must exist (and match their recorded digest when given).
        for artifact in gate.get("artifacts") or []:
            rel = artifact.get("path")
            if not rel:
                findings.append(Finding(gid, "missing_artifact", "artifact entry has no path"))
                continue
            path = Path(rel)
            if not path.is_absolute():
                path = project_root / path
            if not path.exists():
                findings.append(Finding(gid, "missing_artifact", f"artifact not found: {rel}"))
                continue
            expected = artifact.get("sha256")
            if expected:
                actual = _sha256(path)
                if actual != expected:
                    findings.append(
                        Finding(gid, "artifact_checksum_mismatch", f"{rel}: expected {expected}, got {actual}")
                    )

        # Metrics: a passing gate must carry measured values, never empty or null.
        metrics = gate.get("metrics") or []
        if required and not metrics:
            findings.append(Finding(gid, "missing_metrics", "required gate has no metrics"))
        for metric in metrics:
            name = metric.get("name", "<unnamed>")
            if metric.get("value") is None:
                findings.append(
                    Finding(gid, "zero_measurements", f"metric {name} has no value (must not default to zero)")
                )

        # Tests: an empty selection, a failure, or a skipped required test cannot pass.
        tests = gate.get("tests") or {}
        nodes = tests.get("nodes") or []
        total = tests.get("total")
        if total is None:
            total = len(nodes)

        if required and total == 0:
            findings.append(Finding(gid, "empty_test_selection", "required gate selected no tests"))
        if required and not nodes and total > 0:
            findings.append(Finding(gid, "empty_test_selection", "required gate reports tests but names no nodes"))

        failed = tests.get("failed", 0) or 0
        if required and failed:
            findings.append(Finding(gid, "failed_tests", f"{failed} test(s) failed"))

        skipped = tests.get("skipped", 0) or 0
        if required and skipped:
            findings.append(
                Finding(gid, "skipped_required_test", f"{skipped} required test(s) were skipped")
            )

        if required and total and (tests.get("passed", 0) or 0) + failed + skipped > total:
            findings.append(
                Finding(gid, "inconsistent_counts", "passed + failed + skipped exceeds the reported total")
            )

    return findings


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("report", help="path to the release report JSON")
    parser.add_argument("--json", metavar="PATH", dest="json_path", help="write findings as JSON")
    parser.add_argument(
        "--allow-deferred",
        action="store_true",
        help="treat DEFERRED required gates as acceptable (still reported as findings)",
    )
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parent.parent
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    findings = validate_report(report, project_root)

    if args.allow_deferred:
        findings = [f for f in findings if f.code != "nonpassing_status"]

    payload = {"ok": not findings, "findings": [asdict(f) for f in findings]}
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if findings:
        print(f"REJECTED — {len(findings)} evidence defect(s):", file=sys.stderr)
        for f in findings:
            print(f"  [{f.gate}] {f.code}: {f.message}", file=sys.stderr)
        return 1

    print(f"ACCEPTED — {len(report.get('gates') or [])} gate(s) carry complete evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
