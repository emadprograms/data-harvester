#!/usr/bin/env python3
"""
Release report validator for Milestone v4.3 (VALD-02, VALD-03 / C43-06).

A gate may only be reported as PASS when it carries real, fail-closed evidence:
- Authoritative required-gate inventory across functional, performance, and operational gates.
- Enforces report structure, candidate SHA identity, and gate uniqueness.
- Enforces artifact existence, regular file type (not directories), non-zero size, and SHA-256 digests.
- Enforces finite positive latencies, positive sample counts, and threshold bounds.
- Enforces test count conservation, JUnit XML verification, and zero failed/skipped/xfailed on required gates.
- Strictly disallows waiving required FAIL or BLOCKED gates under --allow-deferred.
- Draft reports under --allow-deferred are explicitly labelled draft/incomplete and never emit release approval.

Usage:
    python tools/validate_release_report.py report.json
    python tools/validate_release_report.py report.json --json out.json
    python tools/validate_release_report.py report.json --allow-deferred
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Union

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.write_guard import assert_safe_write_path

VALID_STATUSES = {"PASS", "FAIL", "BLOCKED", "DEFERRED"}


@dataclass(frozen=True)
class MetricSpec:
    name: str
    unit: Optional[str] = None
    max_value: Optional[float] = None
    min_value: Optional[float] = None
    comparator: Optional[str] = None  # e.g. "<=", ">="
    is_latency: bool = False
    is_sample_count: bool = False


@dataclass(frozen=True)
class GateSpec:
    id: str
    title: str
    gate_type: str  # "functional", "performance", "operational", "characterization"
    required: bool = True
    require_artifacts: bool = True
    require_metrics: bool = True
    require_tests: bool = True
    metrics: Dict[str, MetricSpec] = field(default_factory=dict)


# Authoritative inventory of required gates across functional, performance, and operational categories
DEFAULT_REQUIRED_GATES: Dict[str, GateSpec] = {
    "Q01": GateSpec(
        id="Q01",
        title="CI evidence and requirement traceability",
        gate_type="functional",
        required=True,
        metrics={
            "collected_nodes": MetricSpec(name="collected_nodes", unit="nodes", min_value=1, comparator=">="),
        },
    ),
    "Q02": GateSpec(
        id="Q02",
        title="Test isolation and fixture determinism",
        gate_type="functional",
        required=True,
    ),
    "Q03": GateSpec(
        id="Q03",
        title="Production-scale lake benchmarks",
        gate_type="performance",
        required=True,
        metrics={
            "p95_ms": MetricSpec(name="p95_ms", unit="ms", max_value=250.0, comparator="<=", is_latency=True),
        },
    ),
    "Q04": GateSpec(
        id="Q04",
        title="Sustained multi-process endurance run",
        gate_type="operational",
        required=True,
    ),
    "Q05": GateSpec(
        id="Q05",
        title="Durability boundary and crash recovery",
        gate_type="operational",
        required=True,
    ),
    "Q06": GateSpec(
        id="Q06",
        title="Reader contract and consumer integration",
        gate_type="functional",
        required=True,
    ),
    "Q07": GateSpec(
        id="Q07",
        title="Historical migration and restore rehearsal",
        gate_type="functional",
        required=True,
    ),
    "Q08": GateSpec(
        id="Q08",
        title="Capacity controls and maintenance boundaries",
        gate_type="operational",
        required=True,
    ),
    "Q09": GateSpec(
        id="Q09",
        title="Documentation and contract verification",
        gate_type="functional",
        required=True,
    ),
}


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


def _is_latency_metric(name: str, unit: Optional[str]) -> bool:
    lowered = name.lower()
    latency_keywords = ("latency", "lag", "duration", "time_ms", "cpu_seconds", "p50", "p90", "p95", "p99")
    if any(k in lowered for k in latency_keywords):
        return True
    if unit and unit.lower() in ("ms", "s", "sec", "seconds", "us", "ns"):
        return True
    return False


def _is_sample_count_metric(name: str) -> bool:
    lowered = name.lower()
    sample_keywords = ("sample", "samples", "count", "iterations", "total_rows")
    return any(k in lowered for k in sample_keywords)


def _validate_junit_xml(path: Path, gid: str) -> List[Finding]:
    findings = []
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        # Find all testsuites / testcase elements
        failures = int(root.attrib.get("failures", 0))
        errors = int(root.attrib.get("errors", 0))
        for suite in root.iter("testsuite"):
            failures += int(suite.attrib.get("failures", 0))
            errors += int(suite.attrib.get("errors", 0))
        if failures > 0 or errors > 0:
            findings.append(
                Finding(gid, "failed_tests", f"JUnit XML artifact {path.name} contains {failures} failures, {errors} errors")
            )
    except ET.ParseError as err:
        findings.append(Finding(gid, "invalid_junit_xml", f"artifact {path.name} is malformed XML: {err}"))
    except Exception as err:
        findings.append(Finding(gid, "invalid_junit_xml", f"artifact {path.name} cannot be parsed: {err}"))
    return findings


def validate_report(
    report: Dict[str, Any],
    project_root: Path,
    required_inventory: Optional[Union[Dict[str, GateSpec], Sequence[str], Set[str]]] = None,
) -> List[Finding]:
    """
    Validate release report evidence against authoritative requirements.
    Returns every evidence defect found. An empty list indicates complete, passing evidence.
    """
    findings: List[Finding] = []

    if not isinstance(report, dict):
        return [Finding("<report>", "invalid_report", "report must be a JSON object")]

    # Candidate SHA identity
    candidate_sha = report.get("candidate_sha")
    if not candidate_sha or not isinstance(candidate_sha, str):
        findings.append(Finding("<report>", "missing_candidate_sha", "report has no candidate_sha"))
    elif not re.fullmatch(r"[0-9a-fA-F]{7,40}", candidate_sha.strip()):
        findings.append(
            Finding("<report>", "invalid_candidate_sha", f"candidate_sha {candidate_sha!r} is not a valid hex commit SHA")
        )

    # Resolve required gates inventory
    inventory: Dict[str, GateSpec]
    if required_inventory is None:
        inventory = DEFAULT_REQUIRED_GATES
    elif isinstance(required_inventory, dict):
        inventory = required_inventory
    else:
        # Sequence or set of gate IDs
        inventory = {
            gid: DEFAULT_REQUIRED_GATES.get(
                gid,
                GateSpec(id=gid, title=f"Gate {gid}", gate_type="functional", required=True),
            )
            for gid in required_inventory
        }

    gates = report.get("gates")
    if not isinstance(gates, list) or not gates:
        findings.append(Finding("<report>", "empty_gate_selection", "report contains no gates"))
        return findings

    seen_ids: Set[str] = set()
    report_gates_by_id: Dict[str, Dict[str, Any]] = {}

    for gate in gates:
        if not isinstance(gate, dict):
            findings.append(Finding("<report>", "malformed_gate_shape", f"gate entry must be a JSON object: {gate!r}"))
            continue

        gid = str(gate.get("id", "<unnamed>"))
        if gid in seen_ids:
            findings.append(Finding(gid, "duplicate_gate", f"gate id appears more than once: {gid}"))
        seen_ids.add(gid)
        report_gates_by_id[gid] = gate

        spec = inventory.get(gid)
        status = str(gate.get("status", "")).upper()
        reported_required = gate.get("required")

        # Authoritative required gate check: cannot be marked optional
        is_authoritatively_required = spec.required if spec else bool(reported_required)
        if spec and spec.required and reported_required is False:
            findings.append(
                Finding(gid, "unauthorized_optional_gate", f"gate {gid} is required by authoritative inventory but marked optional")
            )

        required = is_authoritatively_required

        # Status validation
        if status not in VALID_STATUSES:
            findings.append(
                Finding(gid, "invalid_status", f"status {status!r} is not one of {sorted(VALID_STATUSES)}")
            )
        elif required and status == "FAIL":
            findings.append(Finding(gid, "failed_gate", "required gate reported FAIL"))
            findings.append(Finding(gid, "nonpassing_status", "required gate reported FAIL, not PASS"))
        elif required and status == "BLOCKED":
            findings.append(Finding(gid, "blocked_gate", "required gate reported BLOCKED"))
            findings.append(Finding(gid, "nonpassing_status", "required gate reported BLOCKED, not PASS"))
        elif required and status == "DEFERRED":
            findings.append(Finding(gid, "deferred_required_gate", "required gate reported DEFERRED"))
            findings.append(Finding(gid, "nonpassing_status", "required gate reported DEFERRED, not PASS"))

        # Code identity: the measurement must come from the candidate under test
        gate_sha = gate.get("code_sha")
        if candidate_sha and gate_sha and gate_sha != candidate_sha:
            findings.append(
                Finding(gid, "sha_mismatch", f"gate measured on {gate_sha}, candidate is {candidate_sha}")
            )
        elif candidate_sha and not gate_sha:
            findings.append(Finding(gid, "sha_mismatch", "gate does not record the code_sha it was measured on"))

        # Artifacts validation
        artifacts = gate.get("artifacts")
        if required or status == "PASS":
            if not isinstance(artifacts, list) or len(artifacts) == 0:
                findings.append(Finding(gid, "missing_artifact", f"gate {gid} carries no artifacts"))
                artifacts = []

        if isinstance(artifacts, list):
            for artifact in artifacts:
                if not isinstance(artifact, dict):
                    findings.append(Finding(gid, "missing_artifact", "artifact entry is not an object"))
                    continue
                rel = artifact.get("path")
                if not rel or not isinstance(rel, str):
                    findings.append(Finding(gid, "missing_artifact", "artifact entry has no path"))
                    continue

                path = Path(rel)
                if not path.is_absolute():
                    path = project_root / path

                if not path.exists():
                    findings.append(Finding(gid, "missing_artifact", f"artifact not found: {rel}"))
                    continue

                if path.is_dir():
                    findings.append(Finding(gid, "artifact_is_directory", f"artifact path is a directory, not a file: {rel}"))
                    continue

                try:
                    if path.stat().st_size == 0:
                        findings.append(Finding(gid, "empty_artifact", f"artifact is 0 bytes: {rel}"))
                        continue
                except OSError as err:
                    findings.append(Finding(gid, "missing_artifact", f"unable to inspect artifact {rel}: {err}"))
                    continue

                expected = artifact.get("sha256")
                if expected:
                    actual = _sha256(path)
                    if actual != expected:
                        findings.append(
                            Finding(gid, "artifact_checksum_mismatch", f"{rel}: expected {expected}, got {actual}")
                        )

                # If artifact is a JUnit XML, validate outcomes
                if path.suffix.lower() == ".xml" or artifact.get("type") == "junit":
                    findings.extend(_validate_junit_xml(path, gid))

        # Metrics validation
        metrics = gate.get("metrics")
        if required or status == "PASS":
            if not isinstance(metrics, list) or len(metrics) == 0:
                findings.append(Finding(gid, "missing_metrics", f"gate {gid} carries no metrics"))
                metrics = []

        if isinstance(metrics, list):
            metrics_by_name: Dict[str, Any] = {}
            for metric in metrics:
                if not isinstance(metric, dict):
                    findings.append(Finding(gid, "missing_metrics", "metric entry is not an object"))
                    continue
                name = str(metric.get("name", "<unnamed>"))
                val = metric.get("value")
                unit = metric.get("unit")
                metrics_by_name[name] = val

                if val is None:
                    findings.append(
                        Finding(gid, "zero_measurements", f"metric {name} has no value (must not default to null)")
                    )
                    continue

                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    findings.append(
                        Finding(gid, "invalid_metric_value", f"metric {name} value {val!r} is not a valid number")
                    )
                    continue

                if math.isnan(val) or math.isinf(val):
                    findings.append(
                        Finding(gid, "nonfinite_metric", f"metric {name} has non-finite value {val}")
                    )
                    continue

                # Positive finite latency and duration validation
                if _is_latency_metric(name, unit):
                    if val <= 0:
                        findings.append(
                            Finding(gid, "nonpositive_latency", f"metric {name} latency/duration {val} must be strictly positive")
                        )

                # Positive sample count validation
                if _is_sample_count_metric(name):
                    if val <= 0:
                        findings.append(
                            Finding(gid, "nonpositive_samples", f"metric {name} sample/count {val} must be strictly positive")
                        )

            # Check required metrics from spec
            if spec and spec.metrics:
                for req_m_name, req_spec in spec.metrics.items():
                    if req_m_name not in metrics_by_name:
                        findings.append(
                            Finding(gid, "missing_required_metric", f"required metric {req_m_name} missing from gate {gid}")
                        )
                    else:
                        m_val = metrics_by_name[req_m_name]
                        if m_val is not None and isinstance(m_val, (int, float)) and not math.isnan(m_val) and not math.isinf(m_val):
                            if req_spec.max_value is not None and m_val > req_spec.max_value:
                                findings.append(
                                    Finding(
                                        gid,
                                        "threshold_exceeded",
                                        f"metric {req_m_name} ({m_val}) exceeds target maximum {req_spec.max_value}",
                                    )
                                )
                            if req_spec.min_value is not None and m_val < req_spec.min_value:
                                findings.append(
                                    Finding(
                                        gid,
                                        "threshold_exceeded",
                                        f"metric {req_m_name} ({m_val}) below target minimum {req_spec.min_value}",
                                    )
                                )

        # Tests & Count conservation validation
        tests = gate.get("tests")
        if required or status == "PASS":
            if not isinstance(tests, dict) or not tests:
                findings.append(Finding(gid, "empty_test_selection", f"gate {gid} carries no test results"))
                tests = {}

        if isinstance(tests, dict) and tests:
            nodes = tests.get("nodes") or []
            if not isinstance(nodes, list):
                findings.append(Finding(gid, "empty_test_selection", "tests.nodes must be a list"))
                nodes = []

            for node in nodes:
                if not isinstance(node, str) or not node.strip():
                    findings.append(Finding(gid, "invalid_test_node", f"invalid test node entry: {node!r}"))
                elif "::" not in node and not node.endswith(".py"):
                    findings.append(Finding(gid, "invalid_test_node", f"test node {node!r} lacks file or test separator"))

            total = tests.get("total")
            if total is None:
                total = len(nodes)

            passed = tests.get("passed", 0) or 0
            failed = tests.get("failed", 0) or 0
            skipped = tests.get("skipped", 0) or 0
            xfailed = tests.get("xfailed", 0) or 0

            if (required or status == "PASS") and total <= 0:
                findings.append(Finding(gid, "empty_test_selection", f"gate {gid} selected 0 tests"))
            if (required or status == "PASS") and not nodes and total > 0:
                findings.append(Finding(gid, "empty_test_selection", f"gate {gid} reports tests but names no nodes"))

            # Count conservation: total == passed + failed + skipped + xfailed
            if total is not None and (passed + failed + skipped + xfailed != total):
                findings.append(
                    Finding(
                        gid,
                        "inconsistent_counts",
                        f"test count conservation failure: passed({passed}) + failed({failed}) + skipped({skipped}) + xfailed({xfailed}) != total({total})",
                    )
                )

            # PASS and required gate requirements
            if required or status == "PASS":
                if failed > 0:
                    findings.append(Finding(gid, "failed_tests", f"{failed} test(s) failed"))
                if skipped > 0:
                    findings.append(Finding(gid, "skipped_required_test", f"{skipped} required test(s) were skipped"))
                if xfailed > 0:
                    findings.append(Finding(gid, "xfailed_required_test", f"{xfailed} required test(s) marked xfail"))
                if status == "PASS" and passed <= 0:
                    findings.append(Finding(gid, "zero_tests_passed", f"gate {gid} reported PASS but 0 tests passed"))
                elif status == "PASS" and total is not None and passed != total:
                    findings.append(Finding(gid, "incomplete_passed_count", f"gate {gid} reported PASS but only {passed}/{total} passed"))

    # Check for missing required gates from authoritative inventory
    for req_gid, req_spec in inventory.items():
        if req_spec.required and req_gid not in seen_ids:
            findings.append(
                Finding(req_gid, "missing_required_gate", f"required gate {req_gid} ({req_spec.title}) is missing from report")
            )

    return findings


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("report", help="path to the release report JSON")
    parser.add_argument("--json", metavar="PATH", dest="json_path", help="write findings as JSON")
    parser.add_argument(
        "--allow-deferred",
        action="store_true",
        help="allow DEFERRED required gates for draft reports (never waives FAIL/BLOCKED or emits release approval)",
    )
    parser.add_argument(
        "--required-gates",
        metavar="GATES",
        help="comma-separated gate IDs or path to custom required gates JSON inventory",
    )
    args = parser.parse_args(argv)

    project_root = REPO_ROOT

    # Write guard on output json if specified
    if args.json_path:
        assert_safe_write_path(args.json_path, operation="report json output")

    report_path = Path(args.report)
    if not report_path.is_file():
        print(f"ERROR: Report file not found: {args.report}", file=sys.stderr)
        return 1

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        print(f"REJECTED — Malformed JSON in report {args.report}: {err}", file=sys.stderr)
        return 1

    # Load custom inventory if specified
    custom_inventory: Optional[Dict[str, GateSpec]] = None
    if args.required_gates:
        if Path(args.required_gates).is_file():
            inv_data = json.loads(Path(args.required_gates).read_text(encoding="utf-8"))
            custom_inventory = {
                k: GateSpec(
                    id=k,
                    title=v.get("title", k),
                    gate_type=v.get("gate_type", "functional"),
                    required=v.get("required", True),
                )
                for k, v in inv_data.items()
            }
        else:
            custom_inventory = {
                gid.strip(): DEFAULT_REQUIRED_GATES.get(
                    gid.strip(),
                    GateSpec(id=gid.strip(), title=f"Gate {gid.strip()}", gate_type="functional", required=True),
                )
                for gid in args.required_gates.split(",")
                if gid.strip()
            }

    findings = validate_report(report, project_root, required_inventory=custom_inventory)

    deferred_findings = [
        f for f in findings if f.code == "deferred_required_gate" or (f.code == "nonpassing_status" and "DEFERRED" in f.message)
    ]
    hard_findings = [f for f in findings if f not in deferred_findings]

    if args.allow_deferred:
        # If there are hard defects (FAIL, BLOCKED, missing artifacts, checksum mismatches, etc.), fail immediately!
        if hard_findings:
            print(f"REJECTED — {len(hard_findings)} defect(s) cannot be waived with --allow-deferred:", file=sys.stderr)
            for f in hard_findings:
                print(f"  [{f.gate}] {f.code}: {f.message}", file=sys.stderr)
            if args.json_path:
                payload = {
                    "ok": False,
                    "release_approved": False,
                    "draft": True,
                    "findings": [asdict(f) for f in findings],
                }
                Path(args.json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            return 1

        # Only deferred gates exist: generate draft output, but NEVER release approval
        print(
            f"DRAFT / INCOMPLETE — {len(deferred_findings)} required gate(s) deferred. Release approval is NOT granted.",
            file=sys.stderr,
        )
        if args.json_path:
            payload = {
                "ok": False,
                "release_approved": False,
                "draft": True,
                "deferred_gates": [f.gate for f in deferred_findings],
                "findings": [asdict(f) for f in deferred_findings],
            }
            Path(args.json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return 0

    payload = {
        "ok": not findings,
        "release_approved": not findings,
        "draft": False,
        "findings": [asdict(f) for f in findings],
    }
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
