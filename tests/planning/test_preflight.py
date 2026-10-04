"""
Unit and integration tests for tools/preflight.py (VALD-04).

Verifies that:
1. Candidate SHA, branch, and clean/dirty tree status are accurately detected.
2. Dependency versions and hardware metrics are collected.
3. Explicit capability probes (local socket binding, subprocess lifecycle, process metrics) pass.
4. Production write guards reject writing preflight reports to protected paths.
5. CLI execution succeeds and outputs valid JSON.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools.preflight import (
    get_dependency_versions,
    get_git_info,
    probe_local_socket_binding,
    probe_process_metrics,
    probe_subprocess_lifecycle,
    run_preflight,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_git_info_collects_sha_and_branch():
    info = get_git_info(PROJECT_ROOT)
    assert info["candidate_sha"] != "unknown"
    assert len(info["candidate_sha"]) >= 7
    assert info["branch"] != "unknown"
    assert isinstance(info["is_dirty"], bool)


def test_dependency_versions_collects_installed_packages():
    versions = get_dependency_versions()
    assert "duckdb" in versions
    assert "pyarrow" in versions
    assert "psutil" in versions
    assert "pytest" in versions
    for pkg, ver in versions.items():
        assert ver != "not installed", f"Package {pkg} was not found installed"


def test_probe_local_socket_binding():
    res = probe_local_socket_binding()
    assert res.passed is True
    assert "Successfully bound local socket" in res.details
    assert res.metadata and res.metadata.get("port", 0) > 0


def test_probe_subprocess_lifecycle():
    res = probe_subprocess_lifecycle()
    assert res.passed is True
    assert "verified" in res.details.lower()


def test_probe_process_metrics():
    res = probe_process_metrics()
    assert res.passed is True
    assert res.metadata and res.metadata.get("rss_bytes", 0) > 0


def test_run_preflight_structure():
    report = run_preflight(PROJECT_ROOT)
    assert "candidate_sha" in report
    assert "git" in report
    assert "environment" in report
    assert "dependencies" in report
    assert "hardware" in report
    assert "storage" in report
    assert "capabilities" in report
    assert report["preflight_passed"] is True

    caps = report["capabilities"]
    assert caps["local_socket_binding"]["passed"] is True
    assert caps["subprocess_lifecycle"]["passed"] is True
    assert caps["process_metrics"]["passed"] is True


def test_preflight_cli_execution(tmp_path):
    out_file = tmp_path / "preflight_report.json"
    proc = subprocess.run(
        [sys.executable, "tools/preflight.py", "--json", str(out_file)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"Preflight CLI failed:\n{proc.stdout}\n{proc.stderr}"
    assert "PREFLIGHT PASSED" in proc.stdout
    assert out_file.is_file()

    data = json.loads(out_file.read_text(encoding="utf-8"))
    assert data["preflight_passed"] is True
    assert data["candidate_sha"] != "unknown"


def test_preflight_refuses_protected_output_destination():
    """VALD-01 / VALD-04: preflight must refuse writing output into protected paths."""
    proc = subprocess.run(
        [
            sys.executable,
            "tools/preflight.py",
            "--json",
            "data/preflight.json",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert not (PROJECT_ROOT / "data" / "preflight.json").exists()
