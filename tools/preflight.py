#!/usr/bin/env python3
"""
Preflight Environment Characterization & Capability Probes (VALD-04).

Records candidate git commit SHA, clean/dirty tree status, dependency versions,
OS/platform, hardware, filesystem headroom, and runs explicit capability probes:
- local socket binding (127.0.0.1 ephemeral port)
- subprocess launch, communication, and termination lifecycle
- process CPU and RSS metrics measurement (psutil)
- Node.js runtime availability
- public network connectivity probe (honest reporting; does not fail in offline environments)
- write safety guard on report output

Usage:
    python tools/preflight.py
    python tools/preflight.py --json reports/preflight.json
    python tools/preflight.py --check-only
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.write_guard import assert_safe_write_path, get_run_artifacts_dir, ProductionAccessBlockedError


@dataclass
class ProbeResult:
    passed: bool
    details: str
    metadata: Optional[Dict[str, Any]] = None


def get_git_info(repo_root: Path) -> Dict[str, Any]:
    """Inspect git repository state: candidate SHA, branch, and clean/dirty working tree."""
    sha = "unknown"
    branch = "unknown"
    is_dirty = False
    uncommitted: List[str] = []

    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            sha = proc.stdout.strip()
    except Exception:
        pass

    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            branch = proc.stdout.strip()
    except Exception:
        pass

    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            lines = [l.strip() for l in proc.stdout.splitlines() if l.strip()]
            uncommitted = lines
            is_dirty = len(lines) > 0
    except Exception:
        pass

    return {
        "candidate_sha": sha,
        "branch": branch,
        "is_dirty": is_dirty,
        "uncommitted_count": len(uncommitted),
        "uncommitted_files": uncommitted[:20],
    }


def get_dependency_versions() -> Dict[str, str]:
    """Retrieve installed versions of key project dependencies."""
    packages = ["duckdb", "pyarrow", "psutil", "pytest"]
    versions = {}
    for pkg in packages:
        try:
            versions[pkg] = importlib.metadata.version(pkg)
        except Exception:
            versions[pkg] = "not installed"
    return versions


def get_hardware_and_storage_info(repo_root: Path, scratch_dir: Path) -> Dict[str, Any]:
    """Inspect CPU, RAM, and disk storage headroom."""
    import psutil

    vm = psutil.virtual_memory()
    total_mem_mb = round(vm.total / (1024 * 1024), 1)
    avail_mem_mb = round(vm.available / (1024 * 1024), 1)

    usage = shutil.disk_usage(scratch_dir if scratch_dir.exists() else repo_root)
    free_mb = round(usage.free / (1024 * 1024), 1)
    total_disk_mb = round(usage.total / (1024 * 1024), 1)
    headroom_ok = free_mb >= 500.0  # require at least 500 MB free space

    return {
        "cpu_count": os.cpu_count() or 1,
        "total_memory_mb": total_mem_mb,
        "available_memory_mb": avail_mem_mb,
        "scratch_path": str(scratch_dir),
        "total_disk_mb": total_disk_mb,
        "free_disk_mb": free_mb,
        "storage_headroom_ok": headroom_ok,
    }


def probe_local_socket_binding() -> ProbeResult:
    """Verify local loopback socket binding (essential for mock servers and IPC)."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]
        return ProbeResult(
            passed=True,
            details=f"Successfully bound local socket to 127.0.0.1:{port}",
            metadata={"port": port},
        )
    except Exception as err:
        return ProbeResult(passed=False, details=f"Failed to bind local loopback socket: {err}")
    finally:
        server_sock.close()


def probe_subprocess_lifecycle() -> ProbeResult:
    """Verify ability to launch, communicate with, and terminate child processes."""
    code = "import sys; print('ready'); sys.stdout.flush()"
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(timeout=10)
        if proc.returncode == 0 and "ready" in stdout:
            return ProbeResult(passed=True, details="Subprocess launch, communication, and exit verified")
        return ProbeResult(
            passed=False,
            details=f"Subprocess returned code {proc.returncode}, stdout: {stdout!r}, stderr: {stderr!r}",
        )
    except Exception as err:
        return ProbeResult(passed=False, details=f"Subprocess lifecycle failed: {err}")


def probe_process_metrics() -> ProbeResult:
    """Verify ability to sample CPU times and memory info using psutil."""
    import psutil
    try:
        p = psutil.Process()
        cpu = p.cpu_times()
        mem = p.memory_info()
        if cpu.user >= 0 and mem.rss > 0:
            return ProbeResult(
                passed=True,
                details=f"Process metrics accessible: RSS={round(mem.rss / (1024*1024), 2)}MB, User CPU={cpu.user}s",
                metadata={"rss_bytes": mem.rss, "cpu_user": cpu.user},
            )
        return ProbeResult(passed=False, details="Process metrics returned non-positive values")
    except Exception as err:
        return ProbeResult(passed=False, details=f"Failed to inspect process metrics: {err}")


def probe_node_availability() -> ProbeResult:
    """Inspect Node.js availability for frontend simulations."""
    node_bin = shutil.which("node")
    if not node_bin:
        return ProbeResult(
            passed=False,
            details="Node.js executable ('node') not found in PATH",
        )
    try:
        proc = subprocess.run([node_bin, "-v"], capture_output=True, text=True, timeout=5)
        if proc.returncode == 0:
            version = proc.stdout.strip()
            return ProbeResult(
                passed=True,
                details=f"Node.js found: {version}",
                metadata={"version": version, "path": node_bin},
            )
        return ProbeResult(passed=False, details=f"node -v returned {proc.returncode}")
    except Exception as err:
        return ProbeResult(passed=False, details=f"Failed to execute node: {err}")


def probe_public_network(timeout_sec: float = 1.0) -> ProbeResult:
    """
    Test public network connectivity.
    Honest reporting: returns passed=False in offline/restricted sandbox environments
    without failing overall preflight qualification.
    """
    # Attempt quick connection to public DNS or endpoint
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_sec)
    try:
        # 1.1.1.1:53 or 8.8.8.8:53
        sock.connect(("1.1.1.1", 53))
        return ProbeResult(passed=True, details="Public network connection succeeded (1.1.1.1:53)")
    except Exception as err:
        return ProbeResult(
            passed=False,
            details=f"Public network unavailable ({type(err).__name__}: {err})",
        )
    finally:
        sock.close()


def run_preflight(repo_root: Optional[Path] = None) -> Dict[str, Any]:
    """Execute all preflight characterization checks and capability probes."""
    root = repo_root or REPO_ROOT
    scratch_dir = get_run_artifacts_dir()

    git_info = get_git_info(root)
    dep_versions = get_dependency_versions()
    hw_storage = get_hardware_and_storage_info(root, scratch_dir)

    probes = {
        "local_socket_binding": probe_local_socket_binding(),
        "subprocess_lifecycle": probe_subprocess_lifecycle(),
        "process_metrics": probe_process_metrics(),
        "node_availability": probe_node_availability(),
        "public_network": probe_public_network(),
    }

    # Core required capabilities: socket, subprocess, metrics, storage headroom
    core_ok = (
        probes["local_socket_binding"].passed
        and probes["subprocess_lifecycle"].passed
        and probes["process_metrics"].passed
        and hw_storage["storage_headroom_ok"]
        and all(v != "not installed" for v in dep_versions.values())
        and git_info["candidate_sha"] != "unknown"
    )

    report = {
        "candidate_sha": git_info["candidate_sha"],
        "git": git_info,
        "environment": {
            "python_version": sys.version,
            "python_executable": sys.executable,
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "dependencies": dep_versions,
        "hardware": {
            "cpu_count": hw_storage["cpu_count"],
            "total_memory_mb": hw_storage["total_memory_mb"],
            "available_memory_mb": hw_storage["available_memory_mb"],
        },
        "storage": {
            "repo_root": str(root),
            "scratch_dir": hw_storage["scratch_path"],
            "free_disk_mb": hw_storage["free_disk_mb"],
            "total_disk_mb": hw_storage["total_disk_mb"],
            "storage_headroom_ok": hw_storage["storage_headroom_ok"],
        },
        "capabilities": {k: asdict(v) for k, v in probes.items()},
        "preflight_passed": core_ok,
    }

    return report


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", metavar="PATH", dest="json_path", help="write preflight report as JSON")
    parser.add_argument("--check-only", action="store_true", help="only check capabilities and print summary")
    args = parser.parse_args(argv)

    if args.json_path:
        assert_safe_write_path(args.json_path, operation="preflight report json")

    report = run_preflight(REPO_ROOT)

    print("=" * 72)
    print("🔍 PREFLIGHT ENVIRONMENT & CAPABILITY CHARACTERIZATION (VALD-04)")
    print("=" * 72)
    print(f"Candidate SHA:       {report['candidate_sha']}")
    print(f"Working Tree:        {'DIRTY (' + str(report['git']['uncommitted_count']) + ' uncommitted)' if report['git']['is_dirty'] else 'CLEAN'}")
    print(f"Python:              {report['environment']['python_version'].split()[0]}")
    print(f"Platform:            {report['environment']['platform']}")
    print(f"CPUs / Available RAM:{report['hardware']['cpu_count']} cores / {report['hardware']['available_memory_mb']} MB")
    print(f"Scratch Directory:   {report['storage']['scratch_dir']}")
    print(f"Free Disk Space:     {report['storage']['free_disk_mb']} MB (headroom: {'OK' if report['storage']['storage_headroom_ok'] else 'LOW'})")
    print("-" * 72)
    print("Capability Probes:")
    for name, probe in report["capabilities"].items():
        status = "✅ PASS" if probe["passed"] else ("⚠️ UNAVAILABLE" if name in ("public_network", "node_availability") else "❌ FAIL")
        print(f"  {name:25s}: {status} - {probe['details']}")
    print("=" * 72)

    if args.json_path:
        out = Path(args.json_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Preflight report written to: {out}")

    if report["preflight_passed"]:
        print("PREFLIGHT PASSED: Environment and required capabilities verified.")
        return 0
    else:
        print("PREFLIGHT FAILED: One or more required core capabilities failed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
