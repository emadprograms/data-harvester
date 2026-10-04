"""
Production Write Guards & Safe Run Directory Management (VALD-01).

Ensures that tests, benchmark runs, migration tools, and validators never mutate
or write into production paths (such as the Micron volume or repo data directory),
whether addressed directly, via symlinks, or via inherited ambient environment variables.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
from pathlib import Path
from typing import Optional, Set, Union


class ProductionAccessBlockedError(PermissionError):
    """Raised when an operation attempts to access or mutate protected storage paths."""
    pass


REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_DATA_DIR = REPO_ROOT / "data"
REPO_DATA_REAL = Path(os.path.realpath(str(REPO_DATA_DIR)))
MICRON_DATA_DIR = "/Volumes/Micron-E 0256 A/data-harvester/data"

DATA_HARVESTER_RUN_DIR_ENV = "DATA_HARVESTER_RUN_DIR"
DATA_HARVESTER_PROTECTED_ROOTS_ENV = "DATA_HARVESTER_PROTECTED_ROOTS"

_EXTRA_PROTECTED_ROOTS: Set[str] = set()
_EXTRA_PROTECTED_LOCK = threading.RLock()


def register_protected_root(path: Union[str, Path, os.PathLike]) -> None:
    """Register an additional protected directory root (e.g. for isolation tests)."""
    with _EXTRA_PROTECTED_LOCK:
        _EXTRA_PROTECTED_ROOTS.add(os.path.abspath(os.fspath(path)))


def unregister_protected_root(path: Union[str, Path, os.PathLike]) -> None:
    """Unregister an additional protected directory root."""
    with _EXTRA_PROTECTED_LOCK:
        _EXTRA_PROTECTED_ROOTS.discard(os.path.abspath(os.fspath(path)))


def _get_all_protected_roots() -> list[str]:
    roots = [MICRON_DATA_DIR, str(REPO_DATA_DIR), str(REPO_DATA_REAL)]
    with _EXTRA_PROTECTED_LOCK:
        roots.extend(_EXTRA_PROTECTED_ROOTS)
    env_extra = os.environ.get(DATA_HARVESTER_PROTECTED_ROOTS_ENV)
    if env_extra:
        for item in env_extra.split(os.pathsep):
            item = item.strip()
            if item:
                roots.append(os.path.abspath(item))
    return roots


def is_production_path(path: Union[str, Path, os.PathLike, None]) -> bool:
    """
    Check if a path points to or is a descendant of a protected production path,
    or a symlink alias resolving to one.
    """
    if path is None:
        return False
    if hasattr(path, "name") and not isinstance(path, (str, bytes, os.PathLike)):
        path = path.name
    try:
        path_str = os.fspath(path)
    except TypeError:
        return False

    if isinstance(path_str, bytes):
        path_str = os.fsdecode(path_str)
    path_str = path_str.strip()
    if not path_str or path_str == ":memory:":
        return False

    # Check for direct relative paths to repo data
    norm = os.path.normpath(path_str)
    if norm == "data" or norm.startswith("data" + os.sep) or norm.startswith("data/"):
        return True
    if norm == "." + os.sep + "data" or norm.startswith("." + os.sep + "data" + os.sep) or norm.startswith("./data/"):
        return True

    # Check Micron volume string markers
    if "/Volumes/Micron-E" in path_str:
        return True

    abs_path = os.path.abspath(path_str)
    if "/Volumes/Micron-E" in abs_path:
        return True

    real_path = os.path.realpath(abs_path)
    if "/Volumes/Micron-E" in real_path:
        return True

    # Check repo data directory
    repo_data_str = str(REPO_DATA_DIR)
    repo_data_real_str = str(REPO_DATA_REAL)
    for candidate in (abs_path, real_path):
        for protected in (repo_data_str, repo_data_real_str):
            try:
                if os.path.commonpath([candidate, protected]) == protected:
                    return True
            except ValueError:
                continue

    # Check all registered protected roots (including symlink resolution)
    for root in _get_all_protected_roots():
        root_abs = os.path.abspath(root)
        root_real = os.path.realpath(root_abs)
        for cand, p_root in ((abs_path, root_abs), (real_path, root_real), (real_path, root_abs)):
            try:
                if os.path.commonpath([cand, p_root]) == p_root:
                    return True
            except ValueError:
                continue

    # Check if inherited ambient environment DATA_DIR or TICK_LAKE_ROOT points to production
    for env_var in ("DATA_DIR", "TICK_LAKE_ROOT"):
        val = os.environ.get(env_var)
        if val:
            val_abs = os.path.abspath(val)
            val_real = os.path.realpath(val_abs)
            if "/Volumes/Micron-E" in val_abs or "/Volumes/Micron-E" in val_real:
                try:
                    if os.path.commonpath([abs_path, val_abs]) == val_abs or os.path.commonpath([real_path, val_real]) == val_real:
                        return True
                except ValueError:
                    pass

    return False


def is_protected_path(path: Union[str, Path, os.PathLike, None]) -> bool:
    """Alias for is_production_path, compatible with tests.conftest."""
    return is_production_path(path)


def assert_safe_write_path(path: Union[str, Path, os.PathLike], operation: str = "write") -> Path:
    """
    Verify that path is NOT a protected production path or symlink alias.
    Raises ProductionAccessBlockedError if unsafe.
    Returns canonical resolved Path if safe.
    """
    if is_production_path(path):
        raise ProductionAccessBlockedError(f"Blocked {operation} to protected path: {path}")

    p = Path(path).expanduser()
    abs_p = p.resolve() if p.exists() else p.parent.resolve() / p.name
    if is_production_path(abs_p) or is_production_path(p.resolve()):
        raise ProductionAccessBlockedError(f"Blocked {operation} to protected path alias: {path}")

    return p.resolve()


def get_run_artifacts_dir(subdir: Optional[str] = None, create: bool = True) -> Path:
    """
    Return a designated safe run directory outside tracked historical artifacts.
    Supports DATA_HARVESTER_RUN_DIR environment variable with safe default fallback.
    """
    env_run_dir = os.environ.get(DATA_HARVESTER_RUN_DIR_ENV)
    if env_run_dir:
        run_dir = assert_safe_write_path(env_run_dir, operation="use run directory")
    else:
        # Default safe scratch path outside tracked historical artifacts (.planning/artifacts)
        base_scratch = Path(tempfile.gettempdir()) / "data_harvester_runs"
        run_dir = assert_safe_write_path(base_scratch, operation="use default run directory")

    if subdir:
        run_dir = run_dir / subdir

    if create:
        run_dir.mkdir(parents=True, exist_ok=True)

    return run_dir
