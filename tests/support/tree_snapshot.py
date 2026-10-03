"""Filesystem snapshots for proving migration dry-run and immutable publication."""
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
from typing import Tuple


@dataclass(frozen=True)
class EntrySnapshot:
    relative_path: str
    kind: str
    size: int
    mtime_ns: int
    sha256: str = ""
    symlink_target: str = ""


@dataclass(frozen=True)
class TreeSnapshot:
    root: str
    root_exists: bool
    entries: Tuple[EntrySnapshot, ...]


def snapshot_tree(root: Path) -> TreeSnapshot:
    """Capture relative path, file hash/size/mtime, and directory existence (not atime)."""
    root = Path(root).resolve()
    if not root.exists():
        return TreeSnapshot(str(root), False, ())

    entries = []
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        dirs.sort()
        files.sort()
        for name in list(dirs):
            path = current_path / name
            rel = path.relative_to(root).as_posix()
            stat = path.lstat()
            if path.is_symlink():
                entries.append(EntrySnapshot(rel, "symlink", stat.st_size, stat.st_mtime_ns,
                                             symlink_target=os.readlink(path)))
                dirs.remove(name)
            else:
                entries.append(EntrySnapshot(rel, "directory", stat.st_size, stat.st_mtime_ns))
        for name in files:
            path = current_path / name
            rel = path.relative_to(root).as_posix()
            stat = path.lstat()
            if path.is_symlink():
                entries.append(EntrySnapshot(rel, "symlink", stat.st_size, stat.st_mtime_ns,
                                             symlink_target=os.readlink(path)))
            elif path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                entries.append(EntrySnapshot(rel, "file", stat.st_size, stat.st_mtime_ns, digest))
            else:
                entries.append(EntrySnapshot(rel, "other", stat.st_size, stat.st_mtime_ns))
    return TreeSnapshot(str(root), True, tuple(sorted(entries, key=lambda entry: entry.relative_path)))


def assert_tree_unchanged(root: Path, before: TreeSnapshot) -> None:
    after = snapshot_tree(root)
    assert after == before, f"filesystem tree changed during read-only operation:\n{before!r}\n!=\n{after!r}"
