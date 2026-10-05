"""
Named persistence boundaries for durability fault injection (Milestone 4.3 - DURB-02).
"""
from __future__ import annotations

import contextlib
import threading
from typing import Any, Callable, Dict, Optional

PERSISTENCE_BOUNDARIES = (
    "admission",
    "intent_durability",
    "staged_fsync",
    "staged_promotion",
    "directory_fsync",
    "receipt_durability",
    "acknowledgment",
)

_barrier_lock = threading.RLock()
_active_barrier_hooks: Dict[str, Callable[..., None]] = {}


def register_barrier_hook(boundary: str, hook: Callable[..., None]) -> None:
    """Register a hook callable for a named persistence boundary."""
    with _barrier_lock:
        _active_barrier_hooks[boundary] = hook


def unregister_barrier_hook(boundary: str) -> None:
    """Unregister a hook callable for a named persistence boundary."""
    with _barrier_lock:
        _active_barrier_hooks.pop(boundary, None)


def clear_barrier_hooks() -> None:
    """Clear all registered persistence barrier hooks."""
    with _barrier_lock:
        _active_barrier_hooks.clear()


@contextlib.contextmanager
def persistence_barrier_context(boundary: str, hook: Callable[..., None]):
    """Scoped context manager for temporary persistence barrier registration."""
    register_barrier_hook(boundary, hook)
    try:
        yield
    finally:
        unregister_barrier_hook(boundary)


def trigger_persistence_barrier(boundary: str, **kwargs: Any) -> None:
    """
    Invoke registered persistence barrier hook if present.

    Named assertion-bearing barrier injection points across key persistence boundaries:
      1. admission
      2. intent_durability
      3. staged_fsync
      4. staged_promotion
      5. directory_fsync
      6. receipt_durability
      7. acknowledgment
    """
    with _barrier_lock:
        hook = _active_barrier_hooks.get(boundary)
    if hook is not None:
        hook(boundary, **kwargs)
