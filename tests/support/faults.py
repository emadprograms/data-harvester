"""Scoped deterministic fault injection for persistence boundaries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from src.storage.barriers import (
    PERSISTENCE_BOUNDARIES,
    clear_barrier_hooks,
    register_barrier_hook,
    unregister_barrier_hook,
)


@dataclass
class FaultCounter:
    calls: int = 0
    injected: int = 0


def inject_failure(
    monkeypatch,
    target,
    attribute: str,
    *,
    predicate: Optional[Callable[..., bool]] = None,
    failures: int = 1,
    error: Optional[BaseException] = None,
    error_factory: Optional[Callable[[], BaseException]] = None,
) -> FaultCounter:
    """Wrap one operation, fail matching calls N times, and retain the original behavior otherwise.

    The predicate sees the original positional and keyword arguments. Keep it narrowly
    scoped (for example, match the exact receipt path) so status-file I/O cannot be
    mistaken for a failed data publication.
    """
    original = getattr(target, attribute)
    counter = FaultCounter()

    def wrapped(*args, **kwargs):
        counter.calls += 1
        matches = predicate is None or predicate(*args, **kwargs)
        if matches and counter.injected < failures:
            counter.injected += 1
            exc = error_factory() if error_factory else error
            if exc is None:
                exc = OSError("injected persistence failure")
            raise exc
        return original(*args, **kwargs)

    monkeypatch.setattr(target, attribute, wrapped)
    return counter


def path_matches(argument_index: int, expected_path) -> Callable[..., bool]:
    """Build a path predicate for functions such as os.replace(src, dst)."""
    expected = str(expected_path)

    def matches(*args, **kwargs):
        if len(args) > argument_index:
            value = args[argument_index]
        else:
            value = kwargs.get("path")
        return value is not None and str(value) == expected

    return matches


class PersistenceFaultInjector:
    """
    Assertion-bearing persistence fault injector for durability boundaries (DURB-02).
    Tracks faults injected at each boundary and asserts that every targeted fault
    was actually reached and triggered.
    """

    def __init__(self) -> None:
        self.counters: Dict[str, FaultCounter] = {}
        self._registered_boundaries: list[str] = []

    def inject_barrier(
        self,
        boundary: str,
        failures: int = 1,
        error: Optional[BaseException] = None,
        predicate: Optional[Callable[..., bool]] = None,
    ) -> FaultCounter:
        """Register a fault for a named persistence boundary."""
        valid_boundaries = set(PERSISTENCE_BOUNDARIES) | {"intent_fsync", "receipt_fsync"}
        if boundary not in valid_boundaries:
            raise ValueError(f"Unknown persistence boundary: {boundary!r}. Expected one of {valid_boundaries}")

        counter = self.counters.setdefault(boundary, FaultCounter())

        def hook(b_name: str, **kwargs: Any) -> None:
            counter.calls += 1
            if predicate is not None and not predicate(**kwargs):
                return
            if counter.injected < failures:
                counter.injected += 1
                exc = error if error is not None else OSError(f"injected fault at boundary {boundary}")
                raise exc

        register_barrier_hook(boundary, hook)
        self._registered_boundaries.append(boundary)
        return counter

    def assert_triggered(self, boundary: str, min_injections: int = 1) -> None:
        """Explicitly assert that the targeted fault was reached and triggered."""
        counter = self.counters.get(boundary)
        assert counter is not None, f"No fault was registered for boundary '{boundary}'"
        assert counter.injected >= min_injections, (
            f"Targeted fault at boundary '{boundary}' was NEVER triggered "
            f"(calls: {counter.calls}, injected: {counter.injected}). The test proved nothing!"
        )

    def close(self) -> None:
        for b in self._registered_boundaries:
            unregister_barrier_hook(b)
        self._registered_boundaries.clear()

    def __enter__(self) -> "PersistenceFaultInjector":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
