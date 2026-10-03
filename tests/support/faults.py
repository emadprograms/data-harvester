"""Scoped deterministic fault injection for persistence boundaries."""
from dataclasses import dataclass
from typing import Callable, Optional


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
