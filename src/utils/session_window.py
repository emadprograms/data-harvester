"""Single timezone policy for the ingestion window (SCHED-01).

The toolkit ingests **weekdays 04:00–20:00 ET**, half-open: 04:00 is inside the
window, 20:00 is outside. Every scheduling decision (the runner's start gate, the
supervisor's lifecycle, the off-hours compactor) reads this module so the
definition exists in exactly one place.

Design notes:

- ``ZoneInfo("America/New_York")`` is the only timezone authority; DST is handled
  by the tz database rather than by arithmetic on fixed offsets.
- Naive timestamps are interpreted as exchange-local time. Callers that hold UTC
  values must attach ``timezone.utc`` first (``is_eligible`` converts).
- ``next_open`` returns the earliest eligible instant **at or after** the given
  moment, so a scheduler can ask "are we open, and if not, when?" without a
  separate branch.
- ``STREAM_NOW_OVERRIDE`` is a deliberate test seam: an ISO-8601 timestamp that
  replaces the wall clock so the window logic can be exercised end-to-end in a
  subprocess. It is documented as test-only in the operations guide.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, time as dtime
from typing import Callable, Optional
from zoneinfo import ZoneInfo

EXCHANGE_TZ = ZoneInfo("America/New_York")
WINDOW_START = dtime(4, 0)
WINDOW_END = dtime(20, 0)
# Monday .. Friday (datetime.weekday(): Monday is 0)
OPEN_WEEKDAYS = frozenset({0, 1, 2, 3, 4})
NOW_OVERRIDE_ENV = "STREAM_NOW_OVERRIDE"


def _as_exchange(moment: datetime) -> datetime:
    """Return ``moment`` as an exchange-local aware timestamp."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=EXCHANGE_TZ)
    return moment.astimezone(EXCHANGE_TZ)


def now_et(clock: Optional[Callable[[], datetime]] = None) -> datetime:
    """Current exchange-local timestamp.

    Resolution order: an explicit ``clock`` callable (unit-test seam), then
    ``STREAM_NOW_OVERRIDE``, then the wall clock. An override that is not a valid
    ISO-8601 timestamp raises ``ValueError`` instead of silently falling back.
    """
    if clock is not None:
        return _as_exchange(clock())
    raw = os.environ.get(NOW_OVERRIDE_ENV)
    if raw:
        try:
            return _as_exchange(datetime.fromisoformat(raw.strip()))
        except ValueError as exc:
            raise ValueError(
                f"{NOW_OVERRIDE_ENV} must be an ISO-8601 timestamp, got {raw!r}"
            ) from exc
    return datetime.now(EXCHANGE_TZ)


def is_eligible(moment: datetime) -> bool:
    """True when ``moment`` falls inside [04:00, 20:00) ET on a weekday."""
    local = _as_exchange(moment)
    if local.weekday() not in OPEN_WEEKDAYS:
        return False
    return WINDOW_START <= local.time().replace(tzinfo=None) < WINDOW_END


def _open_on(day) -> datetime:
    return datetime.combine(day, WINDOW_START, tzinfo=EXCHANGE_TZ)


def _close_on(day) -> datetime:
    return datetime.combine(day, WINDOW_END, tzinfo=EXCHANGE_TZ)


def window_close(moment: datetime) -> datetime:
    """The end of the window containing ``moment`` (or of its day when closed)."""
    return _close_on(_as_exchange(moment).date())


def next_open(moment: datetime) -> datetime:
    """Earliest eligible instant at or after ``moment``."""
    local = _as_exchange(moment)
    if is_eligible(local):
        return local
    for offset in range(0, 8):
        day = local.date() + timedelta(days=offset)
        if day.weekday() not in OPEN_WEEKDAYS:
            continue
        opening = _open_on(day)
        if opening > local:
            return opening
    # Unreachable: open weekdays recur within a week.
    raise AssertionError(f"no window open found within eight days of {local.isoformat()}")


def seconds_until_open(moment: datetime) -> float:
    """Seconds until the window opens; 0.0 while it is open."""
    local = _as_exchange(moment)
    return max(0.0, (next_open(local) - local).total_seconds())


def describe(moment: datetime) -> str:
    """One-line, operator-facing state for logs and the supervisor state file."""
    local = _as_exchange(moment)
    if is_eligible(local):
        close = window_close(local)
        return f"INGESTING (window closes {close:%Y-%m-%d %H:%M} ET)"
    opening = next_open(local)
    remaining = int((opening - local).total_seconds())
    hours, remainder = divmod(max(remaining, 0), 3600)
    minutes = remainder // 60
    return (
        f"WAITING_FOR_WINDOW (opens {opening:%Y-%m-%d %H:%M} ET, "
        f"in {hours}h {minutes:02d}m)"
    )
