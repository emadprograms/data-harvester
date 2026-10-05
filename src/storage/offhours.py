"""SCHED-05 — unattended off-hours compaction.

Compaction runs once per eligible closed interval (the gap between two
ingestion windows, and only after a confirmed drain), and it must be safe to
call repeatedly:

- **Idempotent per interval.** A ledger in ``_maintenance/compaction_ledger.json``
  records every finished interval. A second run in the same interval is a no-op
  success (`ALREADY_COMPLETED`), including when the first run found nothing to do.
- **A no-op counts as success.** "There was nothing to compact" is the normal
  case, not a failure.
- **One launcher at a time.** A lease file (``_maintenance/compaction.lock``) is
  created with ``O_EXCL``; a stale lease from a dead process is reclaimed.
- **A failed drain blocks compaction.** If the streamer's status file says
  ``DRAIN_FAILED``, or a writer is still live, compaction refuses to run and
  reports ``BLOCKED_DRAIN_FAILED`` — never silently.
- **Best-effort notification.** Outcomes go to Discord (NOTIF-01); a webhook
  failure never changes the result.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from src.utils.session_window import (
    describe as describe_window,
)
from src.utils.session_window import (
    is_eligible,
    next_open,
    now_et,
    window_close,
)

LEDGER_NAME = "compaction_ledger.json"
LEASE_NAME = "compaction.lock"
LEASE_TTL_SECONDS = 3600.0

ALREADY_COMPLETED = "ALREADY_COMPLETED"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
LEASE_HELD = "LEASE_HELD"
BLOCKED_DRAIN_FAILED = "BLOCKED_DRAIN_FAILED"
NOT_ELIGIBLE = "NOT_ELIGIBLE"

DRAIN_FAILED_STATUSES = {"DRAIN_FAILED"}
LIVE_STATUSES = {"RUNNING", "DEGRADED", "CLOSING"}


# ------------------------------------------------------------------ interval


def closed_interval_id(moment: datetime) -> str:
    """Identifier of the closed interval that precedes the next window opening.

    The id is the ISO timestamp of the next window opening, so every
    ``now`` between two windows maps to the same interval — which is exactly the
    unit of "run compaction once".
    """
    return next_open(moment).isoformat()


def _maintenance_dir(lake_root: Path) -> Path:
    directory = Path(lake_root) / "_maintenance"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def read_ledger(lake_root: Path) -> Dict[str, Any]:
    path = _maintenance_dir(lake_root) / LEDGER_NAME
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_ledger(lake_root: Path, ledger: Dict[str, Any]) -> None:
    path = _maintenance_dir(lake_root) / LEDGER_NAME
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(ledger, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def completed_intervals(lake_root: Path) -> Dict[str, Any]:
    return dict(read_ledger(lake_root).get("intervals", {}))


# --------------------------------------------------------------------- lease


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def acquire_lease(lake_root: Path, owner: str, ttl_seconds: float = LEASE_TTL_SECONDS) -> bool:
    """Take the single maintenance lease; reclaim a stale one from a dead owner."""
    path = _maintenance_dir(lake_root) / LEASE_NAME
    payload = {
        "owner": owner,
        "pid": os.getpid(),
        "acquired_at": time.time(),
        "heartbeat": time.time(),
    }
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            existing = {}
        age = time.time() - float(existing.get("heartbeat") or existing.get("acquired_at") or 0.0)
        owner_pid = int(existing.get("pid") or 0)
        if age < ttl_seconds and (_pid_alive(owner_pid) or owner_pid == 0):
            return False
        # Stale: the launcher died, so the lease is reclaimable.
        try:
            path.unlink()
        except OSError:
            return False
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return True


def release_lease(lake_root: Path) -> None:
    try:
        (_maintenance_dir(lake_root) / LEASE_NAME).unlink()
    except OSError:
        pass


# ---------------------------------------------------------------- drain gate


def drain_evidence(lake_root: Path) -> Dict[str, Any]:
    """What the writer status file says about the last drain, if anything."""
    status_path = Path(lake_root) / "_control" / "writer_status.json"
    try:
        with open(status_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return {"present": False, "status": None}
    payload["present"] = True
    return payload


def drain_allows_compaction(lake_root: Path, stale_after_seconds: float = 120.0) -> tuple:
    """(allowed, reason). A live writer or a failed drain blocks compaction."""
    evidence = drain_evidence(lake_root)
    if not evidence.get("present"):
        # Nothing ever ran: there is no un-drained work to protect.
        return True, "no writer status file; nothing to drain"
    status = str(evidence.get("status") or "").upper()
    if status in DRAIN_FAILED_STATUSES:
        return False, f"streamer reported {status}"
    if status in LIVE_STATUSES:
        heartbeat = evidence.get("heartbeat_monotonic")
        age = None
        if isinstance(heartbeat, (int, float)):
            age = time.monotonic() - float(heartbeat)
        if age is None or age < stale_after_seconds:
            return False, f"a writer is still live ({status})"
        return True, f"writer status {status} is stale ({age:.0f}s old)"
    return True, f"writer status {status or 'unknown'}"


# ----------------------------------------------------------------- run entry


def run_scheduled_compaction(
    lake_root=None,
    now: Optional[datetime] = None,
    compactor_factory: Optional[Callable[[Path], Any]] = None,
    notifier: Optional[Callable[..., Any]] = None,
    owner: Optional[str] = None,
    stale_drain_after_seconds: float = 120.0,
) -> Dict[str, Any]:
    """Compact the lake once for the interval that just closed.

    Returns a JSON-safe result describing exactly what happened. Never raises for
    expected conditions (already done, leased, blocked, nothing to compact);
    unexpected errors come back as ``FAILED`` with the message.
    """
    from src.storage.config import resolve_tick_lake_root

    moment = now if now is not None else now_et()
    root = Path(lake_root) if lake_root is not None else Path(resolve_tick_lake_root())
    interval = closed_interval_id(moment)
    result: Dict[str, Any] = {
        "interval": interval,
        "lake_root": str(root),
        "window": describe_window(moment),
        "compacted": 0,
        "status": None,
    }

    if is_eligible(moment):
        result["status"] = NOT_ELIGIBLE
        result["message"] = "the ingestion window is open; compaction waits for the close"
        return result

    ledger = read_ledger(root)
    if interval in (ledger.get("intervals") or {}):
        result["status"] = ALREADY_COMPLETED
        result["message"] = "compaction already completed for this interval"
        result["previous"] = ledger["intervals"][interval]
        return result

    allowed, reason = drain_allows_compaction(root, stale_after_seconds=stale_drain_after_seconds)
    if not allowed:
        result["status"] = BLOCKED_DRAIN_FAILED
        result["message"] = f"compaction blocked: {reason}"
        if notifier is not None:
            try:
                notifier("compaction_failed", detail=result["message"], fields={"Interval": interval})
            except Exception:
                pass
        return result

    lease_owner = owner or f"compaction:{os.getpid()}"
    if not acquire_lease(root, owner=lease_owner):
        result["status"] = LEASE_HELD
        result["message"] = "another launcher holds the maintenance lease"
        return result

    try:
        if compactor_factory is None:
            from src.storage.compaction import LakeCompactor

            compactor_factory = lambda lake: LakeCompactor(lake_root=lake)

        try:
            compactor = compactor_factory(root)
            outcome = compactor.compact()
        except Exception as exc:  # noqa: BLE001 — report, never crash the supervisor
            result["status"] = FAILED
            result["message"] = f"{type(exc).__name__}: {exc}"
            if notifier is not None:
                try:
                    notifier("compaction_failed", detail=result["message"], fields={"Interval": interval})
                except Exception:
                    pass
            return result

        compacted = int(outcome.get("compacted_partitions") or 0)
        result["compacted"] = compacted
        result["status"] = COMPLETED
        result["message"] = outcome.get("message") or outcome.get("status") or "compaction finished"
        result["outcome"] = outcome

        ledger = read_ledger(root)
        intervals = ledger.setdefault("intervals", {})
        intervals[interval] = {
            "completed_at": moment.isoformat(),
            "compacted_partitions": compacted,
            "outcome_status": outcome.get("status"),
        }
        _write_ledger(root, ledger)
        return result
    finally:
        release_lease(root)
