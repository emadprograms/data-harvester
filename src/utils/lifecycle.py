"""Service lifecycle states and their published state file (SCHED-02).

`LifecycleState` is the vocabulary the supervisor, the dashboard and the macOS
status script share. `LifecycleRecorder` writes it atomically to
``<log_dir>/<name>.state.json`` so an operator (or the status script) can always
tell *why* the streamer is or is not running:

- ``WAITING_FOR_WINDOW`` — outside the weekday 04:00–20:00 ET window, by design.
- ``STARTING`` — the window just opened and the child is being launched.
- ``INGESTING`` — the child is up and inside the window.
- ``DRAINING`` — the window closed (or a stop was requested) and the child is
  finishing its final flush.
- ``MAINTENANCE`` — off-hours work (compaction) owns the lake.
- ``ERROR`` — the child failed in a way the supervisor could not absorb.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional


class LifecycleState(str, Enum):
    WAITING_FOR_WINDOW = "WAITING_FOR_WINDOW"
    STARTING = "STARTING"
    INGESTING = "INGESTING"
    DRAINING = "DRAINING"
    MAINTENANCE = "MAINTENANCE"
    ERROR = "ERROR"


class LifecycleRecorder:
    """Atomic publisher of the supervisor's current state."""

    def __init__(self, log_dir: Path, name: str):
        self.name = name
        self.path = Path(log_dir) / f"{name}.state.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state: Optional[LifecycleState] = None

    @property
    def state(self) -> Optional[LifecycleState]:
        return self._state

    def transition(
        self,
        state: LifecycleState,
        detail: Optional[str] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "state": LifecycleState(state).value,
            "detail": detail or "",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        payload.update({key: value for key, value in extra.items() if value is not None})
        self._state = LifecycleState(state)
        tmp = self.path.with_suffix(".state.json.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        return payload

    def read(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, ValueError):
            return None
