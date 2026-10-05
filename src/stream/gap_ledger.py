"""
Provider Gap Ledger & Disconnect Tracking (Milestone 4.3 - DURB-03).

Records visible capture, disconnect, backpressure, and supervisor handoff gaps in:
  <lake_root>/_control/gaps.json

Guarantees:
  - Each gap entry records: gap_id, provider/source, symbol, start_time, end_time, reason, status.
  - When exact count of unreceived provider ticks cannot be known, status is 'LOSS_UNKNOWN'.
  - Never invent a fake count of lost ticks during disconnects.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import threading
from typing import Any, Dict, List, Optional, Union
import uuid

logger = logging.getLogger("gap_ledger")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class GapEntry:
    gap_id: str
    provider: str
    symbol: str = "all"
    start_time: str = field(default_factory=_utc_now_iso)
    end_time: Optional[str] = None
    reason: str = "DISCONNECT"
    status: str = "LOSS_UNKNOWN"
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def source(self) -> str:
        return self.provider

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "provider": self.provider,
            "source": self.provider,
            "symbol": self.symbol,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "reason": self.reason,
            "status": self.status,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GapEntry":
        return cls(
            gap_id=str(data["gap_id"]),
            provider=str(data.get("provider") or data.get("source", "UNKNOWN")),
            symbol=str(data.get("symbol", "all")),
            start_time=str(data.get("start_time", _utc_now_iso())),
            end_time=data.get("end_time"),
            reason=str(data.get("reason", "DISCONNECT")),
            status=str(data.get("status", "LOSS_UNKNOWN")),
            details=dict(data.get("details") or {}),
        )


class GapLedger:
    """
    Durable ledger for tracking provider capture gaps, disconnections,
    buffer overflows, and supervisor handoffs.
    """

    def __init__(self, root: Union[str, Path]):
        self.root = Path(root).resolve()
        self.control_dir = self.root / "_control"
        self.staging_dir = self.root / "_staging"
        self.ledger_path = self.control_dir / "gaps.json"
        self._lock = threading.RLock()

    def _ensure_dirs(self) -> None:
        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    def _load_gaps_locked(self) -> List[GapEntry]:
        if not self.ledger_path.is_file():
            return []
        try:
            with open(self.ledger_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [GapEntry.from_dict(item) for item in data if isinstance(item, dict)]
            if isinstance(data, dict) and "gaps" in data and isinstance(data["gaps"], list):
                return [GapEntry.from_dict(item) for item in data["gaps"] if isinstance(item, dict)]
            return []
        except Exception as e:
            logger.warning("Failed to load gap ledger from %s: %s", self.ledger_path, e)
            return []

    def _save_gaps_locked(self, entries: List[GapEntry]) -> None:
        self._ensure_dirs()
        payload = [entry.to_dict() for entry in entries]
        tmp_path = self.staging_dir / f"tmp_gaps_{uuid.uuid4().hex}.json"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.ledger_path)

    def open_gap(
        self,
        provider: str,
        symbol: str = "all",
        reason: str = "DISCONNECT",
        status: str = "LOSS_UNKNOWN",
        start_time: Optional[Union[str, datetime]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Record the start of a gap (e.g. on provider disconnect or backpressure surge).
        Returns the unique gap_id.
        """
        if isinstance(start_time, datetime):
            st_str = start_time.astimezone(timezone.utc).isoformat()
        elif isinstance(start_time, str):
            st_str = start_time
        else:
            st_str = _utc_now_iso()

        gap_id = f"gap_{uuid.uuid4().hex[:12]}"
        entry = GapEntry(
            gap_id=gap_id,
            provider=provider,
            symbol=symbol,
            start_time=st_str,
            end_time=None,
            reason=reason,
            status=status,
            details=details or {},
        )

        with self._lock:
            entries = self._load_gaps_locked()
            entries.append(entry)
            self._save_gaps_locked(entries)

        logger.info("Opened provider gap %s (provider=%s, symbol=%s, reason=%s)", gap_id, provider, symbol, reason)
        return gap_id

    def close_gap(
        self,
        gap_id: str,
        end_time: Optional[Union[str, datetime]] = None,
        details_update: Optional[Dict[str, Any]] = None,
    ) -> Optional[GapEntry]:
        """
        Close an ongoing gap (e.g. on provider reconnect).
        """
        if isinstance(end_time, datetime):
            et_str = end_time.astimezone(timezone.utc).isoformat()
        elif isinstance(end_time, str):
            et_str = end_time
        else:
            et_str = _utc_now_iso()

        with self._lock:
            entries = self._load_gaps_locked()
            target: Optional[GapEntry] = None
            for e in entries:
                if e.gap_id == gap_id:
                    target = e
                    break
            if target is None:
                logger.warning("Attempted to close nonexistent gap %s", gap_id)
                return None

            target.end_time = et_str
            if details_update:
                target.details.update(details_update)
            self._save_gaps_locked(entries)

        logger.info("Closed provider gap %s (ended at %s)", gap_id, et_str)
        return target

    def record_gap(
        self,
        provider: str,
        symbol: str = "all",
        reason: str = "DISCONNECT",
        status: str = "LOSS_UNKNOWN",
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> GapEntry:
        """
        Record a completed or instant gap incident (e.g., buffer drop event or supervisor handoff).
        """
        if isinstance(start_time, datetime):
            st_str = start_time.astimezone(timezone.utc).isoformat()
        elif isinstance(start_time, str):
            st_str = start_time
        else:
            st_str = _utc_now_iso()

        if isinstance(end_time, datetime):
            et_str = end_time.astimezone(timezone.utc).isoformat()
        elif isinstance(end_time, str):
            et_str = end_time
        elif end_time is None:
            et_str = st_str
        else:
            et_str = str(end_time)

        gap_id = f"gap_{uuid.uuid4().hex[:12]}"
        entry = GapEntry(
            gap_id=gap_id,
            provider=provider,
            symbol=symbol,
            start_time=st_str,
            end_time=et_str,
            reason=reason,
            status=status,
            details=details or {},
        )

        with self._lock:
            entries = self._load_gaps_locked()
            entries.append(entry)
            self._save_gaps_locked(entries)

        logger.info("Recorded gap %s (provider=%s, symbol=%s, reason=%s)", gap_id, provider, symbol, reason)
        return entry

    def read_gaps(
        self,
        symbol: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Read all persisted gaps from _control/gaps.json, optionally filtered.
        """
        with self._lock:
            entries = self._load_gaps_locked()

        results = []
        for e in entries:
            if symbol is not None and e.symbol not in (symbol, "all"):
                continue
            if provider is not None and e.provider != provider:
                continue
            results.append(e.to_dict())
        return results

    def clear(self) -> None:
        """Clear all entries in the ledger file (used for isolated tests)."""
        with self._lock:
            if self.ledger_path.is_file():
                self.ledger_path.unlink()
