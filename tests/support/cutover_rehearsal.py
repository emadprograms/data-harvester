"""
Cutover rehearsal harness (Phase 34 / MIGR-04).

The product ships no cutover script, so the operational sequence is encoded here
and then tested. That is deliberate: the point of the requirement is that the
rehearsal *behaves honestly* — a stalled drain must abort before publication, and
a failed restart must not be reported as success — and that can only be checked
against an executable sequence.

The steps mirror the runbook: stop and drain the live writer, capture a frozen
source snapshot, release lake ownership, publish the migration, restart the live
writer, and reconcile. Each step is recorded, and a failure aborts the run
instead of being quietly skipped.

This harness is test scaffolding for the rehearsal, not production tooling. If
the procedure is promoted to an operational runbook, it should be lifted from
here rather than rewritten.
"""

from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional

from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import LakePublisherLock


@dataclass
class Step:
    name: str
    status: str  # PASSED | FAILED | SKIPPED
    detail: str = ""


@dataclass
class RehearsalResult:
    steps: List[Step] = field(default_factory=list)
    published: bool = False
    restarted: bool = False

    @property
    def published_rows_present(self) -> bool:
        return self.published

    def step(self, name: str) -> Optional[Step]:
        for entry in self.steps:
            if entry.name == name:
                return entry
        return None

    @property
    def failed_step(self) -> Optional[Step]:
        for entry in self.steps:
            if entry.status == "FAILED":
                return entry
        return None

    def report(self) -> str:
        lines = [f"  {s.name:<22} {s.status}{(' — ' + s.detail) if s.detail else ''}" for s in self.steps]
        return "\n".join(lines)


def _default_drain(writer: TickLakeWriter) -> None:
    """Graceful stop: flush everything buffered, then release."""
    writer.flush(block=True)
    writer.close()


class CutoverRehearsal:
    """Run the cutover sequence with injectable drain and restart behaviour."""

    def __init__(
        self,
        lake: Path,
        source_db: Path,
        *,
        writer: Optional[TickLakeWriter] = None,
        drain: Optional[Callable[[TickLakeWriter], None]] = None,
        restart: Optional[Callable[[Path], object]] = None,
        publish: Optional[Callable[[], object]] = None,
        reconcile: Optional[Callable[[], tuple]] = None,
        coordinator: Optional[Any] = None,
        supervisor: Optional[Any] = None,
    ) -> None:
        self.lake = Path(lake)
        self.source_db = Path(source_db)
        self.writer = writer
        self.coordinator = coordinator
        self.supervisor = supervisor
        self._drain = drain or _default_drain
        self._restart = restart or (lambda root: TickLakeWriter(root=root, writer_id="post_cutover", max_batch_rows=10**9))
        self._publish = publish or (coordinator.execute if coordinator is not None else None)
        self._reconcile = reconcile

    # -- steps ---------------------------------------------------------
    def _stop_and_drain(self, result: RehearsalResult) -> bool:
        try:
            if self.coordinator is not None:
                pass
            elif self.supervisor is not None and self._drain is _default_drain:
                exit_code = self.supervisor.suspend_for_handoff(timeout=15.0)
                if exit_code not in (0, None):
                    raise RuntimeError(f"Supervisor suspend failed: {exit_code}")
            elif self.writer is not None:
                self._drain(self.writer)
            else:
                self._drain(None)
        except Exception as exc:  # noqa: BLE001 - the step outcome is the point
            result.steps.append(Step("stop_and_drain", "FAILED", f"{type(exc).__name__}: {exc}"))
            return False
        result.steps.append(Step("stop_and_drain", "PASSED"))
        return True

    def _capture_frozen_source(self, result: RehearsalResult) -> Optional[Path]:
        frozen = self.lake.parent / "frozen_source.duckdb"
        try:
            shutil.copy2(self.source_db, frozen)
            digest = hashlib.sha256(frozen.read_bytes()).hexdigest()
        except Exception as exc:  # noqa: BLE001
            result.steps.append(Step("capture_frozen_source", "FAILED", f"{type(exc).__name__}: {exc}"))
            return None
        result.steps.append(Step("capture_frozen_source", "PASSED", digest[:16]))
        return frozen

    def _release_ownership(self, result: RehearsalResult) -> bool:
        """Ownership must be free, and no competing writer may hold the lock."""
        try:
            lock = LakePublisherLock(self.lake, writer_id="cutover_probe")
            acquired = lock.acquire(blocking=False)
            if not acquired:
                result.steps.append(
                    Step("release_ownership", "FAILED", "another writer still owns the lake")
                )
                return False
            lock.release()
        except Exception as exc:  # noqa: BLE001
            result.steps.append(Step("release_ownership", "FAILED", f"{type(exc).__name__}: {exc}"))
            return False
        result.steps.append(Step("release_ownership", "PASSED"))
        return True

    def run(self) -> RehearsalResult:
        result = RehearsalResult()

        if not self._stop_and_drain(result):
            # A stalled drain must abort before anything is published.
            result.steps.append(Step("capture_frozen_source", "SKIPPED"))
            result.steps.append(Step("release_ownership", "SKIPPED"))
            result.steps.append(Step("publish", "SKIPPED", "aborted: drain did not complete"))
            result.steps.append(Step("restart", "SKIPPED"))
            result.steps.append(Step("reconcile", "SKIPPED"))
            return result

        frozen = self._capture_frozen_source(result)
        if frozen is None or not self._release_ownership(result):
            result.steps.append(Step("publish", "SKIPPED", "aborted: preconditions unmet"))
            result.steps.append(Step("restart", "SKIPPED"))
            result.steps.append(Step("reconcile", "SKIPPED"))
            return result

        try:
            if self._publish is not None:
                self._publish()
            elif self.coordinator is not None:
                self.coordinator.execute()
            else:
                raise RuntimeError("No publish callable or coordinator provided")
        except Exception as exc:  # noqa: BLE001
            result.steps.append(Step("publish", "FAILED", f"{type(exc).__name__}: {exc}"))
            result.steps.append(Step("restart", "SKIPPED"))
            result.steps.append(Step("reconcile", "SKIPPED"))
            return result
        result.steps.append(Step("publish", "PASSED"))
        result.published = True

        try:
            self._restart(self.lake)
            if self.supervisor is not None and getattr(self.supervisor, "is_handoff_suspended", False):
                self.supervisor.resume_after_handoff()
        except Exception as exc:  # noqa: BLE001
            # Publication already happened; the run must not claim success.
            result.steps.append(Step("restart", "FAILED", f"{type(exc).__name__}: {exc}"))
            result.steps.append(Step("reconcile", "SKIPPED"))
            return result
        result.steps.append(Step("restart", "PASSED"))
        result.restarted = True

        try:
            missing, extra = self._reconcile()
        except Exception as exc:  # noqa: BLE001
            result.steps.append(Step("reconcile", "FAILED", f"{type(exc).__name__}: {exc}"))
            return result
        if missing or extra:
            result.steps.append(
                Step("reconcile", "FAILED", f"missing={missing} extra={extra}")
            )
            return result
        result.steps.append(Step("reconcile", "PASSED"))
        return result
