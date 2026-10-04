"""Spawned-process barriers with bounded joins and unconditional cleanup."""
import multiprocessing
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0, interval: float = 0.05) -> bool:
    """Poll `predicate` until it is true or `timeout` elapses.

    Deterministic replacement for fixed ``time.sleep`` synchronization: it waits
    only as long as the condition actually needs, which keeps tests stable on
    slower hosts (CI runners, loaded machines) without widening any product SLA.
    """
    deadline = time.monotonic() + float(timeout)
    while True:
        try:
            if predicate():
                return True
        except Exception:
            # Predicates may inspect not-yet-initialized state (e.g. a child
            # that has not written its readiness file yet); keep polling.
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(float(interval))


def wait_until_or_fail(
    predicate: Callable[[], bool],
    description: str,
    timeout: float = 10.0,
    interval: float = 0.05,
) -> float:
    """Wait for `predicate`, raising AssertionError with elapsed-time evidence."""
    started = time.monotonic()
    if wait_until(predicate, timeout=timeout, interval=interval):
        return time.monotonic() - started
    raise AssertionError(
        f"Timed out after {timeout}s waiting for {description} "
        f"(poll interval {interval}s)"
    )


def barrier_file_worker(ready, release, committed, output_path):
    """Tiny test worker proving barriers, without sleep-based synchronization."""
    from pathlib import Path

    ready.set()
    if not release.wait(20.0):
        return
    Path(output_path).write_text("committed", encoding="utf-8")
    committed.set()


def hold_lake_publisher_lock(ready, release, committed, root, writer_id="barrier-writer"):
    """Child-process ownership barrier used to test exclusive writer/migration handoff."""
    from src.storage.publication import LakePublisherLock

    with LakePublisherLock(root, writer_id=writer_id):
        ready.set()
        if release.wait(20.0):
            committed.set()


@dataclass
class BarrierProcess:
    target: Callable[..., Any]
    args: Sequence[Any] = ()
    kwargs: Optional[dict] = None
    start_method: str = "spawn"

    def __post_init__(self):
        self.context = multiprocessing.get_context(self.start_method)
        self.ready = self.context.Event()
        self.release = self.context.Event()
        self.committed = self.context.Event()
        self.process = self.context.Process(
            target=self.target,
            args=(self.ready, self.release, self.committed, *self.args),
            kwargs=self.kwargs or {},
        )

    def start(self):
        self.process.start()
        return self

    def wait_ready(self, timeout: float = 10.0) -> bool:
        return self.ready.wait(timeout)

    def allow(self) -> None:
        self.release.set()

    def wait_committed(self, timeout: float = 10.0) -> bool:
        return self.committed.wait(timeout)

    def join(self, timeout: float = 10.0) -> Optional[int]:
        self.process.join(timeout)
        return self.process.exitcode

    def close(self, timeout: float = 5.0) -> None:
        """Always unblock, join, then terminate/kill if the child ignores cleanup."""
        self.release.set()
        if self.process.pid is None:
            return
        self.process.join(timeout)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout)
        if self.process.is_alive() and hasattr(self.process, "kill"):
            self.process.kill()
            self.process.join(timeout)
        if self.process.is_alive():
            raise RuntimeError(f"child process {self.process.pid} survived cleanup")

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
