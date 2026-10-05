"""
Data Harvester Service Supervisor (macOS launchd / manual).

Provides a resilient background process manager:
- Spawns and supervises target Python modules (e.g. streamer, dashboard)
- Owns the ingestion window (SCHED-02): with --enforce-window the streamer is
  only launched inside weekdays 04:00-20:00 ET, so no provider authentication or
  subscription happens off-hours, and the deliberate off-hours stop is never
  counted as a crash.
- Publishes a lifecycle state file (logs/<name>.state.json) with one of
  WAITING_FOR_WINDOW / STARTING / INGESTING / DRAINING / MAINTENANCE / ERROR.
- Runs the off-hours maintenance hook (compaction) under MAINTENANCE.
- Automatically restarts processes on code changes (watches src/ and .git/HEAD)
- Auto-heals on unexpected crashes with backoff protection
- Redirects output to rotating log files in logs/
- Handles clean shutdown signals
"""
import os
import sys
import time
import signal
import argparse
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

WATCH_EXTENSIONS = {".py", ".html", ".js", ".css", ".json"}
REPO_ROOT = Path(__file__).resolve().parent.parent
# Running this file directly (the macOS launchd agents do) puts `tools/` on
# sys.path, not the repo root, so make the package imports work either way.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.lifecycle import LifecycleRecorder, LifecycleState  # noqa: E402
from src.utils.session_window import describe as describe_window  # noqa: E402
from src.utils.session_window import is_eligible, next_open, now_et  # noqa: E402


def get_watched_files(watch_dirs):
    """Gathers all relevant source files and their modification timestamps."""
    files_mtime = {}
    for d in watch_dirs:
        dir_path = REPO_ROOT / d
        if not dir_path.exists():
            continue
        for p in dir_path.rglob("*"):
            if p.is_file() and p.suffix.lower() in WATCH_EXTENSIONS:
                try:
                    files_mtime[str(p)] = p.stat().st_mtime
                except OSError:
                    pass

    # Also track git HEAD / main branch so 'git pull' triggers instant reload
    git_head = REPO_ROOT / ".git" / "HEAD"
    if git_head.exists():
        try:
            files_mtime[str(git_head)] = git_head.stat().st_mtime
            # Track current branch ref if HEAD is symbolic
            with open(git_head, "r", encoding="utf-8") as f:
                ref_line = f.read().strip()
            if ref_line.startswith("ref: "):
                ref_path = REPO_ROOT / ".git" / ref_line[5:].strip()
                if ref_path.exists():
                    files_mtime[str(ref_path)] = ref_path.stat().st_mtime
        except Exception:
            pass

    return files_mtime


def rotate_log_if_needed(log_path, max_bytes=20 * 1024 * 1024):
    """Keeps log files manageable by renaming when exceeding max_bytes."""
    if log_path.exists() and log_path.stat().st_size > max_bytes:
        old_path = log_path.with_suffix(".old.log")
        try:
            if old_path.exists():
                old_path.unlink()
            log_path.rename(old_path)
        except Exception:
            pass


class ProcessSupervisor:
    def __init__(
        self,
        name: str,
        module: str,
        watch_dirs: Optional[List[str]] = None,
        git_sync_interval: int = 0,
        poll_interval: float = 2.0,
        backoff_factor: float = 3.0,
        max_backoff: float = 30.0,
        stability_threshold: float = 30.0,
        max_restarts: Optional[int] = None,
        module_args: Optional[List[str]] = None,
        extra_env: Optional[Dict[str, str]] = None,
        log_dir: Optional[Path] = None,
        handle_signals: bool = True,
        enforce_window: bool = False,
        clock: Optional[Callable[[], "datetime"]] = None,
        maintenance_hook: Optional[Callable[[], object]] = None,
    ):
        self.name = name
        self.module = module
        self.watch_dirs = watch_dirs or ["src"]
        self.git_sync_interval = git_sync_interval
        self.poll_interval = float(poll_interval)
        self.backoff_factor = float(backoff_factor)
        self.max_backoff = float(max_backoff)
        self.stability_threshold = float(stability_threshold)
        self.max_restarts = max_restarts
        self.module_args = list(module_args) if module_args else []
        self.extra_env = dict(extra_env) if extra_env else {}
        self.handle_signals = handle_signals
        self.last_git_sync = time.time()
        self.running = True
        self._stop_event = threading.Event()
        self.process = None
        self._child_log_file = None
        self._child_start_time = 0.0
        self._handoff_suspended = False
        self._lifecycle_lock = threading.RLock()
        self.last_child_exit_code = None
        self._consecutive_crashes = 0
        self.last_snapshot = get_watched_files(self.watch_dirs)

        # Ingestion-window ownership (SCHED-02). Non-ingestion services (the
        # dashboard) leave this off and stay up 24/7.
        self.enforce_window = bool(enforce_window)
        self._clock = clock
        self.maintenance_hook = maintenance_hook
        self._maintenance_ran_for: Optional[str] = None

        target_log_dir = log_dir if log_dir is not None else (REPO_ROOT / "logs")
        self.log_dir = Path(target_log_dir).resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = self.log_dir / f"{self.name}.log"
        self._recorder = LifecycleRecorder(self.log_dir, self.name)
        self._last_state: Optional[LifecycleState] = None

        if self.handle_signals:
            try:
                signal.signal(signal.SIGINT, self._handle_signal)
                signal.signal(signal.SIGTERM, self._handle_signal)
                if hasattr(signal, "SIGHUP"):
                    signal.signal(signal.SIGHUP, signal.SIG_IGN)
            except (ValueError, AttributeError):
                pass

    @property
    def child_pid(self) -> Optional[int]:
        return self.process.pid if self.process else None

    @property
    def is_running(self) -> bool:
        return self.running and (self.process is not None and self.process.poll() is None)

    @property
    def consecutive_crashes(self) -> int:
        return self._consecutive_crashes

    @property
    def lifecycle_state(self) -> Optional[LifecycleState]:
        """The last state the supervisor published (None before the first tick)."""
        return self._last_state or self._recorder.state

    def _now(self) -> "datetime":
        return self._clock() if self._clock is not None else now_et()

    def _record(self, state: LifecycleState, detail: str = "") -> LifecycleState:
        self._log(f"Lifecycle: {state.value}" + (f" — {detail}" if detail else ""))
        self._recorder.transition(
            state,
            detail=detail,
            window=describe_window(self._now()),
            pid=self.child_pid,
        )
        self._last_state = state
        return state

    def stop(self, timeout: float = 15.0):
        """Cleanly stops the supervisor and terminates the supervised child process."""
        self.running = False
        self._stop_event.set()
        self._stop_child(timeout=timeout)

    def _handle_signal(self, signum, frame):
        self._log(f"Received stop signal ({signum}). Stopping {self.name}...")
        self.stop(timeout=15.0)
        self._log(f"Child process stopped. Supervisor for {self.name} exiting cleanly.")
        sys.exit(0)

    def _log(self, message):
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        formatted = f"[{timestamp}] [{self.name}-supervisor] {message}"
        if sys.stdout is not None:
            try:
                print(formatted, flush=True)
            except Exception:
                pass
        try:
            rotate_log_if_needed(self.log_file_path)
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(formatted + "\n")
        except Exception:
            pass

    @property
    def is_handoff_suspended(self) -> bool:
        with self._lifecycle_lock:
            return self._handoff_suspended

    def _start_child(self):
        """Start one child unless a coordinated handoff currently owns lifecycle control."""
        with self._lifecycle_lock:
            if self._handoff_suspended:
                return None
            if self.process is not None and self.process.poll() is None:
                return self.process
            rotate_log_if_needed(self.log_file_path)
            python_exe = sys.executable
            if python_exe.lower().endswith("pythonw.exe"):
                cand = Path(python_exe).with_name("python.exe")
                if cand.exists():
                    python_exe = str(cand)

            cmd = [python_exe, "-m", self.module] + self.module_args
            self._log(f"Launching process: {' '.join(cmd)}")
            self._child_log_file = open(self.log_file_path, "a", encoding="utf-8", buffering=1)
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"
            env["PYTHONPATH"] = str(REPO_ROOT)
            env.update(self.extra_env)

            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000) if sys.platform == "win32" else 0
            self.process = subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=self._child_log_file,
                stderr=subprocess.STDOUT,
                env=env,
                creationflags=flags,
            )
            self._child_start_time = time.monotonic()
            self.last_child_exit_code = None
            self._log(f"Started child process PID={self.process.pid}")
            return self.process

    def _stop_child(self, timeout=15.0):
        """Stop the current child and return its observed exit code."""
        with self._lifecycle_lock:
            process = self.process
            if process is None:
                if self._child_log_file:
                    try:
                        self._child_log_file.flush()
                        self._child_log_file.close()
                    except Exception:
                        pass
                    self._child_log_file = None
                return self.last_child_exit_code
            pid = process.pid
            self._log(f"Stopping child process PID={pid}...")
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=timeout)
                        self._log(f"Child process PID={pid} exited (exitcode={process.returncode}).")
                    except subprocess.TimeoutExpired:
                        self._log(f"Process PID={pid} did not exit within {timeout}s; sending SIGKILL.")
                        process.kill()
                        process.wait()
                        self._log(f"Child process PID={pid} killed (exitcode={process.returncode}).")
                else:
                    self._log(f"Child process PID={pid} already exited (exitcode={process.returncode}).")
                self.last_child_exit_code = process.returncode
                return process.returncode
            except Exception as exc:
                self._log(f"Error stopping child PID={pid}: {exc}")
                raise
            finally:
                self.process = None
                if self._child_log_file:
                    try:
                        self._child_log_file.flush()
                        self._child_log_file.close()
                    except Exception:
                        pass
                    self._child_log_file = None

    def suspend_for_handoff(self, timeout: float = 15.0):
        """Fence automatic restart/reload and drain the managed child for cutover."""
        with self._lifecycle_lock:
            self._handoff_suspended = True
        return self._stop_child(timeout=timeout)

    def resume_after_handoff(self):
        """Re-enable supervisor lifecycle and start one fresh child after cutover."""
        with self._lifecycle_lock:
            if not self.running or self._stop_event.is_set():
                raise RuntimeError(f"Supervisor for {self.name} is stopping and cannot resume capture")
            self._handoff_suspended = False
            return self._start_child()

    def _check_code_changes(self):
        current_snapshot = get_watched_files(self.watch_dirs)
        changed_file = None

        for path, mtime in current_snapshot.items():
            if path not in self.last_snapshot or self.last_snapshot[path] != mtime:
                changed_file = path
                break

        if not changed_file and len(current_snapshot) != len(self.last_snapshot):
            changed_file = "file addition/deletion"

        if changed_file:
            self.last_snapshot = current_snapshot
            return changed_file
        return None

    def _check_git_sync(self):
        if self.git_sync_interval <= 0:
            return
        now = time.time()
        if now - self.last_git_sync >= self.git_sync_interval:
            self.last_git_sync = now
            try:
                flags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000) if sys.platform == "win32" else 0
                res = subprocess.run(
                    ["git", "pull", "--ff-only"],
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    text=True,
                    timeout=30,
                    creationflags=flags
                )
                output = (res.stdout or "").strip()
                if "Already up to date." not in output and res.returncode == 0:
                    self._log(f"Git pull pulled new commits: {output}")
            except Exception as e:
                self._log(f"Git auto-sync check encountered error: {e}")

    def _windowed_tick(self) -> LifecycleState:
        """One control decision for an ingestion service (SCHED-02).

        Inside the window the child runs; outside it the supervisor keeps the
        child stopped on purpose — no provider authentication, no subscription —
        and records that as a deliberate state, not a crash.
        """
        now = self._now()
        if not is_eligible(now):
            if self.process is not None:
                self._record(
                    LifecycleState.DRAINING,
                    detail="window closed; asking the streamer to drain and stop",
                )
                stop_code = self._stop_child()
                # An intentional stop is not a crash: no backoff, no crash count.
                self._consecutive_crashes = 0
                self._log(
                    f"Window closed; streamer stopped intentionally (exit={stop_code})."
                )
                # Report DRAINING for this tick; the next one settles into waiting.
                return LifecycleState.DRAINING

            gap_key = next_open(now).isoformat()
            if self.maintenance_hook is not None and self._maintenance_ran_for != gap_key:
                self._maintenance_ran_for = gap_key
                self._record(
                    LifecycleState.MAINTENANCE,
                    detail=f"off-hours maintenance for the interval opening {gap_key}",
                )
                try:
                    result = self.maintenance_hook()
                    self._log(f"Off-hours maintenance finished: {result}")
                except Exception as exc:  # noqa: BLE001 — maintenance must not kill the supervisor
                    self._log(f"Off-hours maintenance failed: {exc}")
                    return self._record(LifecycleState.ERROR, detail=f"maintenance failed: {exc}")
                return LifecycleState.MAINTENANCE

            return self._record(
                LifecycleState.WAITING_FOR_WINDOW, detail=describe_window(now)
            )

        if self.process is None:
            self._record(LifecycleState.STARTING, detail=f"window open: {describe_window(now)}")
            self._start_child()
            return LifecycleState.STARTING

        ret_code = self.process.poll()
        if ret_code is None:
            if self._consecutive_crashes > 0 and (
                time.monotonic() - self._child_start_time >= self.stability_threshold
            ):
                self._log(
                    f"Child process has run stably for >= {self.stability_threshold}s. "
                    f"Resetting consecutive crashes from {self._consecutive_crashes} to 0."
                )
                self._consecutive_crashes = 0
            return self._record(LifecycleState.INGESTING, detail=describe_window(now))

        # The child died inside the window: that is a genuine crash.
        self._consecutive_crashes += 1
        self._stop_child()
        self._record(
            LifecycleState.ERROR,
            detail=f"child exited with code {ret_code} inside the window "
                   f"(crash #{self._consecutive_crashes})",
        )
        if self.max_restarts is not None and self._consecutive_crashes >= self.max_restarts:
            self._log(
                f"Child process reached max restarts ({self._consecutive_crashes} >= "
                f"{self.max_restarts}). Exiting supervisor."
            )
            self.running = False
            return LifecycleState.ERROR

        backoff = min(self.max_backoff, self._consecutive_crashes * self.backoff_factor)
        self._log(
            f"Child process exited unexpectedly with code {ret_code}. "
            f"Auto-restarting in {backoff}s (crash #{self._consecutive_crashes})..."
        )
        if self._stop_event.wait(backoff):
            return LifecycleState.ERROR
        if self.running and not self._stop_event.is_set():
            self._record(LifecycleState.STARTING, detail="restarting after a crash")
            self._start_child()
            return LifecycleState.STARTING
        return LifecycleState.ERROR

    def tick(self) -> LifecycleState:
        """One control iteration; returns the lifecycle state after it."""
        if self.is_handoff_suspended:
            return self.lifecycle_state or LifecycleState.WAITING_FOR_WINDOW

        if self.enforce_window:
            return self._windowed_tick()

        # 1. Periodic git pull sync if configured
        self._check_git_sync()

        # 2. Check if code has changed
        changed = self._check_code_changes()
        if changed:
            self._log(f"Detected code change ({Path(changed).name}). Auto-restarting...")
            self._stop_child()
            if self._stop_event.wait(1.0):
                return self.lifecycle_state or LifecycleState.INGESTING
            self._start_child()
            self._consecutive_crashes = 0
            return self._record(LifecycleState.INGESTING, detail="restarted after a code change")

        # 3. Check child health
        if self.process is None:
            if self.running and not self._stop_event.is_set():
                self._start_child()
            return self.lifecycle_state or LifecycleState.INGESTING

        ret_code = self.process.poll()
        if ret_code is None:
            if self._consecutive_crashes > 0 and (
                time.monotonic() - self._child_start_time >= self.stability_threshold
            ):
                self._log(
                    f"Child process has run stably for >= {self.stability_threshold}s. "
                    f"Resetting consecutive crashes from {self._consecutive_crashes} to 0."
                )
                self._consecutive_crashes = 0
            return self._record(LifecycleState.INGESTING, detail="child healthy")

        self._consecutive_crashes += 1
        self._stop_child()
        self._record(
            LifecycleState.ERROR,
            detail=f"child exited with code {ret_code} (crash #{self._consecutive_crashes})",
        )

        if self.max_restarts is not None and self._consecutive_crashes >= self.max_restarts:
            self._log(
                f"Child process reached max restarts ({self._consecutive_crashes} >= "
                f"{self.max_restarts}). Exiting supervisor."
            )
            self.running = False
            return LifecycleState.ERROR

        backoff = min(self.max_backoff, self._consecutive_crashes * self.backoff_factor)
        self._log(
            f"Child process exited unexpectedly with code {ret_code}. "
            f"Auto-restarting in {backoff}s (crash #{self._consecutive_crashes})..."
        )
        if self._stop_event.wait(backoff):
            return LifecycleState.ERROR
        if self.running and not self._stop_event.is_set():
            self._start_child()
            self._record(LifecycleState.INGESTING, detail="restarted after a crash")
        return self.lifecycle_state or LifecycleState.ERROR

    def run(self):
        self._log(f"Supervisor started for {self.name}. Monitoring code in {self.watch_dirs}")
        if self.enforce_window:
            self._log(
                f"Ingestion window enforced (weekdays 04:00-20:00 ET): {describe_window(self._now())}"
            )
        else:
            self._start_child()

        while self.running and not self._stop_event.is_set():
            if self._stop_event.wait(self.poll_interval):
                break
            if self.is_handoff_suspended:
                continue
            self.tick()


def _maintenance_hook():
    """Off-hours compaction, imported lazily so the supervisor starts fast."""
    from src.storage.offhours import run_scheduled_compaction
    from src.utils.notifications import notify_detached

    return run_scheduled_compaction(notifier=notify_detached)


def main():
    parser = argparse.ArgumentParser(description="Data Harvester Service Supervisor")
    parser.add_argument("--name", required=True, help="Friendly service name (e.g. streamer, dashboard)")
    parser.add_argument("--module", required=True, help="Python module to execute (e.g. src.stream.runner)")
    parser.add_argument("--watch", nargs="+", default=["src"], help="Directories to watch for changes")
    parser.add_argument("--git-sync", type=int, default=0, help="Interval in seconds to check git pull (0=disabled)")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Process polling interval in seconds")
    parser.add_argument("--backoff-factor", type=float, default=3.0, help="Multiplier for exponential backoff")
    parser.add_argument("--max-backoff", type=float, default=30.0, help="Maximum backoff interval in seconds")
    parser.add_argument("--stability-threshold", type=float, default=30.0, help="Seconds before crash counter resets")
    parser.add_argument("--max-restarts", type=int, default=None, help="Maximum restart attempts before stopping")
    parser.add_argument("--log-dir", type=str, default=None, help="Directory to store log files")
    parser.add_argument("--args", nargs=argparse.REMAINDER, default=None, help="Additional CLI arguments to pass to child module")
    parser.add_argument(
        "--enforce-window",
        action="store_true",
        help="Only run the child inside the weekday 04:00-20:00 ET ingestion window (streamer only)",
    )
    parser.add_argument(
        "--maintenance",
        action="store_true",
        help="Run off-hours compaction (once per closed interval, after confirmed drain) while off the window",
    )

    args = parser.parse_args()
    supervisor = ProcessSupervisor(
        name=args.name,
        module=args.module,
        watch_dirs=args.watch,
        git_sync_interval=args.git_sync,
        poll_interval=args.poll_interval,
        backoff_factor=args.backoff_factor,
        max_backoff=args.max_backoff,
        stability_threshold=args.stability_threshold,
        max_restarts=args.max_restarts,
        log_dir=Path(args.log_dir) if args.log_dir else None,
        module_args=args.args,
        enforce_window=args.enforce_window,
        maintenance_hook=_maintenance_hook if args.maintenance else None,
    )
    supervisor.run()


if __name__ == "__main__":
    main()
