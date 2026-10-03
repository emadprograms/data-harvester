"""
Data Harvester Service Supervisor.

Provides a resilient 24/7 background process manager for Windows and macOS.
Features:
- Spawns and supervises target Python modules (e.g. streamer, dashboard)
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
from typing import Dict, List, Optional

WATCH_EXTENSIONS = {".py", ".html", ".js", ".css", ".json"}
REPO_ROOT = Path(__file__).resolve().parent.parent


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

        target_log_dir = log_dir if log_dir is not None else (REPO_ROOT / "logs")
        self.log_dir = Path(target_log_dir).resolve()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = self.log_dir / f"{self.name}.log"

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

    def run(self):
        self._log(f"Supervisor started for {self.name}. Monitoring code in {self.watch_dirs}")
        self._start_child()

        while self.running and not self._stop_event.is_set():
            if self._stop_event.wait(self.poll_interval):
                break
            if self.is_handoff_suspended:
                continue

            # 1. Periodic git pull sync if configured
            self._check_git_sync()

            # 2. Check if code has changed
            changed = self._check_code_changes()
            if changed:
                self._log(f"Detected code change ({Path(changed).name}). Auto-restarting...")
                self._stop_child()
                if self._stop_event.wait(1.0):
                    break
                self._start_child()
                self._consecutive_crashes = 0
                continue

            # 3. Check child health
            if self.process is None:
                if self.running and not self._stop_event.is_set():
                    self._start_child()
                continue

            ret_code = self.process.poll()
            if ret_code is None:
                if self._consecutive_crashes > 0 and (time.monotonic() - self._child_start_time >= self.stability_threshold):
                    self._log(
                        f"Child process has run stably for >= {self.stability_threshold}s. "
                        f"Resetting consecutive crashes from {self._consecutive_crashes} to 0."
                    )
                    self._consecutive_crashes = 0
            else:
                self._consecutive_crashes += 1
                self._stop_child()

                if self.max_restarts is not None and self._consecutive_crashes >= self.max_restarts:
                    self._log(
                        f"Child process reached max restarts ({self._consecutive_crashes} >= {self.max_restarts}). "
                        f"Exiting supervisor."
                    )
                    self.running = False
                    break

                backoff = min(self.max_backoff, self._consecutive_crashes * self.backoff_factor)
                self._log(
                    f"Child process exited unexpectedly with code {ret_code}. "
                    f"Auto-restarting in {backoff}s (crash #{self._consecutive_crashes})..."
                )
                if self._stop_event.wait(backoff):
                    break
                if self.running and not self._stop_event.is_set():
                    self._start_child()


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
    )
    supervisor.run()


if __name__ == "__main__":
    main()
