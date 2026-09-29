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
from pathlib import Path
from datetime import datetime, timezone

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
    def __init__(self, name, module, watch_dirs=None, git_sync_interval=0):
        self.name = name
        self.module = module
        self.watch_dirs = watch_dirs or ["src"]
        self.git_sync_interval = git_sync_interval
        self.last_git_sync = time.time()
        self.running = True
        self.process = None
        self.last_snapshot = get_watched_files(self.watch_dirs)

        log_dir = REPO_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = log_dir / f"{self.name}.log"

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self._log(f"Received stop signal ({signum}). Stopping {self.name}...")
        self.running = False
        self._stop_child()
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

    def _start_child(self):
        rotate_log_if_needed(self.log_file_path)
        python_exe = sys.executable
        if python_exe.lower().endswith("pythonw.exe"):
            cand = Path(python_exe).with_name("python.exe")
            if cand.exists():
                python_exe = str(cand)

        self._log(f"Launching process: {python_exe} -m {self.module}")
        log_file = open(self.log_file_path, "a", encoding="utf-8", buffering=1)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        env["PYTHONPATH"] = str(REPO_ROOT)

        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000) if sys.platform == "win32" else 0
        self.process = subprocess.Popen(
            [python_exe, "-m", self.module],
            cwd=str(REPO_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=flags
        )
        self._log(f"Started child process PID={self.process.pid}")

    def _stop_child(self, timeout=6):
        if not self.process:
            return
        pid = self.process.pid
        self._log(f"Stopping child process PID={pid}...")
        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
                self._log(f"Child process PID={pid} exited cleanly.")
            except subprocess.TimeoutExpired:
                self._log(f"Process PID={pid} did not exit within {timeout}s; sending SIGKILL.")
                self.process.kill()
                self.process.wait()
        except Exception as e:
            self._log(f"Error stopping child PID={pid}: {e}")
        finally:
            self.process = None

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

        consecutive_crashes = 0

        while self.running:
            time.sleep(2)

            # 1. Periodic git pull sync if configured
            self._check_git_sync()

            # 2. Check if code has changed
            changed = self._check_code_changes()
            if changed:
                self._log(f"Detected code change ({Path(changed).name}). Auto-restarting...")
                self._stop_child()
                time.sleep(1)
                self._start_child()
                consecutive_crashes = 0
                continue

            # 3. Check child health
            ret_code = self.process.poll()
            if ret_code is not None:
                consecutive_crashes += 1
                backoff = min(30, consecutive_crashes * 3)
                self._log(f"Child process exited unexpectedly with code {ret_code}. Auto-restarting in {backoff}s...")
                time.sleep(backoff)
                if self.running:
                    self._start_child()


def main():
    parser = argparse.ArgumentParser(description="Data Harvester Service Supervisor")
    parser.add_argument("--name", required=True, help="Friendly service name (e.g. streamer, dashboard)")
    parser.add_argument("--module", required=True, help="Python module to execute (e.g. src.stream.runner)")
    parser.add_argument("--watch", nargs="+", default=["src"], help="Directories to watch for changes")
    parser.add_argument("--git-sync", type=int, default=0, help="Interval in seconds to check git pull (0=disabled)")

    args = parser.parse_args()
    supervisor = ProcessSupervisor(
        name=args.name,
        module=args.module,
        watch_dirs=args.watch,
        git_sync_interval=args.git_sync
    )
    supervisor.run()


if __name__ == "__main__":
    main()
