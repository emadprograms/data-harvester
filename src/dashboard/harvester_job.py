"""
Harvester Job Runner for Web Dashboard.
Executes main.py asynchronously in a background subprocess,
captures stdout/stderr into a thread-safe ring buffer,
and tracks execution status.
"""
import os
import sys
import time
import subprocess
import threading
from collections import deque
from datetime import datetime, timezone

class HarvesterJobManager:
    def __init__(self, max_log_lines=1000):
        self._lock = threading.Lock()
        self._max_log_lines = max_log_lines
        self.logs = deque(maxlen=max_log_lines)
        self.status = "IDLE"  # IDLE, RUNNING, COMPLETED, FAILED
        self.current_job_id = None
        self.start_time = None
        self.end_time = None
        self.exit_code = None
        self.target_date = None
        self._process = None

    def start_job(self, date_str=None):
        with self._lock:
            if self.status == "RUNNING":
                return False, "A harvest job is already in progress.", self.get_status()

            self.status = "RUNNING"
            self.current_job_id = f"harvest_{int(time.time())}"
            self.start_time = datetime.now(timezone.utc).isoformat()
            self.end_time = None
            self.exit_code = None
            self.target_date = date_str
            self.logs.clear()
            self.logs.append(f"[{self.start_time}] 🚀 Starting harvest job (Target date: {date_str or 'Auto-detect'})...\n")

            # Determine python executable and project root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            python_bin = sys.executable

            cmd = [python_bin, "main.py"]
            if date_str:
                cmd.extend(["--date", date_str])

            thread = threading.Thread(target=self._run_subprocess, args=(cmd, project_root), daemon=True)
            thread.start()
            return True, f"Harvest job {self.current_job_id} initiated.", self.get_status()

    def _run_subprocess(self, cmd, cwd):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONPATH"] = cwd

        try:
            self._process = subprocess.Popen(
                cmd,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            for line in self._process.stdout:
                with self._lock:
                    self.logs.append(line)

            self._process.wait()
            ret_code = self._process.returncode

            with self._lock:
                self.exit_code = ret_code
                self.end_time = datetime.now(timezone.utc).isoformat()
                if ret_code == 0:
                    self.status = "COMPLETED"
                    self.logs.append(f"\n[{self.end_time}] ✅ Harvest completed successfully (exit code 0).\n")
                else:
                    self.status = "FAILED"
                    self.logs.append(f"\n[{self.end_time}] ❌ Harvest failed with exit code {ret_code}.\n")

        except Exception as e:
            with self._lock:
                self.exit_code = -1
                self.status = "FAILED"
                self.end_time = datetime.now(timezone.utc).isoformat()
                self.logs.append(f"\n[{self.end_time}] ❌ Execution exception: {e}\n")
        finally:
            self._process = None

    def get_status(self):
        with self._lock:
            return {
                "job_id": self.current_job_id,
                "status": self.status,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "exit_code": self.exit_code,
                "target_date": self.target_date,
                "log_count": len(self.logs),
                "recent_logs": list(self.logs)[-100:]  # last 100 lines
            }

    def get_all_logs(self):
        with self._lock:
            return list(self.logs)


# Global singleton instance
harvester_manager = HarvesterJobManager()
