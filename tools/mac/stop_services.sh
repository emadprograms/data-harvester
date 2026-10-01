#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS Background Services Stopper
# Stops all supervisor, streamer, and dashboard processes cleanly.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

echo "Stopping Data Harvester background services on macOS..."

# Resolve Python executable
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -c "
import psutil, os, time

targets = []
for p in psutil.process_iter(['name', 'cmdline']):
    try:
        cmdline = p.info['cmdline'] or []
        if p.pid != os.getpid() and not any('-c' == arg for arg in cmdline):
            if any('service_supervisor.py' in arg or 'src.stream.runner' in arg or 'src.dashboard.server' in arg for arg in cmdline):
                targets.append(p)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

if not targets:
    print('  (No running Data Harvester processes found)')
else:
    for p in targets:
        try:
            print(f'  Stopping PID {p.pid} ({p.name()})...')
            p.terminate()
        except Exception:
            pass

    # Wait up to 4s for graceful shutdown
    gone, alive = psutil.wait_procs(targets, timeout=4)
    for p in alive:
        try:
            print(f'  Force killing lingering PID {p.pid}...')
            p.kill()
        except Exception:
            pass
    print('✓ All Data Harvester background services have been stopped.')
"

echo ""
