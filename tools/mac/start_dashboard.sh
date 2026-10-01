#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS Dashboard Launcher
# Starts only the Observability Command Center Dashboard under supervisor.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "❌ ERROR: Python executable not found!"
    exit 1
fi

mkdir -p "$REPO_ROOT/logs"

# Stop any running dashboard first
"$PYTHON_BIN" -c "
import psutil, os
for p in psutil.process_iter(['name', 'cmdline']):
    try:
        cmdline = p.info['cmdline'] or []
        if p.pid != os.getpid() and not any('-c' == arg for arg in cmdline):
            if any('dashboard' in arg for arg in cmdline) and any('service_supervisor.py' in arg or 'src.dashboard.server' in arg for arg in cmdline):
                p.terminate()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
" 2>/dev/null || true
sleep 1

echo "Starting Dashboard supervisor (port 8420)..."
nohup "$PYTHON_BIN" "$REPO_ROOT/tools/service_supervisor.py" --name dashboard --module src.dashboard.server >/dev/null 2>&1 &
disown -h $! 2>/dev/null || true
echo "✓ Dashboard started at http://localhost:8420. Logs: $REPO_ROOT/logs/dashboard.log"
