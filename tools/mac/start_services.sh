#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS Background Services Launcher
# Starts 24/7 Capital.com Streamer and Observability Dashboard under supervisor.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

echo "===================================================="
echo "  Data Harvester - 24/7 macOS Services Launcher"
echo "===================================================="
echo "Repo Root: $REPO_ROOT"

# 1. Resolve Python executable
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "❌ ERROR: Python executable not found! Ensure .venv or python3 exists."
    exit 1
fi
echo "Python:    $PYTHON_BIN"

# 2. Ensure logs directory exists
mkdir -p "$REPO_ROOT/logs"

# 3. Stop any existing runs first to prevent duplicate supervisors
echo "Stopping any existing background services..."
"$PYTHON_BIN" -c "
import psutil, os
for p in psutil.process_iter(['name', 'cmdline']):
    try:
        cmdline = p.info['cmdline'] or []
        if p.pid != os.getpid() and not any('-c' == arg for arg in cmdline):
            if any('service_supervisor.py' in arg or 'src.stream.runner' in arg or 'src.dashboard.server' in arg for arg in cmdline):
                p.terminate()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
" 2>/dev/null || true
sleep 1

# 4. Launch Streamer under Supervisor
echo "Starting Streamer supervisor (Capital.com 24/7 tick engine)..."
nohup "$PYTHON_BIN" "$REPO_ROOT/tools/service_supervisor.py" --name streamer --module src.stream.runner >/dev/null 2>&1 &
STREAMER_SUPERVISOR_PID=$!

# 5. Launch Dashboard under Supervisor
echo "Starting Dashboard supervisor (Command Center on port 8420)..."
nohup "$PYTHON_BIN" "$REPO_ROOT/tools/service_supervisor.py" --name dashboard --module src.dashboard.server >/dev/null 2>&1 &
DASHBOARD_SUPERVISOR_PID=$!

sleep 2

# 6. Verify processes
echo ""
echo "===================================================="
echo "  Active Supervisor Processes"
echo "===================================================="
"$PYTHON_BIN" -c "
import psutil, os
pids = [(p.pid, ' '.join(p.info['cmdline'])) for p in psutil.process_iter(['name', 'cmdline'])
        if p.info['name'] and 'python' in p.info['name'].lower()
        and p.pid != os.getpid()
        and any('service_supervisor.py' in arg for arg in (p.info['cmdline'] or []))
        and not any('-c' == arg for arg in (p.info['cmdline'] or []))]
if pids:
    for pid, cmd in pids:
        print(f'  ✓ PID {pid}: {cmd[:80]}...')
else:
    print('  ⚠️ Warning: No active supervisors detected!')
"

echo ""
echo "🚀 Services successfully launched in the background!"
echo "   - Streamer:  Ingesting live ticks into data/streaming.duckdb"
echo "   - Dashboard: Running at http://localhost:8420"
echo "   - Auto-Reload: Watching src/ and git updates"
echo "   - Logs:      $REPO_ROOT/logs/"
echo ""
echo "To check status: ./tools/mac/status_services.sh"
echo "To stop:         ./tools/mac/stop_services.sh"
