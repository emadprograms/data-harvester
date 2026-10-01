#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS Background Services Status Inspector
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

echo "===================================================="
echo "  Data Harvester - macOS Services Status"
echo "===================================================="
echo ""

# Resolve Python executable
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -c "
import psutil, os

def find_procs(pattern):
    matches = []
    for p in psutil.process_iter(['name', 'cmdline', 'cpu_percent', 'memory_info']):
        try:
            cmdline = p.info['cmdline'] or []
            if p.pid != os.getpid() and not any('-c' == arg for arg in cmdline):
                if any(pattern in arg for arg in cmdline):
                    mem_mb = (p.info['memory_info'].rss / (1024 * 1024)) if p.info['memory_info'] else 0
                    matches.append((p.pid, ' '.join(cmdline), mem_mb))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return matches

supervisors = find_procs('service_supervisor.py')
streamers = find_procs('src.stream.runner')
dashboards = find_procs('src.dashboard.server')

print(f'Supervisors Running: {len(supervisors)}')
for pid, cmd, mem in supervisors:
    print(f'  • PID {pid} (RAM: {mem:.1f} MB): {cmd[:75]}...')

print(f'\nWorker Processes:')
if streamers:
    for pid, cmd, mem in streamers:
        print(f'  • [Streamer]  PID {pid} (RAM: {mem:.1f} MB)')
else:
    print('  • [Streamer]  (Not running)')

if dashboards:
    for pid, cmd, mem in dashboards:
        print(f'  • [Dashboard] PID {pid} (RAM: {mem:.1f} MB)')
else:
    print('  • [Dashboard] (Not running)')
"

echo ""
echo "--- Dashboard Port 8420 Check ---"
if curl -s -f -o /dev/null "http://localhost:8420/" 2>/dev/null; then
    echo "  ✓ Dashboard is ONLINE at http://localhost:8420"
else
    echo "  ⚠️ Dashboard is NOT responding at http://localhost:8420"
fi

echo ""
echo "--- Recent Streamer Log (last 10 lines) ---"
if [ -f "$REPO_ROOT/logs/streamer.log" ]; then
    tail -n 10 "$REPO_ROOT/logs/streamer.log"
else
    echo "  (No streamer log file found yet)"
fi

echo ""
echo "--- Recent Dashboard Log (last 10 lines) ---"
if [ -f "$REPO_ROOT/logs/dashboard.log" ]; then
    tail -n 10 "$REPO_ROOT/logs/dashboard.log"
else
    echo "  (No dashboard log file found yet)"
fi

echo ""
echo "===================================================="
