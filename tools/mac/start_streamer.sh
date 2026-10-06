#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS Streamer Launcher
# Starts only the 24/7 Capital.com live tick ingestion streamer under supervisor.
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

# Stop any running streamer first
"$PYTHON_BIN" -c "
import psutil, os
for p in psutil.process_iter(['name', 'cmdline']):
    try:
        cmdline = p.info['cmdline'] or []
        if p.pid != os.getpid() and not any('-c' == arg for arg in cmdline):
            if any('streamer' in arg for arg in cmdline) and any('service_supervisor.py' in arg or 'src.stream.runner' in arg for arg in cmdline):
                p.terminate()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
" 2>/dev/null || true
sleep 1

TICK_LAKE_PATH="${TICK_LAKE_ROOT:-$REPO_ROOT/data/tick_lake}"
echo "Tick Lake: $TICK_LAKE_PATH"

echo "Starting Streamer supervisor (Capital.com 24/7 tick engine)..."
nohup "$PYTHON_BIN" "$REPO_ROOT/tools/service_supervisor.py" --name streamer --module src.stream.runner --enforce-window --maintenance --watch-ingestion-progress >/dev/null 2>&1 &
disown -h $! 2>/dev/null || true
echo "✓ Streamer started (Partitioned Parquet Tick Lake: $TICK_LAKE_PATH)"
echo "  Logs: $REPO_ROOT/logs/streamer.log"
