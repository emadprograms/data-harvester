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
echo "--- Supervisor Lifecycle State ---"
"$PYTHON_BIN" -c "
import json
from pathlib import Path
repo_root = Path('$REPO_ROOT')
for name in ('streamer', 'dashboard'):
    state_file = repo_root / 'logs' / f'{name}.state.json'
    if state_file.exists():
        try:
            payload = json.loads(state_file.read_text(encoding='utf-8'))
            state = payload.get('state', 'UNKNOWN')
            detail = payload.get('detail', '')
            marker = 'OK' if state in ('INGESTING',) else ('WAIT' if state in ('WAITING_FOR_WINDOW', 'MAINTENANCE', 'DRAINING', 'STARTING') else 'ATTENTION')
            print(f'  [{marker}] {name}: {state}' + (f' — {detail}' if detail else ''))
            if state == 'STALLED':
                print('        ^ the process is alive but no ticks are being written.')
        except Exception as e:
            print(f'  [??] {name}: unreadable state file ({e})')
    else:
        print(f'  [??] {name}: no state file (supervisor not running?)')
"

echo ""
echo "--- Registry (the single symbol authority) ---"
"$PYTHON_BIN" -c "
import json, os, sys
from pathlib import Path
repo_root = Path('$REPO_ROOT')
try:
    from src.storage.config import resolve_tick_lake_root
    lake_root = resolve_tick_lake_root()
except Exception:
    env_root = os.environ.get('TICK_LAKE_ROOT') or os.environ.get('DATA_DIR')
    lake_root = Path(env_root).resolve() if env_root else repo_root / 'data' / 'tick_lake'
reg_path = Path(lake_root) / '_control' / 'registry.json'
if not reg_path.exists():
    print(f'  [ATTENTION] No registry at {reg_path}')
    print('        A live streamer will refuse to start until the registry is seeded:')
    print(f'        python -m src.storage.registry --root {lake_root} --seed approved')
else:
    try:
        data = json.loads(reg_path.read_text(encoding='utf-8'))
        symbols = data.get('symbols') or {}
        active = [k for k, v in symbols.items() if isinstance(v, dict) and v.get('active') and v.get('status') == 'ACTIVE']
        print(f'  [{\"OK\" if active else \"ATTENTION\"}] Active ingest symbols: {len(active)} of {len(symbols)} registered')
        if not active:
            print('        No active symbols: nothing will be subscribed and no ticks will arrive.')
            print(f'        Seed it: python -m src.storage.registry --root {lake_root} --seed approved')
        else:
            print(f'        {sorted(active)}')
    except Exception as e:
        print(f'  [ATTENTION] Registry unreadable: {e}')
"

echo ""
echo "--- Tick Lake Storage & Writer Status ---"
"$PYTHON_BIN" -c "
import json, os
from pathlib import Path
repo_root = Path('$REPO_ROOT')
lake_root = None
try:
    from src.storage.config import resolve_tick_lake_root
    lake_root = resolve_tick_lake_root()
except Exception:
    lake_env = os.environ.get('TICK_LAKE_ROOT')
    lake_root = Path(lake_env).resolve() if lake_env else repo_root / 'data' / 'tick_lake'

if not (lake_root / 'lake.json').exists():
    print(f'  • Tick Lake: Not found at {lake_root}')
else:
    ticks_dir = lake_root / 'ticks'
    partitions = list(ticks_dir.glob('symbol=*/date=*')) if ticks_dir.exists() else []
    parquet_files = list(ticks_dir.rglob('*.parquet')) if ticks_dir.exists() else []
    total_bytes = sum(f.stat().st_size for f in parquet_files)
    total_mb = total_bytes / (1024 * 1024)
    symbols = set(p.parent.name.replace('symbol=', '') for p in partitions)
    print(f'  ✓ Lake Root: {lake_root}')
    print(f'  ✓ Lake Partitions: {len(partitions)} date partitions across {len(symbols)} symbols')
    print(f'  ✓ Parquet Files: {len(parquet_files)} files ({total_mb:.2f} MB total)')

    status_file = lake_root / '_control' / 'writer_status.json'
    if status_file.exists():
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                st = json.load(f)
            w_status = st.get('status', 'UNKNOWN')
            writer_id = st.get('writer_id', 'unknown')
            rows = st.get('total_rows_written', 0)
            batches = st.get('batches_published', 0)
            hb = st.get('updated_at', 'N/A')
            # The status string is not evidence of life: a crashed writer leaves
            # its last file behind still saying RUNNING (INCIDENT-2026-10-06).
            pid = st.get('pid')
            alive = None
            if isinstance(pid, int):
                try:
                    import psutil
                    alive = psutil.pid_exists(pid)
                except Exception:
                    try:
                        os.kill(pid, 0); alive = True
                    except OSError:
                        alive = False
            label = w_status
            if alive is False:
                label = f'{w_status} but PID {pid} IS NOT RUNNING (stale status file)'
            print(f'  ✓ Writer Status: [{label}] ID: {writer_id} | Rows: {rows:,d} | Batches: {batches:,d} | Heartbeat: {hb}')
        except Exception as e:
            print(f'  ⚠️ Error reading writer_status.json: {e}')
    else:
        print('  • Writer Status: No writer_status.json detected (idle/waiting)')
"

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
