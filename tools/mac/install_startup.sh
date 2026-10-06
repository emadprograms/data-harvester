#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS 24/7 LaunchAgent Installer
# Registers Streamer and Dashboard to start automatically on macOS login.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

mkdir -p "$LAUNCH_AGENTS_DIR"
mkdir -p "$REPO_ROOT/logs"

# Resolve Python executable
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "❌ ERROR: Python executable not found!"
    exit 1
fi

echo "===================================================="
echo "  Data Harvester - 24/7 macOS Startup Installer"
echo "===================================================="
echo "Repo Root:     $REPO_ROOT"
echo "Python:        $PYTHON_BIN"
echo "LaunchAgents:  $LAUNCH_AGENTS_DIR"
echo ""

# 1. Streamer LaunchAgent
STREAMER_PLIST="$LAUNCH_AGENTS_DIR/com.dataharvester.streamer.plist"
cat <<EOF > "$STREAMER_PLIST"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.dataharvester.streamer</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON_BIN</string>
        <string>-u</string>
        <string>$REPO_ROOT/tools/service_supervisor.py</string>
        <string>--name</string>
        <string>streamer</string>
        <string>--module</string>
        <string>src.stream.runner</string>
        <string>--enforce-window</string>
        <string>--maintenance</string>
        <string>--watch-ingestion-progress</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>$PATH</string>
        <key>HOME</key>
        <string>$HOME</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
        <key>PYTHONPATH</key>
        <string>$REPO_ROOT</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$REPO_ROOT</string>
    <key>StandardInPath</key>
    <string>/dev/null</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$REPO_ROOT/logs/streamer_launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$REPO_ROOT/logs/streamer_launchd.log</string>
</dict>
</plist>
EOF

# 2. Dashboard LaunchAgent
DASHBOARD_PLIST="$LAUNCH_AGENTS_DIR/com.dataharvester.dashboard.plist"
cat <<EOF > "$DASHBOARD_PLIST"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.dataharvester.dashboard</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON_BIN</string>
        <string>-u</string>
        <string>$REPO_ROOT/tools/service_supervisor.py</string>
        <string>--name</string>
        <string>dashboard</string>
        <string>--module</string>
        <string>src.dashboard.server</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>$PATH</string>
        <key>HOME</key>
        <string>$HOME</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
        <key>PYTHONPATH</key>
        <string>$REPO_ROOT</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$REPO_ROOT</string>
    <key>StandardInPath</key>
    <string>/dev/null</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$REPO_ROOT/logs/dashboard_launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$REPO_ROOT/logs/dashboard_launchd.log</string>
</dict>
</plist>
EOF

# Unload any existing instances first
launchctl unload "$STREAMER_PLIST" 2>/dev/null || true
launchctl unload "$DASHBOARD_PLIST" 2>/dev/null || true

# Load the LaunchAgents
launchctl load -w "$STREAMER_PLIST"
launchctl load -w "$DASHBOARD_PLIST"

echo "✓ Successfully installed and loaded LaunchAgents:"
echo "   - $STREAMER_PLIST"
echo "   - $DASHBOARD_PLIST"
echo ""
echo "Services will now start automatically whenever you log into macOS."
echo "To uninstall: ./tools/mac/uninstall_startup.sh"
echo "To check:     ./tools/mac/status_services.sh"
