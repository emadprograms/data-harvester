#!/usr/bin/env bash
# ==============================================================================
# Data Harvester - macOS LaunchAgent Uninstaller
# Unregisters and removes macOS Startup LaunchAgents.
# ==============================================================================
set -euo pipefail

LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
STREAMER_PLIST="$LAUNCH_AGENTS_DIR/com.dataharvester.streamer.plist"
DASHBOARD_PLIST="$LAUNCH_AGENTS_DIR/com.dataharvester.dashboard.plist"

echo "Uninstalling Data Harvester macOS LaunchAgents..."

if [ -f "$STREAMER_PLIST" ]; then
    launchctl unload "$STREAMER_PLIST" 2>/dev/null || true
    rm -f "$STREAMER_PLIST"
    echo "  ✓ Removed $STREAMER_PLIST"
else
    echo "  • Streamer LaunchAgent was not present."
fi

if [ -f "$DASHBOARD_PLIST" ]; then
    launchctl unload "$DASHBOARD_PLIST" 2>/dev/null || true
    rm -f "$DASHBOARD_PLIST"
    echo "  • Removed $DASHBOARD_PLIST"
else
    echo "  • Dashboard LaunchAgent was not present."
fi

# Stop any running processes
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/stop_services.sh"

echo "✓ Data Harvester services uninstalled from macOS startup."
