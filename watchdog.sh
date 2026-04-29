#!/usr/bin/env bash
# watchdog.sh — Start the discord_archive daemon if it is not already running.
#
# Add to crontab (crontab -e) to run every 15 minutes:
#   */15 * * * * /home/ubuntu/discord-capture/watchdog.sh
#
# The daemon writes a PID file when it starts and removes it on clean exit.
# This script checks that file and the process behind it before deciding
# whether to start a new daemon instance.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="/tmp/discord_archive.pid"
LOG_FILE="$SCRIPT_DIR/logs/daemon.log"
PYTHON="python3"

mkdir -p "$SCRIPT_DIR/logs"

# If PID file exists and the process is alive, nothing to do.
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        exit 0
    fi
    # Stale PID file — process is gone.
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stale PID file (PID $PID), cleaning up" >> "$LOG_FILE"
    rm -f "$PID_FILE"
fi

# Start the daemon in the background.
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting daemon" >> "$LOG_FILE"
cd "$SCRIPT_DIR"
nohup "$PYTHON" discord_archive.py --daemon >> "$LOG_FILE" 2>&1 &
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daemon started with PID $!" >> "$LOG_FILE"
