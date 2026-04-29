#!/usr/bin/env bash
# setup.sh — Check and install DiscordChatExporter CLI on Ubuntu
# Run once on the Oracle VM before using discord_archive.py

set -euo pipefail

INSTALL_DIR="$HOME/.local/bin"
DCEX_PATH="$INSTALL_DIR/dcex"
DCEX_RELEASE_URL="https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-x64.zip"

echo "=== Discord Archive Setup ==="
echo ""

# --- Check Python 3 ---
echo "[1/4] Checking Python 3..."
if ! command -v python3 &>/dev/null; then
    echo "  ERROR: python3 not found. Install with: sudo apt install python3"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo "  OK: $PYTHON_VERSION"

# --- Check sqlite3 (stdlib, should always be present) ---
echo "[2/4] Checking sqlite3 module..."
if ! python3 -c "import sqlite3" 2>/dev/null; then
    echo "  ERROR: Python sqlite3 module not available."
    exit 1
fi
echo "  OK: sqlite3 available"

# --- Check / Install DiscordChatExporter CLI ---
echo "[3/4] Checking DiscordChatExporter CLI..."
if command -v dcex &>/dev/null; then
    echo "  OK: dcex found at $(command -v dcex)"
elif [ -f "$DCEX_PATH" ]; then
    echo "  OK: dcex found at $DCEX_PATH"
    echo "  Note: Make sure $INSTALL_DIR is in your PATH"
    echo "    Add to ~/.bashrc:  export PATH=\"\$HOME/.local/bin:\$PATH\""
else
    echo "  Not found. Downloading from GitHub releases..."
    mkdir -p "$INSTALL_DIR"

    TMPZIP=$(mktemp /tmp/dcex-XXXXXX.zip)
    if command -v curl &>/dev/null; then
        curl -fsSL -L "$DCEX_RELEASE_URL" -o "$TMPZIP"
    elif command -v wget &>/dev/null; then
        wget -q -O "$TMPZIP" "$DCEX_RELEASE_URL"
    else
        echo "  ERROR: Neither curl nor wget found. Install one and retry."
        exit 1
    fi

    TMPDIR_EXTRACT=$(mktemp -d)
    unzip -q "$TMPZIP" -d "$TMPDIR_EXTRACT"
    rm "$TMPZIP"

    BINARY=$(find "$TMPDIR_EXTRACT" -name "DiscordChatExporter.Cli" -type f | head -1)
    if [ -z "$BINARY" ]; then
        echo "  ERROR: Could not find DiscordChatExporter.Cli binary in zip."
        ls "$TMPDIR_EXTRACT"
        exit 1
    fi

    cp "$BINARY" "$DCEX_PATH"
    chmod +x "$DCEX_PATH"
    rm -rf "$TMPDIR_EXTRACT"

    echo "  Installed to $DCEX_PATH"
    echo ""
    echo "  Add to PATH if not already:"
    echo "    echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
    echo "    source ~/.bashrc"
fi

# --- Check config.json ---
echo "[4/4] Checking config.json..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR/config.json"
if [ -f "$CONFIG_PATH" ]; then
    echo "  OK: config.json exists"
else
    echo "  config.json not found — copying template..."
    cp "$SCRIPT_DIR/config.json.template" "$CONFIG_PATH"
    echo "  Created config.json — edit it before running discord_archive.py"
    echo ""
    echo "  You need:"
    echo "    token    — your Discord user token (see README for how to get it)"
    echo "    guild_id — the OB1 server ID (right-click server icon → Copy Server ID)"
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config.json with your token and guild_id"
echo "  2. Run: python3 discord_archive.py --init"
echo "     This lists channels and does a first full export"
echo "  3. Then run periodically: python3 discord_archive.py --sync"
echo "     Or add to cron: 0 2 * * * cd $SCRIPT_DIR && python3 discord_archive.py --sync >> logs/archive.log 2>&1"
