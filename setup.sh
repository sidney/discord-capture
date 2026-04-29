#!/usr/bin/env bash
# setup.sh — Check and install dependencies for discord-capture.
# Run once on the Oracle VM before using discord_archive.py

set -euo pipefail

INSTALL_DIR="$HOME/.local/bin"
DCEX_PATH="$INSTALL_DIR/dcex"
DCEX_RELEASE_URL="https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-x64.zip"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Discord Archive Setup ==="
echo ""

# --- Check Python 3 ---
echo "[1/5] Checking Python 3..."
if ! command -v python3 &>/dev/null; then
    echo "  ERROR: python3 not found. Install with: sudo apt install python3"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo "  OK: $PYTHON_VERSION"

# --- Check sqlite3 ---
echo "[2/5] Checking sqlite3 module..."
if ! python3 -c "import sqlite3" 2>/dev/null; then
    echo "  ERROR: Python sqlite3 module not available."
    exit 1
fi
echo "  OK: sqlite3 available"

# --- Check / Install DiscordChatExporter CLI ---
echo "[3/5] Checking DiscordChatExporter CLI..."
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
        echo "  ERROR: Neither curl nor wget found."
        exit 1
    fi
    TMPDIR_EXTRACT=$(mktemp -d)
    unzip -q "$TMPZIP" -d "$TMPDIR_EXTRACT"
    rm "$TMPZIP"
    BINARY=$(find "$TMPDIR_EXTRACT" -name "DiscordChatExporter.Cli" -type f | head -1)
    if [ -z "$BINARY" ]; then
        echo "  ERROR: Could not find binary in zip."
        exit 1
    fi
    cp "$BINARY" "$DCEX_PATH"
    chmod +x "$DCEX_PATH"
    rm -rf "$TMPDIR_EXTRACT"
    echo "  Installed to $DCEX_PATH"
    echo "  Add to PATH: echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
fi

# --- OCI SDK (optional, required only for Vault integration) ---
echo "[4/5] Checking OCI Python SDK (optional)..."
if python3 -c "import oci" 2>/dev/null; then
    OCI_VERSION=$(python3 -c "import oci; print(oci.__version__)")
    echo "  OK: oci $OCI_VERSION"
else
    echo "  Not installed. Required only if using OCI Vault for token storage."
    echo "  To install: pip install oci --break-system-packages"
fi

# --- Check / create config.json ---
echo "[5/5] Checking config.json..."
CONFIG_PATH="$SCRIPT_DIR/config.json"
if [ -f "$CONFIG_PATH" ]; then
    echo "  OK: config.json exists"
else
    cp "$SCRIPT_DIR/config.json.template" "$CONFIG_PATH"
    chmod 600 "$CONFIG_PATH"
    echo "  Created config.json (permissions set to 600)"
    echo "  Edit it before running discord_archive.py:"
    echo "    guild_id          — right-click OB1 server icon \u2192 Copy Server ID"
    echo "    token             — your Discord user token (see README)"
    echo "    vault_secret_ocid — OCI Vault secret OCID (leave blank to use token directly)"
fi

mkdir -p "$SCRIPT_DIR/logs"
chmod +x "$SCRIPT_DIR/watchdog.sh" 2>/dev/null || true

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config.json"
echo "  2. python3 discord_archive.py --init"
echo "  3. python3 discord_archive.py --daemon   (run in background via watchdog)"
echo ""
echo "To start the watchdog (runs every 15 min, restarts daemon if needed):"
echo "  crontab -e"
echo "  */15 * * * * $SCRIPT_DIR/watchdog.sh"
