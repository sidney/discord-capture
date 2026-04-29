#!/usr/bin/env bash
# setup.sh — Check and install dependencies for discord-capture.
# Run once on the Oracle VM before using discord_archive.py

set -euo pipefail

INSTALL_DIR="$HOME/.local/bin"
DCEX_PATH="$INSTALL_DIR/dcex"
DCEX_SHARE_DIR="$HOME/.local/share/dcex"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)
        DCEX_RELEASE_URL="https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-x64.zip"
        DCEX_MODE="standalone"
        ;;
    aarch64|arm64)
        DCEX_RELEASE_URL="https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-arm64.zip"
        DCEX_MODE="dotnet"
        ;;
    *)
        echo "  WARNING: Unknown architecture $ARCH — defaulting to x64 build"
        DCEX_RELEASE_URL="https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-x64.zip"
        DCEX_MODE="standalone"
        ;;
esac

echo "=== Discord Archive Setup ==="
echo "  Architecture: $ARCH"
echo ""

# --- Check Python 3 ---
echo "[1/6] Checking Python 3..."
if ! command -v python3 &>/dev/null; then
    echo "  ERROR: python3 not found. Install with: sudo apt install python3"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo "  OK: $PYTHON_VERSION"

# --- Check sqlite3 ---
echo "[2/6] Checking sqlite3 module..."
if ! python3 -c "import sqlite3" 2>/dev/null; then
    echo "  ERROR: Python sqlite3 module not available."
    exit 1
fi
echo "  OK: sqlite3 available"

# --- Check unzip ---
echo "[3/6] Checking unzip..."
if ! command -v unzip &>/dev/null; then
    echo "  Not found. Installing..."
    sudo apt install unzip -y
fi
echo "  OK: unzip available"

# --- Check / Install .NET runtime (ARM64 only) ---
if [ "$DCEX_MODE" = "dotnet" ]; then
    echo "[4/6] Checking .NET runtime (required for ARM64 dcex)..."
    if command -v dotnet &>/dev/null; then
        DOTNET_VERSION=$(dotnet --version 2>/dev/null || echo "unknown")
        echo "  OK: dotnet $DOTNET_VERSION"
    else
        echo "  Not found. Installing via snap..."
        sudo snap install dotnet --classic
        # snap dotnet may not auto-install the runtime; trigger it
        dotnet --version || true
        echo "  OK: dotnet installed"
    fi
fi

# --- Check / Install DiscordChatExporter CLI ---
echo "[5/6] Checking DiscordChatExporter CLI..."
if command -v dcex &>/dev/null && dcex --version &>/dev/null 2>&1; then
    echo "  OK: dcex $(dcex --version)"
elif [ -f "$DCEX_PATH" ] && "$DCEX_PATH" --version &>/dev/null 2>&1; then
    echo "  OK: dcex found at $DCEX_PATH"
    echo "  Note: Make sure $INSTALL_DIR is in your PATH"
    echo "    Add to ~/.bashrc:  export PATH=\"\$HOME/.local/bin:\$PATH\""
else
    echo "  Not found. Downloading from GitHub releases ($ARCH build)..."
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

    if [ "$DCEX_MODE" = "standalone" ]; then
        # x64: self-contained binary, copy directly
        BINARY=$(find "$TMPDIR_EXTRACT" -name "DiscordChatExporter.Cli" -type f | head -1)
        if [ -z "$BINARY" ]; then
            echo "  ERROR: Could not find binary in zip."
            exit 1
        fi
        cp "$BINARY" "$DCEX_PATH"
        chmod +x "$DCEX_PATH"
    else
        # ARM64: framework-dependent DLL, install alongside .NET wrapper
        mkdir -p "$DCEX_SHARE_DIR"
        cp "$TMPDIR_EXTRACT"/* "$DCEX_SHARE_DIR/"

        # Write a wrapper script that calls dotnet with the DLL
        cat > "$DCEX_PATH" << EOF
#!/usr/bin/env bash
exec dotnet "$DCEX_SHARE_DIR/DiscordChatExporter.Cli.dll" "\$@"
EOF
        chmod +x "$DCEX_PATH"
    fi

    rm -rf "$TMPDIR_EXTRACT"
    echo "  Installed dcex to $DCEX_PATH"
    echo "  Add to PATH if not already:"
    echo "    echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
    echo "    source ~/.bashrc"
fi

# --- OCI SDK (optional, required only for Vault integration) ---
echo "[6/6] Checking OCI Python SDK (optional)..."
if python3 -c "import oci" 2>/dev/null; then
    OCI_VERSION=$(python3 -c "import oci; print(oci.__version__)")
    echo "  OK: oci $OCI_VERSION"
else
    echo "  Not installed. Required only if using OCI Vault for token storage."
    echo "  To install: pip3 install oci --break-system-packages"
fi

# --- Check / create config.json ---
CONFIG_PATH="$SCRIPT_DIR/config.json"
if [ -f "$CONFIG_PATH" ]; then
    echo "  OK: config.json exists"
else
    cp "$SCRIPT_DIR/config.json.template" "$CONFIG_PATH"
    chmod 600 "$CONFIG_PATH"
    echo "  Created config.json (permissions set to 600)"
    echo "  Edit it before running discord_archive.py:"
    echo "    guild_id          — right-click OB1 server icon → Copy Server ID"
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
echo "  3. Start the watchdog: bash watchdog.sh"
echo ""
echo "To keep the daemon alive automatically:"
echo "  crontab -e"
echo "  */15 * * * * $SCRIPT_DIR/watchdog.sh"
