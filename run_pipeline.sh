#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$HOME/discord-output"

echo "=== 1. Building Catalogue ==="
python3 "$SCRIPT_DIR/build_catalogue.py"

echo -e "\n=== 2. Clustering Embeddings ==="
python3 "$SCRIPT_DIR/cluster_embed.py" --out-dir "$OUT_DIR"

echo -e "\n=== 3. Generating HTML ==="
python3 "$SCRIPT_DIR/generate_html.py" --out "$OUT_DIR/linkage_map.html"

echo -e "\n=== Pipeline Complete ==="
echo "Run this command on your Mac to download the required files:"
echo "scp ubuntu@144.24.44.81:$OUT_DIR/linkage_map.html ubuntu@144.24.44.81:$OUT_DIR/viewer.html ~/Desktop/"
echo ""