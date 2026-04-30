#!/usr/bin/env python3
"""
build_catalogue.py — Crawl the OB1 GitHub repo and produce ob1_catalogue.json.

For each artefact directory found under recipes/, integrations/, and primitives/,
this script:
  1. Fetches the README.md via the GitHub API.
  2. Extracts slug, category, title, tagline, and a conceptual description.
  3. Generates a text-embedding-3-small embedding for the description via OpenRouter.
  4. Writes the full catalogue to ob1_catalogue.json.

The catalogue JSON is the source of truth for the embedding-based classifier
(cluster_embed.py, forthcoming). It replaces the hand-maintained ARTEFACTS dict
in cluster_discord.py and cluster_github.py.

Usage:
  python3 build_catalogue.py [--out PATH] [--no-embed] [--verbose]

Token resolution — GitHub (first match wins):
  1. GITHUB_TOKEN environment variable
  2. OCI Vault — OCID from GITHUB_VAULT_SECRET_OCID env var or
     github_vault_secret_ocid key in config.json
  3. ~/.github_token plain file
  4. No token (unauthenticated, 60 req/hr)

Token resolution — OpenRouter (first match wins):
  1. OPENROUTER_API_KEY environment variable
  2. OCI Vault — OCID from OPENROUTER_VAULT_SECRET_OCID env var or
     openrouter_vault_secret_ocid key in config.json
  3. ~/.openrouter_key plain file
  4. No key — embeddings are skipped (--no-embed behaviour)
"""

import json
import os
import re
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GH_OWNER = "NateBJones-Projects"
GH_REPO = "OB1"
GH_API_BASE = "https://api.github.com"

# Top-level directories that contain artefact subdirectories.
# architecture/ does not exist as a repo directory — those concepts live in
# issues and discussions rather than as standalone recipe folders.
ARTEFACT_DIRS = ["recipes", "integrations", "primitives"]

# Category label mapped from repo directory name.
DIR_TO_CATEGORY = {
    "recipes": "recipe",
    "integrations": "integration",
    "primitives": "primitive",
}

EMBED_MODEL = "openai/text-embedding-3-small"
EMBED_DIMS = 1536
OPENROUTER_EMBED_URL = "https://openrouter.ai/api/v1/embeddings"


# ---------------------------------------------------------------------------
# Token resolution helpers
# ---------------------------------------------------------------------------

def _load_config():
    """Load config.json from the script's directory, if it exists."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _vault_secret(ocid):
    """Fetch a secret value from OCI Vault using instance principal auth."""
    try:
        import oci
        import base64
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.secrets.SecretsClient({}, signer=signer)
        bundle = client.get_secret_bundle(ocid).data
        return base64.b64decode(
            bundle.secret_bundle_content.content
        ).decode().strip()
    except ImportError:
        print("  [vault] oci package not available")
        return ""
    except Exception as e:
        print(f"  [vault] fetch failed: {e}")
        return ""


def get_github_token():
    """Resolve GitHub token from env → OCI Vault → plain file → empty."""
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        return token

    cfg = _load_config()
    vault_ocid = os.environ.get("GITHUB_VAULT_SECRET_OCID", "") or cfg.get("github_vault_secret_ocid", "")
    if vault_ocid:
        token = _vault_secret(vault_ocid)
        if token:
            print("  [github token] Resolved from OCI Vault")
            return token

    token_path = os.path.expanduser("~/.github_token")
    if os.path.exists(token_path):
        token = open(token_path).read().strip()
        if token:
            print("  [github token] Resolved from ~/.github_token")
            return token

    return ""


def get_openrouter_key():
    """Resolve OpenRouter API key from env → OCI Vault → plain file → empty."""
    key = os.environ.get("OPENROUTER_API_KEY", "")
    if key:
        return key

    cfg = _load_config()
    vault_ocid = os.environ.get("OPENROUTER_VAULT_SECRET_OCID", "") or cfg.get("openrouter_vault_secret_ocid", "")
    if vault_ocid:
        key = _vault_secret(vault_ocid)
        if key:
            print("  [openrouter key] Resolved from OCI Vault")
            return key

    key_path = os.path.expanduser("~/.openrouter_key")
    if os.path.exists(key_path):
        key = open(key_path).read().strip()
        if key:
            print("  [openrouter key] Resolved from ~/.openrouter_key")
            return key

    return ""


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

def gh_get(path, token):
    """GET from the GitHub API. Returns parsed JSON or None."""
    url = f"{GH_API_BASE}{path}"
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        if e.code == 403:
            print(f"  [github] rate-limited on {path}, sleeping 60s")
            time.sleep(60)
            return None
        print(f"  [github] HTTP {e.code} on {path}")
        return None
    except Exception as ex:
        print(f"  [github] error on {path}: {ex}")
        return None


def fetch_readme(path, token):
    """Fetch a README.md at repo path and return its decoded text, or None."""
    data = gh_get(f"/repos/{GH_OWNER}/{GH_REPO}/contents/{path}", token)
    if not data or data.get("type") != "file":
        return None
    import base64
    try:
        return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# README parsing
# ---------------------------------------------------------------------------

def _is_noise_line(line):
    """
    Return True for lines that carry no conceptual content:
    badge images, HTML div tags, GitHub contributor attribution,
    shield.io badges, etc.
    """
    stripped = line.strip()
    if not stripped:
        return True
    # HTML tags
    if stripped.startswith("<") or stripped.startswith("</"):
        return True
    # Markdown image badges: ![...](https://img.shields.io/...)
    if stripped.startswith("!["):
        return True
    # Bold attribution lines like **Created by ...** or *Reviewed ...*
    if re.match(r'^\*{1,2}(Created|Reviewed|Built)', stripped):
        return True
    # Pure horizontal rules
    if re.match(r'^[-*_]{3,}$', stripped):
        return True
    return False


def parse_readme(text):
    """
    Extract structured fields from a README.md.

    Returns a dict with:
      title       — text of the first H1 heading
      tagline     — text of the first blockquote (> ...) near the top, or ""
      description — first substantive prose paragraph (40+ chars)
      embed_text  — tagline + ". " + description (used for the embedding)

    The description is the first paragraph that:
      - is not a heading
      - is not a blockquote
      - is not a code fence or list item
      - is not a table row
      - is not noise (badges, HTML, attribution)
      - has at least 40 characters after stripping markdown backticks
    """
    lines = text.splitlines()

    title = ""
    tagline = ""
    description = ""

    # We scan line-by-line, accumulating paragraph buffers.
    # A paragraph ends at a blank line.
    in_code_block = False
    in_details = False
    current_para_lines = []
    found_title = False
    found_tagline = False
    found_description = False

    def flush_para(para_lines):
        """Try to use a paragraph buffer as the description."""
        nonlocal description, found_description
        if found_description:
            return
        text_block = " ".join(l.strip() for l in para_lines if l.strip())
        # Strip inline code backticks for length check only
        plain = re.sub(r'`[^`]*`', '', text_block).strip()
        if len(plain) >= 40:
            description = text_block
            found_description = True

    for line in lines:
        stripped = line.strip()

        # Track fenced code blocks — skip their contents entirely
        if stripped.startswith("```") or stripped.startswith("~~~"):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue

        # Track <details> blocks — skip their contents (SQL expansions etc)
        if stripped.startswith("<details"):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            in_details = True
            continue
        if stripped.startswith("</details"):
            in_details = False
            continue
        if in_details:
            continue

        # Blank line — flush current paragraph
        if not stripped:
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # Noise lines contribute nothing
        if _is_noise_line(line):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # H1 heading — title
        if stripped.startswith("# ") and not found_title:
            title = stripped[2:].strip()
            found_title = True
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # Any other heading — flush and skip
        if re.match(r'^#{2,}\s', stripped):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # Blockquote — tagline candidate
        if stripped.startswith("> ") and not found_tagline:
            tagline = stripped[2:].strip()
            # Strip leading/trailing bold/italic markers
            tagline = re.sub(r'^[*_]+|[*_]+$', '', tagline).strip()
            found_tagline = True
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue
        if stripped.startswith("> "):  # subsequent blockquotes
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # List items and table rows — flush and skip
        if re.match(r'^[-*+]\s|^\d+\.\s|^\|', stripped):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        # GitHub Admonition markers (> [!NOTE] etc) — skip
        if re.match(r'^\[!(NOTE|WARNING|IMPORTANT|TIP|CAUTION)\]', stripped):
            continue

        # If we already have a description, stop accumulating
        if found_description:
            break

        # Ordinary prose line — add to current paragraph
        current_para_lines.append(stripped)

    # Flush anything remaining
    if current_para_lines and not found_description:
        flush_para(current_para_lines)

    # Build embed_text: tagline + description
    parts = [p for p in [tagline, description] if p]
    embed_text = ". ".join(parts) if parts else title

    return {
        "title": title,
        "tagline": tagline,
        "description": description,
        "embed_text": embed_text,
    }


# ---------------------------------------------------------------------------
# Embedding via OpenRouter
# ---------------------------------------------------------------------------

def embed_texts(texts, api_key):
    """
    Call OpenRouter's embeddings endpoint with a list of texts.
    Returns a list of embedding vectors in the same order as texts.
    Returns a list of None values if the call fails.
    """
    payload = json.dumps({
        "model": EMBED_MODEL,
        "input": texts,
    }).encode()

    req = urllib.request.Request(
        OPENROUTER_EMBED_URL,
        data=payload,
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
        # data["data"] is a list of {"index": N, "embedding": [...], "object": "embedding"}
        ordered = sorted(data["data"], key=lambda x: x["index"])
        return [item["embedding"] for item in ordered]
    except Exception as ex:
        print(f"  [embed] API call failed: {ex}")
        return [None] * len(texts)


# ---------------------------------------------------------------------------
# Catalogue builder
# ---------------------------------------------------------------------------

def crawl_artefacts(gh_token, verbose=False):
    """
    Walk each ARTEFACT_DIR, list subdirectories, fetch README.md for each,
    and return a list of artefact dicts (without embeddings yet).
    """
    artefacts = []

    for dir_name in ARTEFACT_DIRS:
        category = DIR_TO_CATEGORY[dir_name]
        print(f"\nCrawling {dir_name}/...")

        listing = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/contents/{dir_name}",
            gh_token,
        )
        if not listing or not isinstance(listing, list):
            print(f"  [warn] Could not list {dir_name}/")
            continue

        subdirs = [
            item for item in listing
            if item["type"] == "dir" and not item["name"].startswith("_")
        ]
        print(f"  Found {len(subdirs)} artefact directories")

        for item in subdirs:
            slug = item["name"]
            readme_path = f"{dir_name}/{slug}/README.md"

            if verbose:
                print(f"    {slug}")

            readme_text = fetch_readme(readme_path, gh_token)
            time.sleep(0.25)  # stay well under 5000 req/hr

            if not readme_text:
                print(f"  [warn] No README.md found for {slug}")
                parsed = {"title": slug, "tagline": "", "description": "", "embed_text": slug}
            else:
                parsed = parse_readme(readme_text)
                if not parsed["title"]:
                    parsed["title"] = slug
                if not parsed["embed_text"]:
                    parsed["embed_text"] = slug

            artefacts.append({
                "slug": slug,
                "category": category,
                "title": parsed["title"],
                "tagline": parsed["tagline"],
                "description": parsed["description"],
                "embed_text": parsed["embed_text"],
                "embedding": None,  # filled in by embed pass
                "readme_path": readme_path,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

    return artefacts


def add_embeddings(artefacts, api_key):
    """
    Generate embeddings for all artefacts in a single batched API call.
    Updates artefacts in-place.
    """
    texts = [a["embed_text"] for a in artefacts]
    print(f"\nGenerating embeddings for {len(texts)} artefacts...")
    vectors = embed_texts(texts, api_key)
    for artefact, vector in zip(artefacts, vectors):
        artefact["embedding"] = vector
    success = sum(1 for v in vectors if v is not None)
    print(f"  {success}/{len(texts)} embeddings generated successfully")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Crawl the OB1 repo and produce ob1_catalogue.json"
    )
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ob1_catalogue.json"),
        help="Output path for the catalogue JSON (default: ob1_catalogue.json next to this script)",
    )
    parser.add_argument(
        "--no-embed",
        action="store_true",
        help="Skip embedding generation (useful for testing the crawl and parse steps)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each artefact slug as it is fetched",
    )
    args = parser.parse_args()

    # --- Tokens ---
    print("Resolving GitHub token...")
    gh_token = get_github_token()
    if not gh_token:
        print("  No token found — using unauthenticated API (60 req/hr). "
              "Add github_vault_secret_ocid to config.json or set GITHUB_TOKEN.")

    if not args.no_embed:
        print("Resolving OpenRouter key...")
        or_key = get_openrouter_key()
        if not or_key:
            print("  No OpenRouter key found — embeddings will be skipped. "
                  "Add openrouter_vault_secret_ocid to config.json or set OPENROUTER_API_KEY.")
    else:
        or_key = ""

    # --- Crawl ---
    artefacts = crawl_artefacts(gh_token, verbose=args.verbose)
    print(f"\nCrawled {len(artefacts)} artefacts total")

    # --- Embed ---
    if or_key and not args.no_embed:
        add_embeddings(artefacts, or_key)
    else:
        print("\nSkipping embeddings.")

    # --- Write output ---
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo": f"{GH_OWNER}/{GH_REPO}",
        "embed_model": EMBED_MODEL if (or_key and not args.no_embed) else None,
        "artefact_count": len(artefacts),
        "artefacts": artefacts,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\nWritten: {args.out}")

    # --- Summary ---
    print("\nArtefacts by category:")
    by_cat = {}
    for a in artefacts:
        by_cat.setdefault(a["category"], []).append(a["slug"])
    for cat, slugs in sorted(by_cat.items()):
        print(f"  {cat}: {len(slugs)}")

    print("\nParsing quality check (artefacts with short or missing embed_text):")
    warnings = 0
    for a in artefacts:
        if len(a["embed_text"]) < 40:
            print(f"  [short] {a['slug']}: {repr(a['embed_text'])}")
            warnings += 1
    if warnings == 0:
        print("  All artefacts have embed_text >= 40 chars.")

    print("\nDone.")
    if args.no_embed or not or_key:
        print("\nTo generate embeddings, run:")
        print("  python3 build_catalogue.py")
        print("(with openrouter_vault_secret_ocid in config.json or OPENROUTER_API_KEY set)")


if __name__ == "__main__":
    main()
