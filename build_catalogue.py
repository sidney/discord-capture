#!/usr/bin/env python3
"""
build_catalogue.py — Crawl the OB1 GitHub repo and produce ob1_catalogue.json.

For each artefact directory found under recipes/, integrations/, primitives/,
extensions/, skills/, and dashboards/, this script:
  1. Fetches the README.md via the GitHub API.
  2. Extracts slug, category, title, tagline, and a conceptual description.
  3. Generates a text-embedding-3-small embedding for the description via OpenRouter.

In addition, this script extracts architecture-and-philosophy entries from the
docs/ directory:
  - docs/05-tool-audit.md (sections 1-3, conceptual content only)
  - docs/drafts/discord-chunking-discussion.md (whole file)
  - docs/03-faq.md — the H2 sections "How does this work with Obsidian?",
    "Storage, Retrieval, and Architecture", and "Perspective and Philosophy",
    chunked by H3 (with the H2 lead text emitted as its own entry when
    substantial).

The procedural setup docs (01-getting-started, 02-companion-prompts,
04-ai-assisted-setup, video-walkthrough-script, workflow-pipeline.html, the
xlsx guide files) are intentionally excluded — they would only act as proxies
for help-channel discussion, not architectural discussion, and their inclusion
would add noise without adding signal.

The catalogue JSON is the source of truth for the embedding-based classifier
(cluster_embed.py). It replaces the hand-maintained ARTEFACTS dict in
cluster_discord.py and cluster_github.py.

Each entry now also stores:
  gh_path    — repo-relative path to the artefact directory (non-architecture)
               or source doc file (architecture), used by generate_html.py to
               build GitHub deep links.
  line_start — 1-indexed first line of this entry in gh_path (architecture
               entries only; None for directory artefacts).
  line_end   — 1-indexed last line of this entry in gh_path (architecture
               entries only; None for directory artefacts).

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
# Each subdirectory under these dirs is expected to have a README.md.
ARTEFACT_DIRS = [
    "recipes",
    "integrations",
    "primitives",
    "extensions",
    "skills",
    "dashboards",
]

# Category label mapped from repo directory name.
DIR_TO_CATEGORY = {
    "recipes": "recipe",
    "integrations": "integration",
    "primitives": "primitive",
    "extensions": "extension",
    "skills": "skill",
    "dashboards": "dashboard",
}

# Category label for entries derived from docs/ (architecture & philosophy).
ARCHITECTURE_CATEGORY = "architecture"

# H2 titles in docs/03-faq.md whose contents we treat as architecture entries.
# These are matched case-insensitively after stripping non-alphanumerics, so
# minor punctuation drift in the source FAQ won't break the match.
FAQ_TARGET_H2_TITLES = [
    "How does this work with Obsidian?",
    "Storage, Retrieval, and Architecture",
    "Perspective and Philosophy",
]

# Minimum length (in characters) for an H2 lead block to be emitted as its
# own entry. Below this we assume there's no substantive lead and rely on
# the H3 children alone.
H2_LEAD_MIN_CHARS = 200

EMBED_MODEL = "openai/text-embedding-3-small"
EMBED_DIMS = 1536
OPENROUTER_EMBED_URL = "https://openrouter.ai/api/v1/embeddings"

# Phrases that identify rename/deprecation notices rather than descriptions.
# Paragraphs containing any of these are skipped as description candidates.
DEPRECATION_SIGNALS = [
    "historical",
    "for continuity",
    "renamed",
    "no longer compatible",
    "formerly",
    "the old installed name",
]


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
    """Fetch a markdown file at repo path and return its decoded text, or None.

    Despite the name (kept for backwards-compatibility in callers), this
    function fetches any single text file from the repo, not only README.md.
    """
    data = gh_get(f"/repos/{GH_OWNER}/{GH_REPO}/contents/{path}", token)
    if not data or data.get("type") != "file":
        return None
    import base64
    try:
        return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# README parsing (unchanged from prior version — used for artefact subdirs)
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


def _is_admonition(text):
    """Return True if text is a GitHub admonition marker like [!NOTE]."""
    return bool(re.match(r'^\[!(NOTE|WARNING|IMPORTANT|TIP|CAUTION)\]', text))


def parse_readme(text):
    """
    Extract structured fields from a README.md.

    Returns a dict with:
      title       — text of the first H1 heading
      tagline     — text of the first blockquote (> ...) near the top, or ""
      description — first substantive prose paragraph (40+ chars)
      embed_text  — tagline + ". " + description (used for the embedding)
    """
    lines = text.splitlines()

    title = ""
    tagline = ""
    description = ""

    in_code_block = False
    in_details = False
    in_admonition_block = False
    current_para_lines = []
    found_title = False
    found_tagline = False
    found_description = False

    def flush_para(para_lines):
        nonlocal description, found_description
        if found_description:
            return
        text_block = " ".join(l.strip() for l in para_lines if l.strip())
        if not text_block:
            return
        if re.match(r'^\*[^*]+\*$', text_block) or re.match(r'^_[^_]+_$', text_block):
            return
        lower = text_block.lower()
        if any(sig in lower for sig in DEPRECATION_SIGNALS):
            return
        plain = re.sub(r'`[^`]*`', '', text_block).strip()
        if len(plain) >= 40:
            description = text_block
            found_description = True

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("```") or stripped.startswith("~~~"):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            in_code_block = not in_code_block
            in_admonition_block = False
            continue
        if in_code_block:
            continue

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

        if not stripped:
            in_admonition_block = False
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        if _is_noise_line(line):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        if stripped.startswith("# ") and not found_title:
            title = stripped[2:].strip()
            found_title = True
            in_admonition_block = False
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        if re.match(r'^#{2,}\s', stripped):
            in_admonition_block = False
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        if stripped.startswith("> "):
            content = stripped[2:].strip()
            content_clean = re.sub(r'^[*_]+|[*_]+$', '', content).strip()

            if _is_admonition(content_clean):
                in_admonition_block = True
            elif in_admonition_block:
                pass
            elif not found_tagline:
                tagline = content_clean
                found_tagline = True

            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        in_admonition_block = False

        if re.match(r'^[-*+]\s|^\d+\.\s|^\|', stripped):
            if current_para_lines:
                flush_para(current_para_lines)
                current_para_lines = []
            continue

        if _is_admonition(stripped):
            continue

        if found_description:
            break

        current_para_lines.append(stripped)

    if current_para_lines and not found_description:
        flush_para(current_para_lines)

    parts = [p for p in [tagline, description] if p]
    embed_text = ". ".join(parts) if parts else title

    return {
        "title": title,
        "tagline": tagline,
        "description": description,
        "embed_text": embed_text,
    }


# ---------------------------------------------------------------------------
# Doc parsing helpers (architecture extraction)
# ---------------------------------------------------------------------------

def slugify(text):
    """Convert a heading text into a stable slug-friendly identifier."""
    text = text.strip().strip('"\'*_')
    text = re.sub(r'[^a-z0-9]+', '-', text.lower())
    text = re.sub(r'-+', '-', text).strip('-')
    return text


def _normalize_for_match(s):
    """Normalize a heading for loose case- and punctuation-insensitive matching."""
    return re.sub(r'[^a-z0-9]+', '', s.lower())


def extract_doc_lead(text, stop_heading_pattern):
    """Return the portion of text up to (but not including) the first line
    matching stop_heading_pattern."""
    lines = text.splitlines()
    out = []
    for line in lines:
        if re.match(stop_heading_pattern, line.strip()):
            break
        out.append(line)
    return "\n".join(out).rstrip()


def parse_faq_sections(text, target_h2_titles):
    """Parse an FAQ-style markdown document and return architecture-relevant
    chunks for the H2 sections whose titles match target_h2_titles.

    For each matched H2:
      - If the H2 has substantive lead text (>= H2_LEAD_MIN_CHARS) before its
        first H3, that lead becomes its own chunk (is_lead=True, h3_title=None).
      - Each H3 within the H2 section becomes its own chunk.
      - If the H2 has no H3 children, the entire H2 section becomes one chunk
        (h3_title=None, is_lead=False).

    Returns a list of dicts with keys:
      h2_title   — the H2 heading text as it appears in source
      h3_title   — the H3 heading text, or None for H2-lead/whole-section chunks
      content    — chunk body (no leading/trailing whitespace, no trailing rules)
      is_lead    — True if this is the H2 lead-text chunk
      line_start — 1-indexed first line of this chunk in the source file
                   (includes the H2 or H3 heading line)
      line_end   — 1-indexed last line of this chunk in the source file

    Line numbers are absolute positions in the full text passed to this
    function, so callers should pass the complete file content unchanged.
    """
    lines = text.splitlines()

    # Pass 1: find H2 boundaries.
    # sections: list of (h2_title, content_start, end) where
    #   content_start = 0-indexed index of first line AFTER the H2 heading
    #   end           = 0-indexed index of next H2 heading (exclusive), or len(lines)
    # The H2 heading itself is at index (content_start - 1).
    sections = []
    current_h2 = None
    current_start = None

    h2_re = re.compile(r'^##\s+(.+?)\s*$')

    for i, line in enumerate(lines):
        stripped = line.strip()
        m = h2_re.match(stripped)
        if m and not stripped.startswith("###"):
            if current_h2 is not None:
                sections.append((current_h2, current_start, i))
            current_h2 = m.group(1).strip()
            current_start = i + 1
    if current_h2 is not None:
        sections.append((current_h2, current_start, len(lines)))

    # Filter to target H2s.
    target_normalized = {_normalize_for_match(t) for t in target_h2_titles}
    selected = [
        (h2, start, end) for (h2, start, end) in sections
        if _normalize_for_match(h2) in target_normalized
    ]

    # Pass 2: within each selected section, find H3 boundaries and emit chunks.
    #
    # Line number conventions (all 1-indexed for GitHub compatibility):
    #   H2 heading:             content_start      (= 0-indexed content_start - 1, + 1)
    #   H3 heading at h3_idx:   content_start + h3_idx + 1
    #   Last line of chunk at   content_start + chunk_end  (chunk_end is exclusive
    #   chunk_end (relative):   relative index, so last included line is chunk_end-1)
    results = []
    h3_re = re.compile(r'^###\s+(.+?)\s*$')

    for h2_title, start, end in selected:
        # start = first content line after H2 heading (0-indexed)
        # H2 heading is at 0-indexed (start-1), 1-indexed = start
        section_lines = lines[start:end]

        h3_positions = []
        for j, line in enumerate(section_lines):
            stripped = line.strip()
            m = h3_re.match(stripped)
            if m and not stripped.startswith("####"):
                h3_positions.append((j, m.group(1).strip()))

        if not h3_positions:
            # No H3 children — whole H2 section as one chunk.
            content = "\n".join(section_lines).strip()
            content = re.sub(r'\n+\s*-{3,}\s*$', '', content).strip()
            if content:
                results.append({
                    "h2_title":   h2_title,
                    "h3_title":   None,
                    "content":    content,
                    "is_lead":    False,
                    "line_start": start,      # 1-indexed H2 heading
                    "line_end":   end,        # 1-indexed last content line
                })
            continue

        # H2 lead — content from H2 heading to first H3.
        first_h3_idx = h3_positions[0][0]
        lead_lines = section_lines[:first_h3_idx]
        lead_content = "\n".join(lead_lines).strip()
        lead_content = re.sub(r'\n+\s*-{3,}\s*$', '', lead_content).strip()
        if len(lead_content) >= H2_LEAD_MIN_CHARS:
            results.append({
                "h2_title":   h2_title,
                "h3_title":   None,
                "content":    lead_content,
                "is_lead":    True,
                "line_start": start,                     # 1-indexed H2 heading
                "line_end":   start + first_h3_idx,      # 1-indexed last lead line
            })

        # Each H3 child as its own chunk.
        for k, (h3_idx, h3_title) in enumerate(h3_positions):
            chunk_start = h3_idx + 1
            chunk_end = (
                h3_positions[k + 1][0]
                if k + 1 < len(h3_positions)
                else len(section_lines)
            )
            chunk_lines = section_lines[chunk_start:chunk_end]
            content = "\n".join(chunk_lines).strip()
            content = re.sub(r'\n+\s*-{3,}\s*$', '', content).strip()
            if content:
                results.append({
                    "h2_title":   h2_title,
                    "h3_title":   h3_title,
                    "content":    content,
                    "is_lead":    False,
                    "line_start": start + h3_idx + 1,   # 1-indexed H3 heading
                    "line_end":   start + chunk_end,     # 1-indexed last content line
                })

    return results


# ---------------------------------------------------------------------------
# Embedding via OpenRouter
# ---------------------------------------------------------------------------

def embed_texts(texts, api_key):
    """Call OpenRouter's embeddings endpoint. Returns list of vectors (or Nones)."""
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
        ordered = sorted(data["data"], key=lambda x: x["index"])
        return [item["embedding"] for item in ordered]
    except Exception as ex:
        print(f"  [embed] API call failed: {ex}")
        return [None] * len(texts)


# ---------------------------------------------------------------------------
# Catalogue builders
# ---------------------------------------------------------------------------

def crawl_artefacts(gh_token, verbose=False):
    """Walk each ARTEFACT_DIR, fetch README.md for each subdirectory, and
    return a list of artefact dicts (without embeddings yet).

    Each entry includes:
      gh_path    — repo-relative directory path, e.g. "recipes/schema-aware-routing"
      line_start — None (directory artefacts link to the whole directory)
      line_end   — None
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
            print(f"  [warn] Could not list {dir_name}/ — directory missing or empty")
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
            time.sleep(0.25)

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
                "slug":        slug,
                "category":    category,
                "title":       parsed["title"],
                "tagline":     parsed["tagline"],
                "description": parsed["description"],
                "embed_text":  parsed["embed_text"],
                "embedding":   None,
                "readme_path": readme_path,
                "gh_path":     f"{dir_name}/{slug}",  # tree/main/ link target
                "line_start":  None,
                "line_end":    None,
                "fetched_at":  datetime.now(timezone.utc).isoformat(),
            })

    return artefacts


def crawl_docs(gh_token, verbose=False):
    """Fetch and parse architecture-relevant docs from OB1's docs/ directory.

    Each entry includes gh_path, line_start, and line_end so that
    generate_html.py can build GitHub #L{start}-L{end} deep links.
    """
    artefacts = []
    now = datetime.now(timezone.utc).isoformat()

    # ---- 1. Tool audit guide (conceptual content) -----------------------
    print("\nFetching docs/05-tool-audit.md...")
    text = fetch_readme("docs/05-tool-audit.md", gh_token)
    time.sleep(0.25)
    if text:
        lead = extract_doc_lead(text, r'^##\s+4\.\s+Prompt Kits')
        if not lead.strip():
            print("  [warn] tool-audit lead extraction returned empty — using full doc")
            lead = text
        title_match = re.search(r'^#\s+(.+)$', lead, re.MULTILINE)
        title = title_match.group(1).strip() if title_match else "MCP Tool Audit Guide"
        line_end = len(lead.splitlines())
        artefacts.append({
            "slug":        "tool-audit",
            "category":    ARCHITECTURE_CATEGORY,
            "title":       title,
            "tagline":     "",
            "description": (
                "Conceptual guide to auditing MCP tool surface area, "
                "merging redundant CRUD tools, and scoping tools across "
                "capture/query/admin servers based on usage patterns. "
                "Direct response to issue #36."
            ),
            "embed_text":  lead,
            "embedding":   None,
            "readme_path": "docs/05-tool-audit.md",
            "gh_path":     "docs/05-tool-audit.md",
            "line_start":  1,
            "line_end":    line_end,
            "fetched_at":  now,
        })
        if verbose:
            print(f"    tool-audit ({len(lead)} chars, lines 1-{line_end})")
    else:
        print("  [warn] docs/05-tool-audit.md not found")

    # ---- 2. Discord chunking discussion draft ---------------------------
    print("\nFetching docs/drafts/discord-chunking-discussion.md...")
    text = fetch_readme("docs/drafts/discord-chunking-discussion.md", gh_token)
    time.sleep(0.25)
    if text:
        title_match = re.search(r'^#\s+(.+)$', text, re.MULTILINE)
        title = title_match.group(1).strip() if title_match else "Chunking Columns Discussion"
        line_end = len(text.splitlines())
        artefacts.append({
            "slug":        "chunking-discussion",
            "category":    ARCHITECTURE_CATEGORY,
            "title":       title,
            "tagline":     "",
            "description": (
                "Draft Discord post weighing whether chunking columns "
                "(parent_id, chunk_index, full_text) belong in the default "
                "thoughts schema or as an opt-in primitive. References "
                "PRs #27, #53, #54."
            ),
            "embed_text":  text,
            "embedding":   None,
            "readme_path": "docs/drafts/discord-chunking-discussion.md",
            "gh_path":     "docs/drafts/discord-chunking-discussion.md",
            "line_start":  1,
            "line_end":    line_end,
            "fetched_at":  now,
        })
        if verbose:
            print(f"    chunking-discussion ({len(text)} chars, lines 1-{line_end})")
    else:
        print("  [warn] docs/drafts/discord-chunking-discussion.md not found")

    # ---- 3. FAQ — chunked by H3 within architecture/philosophy sections -
    print("\nFetching docs/03-faq.md...")
    text = fetch_readme("docs/03-faq.md", gh_token)
    time.sleep(0.25)
    if text:
        chunks = parse_faq_sections(text, FAQ_TARGET_H2_TITLES)
        if not chunks:
            print(
                f"  [warn] No matching sections found in docs/03-faq.md — "
                f"target H2 titles may have been renamed. Targets: "
                f"{FAQ_TARGET_H2_TITLES}"
            )
        for chunk in chunks:
            h2 = chunk["h2_title"]
            h3 = chunk["h3_title"]

            if chunk["is_lead"]:
                title = h2.strip().strip('"\'')
                slug = f"faq-{slugify(h2)}-overview"
                tagline = ""
            elif h3 is None:
                title = h2.strip().strip('"\'')
                slug = f"faq-{slugify(h2)}"
                tagline = ""
            else:
                title = h3.strip().strip('"\'')
                slug = f"faq-{slugify(h3)}"
                tagline = h2.strip().strip('"\'')

            preamble_parts = [p for p in [title, tagline] if p and p != title]
            preamble = ". ".join([title] + preamble_parts)
            embed_text = f"{preamble}\n\n{chunk['content']}"

            description = chunk["content"][:500]
            if len(chunk["content"]) > 500:
                description = description.rstrip() + "…"

            artefacts.append({
                "slug":        slug,
                "category":    ARCHITECTURE_CATEGORY,
                "title":       title,
                "tagline":     tagline,
                "description": description,
                "embed_text":  embed_text,
                "embedding":   None,
                "readme_path": "docs/03-faq.md",
                "gh_path":     "docs/03-faq.md",
                "line_start":  chunk["line_start"],
                "line_end":    chunk["line_end"],
                "fetched_at":  now,
            })
            if verbose:
                print(
                    f"    {slug} ({len(embed_text)} chars, "
                    f"lines {chunk['line_start']}-{chunk['line_end']})"
                )
    else:
        print("  [warn] docs/03-faq.md not found")

    return artefacts


def add_embeddings(artefacts, api_key):
    """Generate embeddings for all artefacts. Updates artefacts in-place."""
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

    artefacts = crawl_artefacts(gh_token, verbose=args.verbose)
    print(f"\nCrawled {len(artefacts)} artefacts from artefact directories")

    arch_entries = crawl_docs(gh_token, verbose=args.verbose)
    print(f"\nExtracted {len(arch_entries)} architecture entries from docs/")
    artefacts.extend(arch_entries)

    print(f"\nTotal catalogue entries: {len(artefacts)}")

    if or_key and not args.no_embed:
        add_embeddings(artefacts, or_key)
    else:
        print("\nSkipping embeddings.")

    output = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "repo":           f"{GH_OWNER}/{GH_REPO}",
        "embed_model":    EMBED_MODEL if (or_key and not args.no_embed) else None,
        "artefact_count": len(artefacts),
        "artefacts":      artefacts,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\nWritten: {args.out}")

    print("\nEntries by category:")
    by_cat = {}
    for a in artefacts:
        by_cat.setdefault(a["category"], []).append(a["slug"])
    for cat, slugs in sorted(by_cat.items()):
        print(f"  {cat}: {len(slugs)}")

    print("\nGitHub link info:")
    for a in artefacts:
        if a.get("line_start"):
            print(f"  {a['slug']}: {a['gh_path']}#L{a['line_start']}-L{a['line_end']}")
        else:
            print(f"  {a['slug']}: {a['gh_path']}/ (directory)")

    print("\nParsing quality check (entries with short or missing embed_text):")
    warnings = 0
    for a in artefacts:
        if len(a["embed_text"]) < 40:
            print(f"  [short] {a['slug']}: {repr(a['embed_text'])}")
            warnings += 1
    if warnings == 0:
        print("  All entries have embed_text >= 40 chars.")

    print("\nDone.")
    if args.no_embed or not or_key:
        print("\nTo generate embeddings, run:")
        print("  python3 build_catalogue.py")
        print("(with openrouter_vault_secret_ocid in config.json or OPENROUTER_API_KEY set)")


if __name__ == "__main__":
    main()
