#!/usr/bin/env python3
"""
cluster_embed.py — Pass 5: Embedding-based artefact classification.

Complement to cluster_github.py (pass 4). Where pass 4 uses term matching
against a hand-maintained vocabulary, pass 5 uses vector similarity:

  1. Loads ob1_catalogue.json (produced by build_catalogue.py) for artefact
     embeddings derived from README descriptions.
  2. Fetches ALL merged PRs and ALL issues from the OB1 GitHub repo — no
     static fetch list needed. New PRs appear automatically on the next run.
  3. Embeds each source document (title + body) via OpenRouter.
  4. Computes cosine similarity between each source embedding and every
     artefact embedding.
  5. Records matches above SIMILARITY_THRESHOLD as pass 5 rows in the
     artefact_linkages table, with score = int(similarity * 100).
  6. Regenerates the full linkage report including a pass 4 vs pass 5
     comparison section.

Key validation target: schema-aware-routing should score >= 65 on PR #90
purely from embedding similarity, even though the PR body does not repeat
the artefact's vocabulary terms.

Usage:
  python3 cluster_embed.py [--db PATH] [--catalogue PATH] [--out-dir PATH]
                            [--threshold FLOAT] [--dry-run] [--verbose]

Token resolution — GitHub:
  GITHUB_TOKEN env var → OCI Vault (github_vault_secret_ocid in config.json)
  → ~/.github_token plain file → unauthenticated (60 req/hr)

Token resolution — OpenRouter:
  OPENROUTER_API_KEY env var → OCI Vault (openrouter_vault_secret_ocid in
  config.json) → ~/.openrouter_key plain file → error (embeddings required)
"""

import sqlite3
import json
import csv
import re
import time
import argparse
import os
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone

# numpy is used for fast cosine similarity. Falls back to pure Python if
# unavailable, at the cost of slower computation on large corpora.
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("[warn] numpy not found — using pure-Python cosine similarity (slower).")
    print("       Install with: pip install numpy --break-system-packages")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GH_OWNER = "NateBJones-Projects"
GH_REPO = "OB1"
GH_API_BASE = "https://api.github.com"

OPENROUTER_EMBED_URL = "https://openrouter.ai/api/v1/embeddings"
EMBED_MODEL = "openai/text-embedding-3-small"

# Minimum body length (chars) for a source document to be worth embedding.
# Short bodies (bot comments, auto-merge messages) produce noisy embeddings.
MIN_BODY_CHARS = 40

# Maximum body length to embed. Truncated to avoid token limit issues.
# ~3000 chars ≈ 750 tokens, well within text-embedding-3-small limits.
MAX_BODY_CHARS = 3000

# Number of source documents per OpenRouter embedding API call.
EMBED_BATCH_SIZE = 50

# Default similarity threshold. Matches below this are not recorded.
# At 0.65, a PR implementing a pattern without naming it typically scores
# 0.70-0.80; unrelated content scores 0.45-0.60.
DEFAULT_THRESHOLD = 0.65

# Maximum artefact matches to record per source document.
TOP_N_PER_SOURCE = 5


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

def _load_config():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _vault_secret(ocid):
    try:
        import oci
        import base64
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.secrets.SecretsClient({}, signer=signer)
        bundle = client.get_secret_bundle(ocid).data
        return base64.b64decode(bundle.secret_bundle_content.content).decode().strip()
    except ImportError:
        print("  [vault] oci package not available")
        return ""
    except Exception as e:
        print(f"  [vault] fetch failed: {e}")
        return ""


def get_github_token():
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
# Catalogue loading
# ---------------------------------------------------------------------------

def load_catalogue(catalogue_path):
    """
    Load ob1_catalogue.json and return a list of artefact dicts, each with:
      slug, title, category, embed_text, embedding (list of floats)

    Artefacts without embeddings are skipped with a warning.
    """
    with open(catalogue_path, encoding="utf-8") as f:
        data = json.load(f)

    artefacts = []
    skipped = 0
    for a in data["artefacts"]:
        if not a.get("embedding"):
            print(f"  [warn] No embedding for {a['slug']} — skipping")
            skipped += 1
            continue
        artefacts.append({
            "slug": a["slug"],
            "title": a["title"],
            "category": a["category"],
            "embed_text": a["embed_text"],
            "embedding": a["embedding"],
        })

    print(f"Loaded {len(artefacts)} artefacts from catalogue"
          + (f" ({skipped} skipped, no embedding)" if skipped else ""))
    return artefacts


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

def gh_get(path, token, params=None):
    """GET from GitHub API with optional query params dict. Returns JSON or None."""
    url = f"{GH_API_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
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


def fetch_all_merged_prs(token, verbose=False):
    """
    Fetch all merged PRs from the OB1 repo via paginated API.
    Returns a list of dicts with: num, title, body, created_at, author, state.
    """
    prs = []
    page = 1
    while True:
        batch = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/pulls",
            token,
            params={"state": "closed", "per_page": 100, "page": page},
        )
        if not batch or not isinstance(batch, list):
            break

        for pr in batch:
            # closed PRs include both merged and unmerged; filter to merged only
            if not pr.get("merged_at"):
                continue
            num = pr["number"]
            title = pr.get("title", "")
            body = (pr.get("body") or "").strip()
            combined = f"{title}\n{body}"
            if len(combined) < MIN_BODY_CHARS:
                continue
            if verbose:
                print(f"    PR #{num}: {title[:60]}")
            prs.append({
                "num": num,
                "ref": f"PR#{num}",
                "title": title,
                "body": body,
                "embed_text": f"{title}\n{body[:MAX_BODY_CHARS]}",
                "created_at": pr.get("created_at", ""),
                "author": pr.get("user", {}).get("login", ""),
                "state": "merged",
                "channel_name": f"PR#{num} (merged): {title[:70]}",
                "match_basis": "embed_pr_body",
            })

        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)

    return prs


def fetch_all_issues(token, verbose=False):
    """
    Fetch all real issues (excluding PRs) from the OB1 repo.
    The /issues endpoint returns both issues and PRs; we filter by the
    absence of the pull_request key.
    Returns a list of dicts with: num, title, body, created_at, author.
    """
    issues = []
    page = 1
    while True:
        batch = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues",
            token,
            params={"state": "all", "per_page": 100, "page": page},
        )
        if not batch or not isinstance(batch, list):
            break

        for issue in batch:
            # Skip PRs that appear in the issues endpoint
            if "pull_request" in issue:
                continue
            num = issue["number"]
            title = issue.get("title", "")
            body = (issue.get("body") or "").strip()
            combined = f"{title}\n{body}"
            if len(combined) < MIN_BODY_CHARS:
                continue
            if verbose:
                print(f"    issue #{num}: {title[:60]}")
            issues.append({
                "num": num,
                "ref": f"issue#{num}",
                "title": title,
                "body": body,
                "embed_text": f"{title}\n{body[:MAX_BODY_CHARS]}",
                "created_at": issue.get("created_at", ""),
                "author": issue.get("user", {}).get("login", ""),
                "state": issue.get("state", ""),
                "channel_name": f"issue#{num}: {title[:70]}",
                "match_basis": "embed_issue_body",
            })

        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)

    return issues


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_batch(texts, api_key):
    """
    Embed a list of texts via OpenRouter. Returns a list of vectors (lists of
    floats) in the same order as the input. Returns None values on failure.
    """
    payload = json.dumps({"model": EMBED_MODEL, "input": texts}).encode()
    req = urllib.request.Request(OPENROUTER_EMBED_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode())
        ordered = sorted(data["data"], key=lambda x: x["index"])
        return [item["embedding"] for item in ordered]
    except Exception as ex:
        print(f"  [embed] API call failed: {ex}")
        return [None] * len(texts)


def embed_all_sources(sources, api_key):
    """
    Embed all source documents in batches of EMBED_BATCH_SIZE.
    Attaches embedding in-place to each source dict.
    Returns the number of successful embeddings.
    """
    total = len(sources)
    success = 0
    for i in range(0, total, EMBED_BATCH_SIZE):
        batch = sources[i: i + EMBED_BATCH_SIZE]
        texts = [s["embed_text"] for s in batch]
        print(f"  Embedding sources {i + 1}–{min(i + EMBED_BATCH_SIZE, total)} of {total}...")
        vectors = embed_batch(texts, api_key)
        for source, vec in zip(batch, vectors):
            source["embedding"] = vec
            if vec is not None:
                success += 1
        time.sleep(0.5)  # courtesy pause between batches
    return success


# ---------------------------------------------------------------------------
# Cosine similarity
# ---------------------------------------------------------------------------

def _normalize(vec):
    """L2-normalize a list of floats. Returns a list."""
    if HAS_NUMPY:
        v = np.array(vec, dtype=np.float32)
        norm = np.linalg.norm(v)
        return (v / norm).tolist() if norm > 0 else vec
    else:
        norm = sum(x * x for x in vec) ** 0.5
        return [x / norm for x in vec] if norm > 0 else vec


def compute_similarity(source_vec, artefact_vec_norm):
    """
    Dot product of a (normalised) source vector against a pre-normalised
    artefact vector. Equivalent to cosine similarity when both are normalised.
    """
    if HAS_NUMPY:
        return float(np.dot(np.array(source_vec, dtype=np.float32), artefact_vec_norm))
    else:
        return sum(a * b for a, b in zip(source_vec, artefact_vec_norm))


def precompute_artefact_norms(artefacts):
    """
    Normalise all artefact embeddings once. Attaches embedding_norm in-place.
    With numpy this is vectorised; without it processes one at a time.
    """
    if HAS_NUMPY:
        mat = np.array([a["embedding"] for a in artefacts], dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        normed = mat / np.where(norms > 0, norms, 1.0)
        for a, row in zip(artefacts, normed):
            a["embedding_norm"] = row  # numpy array, used directly in dot product
    else:
        for a in artefacts:
            a["embedding_norm"] = _normalize(a["embedding"])


# ---------------------------------------------------------------------------
# Match computation
# ---------------------------------------------------------------------------

def compute_matches(sources, artefacts, threshold, verbose=False):
    """
    For each source document with a valid embedding, compute cosine similarity
    against all artefact embeddings. Record matches above threshold as
    pass-5 linkage rows.

    Returns a list of linkage row dicts.
    """
    results = []
    skipped = 0

    for source in sources:
        vec = source.get("embedding")
        if vec is None:
            skipped += 1
            continue

        # Normalise source vector
        if HAS_NUMPY:
            svec = np.array(vec, dtype=np.float32)
            snorm = np.linalg.norm(svec)
            if snorm == 0:
                skipped += 1
                continue
            svec_norm = svec / snorm
        else:
            svec_norm = _normalize(vec)

        # Score against every artefact
        scored = []
        for a in artefacts:
            anorm = a["embedding_norm"]
            if HAS_NUMPY:
                sim = float(np.dot(svec_norm, anorm))
            else:
                sim = sum(x * y for x, y in zip(svec_norm, anorm))
            if sim >= threshold:
                scored.append((a, sim))

        # Keep top N by similarity
        scored.sort(key=lambda x: -x[1])
        top = scored[:TOP_N_PER_SOURCE]

        if verbose and top:
            print(f"    {source['ref']}: {len(top)} matches — top: "
                  f"{top[0][0]['slug']} ({top[0][1]:.3f})")

        for a, sim in top:
            score = int(sim * 100)
            results.append({
                "pass": 5,
                "match_basis": source["match_basis"],
                "channel_id": None,
                "channel_name": source["channel_name"],
                "category": "github",
                "artefact_slug": a["slug"],
                "artefact_title": a["title"],
                "artefact_category": a["category"],
                "score": score,
                "similarity": round(sim, 4),
                "window_start": source["created_at"],
                "window_end": source["created_at"],
                "github_refs": [source["ref"]],
                "author": source["author"],
                "sample_text": source["body"][:400],
            })

    if skipped:
        print(f"  [warn] {skipped} source(s) skipped (no embedding)")

    return results


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def write_to_db(conn, results, dry_run=False):
    """Replace all pass-5 rows and insert new results."""
    if dry_run:
        print(f"[dry-run] Would write {len(results)} pass-5 rows to artefact_linkages")
        return

    conn.execute("DELETE FROM artefact_linkages WHERE pass = 5")
    for r in results:
        conn.execute(
            """INSERT INTO artefact_linkages
               (pass, match_basis, channel_id, channel_name, category,
                artefact_slug, artefact_title, artefact_category,
                score, window_start, window_end, github_refs, sample_text)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                r["pass"],
                r["match_basis"],
                r["channel_id"],
                r["channel_name"],
                r["category"],
                r["artefact_slug"],
                r["artefact_title"],
                r["artefact_category"],
                r["score"],
                r["window_start"],
                r["window_end"],
                json.dumps(r["github_refs"]),
                r["sample_text"][:500],
            ),
        )
    conn.commit()
    print(f"Written {len(results)} pass-5 rows to artefact_linkages")


# ---------------------------------------------------------------------------
# Validation report: PR #90 schema-aware-routing
# ---------------------------------------------------------------------------

def print_pr90_validation(results, pass4_rows=None):
    """
    Print the score for schema-aware-routing on PR #90 under both methods,
    to validate that embedding-based classification catches what term
    matching misses.
    """
    slug = "schema-aware-routing"
    print("\n--- Validation: schema-aware-routing on PR #90 ---")

    # Pass 5 (embedding)
    embed_match = next(
        (r for r in results
         if r["artefact_slug"] == slug and "PR#90" in (r["github_refs"] or [])),
        None,
    )
    if embed_match:
        sim = embed_match.get("similarity", embed_match["score"] / 100)
        print(f"  Pass 5 (embed): score={embed_match['score']}, similarity={sim:.4f}  ✓")
    else:
        print(f"  Pass 5 (embed): no match above threshold")

    # Pass 4 (term) for comparison, if rows supplied
    if pass4_rows is not None:
        term_match = next(
            (r for r in pass4_rows
             if r.get("artefact_slug") == slug
             and "PR#90" in str(r.get("github_refs", ""))),
            None,
        )
        if term_match:
            print(f"  Pass 4 (term):  score={term_match['score']}")
        else:
            print(f"  Pass 4 (term):  no match (score=0 or 1)")
    print()


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def regenerate_reports(conn, out_dir, pass5_results=None):
    """
    Read all linkage rows from the DB and write updated report files.
    pass5_results is the in-memory list of new rows, used to annotate
    similarity scores in the markdown (similarity is not stored in the DB).
    """
    cur = conn.execute(
        """SELECT pass, match_basis, channel_name, category,
                  artefact_slug, artefact_title, artefact_category,
                  score, window_start, window_end, github_refs, sample_text
           FROM artefact_linkages
           ORDER BY artefact_slug, score DESC"""
    )
    rows = cur.fetchall()
    fields = ["pass", "match_basis", "channel_name", "category",
              "artefact_slug", "artefact_title", "artefact_category",
              "score", "window_start", "window_end", "github_refs", "sample_text"]
    results = [dict(zip(fields, r)) for r in rows]

    # Build similarity lookup from in-memory pass5 results
    sim_lookup = {}
    if pass5_results:
        for r in pass5_results:
            key = (r["channel_name"], r["artefact_slug"])
            sim_lookup[key] = r.get("similarity", r["score"] / 100)

    os.makedirs(out_dir, exist_ok=True)

    # CSV
    csv_path = os.path.join(out_dir, "linkage_report_full.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f"Written: {csv_path}")

    # Markdown
    md_path = os.path.join(out_dir, "linkage_report_full.md")
    _write_markdown(results, sim_lookup, md_path)
    print(f"Written: {md_path}")


def _write_markdown(results, sim_lookup, path):
    by_artefact = defaultdict(list)
    for r in results:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug].append(r)

    # Separate pass buckets
    def rows_for_pass(entries, pass_num):
        return [e for e in entries if e["pass"] == pass_num]

    def discord_score(entries):
        return sum(e["score"] for e in entries if e["pass"] in (1, 2, 3))

    def term_score(entries):
        return sum(e["score"] for e in entries if e["pass"] == 4)

    def embed_score(entries):
        return sum(e["score"] for e in entries if e["pass"] == 5)

    def total_score(entries):
        return discord_score(entries) + term_score(entries) + embed_score(entries)

    lines = ["# OB1 Discord + GitHub → Artefact Linkage Report (Full)", ""]
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Total linkage rows: {len(results)}")
    lines.append(f"Artefacts matched: {len(by_artefact)}")
    lines.append("")
    lines.append("Score columns: Discord (passes 1-3, term) | GitHub term (pass 4) | GitHub embed (pass 5).")
    lines.append("Pass 5 scores are similarity × 100 — not directly comparable to term scores.")
    lines.append("")

    # --- Per-artefact section, sorted by embed score then total ---
    lines.append("## Artefacts by embedding score (pass 5)")
    lines.append("")
    slugs_by_embed = sorted(
        by_artefact.keys(),
        key=lambda s: (-embed_score(by_artefact[s]), -total_score(by_artefact[s]))
    )
    for slug in slugs_by_embed:
        entries = by_artefact[slug]
        ds = discord_score(entries)
        ts = term_score(entries)
        es = embed_score(entries)
        title = next((e["artefact_title"] for e in entries), slug)
        lines.append(
            f"### `{slug}` — {title} "
            f"(Discord: {ds} | term: {ts} | embed: {es})"
        )
        seen = set()
        for e in sorted(entries, key=lambda x: (-x["pass"], -x["score"])):
            ch = e["channel_name"]
            key = (ch, slug)
            if ch in seen:
                continue
            seen.add(ch)
            basis = e["match_basis"]
            score = e["score"]
            sim_str = ""
            if e["pass"] == 5 and key in sim_lookup:
                sim_str = f", sim={sim_lookup[key]:.3f}"
            sample = (e.get("sample_text") or "")[:100].replace("\n", " ")
            lines.append(f"  - [pass {e['pass']}, {basis}, {score}{sim_str}] **{ch}**: {sample}…")
        lines.append("")

    # --- Method comparison: top 15 by embed only ---
    lines.append("## Top 15 artefacts by embedding score alone")
    lines.append("")
    lines.append("| Slug | Embed score | Term score | Discord score |")
    lines.append("|------|-------------|------------|---------------|")
    for slug in slugs_by_embed[:15]:
        entries = by_artefact[slug]
        lines.append(
            f"| `{slug}` | {embed_score(entries)} "
            f"| {term_score(entries)} | {discord_score(entries)} |"
        )
    lines.append("")

    # --- PR #90 callout ---
    lines.append("## Validation: schema-aware-routing on PR #90")
    lines.append("")
    sar_entries = by_artefact.get("schema-aware-routing", [])
    pr90_embed = next(
        (e for e in sar_entries
         if e["pass"] == 5 and "PR#90" in str(e.get("github_refs", ""))),
        None,
    )
    pr90_term = next(
        (e for e in sar_entries
         if e["pass"] == 4 and "PR#90" in str(e.get("github_refs", ""))),
        None,
    )
    if pr90_embed:
        key = (pr90_embed["channel_name"], "schema-aware-routing")
        sim = sim_lookup.get(key, pr90_embed["score"] / 100)
        lines.append(f"- Pass 5 (embed): score={pr90_embed['score']}, similarity={sim:.4f}")
    else:
        lines.append("- Pass 5 (embed): **no match above threshold** (expected >= 65)")
    if pr90_term:
        lines.append(f"- Pass 4 (term):  score={pr90_term['score']}")
    else:
        lines.append("- Pass 4 (term):  score=0 or 1 (term matching blind to PR body)")
    lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pass 5: embedding-based artefact classification"
    )
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/discord-capture/discord_archive.db"),
        help="Path to discord_archive.db",
    )
    parser.add_argument(
        "--catalogue",
        default=os.path.expanduser("~/discord-capture/ob1_catalogue.json"),
        help="Path to ob1_catalogue.json (from build_catalogue.py)",
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.expanduser("~/discord-output"),
        help="Directory to write report files",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Cosine similarity threshold (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute matches but skip writing to DB",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-source match details",
    )
    args = parser.parse_args()

    # --- Tokens ---
    print("Resolving GitHub token...")
    gh_token = get_github_token()
    if not gh_token:
        print("  No token — unauthenticated API (60 req/hr).")

    print("Resolving OpenRouter key...")
    or_key = get_openrouter_key()
    if not or_key:
        print("  ERROR: OpenRouter key required for embeddings. Cannot proceed.")
        return

    # --- Catalogue ---
    print(f"\nLoading catalogue from {args.catalogue}...")
    artefacts = load_catalogue(args.catalogue)
    if not artefacts:
        print("  ERROR: No artefacts with embeddings found. Run build_catalogue.py first.")
        return
    precompute_artefact_norms(artefacts)
    print(f"  Artefact embeddings normalised.")

    # --- Fetch sources ---
    print("\nFetching merged PRs from OB1...")
    prs = fetch_all_merged_prs(gh_token, verbose=args.verbose)
    print(f"  {len(prs)} merged PRs fetched")

    print("\nFetching issues from OB1...")
    issues = fetch_all_issues(gh_token, verbose=args.verbose)
    print(f"  {len(issues)} issues fetched")

    sources = prs + issues
    print(f"\nTotal sources: {len(sources)}")

    # --- Embed sources ---
    print(f"\nEmbedding {len(sources)} source documents (batch size {EMBED_BATCH_SIZE})...")
    n_ok = embed_all_sources(sources, or_key)
    print(f"  {n_ok}/{len(sources)} embeddings successful")

    # --- Compute matches ---
    print(f"\nComputing similarity (threshold={args.threshold})...")
    results = compute_matches(sources, artefacts, args.threshold, verbose=args.verbose)
    print(f"  {len(results)} matches recorded")

    # --- Validation ---
    conn = sqlite3.connect(args.db)
    pass4_rows = [
        dict(zip(["artefact_slug", "score", "github_refs"],
                 row))
        for row in conn.execute(
            "SELECT artefact_slug, score, github_refs FROM artefact_linkages WHERE pass=4"
        ).fetchall()
    ]
    print_pr90_validation(results, pass4_rows)

    # --- Write to DB ---
    write_to_db(conn, results, dry_run=args.dry_run)

    # --- Reports ---
    print("\nRegenerating reports...")
    regenerate_reports(conn, args.out_dir, pass5_results=results)
    conn.close()

    # --- Console summary ---
    by_artefact = defaultdict(int)
    for r in results:
        by_artefact[r["artefact_slug"]] += r["score"]
    print("\nTop 10 artefacts by embedding score:")
    for slug, score in sorted(by_artefact.items(), key=lambda x: -x[1])[:10]:
        print(f"  {score:5d}  {slug}")

    print("\nDone. Fetch reports:")
    print(f"  scp ubuntu@144.24.44.81:{args.out_dir}/linkage_report_full.md ~/Desktop/")


if __name__ == "__main__":
    main()
