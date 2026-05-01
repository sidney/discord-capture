#!/usr/bin/env python3
"""
cluster_embed.py — Pass 5: Embedding-based artefact classification.

Complement to cluster_github.py (pass 4). Where pass 4 uses term matching
against a hand-maintained vocabulary, pass 5 uses vector similarity:

  1. Loads ob1_catalogue.json (produced by build_catalogue.py) for artefact
     embeddings derived from README descriptions.
  2. Fetches ALL merged PRs and ALL issues from the OB1 GitHub repo — no
     static fetch list needed. New PRs appear automatically on the next run.
  3. For each PR, fetches its GitHub timeline to find cross-referenced issues.
     The body of any referencing issue is appended to the PR's embed text,
     so that PRs with thin bodies (implementation-only) are enriched with
     the conceptual discussion from the issue that motivated them.
  4. Embeds each source document (title + body + enrichment) via OpenRouter.
  5. Computes cosine similarity between each source embedding and every
     artefact embedding.
  6. Records matches above SIMILARITY_THRESHOLD as pass 5 rows in the
     artefact_linkages table, with score = int(similarity * 100).
  7. Regenerates the full linkage report including a pass 4 vs pass 5
     comparison section.

Key validation target: schema-aware-routing should score >= 65 on PR #90
after enrichment with issue #68's context, even though the PR body alone
scores only 0.592.

Usage:
  python3 cluster_embed.py [--db PATH] [--catalogue PATH] [--out-dir PATH]
                            [--threshold FLOAT] [--dry-run] [--no-enrich]
                            [--verbose]

  --no-enrich   Skip timeline cross-reference fetches. Faster, but PRs with
                thin bodies won't be enriched. Useful for threshold tuning
                experiments after a full enriched run has already been done.

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
MIN_BODY_CHARS = 40

# Maximum PR/issue body length to embed. ~3000 chars ≈ 750 tokens.
MAX_BODY_CHARS = 3000

# Cross-reference enrichment limits.
# Each referenced issue contributes at most this many chars to the embed text.
MAX_CROSSREF_BODY_CHARS = 600
# Cap at this many cross-referenced issues per PR to avoid dilution.
MAX_CROSSREFS_PER_PR = 3

# Number of source documents per OpenRouter embedding API call.
EMBED_BATCH_SIZE = 50

# Default similarity threshold.
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


# ---------------------------------------------------------------------------
# Cross-reference enrichment via GitHub timeline API
# ---------------------------------------------------------------------------

def fetch_pr_cross_refs(pr_num, token):
    """
    Fetch cross-referenced issues for a PR via the GitHub timeline API.

    GitHub adds a 'cross-referenced' event to a PR's timeline whenever
    another issue or PR mentions it. We extract only those cross-references
    that come from real issues (not other PRs), up to MAX_CROSSREFS_PER_PR.

    Returns a list of dicts: {num, title, body} for each referencing issue.
    The body can be used to enrich the PR's embed text with the conceptual
    discussion that motivated the PR — covering the case where the PR body
    is implementation-only and thin on conceptual vocabulary.
    """
    timeline = gh_get(
        f"/repos/{GH_OWNER}/{GH_REPO}/issues/{pr_num}/timeline",
        token,
        params={"per_page": 100},
    )
    if not timeline or not isinstance(timeline, list):
        return []

    refs = []
    seen_nums = set()
    for event in timeline:
        if event.get("event") != "cross-referenced":
            continue
        source = event.get("source", {})
        if source.get("type") != "issue":
            continue
        issue = source.get("issue", {})
        # Skip if the cross-referencing source is itself a PR
        if "pull_request" in issue:
            continue
        num = issue.get("number")
        if not num or num in seen_nums:
            continue
        seen_nums.add(num)
        title = issue.get("title", "")
        body = (issue.get("body") or "").strip()
        refs.append({"num": num, "title": title, "body": body})
        if len(refs) >= MAX_CROSSREFS_PER_PR:
            break

    return refs


# ---------------------------------------------------------------------------
# GitHub source fetching
# ---------------------------------------------------------------------------

def fetch_all_merged_prs(token, enrich=True, verbose=False):
    """
    Fetch all merged PRs from the OB1 repo via paginated API.

    When enrich=True (default), fetches each PR's timeline to find
    cross-referenced issues and appends their bodies to the embed text.
    PRs enriched this way use match_basis 'embed_pr_enriched' rather than
    'embed_pr_body', so the report can distinguish the two cases.

    Returns a list of source dicts ready for embedding.
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
            if not pr.get("merged_at"):
                continue
            num = pr["number"]
            title = pr.get("title", "")
            body = (pr.get("body") or "").strip()
            combined = f"{title}\n{body}"
            if len(combined) < MIN_BODY_CHARS:
                continue

            embed_text = f"{title}\n{body[:MAX_BODY_CHARS]}"
            github_refs = [f"PR#{num}"]
            match_basis = "embed_pr_body"

            # Fetch cross-referenced issues and enrich embed text
            if enrich:
                cross_refs = fetch_pr_cross_refs(num, token)
                time.sleep(0.2)
                if cross_refs:
                    enrichment_parts = []
                    for ref in cross_refs:
                        ref_text = f"Referenced by issue #{ref['num']}: {ref['title']}"
                        if ref["body"]:
                            ref_text += f"\n{ref['body'][:MAX_CROSSREF_BODY_CHARS]}"
                        enrichment_parts.append(ref_text)
                        github_refs.append(f"issue#{ref['num']}")
                    embed_text = (
                        f"{title}\n{body[:MAX_BODY_CHARS]}\n\n"
                        + "\n\n".join(enrichment_parts)
                    )
                    match_basis = "embed_pr_enriched"

            if verbose:
                enriched_str = f" [+{len(github_refs)-1} refs]" if len(github_refs) > 1 else ""
                print(f"    PR #{num}{enriched_str}: {title[:60]}")

            prs.append({
                "num": num,
                "ref": f"PR#{num}",
                "title": title,
                "body": body,
                "embed_text": embed_text,
                "created_at": pr.get("created_at", ""),
                "author": pr.get("user", {}).get("login", ""),
                "state": "merged",
                "channel_name": f"PR#{num} (merged): {title[:70]}",
                "match_basis": match_basis,
                "github_refs": github_refs,
            })

        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)

    return prs


def fetch_all_issues(token, verbose=False):
    """
    Fetch all real issues (excluding PRs) from the OB1 repo.
    Returns a list of source dicts ready for embedding.
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
                "github_refs": [f"issue#{num}"],
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
    floats) in the same order. Returns None values on failure.
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
    Embed all source documents in batches. Attaches embedding in-place.
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
        time.sleep(0.5)
    return success


# ---------------------------------------------------------------------------
# Cosine similarity
# ---------------------------------------------------------------------------

def precompute_artefact_norms(artefacts):
    """Normalise all artefact embeddings once. Attaches embedding_norm in-place."""
    if HAS_NUMPY:
        mat = np.array([a["embedding"] for a in artefacts], dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        normed = mat / np.where(norms > 0, norms, 1.0)
        for a, row in zip(artefacts, normed):
            a["embedding_norm"] = row
    else:
        for a in artefacts:
            vec = a["embedding"]
            norm = sum(x * x for x in vec) ** 0.5
            a["embedding_norm"] = [x / norm for x in vec] if norm > 0 else vec


# ---------------------------------------------------------------------------
# Match computation
# ---------------------------------------------------------------------------

def compute_matches(sources, artefacts, threshold, verbose=False):
    """
    For each source document with a valid embedding, compute cosine similarity
    against all artefact embeddings. Record top matches above threshold.
    Returns a list of linkage row dicts.
    """
    results = []
    skipped = 0

    for source in sources:
        vec = source.get("embedding")
        if vec is None:
            skipped += 1
            continue

        if HAS_NUMPY:
            svec = np.array(vec, dtype=np.float32)
            snorm = np.linalg.norm(svec)
            if snorm == 0:
                skipped += 1
                continue
            svec_norm = svec / snorm
        else:
            norm = sum(x * x for x in vec) ** 0.5
            svec_norm = [x / norm for x in vec] if norm > 0 else vec

        scored = []
        for a in artefacts:
            anorm = a["embedding_norm"]
            sim = float(np.dot(svec_norm, anorm)) if HAS_NUMPY else sum(x * y for x, y in zip(svec_norm, anorm))
            if sim >= threshold:
                scored.append((a, sim))

        scored.sort(key=lambda x: -x[1])
        top = scored[:TOP_N_PER_SOURCE]

        if verbose and top:
            enriched = " [enriched]" if source["match_basis"] == "embed_pr_enriched" else ""
            print(f"    {source['ref']}{enriched}: {len(top)} matches — top: "
                  f"{top[0][0]['slug']} ({top[0][1]:.3f})")

        for a, sim in top:
            results.append({
                "pass": 5,
                "match_basis": source["match_basis"],
                "channel_id": None,
                "channel_name": source["channel_name"],
                "category": "github",
                "artefact_slug": a["slug"],
                "artefact_title": a["title"],
                "artefact_category": a["category"],
                "score": int(sim * 100),
                "similarity": round(sim, 4),
                "window_start": source["created_at"],
                "window_end": source["created_at"],
                "github_refs": source["github_refs"],
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
                r["pass"], r["match_basis"], r["channel_id"], r["channel_name"],
                r["category"], r["artefact_slug"], r["artefact_title"],
                r["artefact_category"], r["score"], r["window_start"],
                r["window_end"], json.dumps(r["github_refs"]), r["sample_text"][:500],
            ),
        )
    conn.commit()
    print(f"Written {len(results)} pass-5 rows to artefact_linkages")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def print_pr90_validation(results, pass4_rows=None):
    """Print schema-aware-routing score on PR #90 for both methods."""
    slug = "schema-aware-routing"
    print("\n--- Validation: schema-aware-routing on PR #90 ---")
    embed_match = next(
        (r for r in results
         if r["artefact_slug"] == slug and "PR#90" in r["github_refs"]),
        None,
    )
    if embed_match:
        sim = embed_match.get("similarity", embed_match["score"] / 100)
        basis = embed_match["match_basis"]
        print(f"  Pass 5 ({basis}): score={embed_match['score']}, similarity={sim:.4f}  ✓")
        if len(embed_match["github_refs"]) > 1:
            print(f"  Enriched with: {', '.join(embed_match['github_refs'][1:])}")
    else:
        print(f"  Pass 5 (embed): no match above threshold")

    if pass4_rows is not None:
        term_match = next(
            (r for r in pass4_rows
             if r.get("artefact_slug") == slug
             and "PR#90" in str(r.get("github_refs", ""))),
            None,
        )
        print(f"  Pass 4 (term):  score={term_match['score']}" if term_match
              else "  Pass 4 (term):  score=0 or 1")
    print()


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def regenerate_reports(conn, out_dir, pass5_results=None):
    cur = conn.execute(
        """SELECT pass, match_basis, channel_name, category,
                  artefact_slug, artefact_title, artefact_category,
                  score, window_start, window_end, github_refs, sample_text
           FROM artefact_linkages ORDER BY artefact_slug, score DESC"""
    )
    fields = ["pass", "match_basis", "channel_name", "category",
              "artefact_slug", "artefact_title", "artefact_category",
              "score", "window_start", "window_end", "github_refs", "sample_text"]
    results = [dict(zip(fields, r)) for r in cur.fetchall()]

    sim_lookup = {}
    if pass5_results:
        for r in pass5_results:
            sim_lookup[(r["channel_name"], r["artefact_slug"])] = r.get("similarity", r["score"] / 100)

    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "linkage_report_full.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fields).writeheader()
        csv.DictWriter(f, fieldnames=fields).writerows(results)
    print(f"Written: {csv_path}")

    md_path = os.path.join(out_dir, "linkage_report_full.md")
    _write_markdown(results, sim_lookup, md_path)
    print(f"Written: {md_path}")


def _write_markdown(results, sim_lookup, path):
    by_artefact = defaultdict(list)
    for r in results:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug].append(r)

    def discord_score(e): return sum(x["score"] for x in e if x["pass"] in (1, 2, 3))
    def term_score(e):    return sum(x["score"] for x in e if x["pass"] == 4)
    def embed_score(e):   return sum(x["score"] for x in e if x["pass"] == 5)
    def total_score(e):   return discord_score(e) + term_score(e) + embed_score(e)

    lines = ["# OB1 Discord + GitHub → Artefact Linkage Report (Full)", ""]
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Total linkage rows: {len(results)}")
    lines.append(f"Artefacts matched: {len(by_artefact)}")
    lines.append("")
    lines.append("Score columns: Discord (passes 1-3, term) | GitHub term (pass 4) | GitHub embed (pass 5).")
    lines.append("Pass 5 scores are similarity × 100 — not directly comparable to term scores.")
    lines.append("match_basis 'embed_pr_enriched' = PR body + cross-referenced issue context.")
    lines.append("")

    lines.append("## Artefacts by embedding score (pass 5)")
    lines.append("")
    slugs_sorted = sorted(
        by_artefact.keys(),
        key=lambda s: (-embed_score(by_artefact[s]), -total_score(by_artefact[s]))
    )
    for slug in slugs_sorted:
        entries = by_artefact[slug]
        title = next((e["artefact_title"] for e in entries), slug)
        lines.append(
            f"### `{slug}` — {title} "
            f"(Discord: {discord_score(entries)} | term: {term_score(entries)} | embed: {embed_score(entries)})"
        )
        seen = set()
        for e in sorted(entries, key=lambda x: (-x["pass"], -x["score"])):
            ch = e["channel_name"]
            if ch in seen:
                continue
            seen.add(ch)
            sim_str = ""
            if e["pass"] == 5:
                sim = sim_lookup.get((ch, slug))
                if sim:
                    sim_str = f", sim={sim:.3f}"
            sample = (e.get("sample_text") or "")[:100].replace("\n", " ")
            lines.append(
                f"  - [pass {e['pass']}, {e['match_basis']}, {e['score']}{sim_str}] "
                f"**{ch}**: {sample}…"
            )
        lines.append("")

    lines.append("## Top 15 artefacts by embedding score alone")
    lines.append("")
    lines.append("| Slug | Embed score | Term score | Discord score |")
    lines.append("|------|-------------|------------|---------------|")
    for slug in slugs_sorted[:15]:
        entries = by_artefact[slug]
        lines.append(
            f"| `{slug}` | {embed_score(entries)} "
            f"| {term_score(entries)} | {discord_score(entries)} |"
        )
    lines.append("")

    lines.append("## Validation: schema-aware-routing on PR #90")
    lines.append("")
    sar = by_artefact.get("schema-aware-routing", [])
    pr90e = next((e for e in sar if e["pass"] == 5 and "PR#90" in str(e.get("github_refs", ""))), None)
    pr90t = next((e for e in sar if e["pass"] == 4 and "PR#90" in str(e.get("github_refs", ""))), None)
    if pr90e:
        sim = sim_lookup.get((pr90e["channel_name"], "schema-aware-routing"), pr90e["score"] / 100)
        lines.append(f"- Pass 5 ({pr90e['match_basis']}): score={pr90e['score']}, similarity={sim:.4f}")
    else:
        lines.append("- Pass 5 (embed): **no match above threshold**")
    lines.append(f"- Pass 4 (term): score={pr90t['score']}" if pr90t
                 else "- Pass 4 (term): score=0 or 1")
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
    parser.add_argument("--db", default=os.path.expanduser("~/discord-capture/discord_archive.db"))
    parser.add_argument("--catalogue", default=os.path.expanduser("~/discord-capture/ob1_catalogue.json"))
    parser.add_argument("--out-dir", default=os.path.expanduser("~/discord-output"))
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                        help=f"Cosine similarity threshold (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute matches but skip writing to DB")
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip timeline cross-reference fetches (faster, less accurate)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print per-source match details")
    args = parser.parse_args()

    print("Resolving GitHub token...")
    gh_token = get_github_token()
    if not gh_token:
        print("  No token — unauthenticated API (60 req/hr).")

    print("Resolving OpenRouter key...")
    or_key = get_openrouter_key()
    if not or_key:
        print("  ERROR: OpenRouter key required for embeddings. Cannot proceed.")
        return

    print(f"\nLoading catalogue from {args.catalogue}...")
    artefacts = load_catalogue(args.catalogue)
    if not artefacts:
        print("  ERROR: No artefacts with embeddings found. Run build_catalogue.py first.")
        return
    precompute_artefact_norms(artefacts)
    print(f"  Artefact embeddings normalised.")

    enrich = not args.no_enrich
    if enrich:
        print("\nFetching merged PRs from OB1 (with cross-reference enrichment)...")
    else:
        print("\nFetching merged PRs from OB1 (enrichment skipped)...")
    prs = fetch_all_merged_prs(gh_token, enrich=enrich, verbose=args.verbose)
    n_enriched = sum(1 for p in prs if p["match_basis"] == "embed_pr_enriched")
    print(f"  {len(prs)} merged PRs fetched ({n_enriched} enriched with cross-ref context)")

    print("\nFetching issues from OB1...")
    issues = fetch_all_issues(gh_token, verbose=args.verbose)
    print(f"  {len(issues)} issues fetched")

    sources = prs + issues
    print(f"\nTotal sources: {len(sources)}")

    print(f"\nEmbedding {len(sources)} source documents (batch size {EMBED_BATCH_SIZE})...")
    n_ok = embed_all_sources(sources, or_key)
    print(f"  {n_ok}/{len(sources)} embeddings successful")

    print(f"\nComputing similarity (threshold={args.threshold})...")
    results = compute_matches(sources, artefacts, args.threshold, verbose=args.verbose)
    print(f"  {len(results)} matches recorded")

    conn = sqlite3.connect(args.db)
    pass4_rows = [
        dict(zip(["artefact_slug", "score", "github_refs"], row))
        for row in conn.execute(
            "SELECT artefact_slug, score, github_refs FROM artefact_linkages WHERE pass=4"
        ).fetchall()
    ]
    print_pr90_validation(results, pass4_rows)

    write_to_db(conn, results, dry_run=args.dry_run)

    print("\nRegenerating reports...")
    regenerate_reports(conn, args.out_dir, pass5_results=results)
    conn.close()

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
