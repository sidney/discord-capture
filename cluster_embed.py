#!/usr/bin/env python3
"""
cluster_embed.py — Pass 5: Embedding-based artefact classification.

Complement to cluster_github.py (pass 4). Where pass 4 uses term matching
against a hand-maintained vocabulary, pass 5 uses vector similarity:

  1. Loads ob1_catalogue.json (produced by build_catalogue.py) for artefact
     embeddings derived from README descriptions.
  2. Fetches ALL merged PRs and ALL issues from the OB1 GitHub repo — no
     static fetch list needed. New PRs appear automatically on the next run.
  3. For each PR, fetches its GitHub timeline to find cross-referenced issues
     and appends their bodies to the PR's embed text (--no-enrich to skip).
  4. Fetches comments on all issues and PRs as separate embeddable source
     documents. Issue and PR comment threads function as a parallel technical
     forum alongside Discord, particularly for older discussions that predate
     Discord's forum migration. Each comment is embedded individually with
     its parent title prepended for context (--no-comments to skip).
  5. Embeds all source documents via OpenRouter.
  6. Computes cosine similarity between each source embedding and every
     artefact embedding. Records matches above threshold as pass-5 rows in
     artefact_linkages, with score = int(similarity * 100). Thresholds can
     vary by artefact category — see CATEGORY_THRESHOLDS — so e.g. matches
     against architecture-category artefacts use a lower acceptance bar to
     surface conceptual near-misses that strict 0.65 matching would hide.
  7. For every source, records its single best pass-5 similarity regardless
     of threshold (best_pass5_sim). Sources that match no artefact above
     their per-category threshold appear in an "Orphan candidates" section
     of the report so genuinely novel community contributions remain
     visible rather than being silently dropped.
  8. Regenerates the full linkage report with pass 4 vs pass 5 comparison
     and an orphan-candidates section.

Validation: PR #90 and issue #35 (and its VOCABULARY_CONFIG comment) are
canonical test cases. Their pass-5 similarity to schema-aware-routing is
reported in the validation block whether above or below threshold; if
below, they appear in the orphan candidates section instead. Either way
they remain visible.

Usage:
  python3 cluster_embed.py [--db PATH] [--catalogue PATH] [--out-dir PATH]
                            [--threshold FLOAT] [--threshold-architecture FLOAT]
                            [--dry-run] [--no-enrich] [--no-comments] [--verbose]

  --threshold              Base threshold for pass-5 matches (default 0.65).
  --threshold-architecture Threshold override for architecture-category
                           artefacts (default 0.55). Set to the same value
                           as --threshold to disable the category override.
  --no-enrich              Skip PR timeline cross-reference fetches.
  --no-comments            Skip issue and PR comment fetching. Much faster,
                           but misses comment-thread discussions that are
                           the conceptual home of some artefacts.

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

MIN_BODY_CHARS = 40
MAX_BODY_CHARS = 3000
MAX_CROSSREF_BODY_CHARS = 600
MAX_CROSSREFS_PER_PR = 3
EMBED_BATCH_SIZE = 50
DEFAULT_THRESHOLD = 0.65
TOP_N_PER_SOURCE = 5

# Per-category threshold overrides for pass-5 acceptance. A match against
# an artefact is included in the linkage table iff its similarity is at
# least the threshold for that artefact's category, falling back to the
# base threshold (--threshold, default DEFAULT_THRESHOLD) for any category
# not listed here.
#
# Architecture entries (the FAQ chunks, tool-audit, chunking-discussion)
# match against community discussion using softer conceptual overlap rather
# than direct topic vocabulary. A lower threshold for that category lets
# borderline conceptual matches surface so the report can show them; the
# base 0.65 stays in effect for everything else, so regular artefact
# matching is not made noisier.
DEFAULT_CATEGORY_THRESHOLDS = {
    "architecture": 0.55,
}

# Cap for the orphan-candidates section in the markdown report. Sources
# above this cap are written to source_stats.json but not to the markdown.
ORPHAN_REPORT_CAP = 30


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


def _is_bot(login):
    """Return True for GitHub bot accounts whose comments are not human discussion."""
    return login.endswith("[bot]") or login in ("ghost",)


# ---------------------------------------------------------------------------
# Cross-reference enrichment
# ---------------------------------------------------------------------------

def fetch_pr_cross_refs(pr_num, token):
    """
    Fetch cross-referenced issues for a PR via the GitHub timeline API.
    Returns list of {num, title, body} for referencing real issues (not PRs).
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
        if "pull_request" in issue:
            continue
        num = issue.get("number")
        if not num or num in seen_nums:
            continue
        seen_nums.add(num)
        refs.append({
            "num": num,
            "title": issue.get("title", ""),
            "body": (issue.get("body") or "").strip(),
        })
        if len(refs) >= MAX_CROSSREFS_PER_PR:
            break
    return refs


# ---------------------------------------------------------------------------
# GitHub source fetching: bodies
# ---------------------------------------------------------------------------

def fetch_all_merged_prs(token, enrich=True, verbose=False):
    """
    Fetch all merged PRs. When enrich=True, appends cross-referenced issue
    bodies to the embed text and sets match_basis to 'embed_pr_enriched'.
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
            if len(f"{title}\n{body}") < MIN_BODY_CHARS:
                continue

            embed_text = f"{title}\n{body[:MAX_BODY_CHARS]}"
            github_refs = [f"PR#{num}"]
            match_basis = "embed_pr_body"

            if enrich:
                cross_refs = fetch_pr_cross_refs(num, token)
                time.sleep(0.2)
                if cross_refs:
                    parts = []
                    for ref in cross_refs:
                        ref_text = f"Referenced by issue #{ref['num']}: {ref['title']}"
                        if ref["body"]:
                            ref_text += f"\n{ref['body'][:MAX_CROSSREF_BODY_CHARS]}"
                        parts.append(ref_text)
                        github_refs.append(f"issue#{ref['num']}")
                    embed_text = f"{title}\n{body[:MAX_BODY_CHARS]}\n\n" + "\n\n".join(parts)
                    match_basis = "embed_pr_enriched"

            if verbose:
                tag = f" [+{len(github_refs)-1} refs]" if len(github_refs) > 1 else ""
                print(f"    PR #{num}{tag}: {title[:60]}")

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
    """Fetch all real issues (excluding PRs) from the OB1 repo."""
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
            if len(f"{title}\n{body}") < MIN_BODY_CHARS:
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
# GitHub source fetching: comments
# ---------------------------------------------------------------------------

def _comments_as_sources(parent_num, parent_title, parent_ref,
                          parent_channel_name, match_basis, comments):
    """
    Convert a list of GitHub comment objects into embeddable source dicts.
    Each non-bot comment with sufficient body length becomes its own source.
    The parent title is prepended to give the embedding model context.
    """
    sources = []
    for comment in comments:
        login = comment.get("user", {}).get("login", "")
        if _is_bot(login):
            continue
        body = (comment.get("body") or "").strip()
        if len(body) < MIN_BODY_CHARS:
            continue
        sources.append({
            "num": parent_num,
            "ref": parent_ref,
            "title": parent_title,
            "body": body,
            # Prepend parent title so the embedding reflects the discussion topic,
            # not just the comment text in isolation.
            "embed_text": f"{parent_title}\n{body[:MAX_BODY_CHARS]}",
            "created_at": comment.get("created_at", ""),
            "author": login,
            "state": "",
            "channel_name": parent_channel_name,
            "match_basis": match_basis,
            "github_refs": [parent_ref],
        })
    return sources


def fetch_issue_comments(issues, token, verbose=False):
    """
    For each issue already fetched, retrieve its comment thread and return
    each comment as a separate embeddable source document.

    Issue comment threads are a primary venue for technical discussion in OB1,
    particularly for issues that predate Discord's forum migration. Comments
    often contain the conceptual vocabulary that the issue body lacks.
    """
    all_comment_sources = []
    for issue in issues:
        num = issue["num"]
        comments = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues/{num}/comments",
            token,
            params={"per_page": 100},
        )
        time.sleep(0.2)
        if not comments or not isinstance(comments, list):
            continue
        sources = _comments_as_sources(
            parent_num=num,
            parent_title=issue["title"],
            parent_ref=f"issue#{num}",
            parent_channel_name=issue["channel_name"],
            match_basis="embed_issue_comment",
            comments=comments,
        )
        if verbose and sources:
            print(f"    issue #{num}: {len(sources)} comments")
        all_comment_sources.extend(sources)
    return all_comment_sources


def fetch_pr_comments(prs, token, verbose=False):
    """
    For each PR already fetched, retrieve its discussion comment thread
    (not inline review comments) and return each comment as a separate
    embeddable source document.

    PR discussion comments capture review conversations, design clarifications,
    and community reactions that don't appear in the PR body.
    """
    all_comment_sources = []
    for pr in prs:
        num = pr["num"]
        # /issues/{num}/comments gives the discussion thread (not code review
        # inline comments, which live at /pulls/{num}/comments).
        comments = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues/{num}/comments",
            token,
            params={"per_page": 100},
        )
        time.sleep(0.2)
        if not comments or not isinstance(comments, list):
            continue
        sources = _comments_as_sources(
            parent_num=num,
            parent_title=pr["title"],
            parent_ref=f"PR#{num}",
            parent_channel_name=pr["channel_name"],
            match_basis="embed_pr_comment",
            comments=comments,
        )
        if verbose and sources:
            print(f"    PR #{num}: {len(sources)} comments")
        all_comment_sources.extend(sources)
    return all_comment_sources


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_batch(texts, api_key):
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

def compute_matches(sources, artefacts, threshold,
                    category_thresholds=None, verbose=False):
    """Compute pass-5 matches and per-source stats.

    For each source, computes cosine similarity against every artefact and:
      - Accepts matches whose similarity is >= the category-specific
        threshold (falling back to the base `threshold` when the artefact's
        category has no override in `category_thresholds`).
      - Records the single best similarity across all artefacts (regardless
        of any threshold) for the orphan-candidates view.

    Returns (results, source_stats):
      results — list of accepted match rows for the linkage table (one row
                per accepted source-artefact pair, capped at TOP_N_PER_SOURCE
                per source).
      source_stats — list with one record per source containing best pass-5
                similarity, the artefact it best-matched, and whether
                anything was accepted above threshold.
    """
    category_thresholds = category_thresholds or {}
    results = []
    source_stats = []
    skipped = 0

    for source in sources:
        vec = source.get("embedding")
        if vec is None:
            skipped += 1
            source_stats.append(_orphan_stat_for(source, None, None, False))
            continue
        if HAS_NUMPY:
            svec = np.array(vec, dtype=np.float32)
            snorm = np.linalg.norm(svec)
            if snorm == 0:
                skipped += 1
                source_stats.append(_orphan_stat_for(source, None, None, False))
                continue
            svec_norm = svec / snorm
        else:
            norm = sum(x * x for x in vec) ** 0.5
            svec_norm = [x / norm for x in vec] if norm > 0 else vec

        # Score this source against every artefact, tracking accepted matches
        # (above per-category threshold) and the global best (regardless of
        # threshold) in a single pass.
        scored = []                # (artefact, sim) for accepted matches
        best_sim = None            # best similarity over ALL artefacts
        best_artefact = None       # artefact for that best similarity

        for a in artefacts:
            anorm = a["embedding_norm"]
            sim = (float(np.dot(svec_norm, anorm)) if HAS_NUMPY
                   else sum(x * y for x, y in zip(svec_norm, anorm)))

            if best_sim is None or sim > best_sim:
                best_sim = sim
                best_artefact = a

            cat_threshold = category_thresholds.get(a["category"], threshold)
            if sim >= cat_threshold:
                scored.append((a, sim))

        scored.sort(key=lambda x: -x[1])
        top = scored[:TOP_N_PER_SOURCE]

        if verbose and top:
            tag = f" [{source['match_basis']}]" if "comment" in source["match_basis"] else ""
            print(f"    {source['ref']}{tag}: {len(top)} matches — top: "
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

        source_stats.append(_orphan_stat_for(
            source,
            best_sim,
            best_artefact,
            matched=bool(top),
        ))

    if skipped:
        print(f"  [warn] {skipped} source(s) skipped (no embedding)")
    return results, source_stats


def _orphan_stat_for(source, best_sim, best_artefact, matched):
    """Build a single per-source stats record for the orphan-tracking view.

    `best_sim` and `best_artefact` may be None when the source had no
    usable embedding (in which case `matched` is also False).
    """
    return {
        "ref": source["ref"],
        "channel_name": source["channel_name"],
        "match_basis": source["match_basis"],
        "author": source["author"],
        "created_at": source["created_at"],
        "github_refs": list(source["github_refs"]),
        "best_pass5_sim": (round(best_sim, 4) if best_sim is not None else None),
        "best_pass5_artefact": best_artefact["slug"] if best_artefact else None,
        "best_pass5_artefact_category": (
            best_artefact["category"] if best_artefact else None
        ),
        "matched_above_threshold": matched,
        "sample_text": source["body"][:300],
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def write_to_db(conn, results, dry_run=False):
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

def print_validation(results, source_stats=None, pass4_rows=None):
    """
    Print schema-aware-routing scores on PR #90 and issue #35 for both
    methods, as the two canonical validation cases:

    PR #90  — implementation PR with thin body; conceptual content in comments
    issue #35 — body is about extension design; VOCABULARY_CONFIG comment is
                 the actual schema-aware-routing conceptual home

    When a target source has no SAR-specific match above threshold, the
    output distinguishes two situations rather than collapsing them into
    one "no match above threshold" message:

      (a) Source has no SAR match but DOES have other accepted matches —
          report the global best similarity (which may or may not be
          against SAR) and the best accepted non-SAR match. This makes
          clear that the source is not invisible; it just didn't land on
          its expected SAR target.

      (b) Source has no accepted matches anywhere — report the global
          best similarity as a single diagnostic.

    These cases were previously conflated as "no match above threshold",
    which conflicted with `matched_above_threshold: true` in
    source_stats.json when (a) applied.
    """
    slug = "schema-aware-routing"
    print("\n--- Validation: schema-aware-routing ---")

    stats_by_ref = {}
    if source_stats:
        for s in source_stats:
            for ref in s["github_refs"]:
                stats_by_ref.setdefault(ref, []).append(s)

    def report_target(ref, label):
        # Case 0: accepted match against schema-aware-routing.
        accepted_sar = next(
            (r for r in results
             if r["artefact_slug"] == slug and ref in r["github_refs"]),
            None,
        )
        if accepted_sar:
            sim = accepted_sar.get("similarity", accepted_sar["score"] / 100)
            print(
                f"  {label} pass 5 ({accepted_sar['match_basis']}): "
                f"score={accepted_sar['score']}, sim={sim:.4f}  \u2713"
            )
            if len(accepted_sar["github_refs"]) > 1:
                print(f"          enriched with: "
                      f"{', '.join(accepted_sar['github_refs'][1:])}")
            return

        # No SAR match. Gather global best (from source_stats) and best
        # accepted non-SAR match (from results).
        candidates = stats_by_ref.get(ref, [])
        if not candidates:
            print(f"  {label} pass 5: no source found")
            return
        best_source = max(
            candidates,
            key=lambda s: s["best_pass5_sim"] or -1.0,
        )
        global_sim = best_source["best_pass5_sim"]
        if global_sim is None:
            print(f"  {label} pass 5: no embedding available")
            return

        ref_accepted = [
            r for r in results
            if r["pass"] == 5 and ref in r["github_refs"]
        ]
        # All ref_accepted are non-SAR by construction at this point
        # (because accepted_sar is None).
        best_accepted = max(ref_accepted, key=lambda e: e["score"]) if ref_accepted else None

        if not best_accepted:
            # Case (b): nothing accepted anywhere.
            print(
                f"  {label} pass 5: no match above any threshold — "
                f"best sim={global_sim:.4f} against {best_source['best_pass5_artefact']} "
                f"(cat={best_source['best_pass5_artefact_category']}, "
                f"basis={best_source['match_basis']})"
            )
            return

        # Case (a): source has accepted matches, but none against SAR.
        accepted_sim = best_accepted.get("similarity", best_accepted["score"] / 100)
        print(f"  {label} pass 5: no SAR match above threshold")
        # Only print "global best" separately when it differs from the
        # best accepted match (otherwise we'd repeat ourselves).
        if (best_source["best_pass5_artefact"] != best_accepted["artefact_slug"]
                or abs(global_sim - accepted_sim) > 0.001):
            print(
                f"          global best: sim={global_sim:.4f} against "
                f"{best_source['best_pass5_artefact']} "
                f"(cat={best_source['best_pass5_artefact_category']}, "
                f"basis={best_source['match_basis']}) [below its threshold]"
            )
        print(
            f"          best accepted: sim={accepted_sim:.4f} against "
            f"{best_accepted['artefact_slug']} "
            f"(cat={best_accepted['artefact_category']}, "
            f"basis={best_accepted['match_basis']})  \u2713"
        )

    report_target("PR#90", "PR #90 ")
    report_target("issue#35", "issue #35")

    # Pass 4 term score for comparison
    if pass4_rows is not None:
        term_match = next(
            (r for r in pass4_rows
             if r.get("artefact_slug") == slug
             and "PR#90" in str(r.get("github_refs", ""))),
            None,
        )
        print(f"  PR #90  pass 4 (term): score={term_match['score']}" if term_match
              else "  PR #90  pass 4 (term): score=0 or 1")
    print()


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def regenerate_reports(conn, out_dir, pass5_results=None, source_stats=None):
    cur = conn.execute(
        """SELECT pass, match_basis, channel_id, channel_name, category,
                  artefact_slug, artefact_title, artefact_category,
                  score, window_start, window_end, github_refs, sample_text
           FROM artefact_linkages ORDER BY artefact_slug, score DESC"""
    )
    fields = ["pass", "match_basis", "channel_id", "channel_name", "category",
              "artefact_slug", "artefact_title", "artefact_category",
              "score", "window_start", "window_end", "github_refs", "sample_text"]
    results = [dict(zip(fields, r)) for r in cur.fetchall()]

    sim_lookup = {}
    if pass5_results:
        for r in pass5_results:
            sim_lookup[(r["channel_name"], r["artefact_slug"])] = r.get("similarity", r["score"] / 100)

    # Compute per-source pass-4 totals from the linkage rows we already
    # have in memory. Match by github_refs, which is the only stable cross-
    # pass identifier (channel_name format may vary between cluster_github.py
    # and this module).
    pass4_total_by_ref = _compute_pass4_totals_by_ref(results)

    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "linkage_report_full.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"Written: {csv_path}")

    if source_stats is not None:
        stats_path = os.path.join(out_dir, "source_stats.json")
        # Annotate each source stat with its pass-4 total before writing.
        annotated = []
        for s in source_stats:
            total = sum(pass4_total_by_ref.get(ref, 0) for ref in s["github_refs"])
            annotated.append({**s, "pass4_total_score": total})
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_count": len(annotated),
                "sources": annotated,
            }, f, indent=2)
        print(f"Written: {stats_path}")
    else:
        annotated = None

    md_path = os.path.join(out_dir, "linkage_report_full.md")
    _write_markdown(results, sim_lookup, md_path, source_stats=annotated)
    print(f"Written: {md_path}")


def _compute_pass4_totals_by_ref(rows):
    """For each github ref appearing in pass-4 rows, sum the pass-4 scores.

    Returns a dict mapping ref string ('PR#90', 'issue#35') → integer total.
    A row whose github_refs JSON lists multiple refs contributes its score
    to each of them, mirroring how pass-4 enrichment can attribute a single
    match to a parent PR plus cross-referenced issues.
    """
    totals = defaultdict(int)
    for r in rows:
        if r["pass"] != 4:
            continue
        refs_raw = r.get("github_refs") or "[]"
        try:
            refs = json.loads(refs_raw) if isinstance(refs_raw, str) else refs_raw
        except (json.JSONDecodeError, TypeError):
            refs = []
        for ref in refs:
            totals[ref] += r["score"]
    return dict(totals)


def _write_markdown(results, sim_lookup, path, source_stats=None):
    by_artefact = defaultdict(list)
    for r in results:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug].append(r)

    def discord_score(e): return sum(x["score"] for x in e if x["pass"] in (1, 2, 3))
    def term_score(e):    return sum(x["score"] for x in e if x["pass"] == 4)
    def embed_score(e):   return sum(x["score"] for x in e if x["pass"] == 5)
    def total_score(e):   return discord_score(e) + term_score(e) + embed_score(e)

    lines = ["# OB1 Discord + GitHub \u2192 Artefact Linkage Report (Full)", ""]
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Total linkage rows: {len(results)}")
    lines.append(f"Artefacts matched: {len(by_artefact)}")
    if source_stats is not None:
        n_sources = len(source_stats)
        n_orphans = sum(1 for s in source_stats if not s["matched_above_threshold"])
        lines.append(f"Sources scanned: {n_sources}")
        lines.append(f"Orphan sources (no pass-5 match above threshold): {n_orphans}")
    lines.append("")
    lines.append("Score columns: Discord (passes 1-3, term) | GitHub term (pass 4) | GitHub embed (pass 5).")
    lines.append("Pass 5 scores are similarity \u00d7 100 \u2014 not directly comparable to term scores.")
    lines.append("Pass-5 acceptance threshold may vary by artefact category \u2014 see CATEGORY_THRESHOLDS.")
    lines.append("match_basis: embed_pr_body | embed_pr_enriched | embed_pr_comment | embed_issue_body | embed_issue_comment")
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
            f"### `{slug}` \u2014 {title} "
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
                f"**{ch}**: {sample}\u2026"
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

    # Orphan candidates: sources that didn't clear their per-category
    # threshold. Sorted by best pass-5 similarity ascending so the most
    # embedding-orphan items come first. Pass-4 total is shown alongside
    # to distinguish "embedding-orphan but term-rich" (synthesis of known
    # concepts in unfamiliar vocabulary) from "double-orphan" (genuinely
    # novel content).
    if source_stats is not None:
        lines.append("## Orphan candidates")
        lines.append("")
        lines.append(
            "Sources with no pass-5 match above their per-category threshold. "
            "Sorted by best pass-5 similarity ascending \u2014 most embedding-orphan first."
        )
        lines.append("")
        lines.append(
            "Pass-4 total is the sum of all term-match scores attributed to "
            "this source's GitHub ref(s) across all artefacts. High pass-4 + "
            "low pass-5 suggests novel synthesis of existing concepts; low both "
            "suggests genuinely new vocabulary."
        )
        lines.append("")

        orphans = [s for s in source_stats if not s["matched_above_threshold"]]
        # Sort: missing best_sim last (None \u2192 -inf for ascending sort needs
        # special handling), then by best_sim ascending.
        orphans.sort(
            key=lambda s: (
                s["best_pass5_sim"] is None,
                s["best_pass5_sim"] if s["best_pass5_sim"] is not None else 0.0,
            )
        )

        n_total = len(orphans)
        if n_total == 0:
            lines.append("_No orphan sources \u2014 every source matched something above threshold._")
        else:
            lines.append(
                f"Showing {min(ORPHAN_REPORT_CAP, n_total)} of {n_total} orphan sources. "
                f"Full data in `source_stats.json`."
            )
            lines.append("")
            lines.append(
                "| best pass-5 sim | best-match artefact (cat) | pass-4 total | basis | source |"
            )
            lines.append(
                "|-----------------|---------------------------|--------------|-------|--------|"
            )
            for s in orphans[:ORPHAN_REPORT_CAP]:
                sim = s["best_pass5_sim"]
                sim_str = f"{sim:.3f}" if sim is not None else "\u2014"
                art = s["best_pass5_artefact"] or "\u2014"
                cat = s["best_pass5_artefact_category"] or "\u2014"
                p4 = s.get("pass4_total_score", 0)
                ch = s["channel_name"]
                lines.append(
                    f"| {sim_str} | `{art}` ({cat}) | {p4} | "
                    f"{s['match_basis']} | {ch} |"
                )
        lines.append("")

    lines.append("## Validation: schema-aware-routing on PR #90 and issue #35")
    lines.append("")
    sar = by_artefact.get("schema-aware-routing", [])
    pr90e = next((e for e in sar if e["pass"] == 5 and "PR#90" in str(e.get("github_refs", ""))), None)
    i35e  = next((e for e in sar if e["pass"] == 5 and "issue#35" in str(e.get("github_refs", ""))), None)
    pr90t = next((e for e in sar if e["pass"] == 4 and "PR#90" in str(e.get("github_refs", ""))), None)

    # Build a stats lookup for below-threshold reporting
    stats_by_ref = defaultdict(list)
    if source_stats is not None:
        for s in source_stats:
            for ref in s["github_refs"]:
                stats_by_ref[ref].append(s)

    def _validation_lines(label, ref, accepted_sar):
        """Generate markdown bullets for one validation target.

        Returns a list of bullet strings. Three cases:

        Case 0: Accepted match against schema-aware-routing exists →
                report it as a single bullet.
        Case (a): No SAR match but source has accepted matches against
                other artefacts → report "no SAR match" plus best accepted
                non-SAR match. If global best differs from best accepted
                (i.e. a higher-sim artefact was rejected by its category
                threshold, like SAR rejecting PR#90 at 0.5803), also
                report the global best as a separate diagnostic bullet.
        Case (b): No accepted matches anywhere → single bullet with
                global best similarity.
        """
        if accepted_sar:
            sim = sim_lookup.get(
                (accepted_sar["channel_name"], "schema-aware-routing"),
                accepted_sar["score"] / 100,
            )
            return [
                f"- {label} pass 5 ({accepted_sar['match_basis']}): "
                f"score={accepted_sar['score']}, sim={sim:.4f}"
            ]

        candidates = stats_by_ref.get(ref, [])
        if not candidates:
            return [f"- {label} pass 5: **no source found**"]
        best_source = max(candidates, key=lambda s: s["best_pass5_sim"] or -1.0)
        global_sim = best_source["best_pass5_sim"]
        if global_sim is None:
            return [f"- {label} pass 5: **no embedding available**"]

        ref_accepted = [
            r for r in results
            if r["pass"] == 5 and ref in str(r.get("github_refs", ""))
        ]
        best_accepted = max(ref_accepted, key=lambda e: e["score"]) if ref_accepted else None

        if not best_accepted:
            # Case (b): nothing accepted anywhere.
            return [
                f"- {label} pass 5: **no match above any threshold** \u2014 "
                f"best sim={global_sim:.4f} against `{best_source['best_pass5_artefact']}` "
                f"(cat={best_source['best_pass5_artefact_category']}, "
                f"basis={best_source['match_basis']})"
            ]

        # Case (a): source has accepted matches, just not against SAR.
        accepted_sim = best_accepted.get(
            "similarity",
            sim_lookup.get(
                (best_accepted["channel_name"], best_accepted["artefact_slug"]),
                best_accepted["score"] / 100,
            ),
        )
        out = [f"- {label} pass 5: **no SAR match above threshold**"]
        # Only show "global best" as a separate bullet when it differs
        # from the best accepted match.
        if (best_source["best_pass5_artefact"] != best_accepted["artefact_slug"]
                or abs(global_sim - accepted_sim) > 0.001):
            out.append(
                f"  - global best: sim={global_sim:.4f} against "
                f"`{best_source['best_pass5_artefact']}` "
                f"(cat={best_source['best_pass5_artefact_category']}, "
                f"basis={best_source['match_basis']}) [below its threshold]"
            )
        out.append(
            f"  - best accepted: sim={accepted_sim:.4f} against "
            f"`{best_accepted['artefact_slug']}` "
            f"(cat={best_accepted['artefact_category']}, "
            f"basis={best_accepted['match_basis']})"
        )
        return out

    lines.extend(_validation_lines("PR #90", "PR#90", pr90e))
    lines.extend(_validation_lines("issue #35", "issue#35", i35e))
    lines.append(f"- PR #90  pass 4 (term): score={pr90t['score']}" if pr90t
                 else "- PR #90  pass 4 (term): score=0 or 1")
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
                        help=f"Base pass-5 acceptance threshold (default {DEFAULT_THRESHOLD})")
    parser.add_argument(
        "--threshold-architecture",
        type=float,
        default=DEFAULT_CATEGORY_THRESHOLDS["architecture"],
        help=(
            "Threshold override for architecture-category artefacts "
            f"(default {DEFAULT_CATEGORY_THRESHOLDS['architecture']}). "
            "Set equal to --threshold to disable the override."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip PR timeline cross-reference fetches")
    parser.add_argument("--no-comments", action="store_true",
                        help="Skip issue and PR comment fetching")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print("Resolving GitHub token...")
    gh_token = get_github_token()
    if not gh_token:
        print("  No token \u2014 unauthenticated API (60 req/hr).")

    print("Resolving OpenRouter key...")
    or_key = get_openrouter_key()
    if not or_key:
        print("  ERROR: OpenRouter key required. Cannot proceed.")
        return

    print(f"\nLoading catalogue from {args.catalogue}...")
    artefacts = load_catalogue(args.catalogue)
    if not artefacts:
        print("  ERROR: No artefacts with embeddings found.")
        return
    precompute_artefact_norms(artefacts)
    print("  Artefact embeddings normalised.")

    enrich = not args.no_enrich
    print(f"\nFetching merged PRs {'(with cross-ref enrichment)' if enrich else '(enrichment skipped)'}...")
    prs = fetch_all_merged_prs(gh_token, enrich=enrich, verbose=args.verbose)
    n_enriched = sum(1 for p in prs if p["match_basis"] == "embed_pr_enriched")
    print(f"  {len(prs)} merged PRs ({n_enriched} enriched with cross-ref context)")

    print("\nFetching issues...")
    issues = fetch_all_issues(gh_token, verbose=args.verbose)
    print(f"  {len(issues)} issues fetched")

    sources = prs + issues

    if not args.no_comments:
        print("\nFetching issue comments...")
        issue_comments = fetch_issue_comments(issues, gh_token, verbose=args.verbose)
        print(f"  {len(issue_comments)} issue comments fetched")

        print("\nFetching PR discussion comments...")
        pr_comments = fetch_pr_comments(prs, gh_token, verbose=args.verbose)
        print(f"  {len(pr_comments)} PR comments fetched")

        sources = sources + issue_comments + pr_comments
    else:
        print("\nSkipping comment fetching (--no-comments).")

    print(f"\nTotal sources: {len(sources)}")

    print(f"\nEmbedding {len(sources)} source documents (batch size {EMBED_BATCH_SIZE})...")
    n_ok = embed_all_sources(sources, or_key)
    print(f"  {n_ok}/{len(sources)} embeddings successful")

    # Build per-category threshold dict from CLI args.
    category_thresholds = dict(DEFAULT_CATEGORY_THRESHOLDS)
    category_thresholds["architecture"] = args.threshold_architecture

    print(f"\nComputing similarity:")
    print(f"  base threshold = {args.threshold}")
    for cat, t in sorted(category_thresholds.items()):
        if t != args.threshold:
            print(f"  category override: {cat} = {t}")
    results, source_stats = compute_matches(
        sources, artefacts, args.threshold,
        category_thresholds=category_thresholds,
        verbose=args.verbose,
    )
    print(f"  {len(results)} matches recorded")
    n_orphan = sum(1 for s in source_stats if not s["matched_above_threshold"])
    print(f"  {n_orphan} of {len(source_stats)} sources had no match above threshold")

    conn = sqlite3.connect(args.db)
    pass4_rows = [
        dict(zip(["artefact_slug", "score", "github_refs"], row))
        for row in conn.execute(
            "SELECT artefact_slug, score, github_refs FROM artefact_linkages WHERE pass=4"
        ).fetchall()
    ]
    print_validation(results, source_stats=source_stats, pass4_rows=pass4_rows)

    write_to_db(conn, results, dry_run=args.dry_run)

    print("\nRegenerating reports...")
    regenerate_reports(conn, args.out_dir, pass5_results=results, source_stats=source_stats)
    conn.close()

    by_artefact = defaultdict(int)
    for r in results:
        by_artefact[r["artefact_slug"]] += r["score"]
    print("\nTop 10 artefacts by embedding score:")
    for slug, score in sorted(by_artefact.items(), key=lambda x: -x[1])[:10]:
        print(f"  {score:5d}  {slug}")

    print("\nDone. Fetch reports:")
    print(f"  scp ubuntu@144.24.44.81:{args.out_dir}/linkage_report_full.md ~/Desktop/")
    print(f"  scp ubuntu@144.24.44.81:{args.out_dir}/source_stats.json ~/Desktop/")


if __name__ == "__main__":
    main()
