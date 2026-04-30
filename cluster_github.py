#!/usr/bin/env python3
"""
cluster_github.py — Pass 4: GitHub issue comments + PR content → artefact linkages.

Extends the work done by cluster_discord.py by fetching:
  - Comments on key OB1 issues (the real technical discussions)
  - PR bodies and PR discussion comments for all mapped PRs
  - PR review thread comments (inline code review feedback)

Appends results to the same artefact_linkages table in discord_archive.db,
and regenerates the linkage_report files to include GitHub content.

Usage:
  python3 cluster_github.py [--db PATH] [--out-dir PATH] [--dry-run]

Token resolution order (first match wins):
  1. GITHUB_TOKEN environment variable
  2. OCI Vault secret — OCID read from GITHUB_VAULT_SECRET_OCID env var,
     or from github_vault_secret_ocid key in config.json (same file used
     by discord_archive.py for the Discord token)
  3. ~/.github_token plain file (local testing fallback)
  4. No token — unauthenticated (60 req/hr, may hit rate limits)

The token needs only public repo read access: a classic PAT with no scopes
selected, or a fine-grained PAT with "Public Repositories (read-only)".
Either gives 5000 req/hr on public repos with zero write capability.
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
from datetime import datetime


# ---------------------------------------------------------------------------
# Token resolution: env var → OCI Vault → plain file → empty
# ---------------------------------------------------------------------------

def get_token():
    """Resolve GitHub token from the first available source."""

    # 1. Environment variable (works for interactive use and CI)
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        return token

    # 2. OCI Vault — same instance principal pattern as discord_archive.py
    vault_ocid = os.environ.get("GITHUB_VAULT_SECRET_OCID", "")
    if not vault_ocid:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        if os.path.exists(config_path):
            try:
                with open(config_path) as f:
                    cfg = json.load(f)
                vault_ocid = cfg.get("github_vault_secret_ocid", "")
            except Exception:
                pass

    if vault_ocid:
        try:
            import oci
            import base64
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            client = oci.secrets.SecretsClient({}, signer=signer)
            bundle = client.get_secret_bundle(vault_ocid).data
            token = base64.b64decode(
                bundle.secret_bundle_content.content
            ).decode().strip()
            print("  [token] Resolved from OCI Vault")
            return token
        except ImportError:
            print("  [token] oci package not available, skipping Vault")
        except Exception as e:
            print(f"  [token] OCI Vault fetch failed: {e}")

    # 3. Plain file (Mac/local testing)
    token_path = os.path.expanduser("~/.github_token")
    if os.path.exists(token_path):
        token = open(token_path).read().strip()
        if token:
            print("  [token] Resolved from ~/.github_token")
            return token

    return ""


# ---------------------------------------------------------------------------
# Artefact catalogue (copied from cluster_discord.py)
# ---------------------------------------------------------------------------

ARTEFACTS = {
    "adaptive-capture-classification": {
        "title": "Adaptive Capture Classification",
        "category": "recipe",
        "terms": ["adaptive capture", "confidence score", "threshold", "auto-classify",
                  "gating", "feedback loop", "classifier", "classification",
                  "nudge threshold", "capture type"],
    },
    "auto-capture": {
        "title": "Auto-Capture Protocol",
        "category": "recipe",
        "terms": ["auto-capture", "auto capture", "session end", "write flywheel",
                  "flywheel", "panning for gold", "ACT NOW", "session close"],
    },
    "brain-backup": {
        "title": "Brain Backup",
        "category": "recipe",
        "terms": ["brain backup", "backup thoughts", "export thoughts",
                  "disaster recovery", "CSV export", "backup"],
    },
    "bring-your-own-context": {
        "title": "Bring Your Own Context",
        "category": "recipe",
        "terms": ["bring your own context", "BYOC", "portable context",
                  "operating model", "USER.md", "SOUL.md", "HEARTBEAT.md",
                  "context profile", "memory extraction", "operating profile"],
    },
    "chatgpt-conversation-import": {
        "title": "ChatGPT Conversation Import",
        "category": "recipe",
        "terms": ["chatgpt import", "chatgpt conversation", "conversations.json",
                  "chatgpt export", "import chatgpt", "claude export",
                  "claude conversation import"],
    },
    "content-fingerprint-dedup": {
        "title": "Content Fingerprint Dedup",
        "category": "recipe",
        "terms": ["content fingerprint", "fingerprint", "dedup", "deduplicate",
                  "SHA-256", "sha256", "content hash", "content_hash",
                  "upsert_thought", "idempotent", "duplicate protection",
                  "duplicate thoughts", "on conflict", "bulk import duplicate",
                  "content_fingerprint"],
    },
    "daily-digest": {
        "title": "Daily Digest / Weekly Digest",
        "category": "recipe",
        "terms": ["daily digest", "weekly digest", "daily summary",
                  "morning digest", "scheduled summary", "importance-ranked",
                  "digest recipe"],
    },
    "email-history-import": {
        "title": "Email History Import",
        "category": "recipe",
        "terms": ["email import", "gmail import", "gmail history",
                  "email history", "import email"],
    },
    "entity-wiki": {
        "title": "Entity Wiki Pages",
        "category": "recipe",
        "terms": ["entity wiki", "wiki page", "per-entity", "dossier",
                  "Karpathy wiki", "karpathy", "llm wiki", "generate wiki",
                  "entity page", "thought_entities", "entity dossier",
                  "bio worker", "Who is", "wiki-page-per-entity",
                  "generate-wiki"],
    },
    "fingerprint-dedup-backfill": {
        "title": "Fingerprint Dedup Backfill",
        "category": "recipe",
        "terms": ["fingerprint backfill", "dedup backfill",
                  "backfill fingerprint", "near-duplicate", "SHA-256 hashing"],
    },
    "google-activity-import": {
        "title": "Google Activity Import",
        "category": "recipe",
        "terms": ["google activity", "google takeout", "takeout import",
                  "import google", "search history import", "youtube history"],
    },
    "life-engine": {
        "title": "Life Engine",
        "category": "recipe",
        "terms": ["life engine", "personal OS", "life management",
                  "goals tracking", "habits tracking", "rituals",
                  "/loop", "proactive briefings", "weekly self-improvement"],
    },
    "local-ollama-embeddings": {
        "title": "Local Ollama Embeddings",
        "category": "recipe",
        "terms": ["ollama", "local embeddings", "local model", "local llm",
                  "self-hosted embeddings", "replace openrouter",
                  "local openrouter", "local brain", "MLX",
                  "apple silicon embedding", "Qwen", "llama.cpp",
                  "local postgres", "local vector", "go local",
                  "fully local", "self-host", "homelab", "raspberry pi",
                  "docker compose", "pocketbase", "nomic-embed",
                  "mxbai-embed", "gte-qwen", "zero-cost embedding"],
    },
    "ob-graph": {
        "title": "OB-Graph: Knowledge Graph",
        "category": "recipe",
        "terms": ["ob-graph", "knowledge graph", "graph layer",
                  "graph_nodes", "graph_edges", "traverse", "multi-hop",
                  "shortest path", "recursive CTE", "relationship map",
                  "typed edge", "neo4j", "graph database",
                  "relationship modeling", "entity relationship",
                  "find_shortest_path", "BFS", "traverse_graph"],
    },
    "obsidian-vault-import": {
        "title": "Obsidian Vault Import",
        "category": "recipe",
        "terms": ["obsidian import", "obsidian vault", "vault import",
                  "wikilink", "obsidian migration"],
    },
    "panning-for-gold": {
        "title": "Panning for Gold",
        "category": "recipe",
        "terms": ["panning for gold", "brain dump", "voice transcript",
                  "ACT NOW", "PARK", "KILL verdict", "gold found",
                  "fathom", "otter", "fireflies", "transcript process",
                  "idea extraction"],
    },
    "perplexity-conversation-import": {
        "title": "Perplexity Conversation Import",
        "category": "recipe",
        "terms": ["perplexity import", "perplexity integration",
                  "perplexity MCP", "connect perplexity",
                  "perplexity connector", "perplexity conversation",
                  "perplexity memory", "perplexity export"],
    },
    "research-to-decision-workflow": {
        "title": "Research to Decision Workflow",
        "category": "recipe",
        "terms": ["research to decision", "decision workflow",
                  "decision framework", "decision provenance", "decision log"],
    },
    "schema-aware-routing": {
        "title": "Schema-Aware Routing",
        "category": "recipe",
        "terms": ["schema-aware routing", "schema routing", "auto-route",
                  "context routing", "route thoughts", "VOCABULARY_CONFIG",
                  "controlled vocabulary", "metadata vocabulary",
                  "metadata schema", "context silo", "Clay-Mate",
                  "clay-mate", "confidence scoring", "inbox pattern",
                  "slack_ts dedup"],
    },
    "thought-enrichment": {
        "title": "Thought Enrichment",
        "category": "recipe",
        "terms": ["thought enrichment", "enrich thoughts", "re-process",
                  "backfill tags", "backfill metadata", "update embeddings",
                  "update vector", "manual edit supabase", "edit thought",
                  "update thought", "delete thought", "CRUD",
                  "update_thought", "delete_thought", "thought management",
                  "enrichment pipeline", "metadata normalization"],
    },
    "typed-edge-classifier": {
        "title": "Typed Edge Classifier",
        "category": "recipe",
        "terms": ["typed edge", "edge classifier", "supports", "contradicts",
                  "evolved_into", "supersedes", "depends_on", "thought_edges",
                  "reasoning edge", "edge classification", "Haiku filter",
                  "Opus classify", "provenance chain", "derived_from",
                  "trace_provenance", "find_derivatives", "Anamnesis",
                  "Thinking-MCP"],
    },
    "vercel-neon-telegram": {
        "title": "Vercel + Neon + Telegram",
        "category": "recipe",
        "terms": ["vercel neon", "neon postgres", "telegram bot",
                  "telegram capture", "telegram integration", "telegram edge"],
    },
    "wiki-compiler": {
        "title": "Wiki Compiler",
        "category": "recipe",
        "terms": ["wiki compiler", "compile wiki", "wiki pipeline",
                  "compiled wiki", "compiled understanding",
                  "wiki generation", "compile-wiki"],
    },
    "wiki-synthesis": {
        "title": "Wiki Synthesis",
        "category": "recipe",
        "terms": ["wiki synthesis", "topic wiki", "autobiography wiki",
                  "topic synthesis", "synthesize thoughts"],
    },
    "work-operating-model-activation": {
        "title": "Work Operating Model Activation",
        "category": "recipe",
        "terms": ["operating model", "work operating model", "USER.md",
                  "SOUL.md", "HEARTBEAT.md", "operating profile",
                  "interview workflow"],
    },
    "x-twitter-import": {
        "title": "X / Twitter Import",
        "category": "recipe",
        "terms": ["twitter import", "X import", "tweet import",
                  "twitter archive", "twitter takeout"],
    },

    # Integrations
    "entity-extraction-worker": {
        "title": "Entity Extraction Worker",
        "category": "integration",
        "terms": ["entity extraction", "extract entities", "entity worker",
                  "NER", "thought_entities", "entity queue",
                  "entity extraction worker", "entity-extraction",
                  "append_thought_evidence", "SECURITY DEFINER",
                  "concurrent evidence", "FOR UPDATE"],
    },
    "kubernetes-deployment": {
        "title": "Kubernetes / Docker Deployment",
        "category": "integration",
        "terms": ["kubernetes", "k8s", "helm chart", "self-host",
                  "docker compose", "homelab", "VPS deployment",
                  "local supabase", "supabase docker",
                  "supabase self-host", "pgvector docker"],
    },
    "slack-capture": {
        "title": "Slack Capture",
        "category": "integration",
        "terms": ["slack capture", "slack webhook", "slack bot",
                  "slack integration", "ingest-thought", "slack retry",
                  "slack bot token", "slack_ts", "slack retry loop",
                  "webhook retry", "slack dedup"],
    },

    # Primitives
    "deploy-edge-function": {
        "title": "Deploy an Edge Function",
        "category": "primitive",
        "terms": ["deploy edge function", "supabase functions deploy",
                  "edge function deploy", "supabase CLI deploy",
                  "dashboard deploy", "deploy updates button"],
    },
    "remote-mcp": {
        "title": "Remote MCP Connection",
        "category": "primitive",
        "terms": ["remote MCP", "MCP connection", "MCP connector",
                  "connect MCP", "MCP access key", "?key=",
                  "x-brain-key", "claude desktop connector",
                  "ChatGPT MCP", "MCP URL", "streamable HTTP",
                  "OAuth flow", "Bearer token", "Authorization Bearer",
                  "RFC 6750"],
    },
    "rls": {
        "title": "Row-Level Security",
        "category": "primitive",
        "terms": ["RLS", "row-level security", "row level security",
                  "multi-user", "user_id", "auth.uid()", "user isolation",
                  "per-user data", "hardcode user", "DEFAULT_USER_ID",
                  "SECURITY DEFINER", "service_role", "GRANT"],
    },
    "shared-mcp": {
        "title": "Shared MCP",
        "category": "primitive",
        "terms": ["shared MCP", "shared brain", "multi-user brain",
                  "team brain", "shared instance", "organization MCP"],
    },

    # Architecture
    "extensions": {
        "title": "Extensions (domain-specific tables)",
        "category": "architecture",
        "terms": ["extension", "household knowledge", "home maintenance",
                  "family calendar", "professional CRM", "job hunt",
                  "meal planning", "separate table", "extension table",
                  "single table vs", "one table vs multiple",
                  "multi-table", "extension MCP", "entity-based",
                  "topic-based", "shared entity", "Person table",
                  "Organization table"],
    },
    "dashboards": {
        "title": "Dashboards (SvelteKit / Next.js)",
        "category": "architecture",
        "terms": ["dashboard", "SvelteKit", "Next.js", "frontend",
                  "web UI", "browse thoughts", "visual interface",
                  "memory browser", "Vercel deploy", "iron-session",
                  "smart capture", "duplicate detection"],
    },
    "server-core": {
        "title": "Core MCP Server (server/index.ts)",
        "category": "architecture",
        "terms": ["index.ts", "MCP server", "Hono",
                  "StreamableHTTPTransport", "open-brain-mcp",
                  "capture_thought", "search_thoughts", "list_thoughts",
                  "thought_stats", "match_thoughts",
                  "REST gateway", "REST API", "ingestion_jobs",
                  "sensitivity_tier", "sensitivity tiers",
                  "smart ingest", "atomic extraction",
                  "ambient capture", "Claude Code capture"],
    },
}

GH_OWNER = "NateBJones-Projects"
GH_REPO = "OB1"
GH_API_BASE = "https://api.github.com"

# Issues to fetch in full (body + all comments)
ISSUES_TO_FETCH = [33, 35, 36, 61, 67, 68, 73, 82, 84]

# PRs to fetch (body + PR discussion comments + review comments)
# Ordered roughly by artefact importance
PRS_TO_FETCH = [
    21,   # panning-for-gold
    38,   # telegram / vercel-neon-telegram
    40,   # kubernetes / docker full stack
    43,   # telegram refresh
    46,   # REST API gateway
    55,   # update_thought + delete_thought
    82,   # MCP Tool Audit guide
    86,   # life-engine + life-engine-video
    98,   # ingestion jobs schema
    99,   # universal ingest primitives
    100,  # smart ingest edge function
    101,  # fingerprint-dedup-backfill
    109,  # perplexity-conversation-import
    110,  # sensitivity tiers
    111,  # dashboard
    112,  # openclaw memory plugin
    160,  # local-ollama-embeddings
    174,  # cloudflare deployment
    194,  # SQL health/lint views
    197,  # typed entity edges schema
    199,  # entity extraction worker
    200,  # bio worker
    204,  # lint sweep
    205,  # weekly digest
    206,  # text search trigram
    207,  # provenance chains
    208,  # typed reasoning edges
    209,  # dashboard pro
    210,  # ob-graph BFS fix
    211,  # brain smoke test
    212,  # synthesis capture
    213,  # entity wiki pages
    214,  # chrome capture extension
    228,  # update vector embeddings
    238,  # oauth flow / remote-mcp
]


# ---------------------------------------------------------------------------
# Scoring (same logic as cluster_discord.py)
# ---------------------------------------------------------------------------

def score_text(text, artefact_slug):
    text_lower = text.lower()
    terms = ARTEFACTS[artefact_slug]["terms"]
    return sum(1 for t in terms if t.lower() in text_lower)


def top_artefacts(text, min_score=1, top_n=5):
    scores = {}
    for slug in ARTEFACTS:
        s = score_text(text, slug)
        if s >= min_score:
            scores[slug] = s
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]


# ---------------------------------------------------------------------------
# GitHub API client
# ---------------------------------------------------------------------------

def gh_get(path, token, page=1, per_page=100):
    """GET from GitHub API. Returns parsed JSON or None on 404."""
    url = f"{GH_API_BASE}{path}?per_page={per_page}&page={page}"
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
            print(f"  [rate-limit or forbidden] {path}")
            time.sleep(60)
            return None
        print(f"  [HTTP {e.code}] {path}")
        return None
    except Exception as ex:
        print(f"  [error] {path}: {ex}")
        return None


def gh_get_all_pages(path, token):
    """Fetch all pages of a paginated endpoint."""
    results = []
    page = 1
    while True:
        batch = gh_get(path, token, page=page)
        if not batch:
            break
        if isinstance(batch, list):
            results.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        else:
            # Non-list response (single object)
            return batch
    return results


# ---------------------------------------------------------------------------
# Pass 4a: Issue body + comments
# ---------------------------------------------------------------------------

def pass4a_issues(token):
    results = []
    for issue_num in ISSUES_TO_FETCH:
        print(f"  Issue #{issue_num}...")

        # Fetch issue body
        issue = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues/{issue_num}", token
        )
        if not issue:
            continue
        time.sleep(0.3)

        title = issue.get("title", "")
        body = issue.get("body", "") or ""
        combined = f"{title}\n{body}"
        created_at = issue.get("created_at", "")
        user = issue.get("user", {}).get("login", "")

        matches = top_artefacts(combined, min_score=1)
        for slug, score in matches:
            results.append({
                "pass": 4,
                "match_basis": "github_issue_body",
                "channel_id": None,
                "channel_name": f"issue#{issue_num}: {title[:80]}",
                "category": "github",
                "artefact_slug": slug,
                "artefact_title": ARTEFACTS[slug]["title"],
                "artefact_category": ARTEFACTS[slug]["category"],
                "score": score,
                "window_start": created_at,
                "window_end": created_at,
                "github_refs": [f"issue#{issue_num}"],
                "author": user,
                "sample_text": combined[:400],
            })

        # Fetch comments
        comments = gh_get_all_pages(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues/{issue_num}/comments", token
        )
        time.sleep(0.3)

        for comment in (comments if isinstance(comments, list) else []):
            c_body = comment.get("body", "") or ""
            c_user = comment.get("user", {}).get("login", "")
            c_ts = comment.get("created_at", "")
            c_url = comment.get("html_url", "")
            # Include issue title for context
            c_combined = f"{title}\n{c_body}"

            matches = top_artefacts(c_combined, min_score=1)
            for slug, score in matches:
                results.append({
                    "pass": 4,
                    "match_basis": "github_issue_comment",
                    "channel_id": None,
                    "channel_name": f"issue#{issue_num}: {title[:60]}",
                    "category": "github",
                    "artefact_slug": slug,
                    "artefact_title": ARTEFACTS[slug]["title"],
                    "artefact_category": ARTEFACTS[slug]["category"],
                    "score": score,
                    "window_start": c_ts,
                    "window_end": c_ts,
                    "github_refs": [f"issue#{issue_num}"],
                    "author": c_user,
                    "sample_text": c_body[:400],
                })

    return results


# ---------------------------------------------------------------------------
# Pass 4b: PR body + discussion comments + review thread comments
# ---------------------------------------------------------------------------

def pass4b_prs(token):
    results = []
    for pr_num in PRS_TO_FETCH:
        print(f"  PR #{pr_num}...")

        # PR body (via issues endpoint — works for PRs too)
        pr = gh_get(
            f"/repos/{GH_OWNER}/{GH_REPO}/pulls/{pr_num}", token
        )
        if not pr:
            time.sleep(0.3)
            continue
        time.sleep(0.3)

        title = pr.get("title", "")
        body = pr.get("body", "") or ""
        created_at = pr.get("created_at", "")
        user = pr.get("user", {}).get("login", "")
        merged = pr.get("merged", False)
        state = "merged" if merged else pr.get("state", "unknown")

        combined = f"{title}\n{body}"
        matches = top_artefacts(combined, min_score=1)
        for slug, score in matches:
            results.append({
                "pass": 4,
                "match_basis": "github_pr_body",
                "channel_id": None,
                "channel_name": f"PR#{pr_num} ({state}): {title[:70]}",
                "category": "github",
                "artefact_slug": slug,
                "artefact_title": ARTEFACTS[slug]["title"],
                "artefact_category": ARTEFACTS[slug]["category"],
                "score": score,
                "window_start": created_at,
                "window_end": created_at,
                "github_refs": [f"PR#{pr_num}"],
                "author": user,
                "sample_text": combined[:400],
            })

        # PR discussion comments (the threaded conversation, not inline code)
        discussion = gh_get_all_pages(
            f"/repos/{GH_OWNER}/{GH_REPO}/issues/{pr_num}/comments", token
        )
        time.sleep(0.3)

        for comment in (discussion if isinstance(discussion, list) else []):
            c_body = comment.get("body", "") or ""
            c_user = comment.get("user", {}).get("login", "")
            c_ts = comment.get("created_at", "")
            c_combined = f"{title}\n{c_body}"

            matches = top_artefacts(c_combined, min_score=1)
            for slug, score in matches:
                results.append({
                    "pass": 4,
                    "match_basis": "github_pr_comment",
                    "channel_id": None,
                    "channel_name": f"PR#{pr_num} ({state}): {title[:60]}",
                    "category": "github",
                    "artefact_slug": slug,
                    "artefact_title": ARTEFACTS[slug]["title"],
                    "artefact_category": ARTEFACTS[slug]["category"],
                    "score": score,
                    "window_start": c_ts,
                    "window_end": c_ts,
                    "github_refs": [f"PR#{pr_num}"],
                    "author": c_user,
                    "sample_text": c_body[:400],
                })

        # PR review comments (inline code review threads)
        reviews = gh_get_all_pages(
            f"/repos/{GH_OWNER}/{GH_REPO}/pulls/{pr_num}/comments", token
        )
        time.sleep(0.3)

        for comment in (reviews if isinstance(reviews, list) else []):
            c_body = comment.get("body", "") or ""
            c_user = comment.get("user", {}).get("login", "")
            c_ts = comment.get("created_at", "")
            c_path = comment.get("path", "")  # file being reviewed
            c_combined = f"{title}\n[{c_path}]\n{c_body}"

            matches = top_artefacts(c_combined, min_score=1)
            for slug, score in matches:
                results.append({
                    "pass": 4,
                    "match_basis": "github_pr_review_comment",
                    "channel_id": None,
                    "channel_name": f"PR#{pr_num} ({state}): {title[:60]}",
                    "category": "github",
                    "artefact_slug": slug,
                    "artefact_title": ARTEFACTS[slug]["title"],
                    "artefact_category": ARTEFACTS[slug]["category"],
                    "score": score,
                    "window_start": c_ts,
                    "window_end": c_ts,
                    "github_refs": [f"PR#{pr_num}"],
                    "author": c_user,
                    "sample_text": c_body[:400],
                })

    return results


# ---------------------------------------------------------------------------
# Database: append to existing artefact_linkages table
# ---------------------------------------------------------------------------

def append_to_linkage_table(conn, results, dry_run=False):
    if dry_run:
        print(f"[dry-run] Would append {len(results)} GitHub rows")
        return

    # Remove any previous pass-4 rows so re-runs are idempotent
    conn.execute("DELETE FROM artefact_linkages WHERE pass = 4")

    for r in results:
        conn.execute(
            """INSERT INTO artefact_linkages
               (pass, match_basis, channel_id, channel_name, category,
                artefact_slug, artefact_title, artefact_category,
                score, window_start, window_end, github_refs, sample_text)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                r.get("pass"),
                r.get("match_basis"),
                r.get("channel_id"),
                r.get("channel_name"),
                r.get("category"),
                r.get("artefact_slug"),
                r.get("artefact_title"),
                r.get("artefact_category"),
                r.get("score"),
                r.get("window_start"),
                r.get("window_end"),
                json.dumps(r.get("github_refs", [])),
                r.get("sample_text", "")[:500],
            ),
        )
    conn.commit()
    print(f"Appended {len(results)} GitHub rows to artefact_linkages")


# ---------------------------------------------------------------------------
# Regenerate reports (combined Discord + GitHub)
# ---------------------------------------------------------------------------

def regenerate_reports(conn, out_dir):
    """Read all linkage rows and rewrite the report files."""
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

    # CSV
    csv_path = os.path.join(out_dir, "linkage_report_full.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    print(f"Written: {csv_path}")

    # Summary markdown
    md_path = os.path.join(out_dir, "linkage_report_full.md")
    _write_summary_md(results, md_path)
    print(f"Written: {md_path}")


def _write_summary_md(results, path):
    by_artefact = defaultdict(list)
    for r in results:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug].append(r)

    lines = ["# OB1 Discord + GitHub → Artefact Linkage Report (Full)", ""]
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Total linkage rows: {len(results)}")
    lines.append(f"Artefacts matched: {len(by_artefact)}")
    lines.append("")

    # Score by pass so we can show Discord vs GitHub breakdown
    def total_score(entries):
        return sum(e["score"] for e in entries)

    lines.append("## Artefacts by combined score (Discord + GitHub)")
    lines.append("")
    for slug in sorted(by_artefact, key=lambda s: -total_score(by_artefact[s])):
        entries = by_artefact[slug]
        disc = [e for e in entries if e["pass"] in (1, 2, 3)]
        gh = [e for e in entries if e["pass"] == 4]
        title = ARTEFACTS.get(slug, {}).get("title", slug)
        disc_score = sum(e["score"] for e in disc)
        gh_score = sum(e["score"] for e in gh)
        total = disc_score + gh_score
        lines.append(
            f"### `{slug}` — {title} "
            f"(total: {total} | Discord: {disc_score} | GitHub: {gh_score})"
        )
        seen = set()
        for e in sorted(entries, key=lambda x: -x["score"]):
            ch = e["channel_name"]
            if ch in seen:
                continue
            seen.add(ch)
            basis = e["match_basis"]
            score = e["score"]
            sample = (e.get("sample_text") or "")[:100].replace("\n", " ")
            lines.append(f"  - [{basis}, {score}] **{ch}**: {sample}…")
        lines.append("")

    # GitHub refs summary
    lines.append("## GitHub PR/issue references (all passes)")
    lines.append("")
    all_refs = defaultdict(set)
    for r in results:
        refs = r.get("github_refs")
        if refs:
            parsed = json.loads(refs) if isinstance(refs, str) else refs
            for ref in parsed:
                all_refs[ref].add(r["channel_name"])
    for ref in sorted(all_refs,
                      key=lambda x: int(re.search(r"\d+", x).group())):
        channels = "; ".join(sorted(all_refs[ref]))
        lines.append(f"- `{ref}` — {channels}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pass 4: GitHub issue/PR content → artefact linkages"
    )
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/discord-capture/discord_archive.db"),
        help="Path to discord_archive.db (must have artefact_linkages table)",
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.expanduser("~/discord-output"),
        help="Directory to write updated report files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from GitHub but skip writing to DB",
    )
    args = parser.parse_args()

    print("Resolving GitHub token...")
    token = get_token()
    if not token:
        print("  [token] No token found — using unauthenticated API (60 req/hr).")
        print("  Set GITHUB_TOKEN, add github_vault_secret_ocid to config.json,")
        print("  or write a token to ~/.github_token")

    os.makedirs(args.out_dir, exist_ok=True)

    conn = sqlite3.connect(args.db)

    # Verify artefact_linkages exists
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "artefact_linkages" not in tables:
        print("Error: artefact_linkages table not found. Run cluster_discord.py first.")
        conn.close()
        return

    print("Pass 4a: Fetching GitHub issue bodies + comments...")
    p4a = pass4a_issues(token)
    print(f"  {len(p4a)} linkages from issue content")

    print("Pass 4b: Fetching GitHub PR bodies + discussion + review comments...")
    p4b = pass4b_prs(token)
    print(f"  {len(p4b)} linkages from PR content")

    all_gh = p4a + p4b
    print(f"Total GitHub linkages: {len(all_gh)}")

    append_to_linkage_table(conn, all_gh, dry_run=args.dry_run)

    print("Regenerating combined reports...")
    regenerate_reports(conn, args.out_dir)

    conn.close()

    # Quick top-10 by combined GitHub score
    by_artefact = defaultdict(int)
    for r in all_gh:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug] += r["score"]
    print("\nTop artefacts by GitHub score:")
    for slug, score in sorted(by_artefact.items(), key=lambda x: -x[1])[:10]:
        print(f"  {score:4d}  {slug}")

    print("\nDone. Fetch the updated reports:")
    print(f"  scp ubuntu@144.24.44.81:{args.out_dir}/linkage_report_full.md ~/Desktop/")
    print(f"  scp ubuntu@144.24.44.81:{args.out_dir}/linkage_report_full.csv ~/Desktop/")


if __name__ == "__main__":
    main()
