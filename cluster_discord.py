#!/usr/bin/env python3
"""
cluster_discord.py — Link OB1 Discord archive to OB1 repo artefacts.

Three passes:
  1. Forum thread title keyword matching
  2. Message body keyword matching with 30-minute conversation windows
  3. GitHub PR/issue URL extraction

Output:
  - artefact_linkages table in the SQLite DB
  - linkage_report.json  (human-readable summary)
  - linkage_report.csv   (for spreadsheet review)

Usage:
  python3 cluster_discord.py [--db PATH] [--out-dir PATH] [--dry-run]

Default db path: ~/discord-capture/discord_archive.db
Default out dir: ./  (same directory as script)
"""

import sqlite3
import json
import csv
import re
import argparse
import os
from datetime import datetime, timezone
from collections import defaultdict


# ---------------------------------------------------------------------------
# Artefact catalogue
# Key terms are matched case-insensitively against channel names and message
# content. Put the most distinctive terms first — they score higher.
# ---------------------------------------------------------------------------

ARTEFACTS = {
    # --- Recipes ---
    "adaptive-capture-classification": {
        "title": "Adaptive Capture Classification",
        "category": "recipe",
        "terms": [
            "adaptive capture", "confidence score", "threshold", "auto-classify",
            "gating", "feedback loop", "classifier", "classification",
            "nudge threshold", "capture type",
        ],
    },
    "auto-capture": {
        "title": "Auto-Capture Protocol",
        "category": "recipe",
        "terms": [
            "auto-capture", "auto capture", "session end", "write flywheel",
            "flywheel", "panning for gold", "ACT NOW", "session close",
        ],
    },
    "brain-backup": {
        "title": "Brain Backup",
        "category": "recipe",
        "terms": [
            "brain backup", "backup thoughts", "export thoughts", "disaster recovery",
            "CSV export", "backup",
        ],
    },
    "bring-your-own-context": {
        "title": "Bring Your Own Context",
        "category": "recipe",
        "terms": [
            "bring your own context", "BYOC", "portable context", "operating model",
            "USER.md", "SOUL.md", "HEARTBEAT.md", "context profile", "memory extraction",
            "operating profile",
        ],
    },
    "chatgpt-conversation-import": {
        "title": "ChatGPT Conversation Import",
        "category": "recipe",
        "terms": [
            "chatgpt import", "chatgpt conversation", "conversations.json",
            "chatgpt export", "import chatgpt",
        ],
    },
    "claudeception": {
        "title": "Claudeception",
        "category": "recipe",
        "terms": [
            "claudeception", "claude in claude", "nested claude", "claude artifact api",
        ],
    },
    "content-fingerprint-dedup": {
        "title": "Content Fingerprint Dedup",
        "category": "recipe",
        "terms": [
            "content fingerprint", "fingerprint", "dedup", "deduplicate", "SHA-256",
            "sha256", "content hash", "content_hash", "upsert_thought",
            "idempotent", "duplicate protection", "duplicate thoughts",
            "on conflict", "bulk import duplicate",
        ],
    },
    "daily-digest": {
        "title": "Daily Digest",
        "category": "recipe",
        "terms": [
            "daily digest", "daily summary", "morning digest", "scheduled summary",
        ],
    },
    "email-history-import": {
        "title": "Email History Import",
        "category": "recipe",
        "terms": [
            "email import", "gmail import", "gmail history", "email history",
            "import email",
        ],
    },
    "entity-wiki": {
        "title": "Entity Wiki Pages",
        "category": "recipe",
        "terms": [
            "entity wiki", "wiki page", "per-entity", "dossier", "Karpathy wiki",
            "karpathy", "llm wiki", "generate wiki", "entity page",
            "thought_entities", "entity dossier",
        ],
    },
    "fingerprint-dedup-backfill": {
        "title": "Fingerprint Dedup Backfill",
        "category": "recipe",
        "terms": [
            "fingerprint backfill", "dedup backfill", "backfill fingerprint",
        ],
    },
    "google-activity-import": {
        "title": "Google Activity Import",
        "category": "recipe",
        "terms": [
            "google activity", "google takeout", "takeout import", "import google",
            "search history import", "youtube history",
        ],
    },
    "grok-export-import": {
        "title": "Grok Export Import",
        "category": "recipe",
        "terms": ["grok import", "grok export", "xAI import"],
    },
    "infographic-generator": {
        "title": "Infographic Generator",
        "category": "recipe",
        "terms": ["infographic", "generate infographic", "visual summary"],
    },
    "instagram-import": {
        "title": "Instagram Import",
        "category": "recipe",
        "terms": ["instagram import", "instagram takeout", "import instagram"],
    },
    "journals-blogger-import": {
        "title": "Journals / Blogger Import",
        "category": "recipe",
        "terms": ["journal import", "blogger import", "diary import", "blog import"],
    },
    "life-engine": {
        "title": "Life Engine",
        "category": "recipe",
        "terms": [
            "life engine", "personal OS", "life management", "goals tracking",
            "habits tracking", "rituals",
        ],
    },
    "live-retrieval": {
        "title": "Live Retrieval",
        "category": "recipe",
        "terms": [
            "live retrieval", "real-time retrieval", "context injection",
            "inject context", "automatic retrieval",
        ],
    },
    "local-ollama-embeddings": {
        "title": "Local Ollama Embeddings",
        "category": "recipe",
        "terms": [
            "ollama", "local embeddings", "local model", "local llm",
            "self-hosted embeddings", "replace openrouter", "local openrouter",
            "local brain", "MLX", "apple silicon embedding", "Qwen", "llama.cpp",
            "local postgres", "local vector", "go local", "fully local",
            "self-host", "self hosted", "homelab", "raspberry pi", "pi5",
            "docker compose", "pocketbase",
        ],
    },
    "ob-graph": {
        "title": "OB-Graph: Knowledge Graph",
        "category": "recipe",
        "terms": [
            "ob-graph", "knowledge graph", "graph layer", "graph nodes", "graph edges",
            "traverse", "multi-hop", "shortest path", "recursive CTE",
            "relationship map", "node", "typed edge", "neo4j", "graph database",
            "relationship modeling", "entity relationship",
        ],
    },
    "obsidian-vault-import": {
        "title": "Obsidian Vault Import",
        "category": "recipe",
        "terms": [
            "obsidian import", "obsidian vault", "vault import", "wikilink",
            "obsidian migration",
        ],
    },
    "panning-for-gold": {
        "title": "Panning for Gold",
        "category": "recipe",
        "terms": [
            "panning for gold", "brain dump", "voice transcript", "ACT NOW",
            "PARK", "KILL verdict", "gold found", "fathom", "otter", "fireflies",
            "transcript process", "idea extraction",
        ],
    },
    "perplexity-conversation-import": {
        "title": "Perplexity Conversation Import",
        "category": "recipe",
        "terms": [
            "perplexity import", "perplexity integration", "perplexity MCP",
            "connect perplexity", "perplexity connector",
        ],
    },
    "repo-learning-coach": {
        "title": "Repo Learning Coach",
        "category": "recipe",
        "terms": ["repo learning", "learning coach", "repo coach", "repo index"],
    },
    "research-to-decision-workflow": {
        "title": "Research to Decision Workflow",
        "category": "recipe",
        "terms": [
            "research to decision", "decision workflow", "decision framework",
            "decision provenance", "decision log",
        ],
    },
    "schema-aware-routing": {
        "title": "Schema-Aware Routing",
        "category": "recipe",
        "terms": [
            "schema-aware routing", "schema routing", "auto-route", "context routing",
            "route thoughts", "VOCABULARY_CONFIG", "controlled vocabulary",
            "metadata vocabulary", "metadata schema", "context silo",
        ],
    },
    "source-filtering": {
        "title": "Source Filtering",
        "category": "recipe",
        "terms": [
            "source filter", "filter by source", "slack only", "discord only",
            "source scope",
        ],
    },
    "thought-enrichment": {
        "title": "Thought Enrichment",
        "category": "recipe",
        "terms": [
            "thought enrichment", "enrich thoughts", "re-process", "backfill tags",
            "backfill metadata", "update embeddings", "update vector",
            "manual edit supabase", "edit thought", "update thought",
            "delete thought", "CRUD", "update_thought", "delete_thought",
        ],
    },
    "typed-edge-classifier": {
        "title": "Typed Edge Classifier",
        "category": "recipe",
        "terms": [
            "typed edge", "edge classifier", "supports", "contradicts",
            "evolved_into", "supersedes", "depends_on", "thought_edges",
            "reasoning edge", "edge classification", "Haiku filter",
            "Opus classify",
        ],
    },
    "vercel-neon-telegram": {
        "title": "Vercel + Neon + Telegram",
        "category": "recipe",
        "terms": [
            "vercel neon", "neon postgres", "telegram bot", "telegram capture",
            "telegram integration", "telegram edge",
        ],
    },
    "wiki-compiler": {
        "title": "Wiki Compiler",
        "category": "recipe",
        "terms": [
            "wiki compiler", "compile wiki", "wiki pipeline", "compiled wiki",
            "compiled understanding", "wiki generation", "compile-wiki",
        ],
    },
    "wiki-synthesis": {
        "title": "Wiki Synthesis",
        "category": "recipe",
        "terms": [
            "wiki synthesis", "topic wiki", "autobiography wiki",
            "topic synthesis", "synthesize thoughts",
        ],
    },
    "work-operating-model-activation": {
        "title": "Work Operating Model Activation",
        "category": "recipe",
        "terms": [
            "operating model", "work operating model", "USER.md", "SOUL.md",
            "HEARTBEAT.md", "operating profile", "interview workflow",
        ],
    },
    "world-model-diagnostic-activation": {
        "title": "World Model Diagnostic",
        "category": "recipe",
        "terms": [
            "world model diagnostic", "world model", "diagnostic activation",
            "model audit", "knowledge gaps",
        ],
    },
    "x-twitter-import": {
        "title": "X / Twitter Import",
        "category": "recipe",
        "terms": [
            "twitter import", "X import", "tweet import", "twitter archive",
            "twitter takeout",
        ],
    },

    # --- Integrations ---
    "discord-capture-bot": {
        "title": "Discord Capture (Bot Integration)",
        "category": "integration",
        "terms": [
            "discord bot", "discord capture", "discord connector",
            "discord channel capture", "discord token",
        ],
    },
    "entity-extraction-worker": {
        "title": "Entity Extraction Worker",
        "category": "integration",
        "terms": [
            "entity extraction", "extract entities", "entity worker",
            "NER", "thought_entities", "entity queue", "entity extraction worker",
            "entity-extraction",
        ],
    },
    "kubernetes-deployment": {
        "title": "Kubernetes Deployment",
        "category": "integration",
        "terms": [
            "kubernetes", "k8s", "helm chart", "self-host", "self hosted",
            "docker compose", "homelab", "VPS deployment", "local supabase",
            "supabase docker", "supabase self-host",
        ],
    },
    "slack-capture": {
        "title": "Slack Capture",
        "category": "integration",
        "terms": [
            "slack capture", "slack webhook", "slack bot", "slack integration",
            "ingest-thought", "slack retry", "slack bot token",
            "slack channel capture",
        ],
    },

    # --- Primitives ---
    "deploy-edge-function": {
        "title": "Deploy an Edge Function",
        "category": "primitive",
        "terms": [
            "deploy edge function", "supabase functions deploy", "edge function deploy",
            "supabase CLI deploy", "dashboard deploy", "deploy updates button",
        ],
    },
    "remote-mcp": {
        "title": "Remote MCP Connection",
        "category": "primitive",
        "terms": [
            "remote MCP", "MCP connection", "MCP connector", "connect MCP",
            "MCP access key", "?key=", "x-brain-key", "claude desktop connector",
            "ChatGPT MCP", "MCP URL", "streamable HTTP",
        ],
    },
    "rls": {
        "title": "Row-Level Security",
        "category": "primitive",
        "terms": [
            "RLS", "row-level security", "row level security", "multi-user",
            "user_id", "auth.uid()", "user isolation", "per-user data",
            "hardcode user", "DEFAULT_USER_ID",
        ],
    },
    "shared-mcp": {
        "title": "Shared MCP",
        "category": "primitive",
        "terms": [
            "shared MCP", "shared brain", "multi-user brain", "team brain",
            "shared instance", "organization MCP",
        ],
    },

    # --- Structural / architectural ---
    "extensions": {
        "title": "Extensions (domain-specific tables)",
        "category": "architecture",
        "terms": [
            "extension", "household knowledge", "home maintenance", "family calendar",
            "professional CRM", "job hunt", "meal planning", "separate table",
            "extension table", "single table vs", "one table vs multiple",
            "multi-table", "extension MCP",
        ],
    },
    "dashboards": {
        "title": "Dashboards (SvelteKit / Next.js)",
        "category": "architecture",
        "terms": [
            "dashboard", "SvelteKit", "Next.js", "frontend", "web UI",
            "browse thoughts", "visual interface", "memory browser",
        ],
    },
    "server-core": {
        "title": "Core MCP Server (server/index.ts)",
        "category": "architecture",
        "terms": [
            "index.ts", "MCP server", "Hono", "StreamableHTTPTransport",
            "open-brain-mcp", "capture_thought", "search_thoughts",
            "list_thoughts", "thought_stats", "match_thoughts",
            "Authorization Bearer", "Bearer token",
        ],
    },
}


# ---------------------------------------------------------------------------
# GitHub PR/issue number patterns
# ---------------------------------------------------------------------------

GH_PR_RE = re.compile(
    r"github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/(\d+)", re.IGNORECASE
)
GH_ISSUE_RE = re.compile(
    r"github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/issues/(\d+)", re.IGNORECASE
)


def extract_github_refs(text):
    prs = GH_PR_RE.findall(text)
    issues = GH_ISSUE_RE.findall(text)
    return [f"PR#{n}" for n in prs] + [f"issue#{n}" for n in issues]


# ---------------------------------------------------------------------------
# Keyword scoring
# ---------------------------------------------------------------------------

def score_text(text, artefact_slug):
    """Return a hit count for how many terms from the artefact appear in text."""
    text_lower = text.lower()
    terms = ARTEFACTS[artefact_slug]["terms"]
    hits = sum(1 for t in terms if t.lower() in text_lower)
    return hits


def top_artefacts(text, min_score=1, top_n=5):
    """Return ranked list of (slug, score) for artefacts matching text."""
    scores = {}
    for slug in ARTEFACTS:
        s = score_text(text, slug)
        if s >= min_score:
            scores[slug] = s
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]


# ---------------------------------------------------------------------------
# Pass 1: Forum thread title matching
# ---------------------------------------------------------------------------

def pass1_thread_titles(conn):
    """Match GuildPublicThread channel names against artefact key terms."""
    cur = conn.execute(
        "SELECT channel_id, name, category, parent_channel_id "
        "FROM channels WHERE type = 'GuildPublicThread'"
    )
    rows = cur.fetchall()

    results = []
    for channel_id, name, category, parent_id in rows:
        matches = top_artefacts(name, min_score=1)
        for slug, score in matches:
            results.append({
                "pass": 1,
                "match_basis": "thread_title",
                "channel_id": channel_id,
                "channel_name": name,
                "category": category,
                "artefact_slug": slug,
                "artefact_title": ARTEFACTS[slug]["title"],
                "artefact_category": ARTEFACTS[slug]["category"],
                "score": score,
                "window_start": None,
                "window_end": None,
                "github_refs": [],
                "sample_text": name,
            })
    return results


# ---------------------------------------------------------------------------
# Pass 2: Message body keyword matching with conversation windows
# ---------------------------------------------------------------------------

WINDOW_MINUTES = 30


def pass2_message_bodies(conn):
    """
    For all channels, scan message bodies for artefact key terms.
    Group nearby messages (within WINDOW_MINUTES) into conversation windows
    and score each window.
    """
    cur = conn.execute(
        "SELECT m.message_id, m.channel_id, m.timestamp, m.author_name, "
        "       m.content, c.name, c.category, c.type "
        "FROM messages m "
        "JOIN channels c ON c.channel_id = m.channel_id "
        "WHERE length(m.content) > 20 "
        "ORDER BY m.channel_id, m.timestamp"
    )
    rows = cur.fetchall()

    # Group messages by channel
    by_channel = defaultdict(list)
    for row in rows:
        by_channel[row[1]].append(row)

    results = []

    for channel_id, messages in by_channel.items():
        if not messages:
            continue
        channel_name = messages[0][5]
        category = messages[0][6]
        channel_type = messages[0][7]

        # Build conversation windows: new window when gap > WINDOW_MINUTES
        windows = []
        current_window = [messages[0]]
        for msg in messages[1:]:
            ts_prev = parse_ts(current_window[-1][2])
            ts_curr = parse_ts(msg[2])
            if ts_curr and ts_prev:
                gap_minutes = (ts_curr - ts_prev).total_seconds() / 60
                if gap_minutes > WINDOW_MINUTES:
                    windows.append(current_window)
                    current_window = [msg]
                else:
                    current_window.append(msg)
            else:
                current_window.append(msg)
        windows.append(current_window)

        for window in windows:
            combined_text = " ".join(m[4] for m in window if m[4])
            matches = top_artefacts(combined_text, min_score=2)
            gh_refs = extract_github_refs(combined_text)

            for slug, score in matches:
                results.append({
                    "pass": 2,
                    "match_basis": "message_body",
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "category": category,
                    "artefact_slug": slug,
                    "artefact_title": ARTEFACTS[slug]["title"],
                    "artefact_category": ARTEFACTS[slug]["category"],
                    "score": score,
                    "window_start": window[0][2],
                    "window_end": window[-1][2],
                    "github_refs": gh_refs,
                    "sample_text": combined_text[:300],
                })

    return results


# ---------------------------------------------------------------------------
# Pass 3: GitHub PR/issue URL extraction from all messages
# ---------------------------------------------------------------------------

def pass3_github_refs(conn):
    """Extract all GitHub PR/issue references from all messages."""
    cur = conn.execute(
        "SELECT m.message_id, m.channel_id, m.timestamp, m.author_name, "
        "       m.content, c.name, c.category "
        "FROM messages m "
        "JOIN channels c ON c.channel_id = m.channel_id "
        "WHERE m.content LIKE '%github.com%' AND length(m.content) > 10 "
        "ORDER BY m.timestamp"
    )
    rows = cur.fetchall()

    results = []
    for msg_id, channel_id, ts, author, content, channel_name, category in rows:
        prs = GH_PR_RE.findall(content or "")
        issues = GH_ISSUE_RE.findall(content or "")
        if not prs and not issues:
            continue
        refs = [f"PR#{n}" for n in prs] + [f"issue#{n}" for n in issues]
        results.append({
            "pass": 3,
            "match_basis": "github_ref",
            "channel_id": channel_id,
            "channel_name": channel_name,
            "category": category,
            "artefact_slug": None,       # resolved separately via GitHub API
            "artefact_title": None,
            "artefact_category": None,
            "score": len(refs),
            "window_start": ts,
            "window_end": ts,
            "github_refs": refs,
            "author": author,
            "sample_text": content[:300] if content else "",
        })
    return results


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def parse_ts(ts_str):
    if not ts_str:
        return None
    try:
        # Handle +00:00 suffix
        ts_str = ts_str.replace("+00:00", "").rstrip("Z")
        # Truncate to microseconds (6 decimal places)
        if "." in ts_str:
            base, frac = ts_str.split(".", 1)
            frac = frac[:6]
            ts_str = f"{base}.{frac}"
        return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Database output
# ---------------------------------------------------------------------------

def write_linkage_table(conn, results, dry_run=False):
    if dry_run:
        print(f"[dry-run] Would write {len(results)} rows to artefact_linkages table")
        return

    conn.execute("DROP TABLE IF EXISTS artefact_linkages")
    conn.execute("""
        CREATE TABLE artefact_linkages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pass INTEGER,
            match_basis TEXT,
            channel_id TEXT,
            channel_name TEXT,
            category TEXT,
            artefact_slug TEXT,
            artefact_title TEXT,
            artefact_category TEXT,
            score INTEGER,
            window_start TEXT,
            window_end TEXT,
            github_refs TEXT,
            sample_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

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
    print(f"Written {len(results)} rows to artefact_linkages table")


# ---------------------------------------------------------------------------
# CSV and JSON output
# ---------------------------------------------------------------------------

def write_csv(results, path):
    if not results:
        return
    fieldnames = [
        "pass", "match_basis", "channel_name", "category",
        "artefact_slug", "artefact_title", "artefact_category",
        "score", "window_start", "window_end", "github_refs", "sample_text",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            row = dict(r)
            row["github_refs"] = ", ".join(r.get("github_refs", []))
            writer.writerow(row)
    print(f"Written CSV: {path}")


def write_json(results, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Written JSON: {path}")


def write_summary(all_results, path):
    """Write a human-readable summary grouped by artefact."""
    # Collect all linkages per artefact
    by_artefact = defaultdict(list)
    gh_refs_all = []
    for r in all_results:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug].append(r)
        if r.get("match_basis") == "github_ref":
            gh_refs_all.append(r)

    lines = ["# OB1 Discord → Artefact Linkage Report", ""]
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Total linkages: {len(all_results)}")
    lines.append(f"Artefacts matched: {len(by_artefact)}")
    lines.append("")

    # Section 1: artefacts sorted by total score
    lines.append("## Artefacts by match strength")
    lines.append("")
    for slug in sorted(by_artefact, key=lambda s: -sum(r["score"] for r in by_artefact[s])):
        entries = by_artefact[slug]
        total_score = sum(r["score"] for r in entries)
        title = ARTEFACTS[slug]["title"]
        lines.append(f"### `{slug}` — {title} (total score: {total_score})")
        seen_channels = set()
        for r in sorted(entries, key=lambda x: -x["score"]):
            ch = r["channel_name"]
            if ch in seen_channels:
                continue
            seen_channels.add(ch)
            basis = r["match_basis"]
            score = r["score"]
            sample = r.get("sample_text", "")[:120].replace("\n", " ")
            lines.append(f"  - [{basis}, score={score}] **{ch}**: {sample}…")
        lines.append("")

    # Section 2: GitHub refs
    lines.append("## GitHub PR/issue references extracted from messages")
    lines.append("")
    all_refs = defaultdict(list)
    for r in gh_refs_all:
        for ref in r.get("github_refs", []):
            all_refs[ref].append(r["channel_name"])
    for ref in sorted(all_refs, key=lambda x: int(re.search(r"\d+", x).group())):
        channels = ", ".join(sorted(set(all_refs[ref])))
        lines.append(f"- `{ref}` — mentioned in: {channels}")
    lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Written summary: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Cluster OB1 Discord archive → artefacts")
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/discord-capture/discord_archive.db"),
        help="Path to discord_archive.db",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Directory to write output files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing to the database; still write CSV/JSON",
    )
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"Opening: {args.db}")
    conn = sqlite3.connect(args.db)

    print("Pass 1: Forum thread title matching...")
    p1 = pass1_thread_titles(conn)
    print(f"  {len(p1)} linkages from thread titles")

    print("Pass 2: Message body keyword matching...")
    p2 = pass2_message_bodies(conn)
    print(f"  {len(p2)} linkages from message bodies")

    print("Pass 3: GitHub URL extraction...")
    p3 = pass3_github_refs(conn)
    print(f"  {len(p3)} messages with GitHub refs")

    all_results = p1 + p2 + p3

    write_linkage_table(conn, all_results, dry_run=args.dry_run)

    csv_path = os.path.join(args.out_dir, "linkage_report.csv")
    json_path = os.path.join(args.out_dir, "linkage_report.json")
    summary_path = os.path.join(args.out_dir, "linkage_report.md")

    write_csv(all_results, csv_path)
    write_json(all_results, json_path)
    write_summary(all_results, summary_path)

    conn.close()
    print("Done.")

    # Print quick top-10 summary to stdout
    by_artefact = defaultdict(int)
    for r in p1 + p2:
        slug = r.get("artefact_slug")
        if slug:
            by_artefact[slug] += r["score"]
    print("\nTop artefacts by combined score:")
    for slug, score in sorted(by_artefact.items(), key=lambda x: -x[1])[:10]:
        print(f"  {score:4d}  {slug}")

    gh_refs = defaultdict(int)
    for r in p3:
        for ref in r.get("github_refs", []):
            gh_refs[ref] += 1
    print("\nGitHub refs mentioned most:")
    for ref, count in sorted(gh_refs.items(), key=lambda x: -x[1])[:15]:
        print(f"  {count:3d}x  {ref}")


if __name__ == "__main__":
    main()
