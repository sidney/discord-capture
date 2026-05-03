#!/usr/bin/env python3
"""
generate_html.py — Generate a self-contained HTML linkage map.

Reads linkage_report_full.csv and (optionally) source_stats.json from the
discord-output directory, and writes a single self-contained HTML file that
can be opened in any browser without a server.

Usage:
  python3 generate_html.py [--csv PATH] [--stats PATH] [--out PATH]
                            [--guild-id ID] [--gh-owner OWNER] [--gh-repo REPO]

Defaults:
  --csv       ~/discord-output/linkage_report_full.csv
  --stats     ~/discord-output/source_stats.json
  --out       ~/discord-output/linkage_map.html
  --guild-id  1481783256641699840  (OB1 Discord server)
  --gh-owner  NateBJones-Projects
  --gh-repo   OB1

Discord deep links:
  Supply --guild-id to enable clickable links on Discord thread sources
  (pass 1–3). The guild ID is the first numeric segment in any Discord
  channel URL: discord.com/channels/{guild_id}/{channel_id}/...
  Without --guild-id the thread names render as plain text.

GitHub links:
  PR#N and issue#N refs in pass 4–5 sources are always rendered as
  clickable github.com links using --gh-owner and --gh-repo.
"""

import csv
import json
import os
import argparse
from datetime import datetime, timezone
from collections import defaultdict


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:    row["score"] = int(row["score"])
            except: row["score"] = 0
            try:    row["pass"] = int(row["pass"])
            except: row["pass"] = 0
            try:    row["github_refs"] = json.loads(row.get("github_refs") or "[]")
            except: row["github_refs"] = []
            rows.append(row)
    return rows


def load_stats(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("sources", [])


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def build_report(csv_path, stats_path, guild_id="", gh_owner="", gh_repo=""):
    rows = load_csv(csv_path)
    stats = load_stats(stats_path)

    # --- Aggregate by artefact slug ---
    by_slug = {}
    for row in rows:
        slug = row.get("artefact_slug", "")
        if not slug:
            continue
        if slug not in by_slug:
            by_slug[slug] = {
                "slug": slug,
                "title": "",
                "artefact_category": "",
                "discord_score": 0,
                "term_score": 0,
                "embed_score": 0,
                "sources": [],
                "_seen": set(),
            }
        a = by_slug[slug]
        if row.get("artefact_title"):
            a["title"] = row["artefact_title"]
        if row.get("artefact_category"):
            a["artefact_category"] = row["artefact_category"]

        p = row["pass"]
        score = row["score"]
        if p in (1, 2, 3):
            a["discord_score"] += score
        elif p == 4:
            a["term_score"] += score
        elif p == 5:
            a["embed_score"] += score

        # Deduplicate sources: one row per (channel_name, pass, match_basis)
        key = (row.get("channel_name", ""), p, row.get("match_basis", ""))
        if key not in a["_seen"]:
            a["_seen"].add(key)
            a["sources"].append({
                "pass":       p,
                "basis":      row.get("match_basis", ""),
                "channel":    row.get("channel_name", ""),
                "channel_id": row.get("channel_id") or "",
                "score":      score,
                "sim":        round(score / 100, 3) if p == 5 else None,
                "refs":       row.get("github_refs", []),
                "sample":     (row.get("sample_text") or "")[:300],
                "ts":         row.get("window_start", ""),
            })

    artefacts = []
    for a in by_slug.values():
        del a["_seen"]
        a["title"] = a["title"] or a["slug"]
        a["sources"].sort(key=lambda s: (-s["pass"], -s["score"]))
        a["total"] = a["discord_score"] + a["term_score"] + a["embed_score"]
        artefacts.append(a)

    # Default sort: embed desc, total desc
    artefacts.sort(key=lambda a: (-a["embed_score"], -a["total"]))

    # --- Maxes for bar scaling ---
    max_embed   = max((a["embed_score"]   for a in artefacts), default=1) or 1
    max_term    = max((a["term_score"]    for a in artefacts), default=1) or 1
    max_discord = max((a["discord_score"] for a in artefacts), default=1) or 1

    # --- Orphan sources ---
    orphans = []
    for s in stats:
        if s.get("matched_above_threshold", True):
            continue
        orphans.append({
            "ref":      s.get("ref", ""),
            "channel":  s.get("channel_name", ""),
            "basis":    s.get("match_basis", ""),
            "author":   s.get("author", ""),
            "sim":      s.get("best_pass5_sim"),
            "best_art": s.get("best_pass5_artefact", ""),
            "best_cat": s.get("best_pass5_artefact_category", ""),
            "p4":       s.get("pass4_total_score", 0),
            "sample":   (s.get("sample_text") or "")[:200],
        })
    orphans.sort(key=lambda o: o.get("sim") or 0.0)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "n_artefacts":  len(artefacts),
        "n_sources":    len(stats),
        "n_orphans":    len(orphans),
        "n_rows":       len(rows),
        "max_embed":    max_embed,
        "max_term":     max_term,
        "max_discord":  max_discord,
        "guild_id":     guild_id,
        "gh_owner":     gh_owner,
        "gh_repo":      gh_repo,
        "artefacts":    artefacts,
        "orphans":      orphans,
    }


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OB1 Community Map</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:          #060b13;
  --surface:     #0b1420;
  --surface-2:   #101d2e;
  --surface-3:   #152438;
  --border:      #192c42;
  --border-dim:  #112030;
  --text:        #7fa8c8;
  --text-bright: #b8d4e8;
  --text-dim:    #344f68;
  --text-dimmer: #1d3246;

  --embed:    #38bdf8;
  --term:     #4ade80;
  --discord:  #fb923c;
  --total:    #a78bfa;

  --cat-recipe:       #38bdf8;
  --cat-integration:  #34d399;
  --cat-primitive:    #a78bfa;
  --cat-extension:    #fb923c;
  --cat-skill:        #f472b6;
  --cat-dashboard:    #2dd4bf;
  --cat-architecture: #facc15;

  --mono: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  --r:    5px;
  --r-lg: 9px;
}

html { font-size: 14px; }
body {
  font-family: var(--mono);
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  line-height: 1.5;
}

/* ---- header ---- */
header {
  position: sticky;
  top: 0;
  z-index: 100;
  background: rgba(6, 11, 19, 0.92);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border-dim);
  padding: 0 20px;
}
.header-inner {
  max-width: 1200px;
  margin: 0 auto;
  height: 52px;
  display: flex;
  align-items: center;
  gap: 20px;
}
.logo {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-bright);
  letter-spacing: 0.08em;
  white-space: nowrap;
}
.logo span { color: var(--embed); }
.header-stats {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  flex: 1;
}
.stat-chip {
  font-size: 11px;
  color: var(--text-dim);
  display: flex;
  align-items: center;
  gap: 5px;
}
.stat-chip strong { color: var(--text); font-weight: 500; }
.gen-time {
  margin-left: auto;
  font-size: 11px;
  color: var(--text-dimmer);
  white-space: nowrap;
}

/* ---- main layout ---- */
.main {
  max-width: 1200px;
  margin: 0 auto;
  padding: 16px 20px 60px;
}

/* ---- controls ---- */
.controls {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  margin-bottom: 14px;
}
.filter-chips { display: flex; flex-wrap: wrap; gap: 5px; flex: 1; }
.chip {
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  padding: 4px 10px;
  border-radius: 3px;
  border: 1px solid var(--border);
  background: transparent;
  color: var(--text-dim);
  cursor: pointer;
  transition: all 0.12s;
  letter-spacing: 0.04em;
}
.chip:hover { border-color: var(--border); color: var(--text); background: var(--surface); }
.chip.active { border-color: var(--embed); color: var(--embed); background: rgba(56,189,248,0.06); }
.chip[data-cat="recipe"].active      { border-color: var(--cat-recipe);       color: var(--cat-recipe);       background: rgba(56,189,248,0.06); }
.chip[data-cat="integration"].active { border-color: var(--cat-integration);  color: var(--cat-integration);  background: rgba(52,211,153,0.06); }
.chip[data-cat="primitive"].active   { border-color: var(--cat-primitive);    color: var(--cat-primitive);    background: rgba(167,139,250,0.06); }
.chip[data-cat="extension"].active   { border-color: var(--cat-extension);    color: var(--cat-extension);    background: rgba(251,146,60,0.06); }
.chip[data-cat="skill"].active       { border-color: var(--cat-skill);        color: var(--cat-skill);        background: rgba(244,114,182,0.06); }
.chip[data-cat="dashboard"].active   { border-color: var(--cat-dashboard);    color: var(--cat-dashboard);    background: rgba(45,212,191,0.06); }
.chip[data-cat="architecture"].active { border-color: var(--cat-architecture); color: var(--cat-architecture); background: rgba(250,204,21,0.06); }

.controls-right { display: flex; align-items: center; gap: 10px; }
.sort-label { font-size: 11px; color: var(--text-dim); }
select {
  font-family: var(--mono);
  font-size: 11px;
  background: var(--surface);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 8px;
  border-radius: var(--r);
  cursor: pointer;
  outline: none;
}
select:focus { border-color: var(--embed); }
.search-wrap { position: relative; }
.search-wrap::before {
  content: '\2395';
  position: absolute;
  left: 8px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--text-dim);
  font-size: 14px;
  pointer-events: none;
}
#search {
  font-family: var(--mono);
  font-size: 11px;
  background: var(--surface);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 8px 4px 26px;
  border-radius: var(--r);
  width: 200px;
  outline: none;
  transition: border-color 0.12s;
}
#search:focus { border-color: var(--embed); }
#search::placeholder { color: var(--text-dimmer); }

/* ---- tabs ---- */
.tabs {
  display: flex;
  border-bottom: 1px solid var(--border-dim);
  margin-bottom: 14px;
}
.tab-btn {
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.06em;
  padding: 8px 16px;
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  color: var(--text-dim);
  cursor: pointer;
  transition: all 0.12s;
}
.tab-btn:hover { color: var(--text); }
.tab-btn.active { color: var(--text-bright); border-bottom-color: var(--embed); }
.tab-count {
  display: inline-block;
  font-size: 10px;
  background: var(--surface-2);
  padding: 1px 5px;
  border-radius: 10px;
  margin-left: 5px;
  color: var(--text-dim);
}
.tab-btn.active .tab-count { background: rgba(56,189,248,0.12); color: var(--embed); }

/* ---- tab content ---- */
.tab-pane { display: none; }
.tab-pane.active { display: block; }

/* ---- artefact cards ---- */
#artefact-list { display: flex; flex-direction: column; gap: 5px; }
.card {
  background: var(--surface);
  border: 1px solid var(--border-dim);
  border-radius: var(--r-lg);
  overflow: hidden;
  transition: border-color 0.15s;
}
.card:hover { border-color: var(--border); }
.card.open  { border-color: var(--border); }
.card-head {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 14px 10px;
  cursor: pointer;
  user-select: none;
}
.card-head:hover { background: rgba(255,255,255,0.01); }
.card-slug {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-bright);
  letter-spacing: 0.02em;
  flex: 1;
  min-width: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.card-title {
  font-size: 11px;
  color: var(--text-dim);
  flex: 2;
  min-width: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.card-meta { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
.cat-badge {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  padding: 2px 7px;
  border-radius: 3px;
  text-transform: lowercase;
}
.cat-recipe       { color: var(--cat-recipe);       background: rgba(56,189,248,0.08);  border: 1px solid rgba(56,189,248,0.15); }
.cat-integration  { color: var(--cat-integration);  background: rgba(52,211,153,0.08);  border: 1px solid rgba(52,211,153,0.15); }
.cat-primitive    { color: var(--cat-primitive);     background: rgba(167,139,250,0.08); border: 1px solid rgba(167,139,250,0.15); }
.cat-extension    { color: var(--cat-extension);     background: rgba(251,146,60,0.08);  border: 1px solid rgba(251,146,60,0.15); }
.cat-skill        { color: var(--cat-skill);         background: rgba(244,114,182,0.08); border: 1px solid rgba(244,114,182,0.15); }
.cat-dashboard    { color: var(--cat-dashboard);     background: rgba(45,212,191,0.08);  border: 1px solid rgba(45,212,191,0.15); }
.cat-architecture { color: var(--cat-architecture);  background: rgba(250,204,21,0.08);  border: 1px solid rgba(250,204,21,0.15); }
.src-count { font-size: 10px; color: var(--text-dimmer); }
.expand-icon {
  font-size: 9px;
  color: var(--text-dimmer);
  transition: transform 0.2s;
  flex-shrink: 0;
}
.card.open .expand-icon { transform: rotate(90deg); }

/* ---- score bars ---- */
.score-bars { padding: 0 14px 11px; display: flex; gap: 10px; }
.score-bar  { display: flex; align-items: center; gap: 6px; flex: 1; min-width: 0; }
.s-label {
  font-size: 10px;
  color: var(--text-dimmer);
  width: 42px;
  flex-shrink: 0;
  letter-spacing: 0.04em;
}
.bar-track {
  flex: 1;
  height: 3px;
  background: var(--border-dim);
  border-radius: 2px;
  overflow: hidden;
  min-width: 20px;
}
.bar-fill { height: 100%; border-radius: 2px; transition: width 0.3s ease; }
.bar-fill.e { background: var(--embed); }
.bar-fill.t { background: var(--term); }
.bar-fill.d { background: var(--discord); }
.s-val { font-size: 10px; width: 28px; text-align: right; flex-shrink: 0; }
.s-val.e { color: var(--embed); }
.s-val.t { color: var(--term); }
.s-val.d { color: var(--discord); }
.s-val.zero { color: var(--text-dimmer); }

/* ---- source list ---- */
.sources {
  border-top: 1px solid var(--border-dim);
  padding: 8px 0;
  display: none;
}
.card.open .sources { display: block; }
.source-item {
  padding: 7px 14px;
  border-bottom: 1px solid var(--border-dim);
  display: grid;
  grid-template-columns: 28px 1fr auto;
  grid-template-rows: auto auto;
  gap: 2px 8px;
  align-items: start;
}
.source-item:last-child { border-bottom: none; }
.source-item:hover { background: rgba(255,255,255,0.015); }
.pass-badge {
  grid-row: 1 / 3;
  align-self: center;
  font-size: 10px;
  font-weight: 600;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  flex-shrink: 0;
}
.p1 { background: rgba(251,146,60,0.12); color: var(--discord); }
.p2 { background: rgba(251,146,60,0.12); color: var(--discord); }
.p3 { background: rgba(251,146,60,0.12); color: var(--discord); }
.p4 { background: rgba(74,222,128,0.12); color: var(--term); }
.p5 { background: rgba(56,189,248,0.12); color: var(--embed); }
.source-main {
  grid-column: 2;
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-width: 0;
  flex-wrap: wrap;
}
.source-channel {
  font-size: 11px;
  color: var(--text);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 480px;
}
.source-basis { font-size: 10px; color: var(--text-dim); flex-shrink: 0; }
.source-refs  { font-size: 10px; color: var(--text-dim); flex-shrink: 0; }
.source-score {
  grid-column: 3;
  font-size: 11px;
  text-align: right;
  flex-shrink: 0;
  white-space: nowrap;
}
.source-sample {
  grid-column: 2;
  font-size: 10px;
  color: var(--text-dim);
  line-height: 1.5;
  overflow: hidden;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
}
.sim-val  { color: var(--embed); }
.term-val { color: var(--term); }

/* ---- source links ---- */
.source-link {
  color: inherit;
  text-decoration: none;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  transition: color 0.12s, border-color 0.12s;
}
.source-link:hover { border-bottom-color: currentColor; }
a.source-channel.source-link:hover { color: var(--embed); }
.source-refs .source-link { color: var(--text-dim); }
.source-refs .source-link:hover { color: var(--term); }

/* ---- empty state ---- */
.empty-state {
  text-align: center;
  padding: 60px 20px;
  color: var(--text-dimmer);
  font-size: 12px;
}

/* ---- orphan table ---- */
.orphan-intro {
  font-size: 11px;
  color: var(--text-dim);
  margin-bottom: 12px;
  line-height: 1.6;
}
.orphan-table { width: 100%; border-collapse: collapse; font-size: 11px; }
.orphan-table th {
  text-align: left;
  padding: 6px 10px;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  color: var(--text-dimmer);
  border-bottom: 1px solid var(--border-dim);
  cursor: pointer;
  white-space: nowrap;
  user-select: none;
}
.orphan-table th:hover { color: var(--text); }
.orphan-table th.sort-asc::after  { content: ' \2191'; }
.orphan-table th.sort-desc::after { content: ' \2193'; }
.orphan-table td {
  padding: 7px 10px;
  border-bottom: 1px solid rgba(25, 44, 66, 0.5);
  color: var(--text);
  vertical-align: top;
}
.orphan-table tr:hover td { background: var(--surface); }
.orphan-table tr:last-child td { border-bottom: none; }
.sim-low  { color: #94a3b8; }
.sim-mid  { color: var(--discord); }
.sim-high { color: var(--term); }
.orphan-channel {
  color: var(--text-dim);
  font-size: 10px;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.orphan-ref { color: var(--text-bright); font-weight: 500; }
.orphan-sample {
  font-size: 10px;
  color: var(--text-dim);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* ---- scrollbar ---- */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--border); }
</style>
</head>
<body>

<header>
  <div class="header-inner">
    <div class="logo">OB1 <span>Community Map</span></div>
    <div class="header-stats" id="header-stats"></div>
    <div class="gen-time" id="gen-time"></div>
  </div>
</header>

<div class="main">
  <div class="controls">
    <div class="filter-chips" id="filter-chips"></div>
    <div class="controls-right">
      <span class="sort-label">sort</span>
      <select id="sort-select">
        <option value="embed">embed \u2193</option>
        <option value="term">term \u2193</option>
        <option value="discord">discord \u2193</option>
        <option value="total">total \u2193</option>
        <option value="name">slug A\u2013Z</option>
      </select>
      <div class="search-wrap">
        <input type="text" id="search" placeholder="slug or title\u2026" autocomplete="off" spellcheck="false">
      </div>
    </div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" data-tab="artefacts">
      Artefacts <span class="tab-count" id="art-count">\u2014</span>
    </button>
    <button class="tab-btn" data-tab="orphans">
      Orphan Sources <span class="tab-count" id="orp-count">\u2014</span>
    </button>
  </div>

  <div id="tab-artefacts" class="tab-pane active">
    <div id="artefact-list"></div>
  </div>

  <div id="tab-orphans" class="tab-pane">
    <p class="orphan-intro">
      Sources with no pass-5 match above their per-category threshold.
      Sorted ascending by best pass-5 similarity \u2014 lowest first.
      Pass-4 total is the sum of term scores attributed to this source.
      High pass-4 + low pass-5 = novel synthesis; low both = genuinely new vocabulary.
    </p>
    <table class="orphan-table" id="orphan-table">
      <thead>
        <tr>
          <th data-col="sim">Best sim</th>
          <th data-col="best_art">Best match artefact</th>
          <th data-col="p4">Pass-4 total</th>
          <th data-col="basis">Basis</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody id="orphan-tbody"></tbody>
    </table>
  </div>
</div>

<script>
const REPORT = __REPORT_DATA__;

// ---------- state ----------
let currentCat  = '';
let currentSort = 'embed';
let searchQuery = '';
let openCards   = new Set();
let orphanSort  = { col: 'sim', dir: 'asc' };

// ---------- category helpers ----------
const ALL_CATS = ['recipe','integration','primitive','extension','skill','dashboard','architecture'];
function catClass(cat) { return 'cat-' + (cat || 'recipe'); }

// ---------- score bar HTML ----------
function bar(label, score, max, cls) {
  const pct = max > 0 ? Math.round((score / max) * 100) : 0;
  const zeroClass = score === 0 ? ' zero' : '';
  return `
    <div class="score-bar">
      <span class="s-label">${label}</span>
      <div class="bar-track"><div class="bar-fill ${cls}" style="width:${pct}%"></div></div>
      <span class="s-val ${cls}${zeroClass}">${score}</span>
    </div>`;
}

// ---------- source item HTML ----------
function sourceHTML(s) {
  const scoreStr = s.pass === 5
    ? `<span class="sim-val">${s.score}</span>&nbsp;(${s.sim != null ? s.sim.toFixed(3) : '\u2014'})`
    : `<span class="term-val">${s.score}</span>`;

  // Discord channel link (passes 1\u20133) \u2014 requires guild_id + channel_id
  let channelEl;
  if (s.pass <= 3 && s.channel_id && REPORT.guild_id) {
    const url = `https://discord.com/channels/${REPORT.guild_id}/${s.channel_id}`;
    channelEl = `<a class="source-channel source-link" href="${escHtml(url)}" target="_blank" rel="noopener">${escHtml(s.channel)}</a>`;
  } else {
    channelEl = `<span class="source-channel">${escHtml(s.channel)}</span>`;
  }

  // GitHub ref links (passes 4\u20135)
  const refParts = (s.refs || []).map(ref => {
    const prM  = ref.match(/^PR#(\d+)$/);
    const issM = ref.match(/^issue#(\d+)$/);
    if (prM && REPORT.gh_owner && REPORT.gh_repo) {
      const url = `https://github.com/${REPORT.gh_owner}/${REPORT.gh_repo}/pull/${prM[1]}`;
      return `<a class="source-link" href="${escHtml(url)}" target="_blank" rel="noopener">${escHtml(ref)}</a>`;
    } else if (issM && REPORT.gh_owner && REPORT.gh_repo) {
      const url = `https://github.com/${REPORT.gh_owner}/${REPORT.gh_repo}/issues/${issM[1]}`;
      return `<a class="source-link" href="${escHtml(url)}" target="_blank" rel="noopener">${escHtml(ref)}</a>`;
    }
    return escHtml(ref);
  });
  const refsStr = refParts.length
    ? `<span class="source-refs">${refParts.join(' + ')}</span>`
    : '';

  const basisStr = s.basis ? `<span class="source-basis">${escHtml(s.basis)}</span>` : '';
  const sample = (s.sample || '').replace(/\n/g, ' ').trim();
  const sampleStr = sample ? `<div class="source-sample">${escHtml(sample)}</div>` : '';
  return `
    <div class="source-item">
      <span class="pass-badge p${s.pass}">P${s.pass}</span>
      <div class="source-main">
        ${channelEl}
        ${basisStr}
        ${refsStr}
      </div>
      <div class="source-score">${scoreStr}</div>
      ${sampleStr}
    </div>`;
}

// ---------- artefact card HTML ----------
function cardHTML(a) {
  const isOpen = openCards.has(a.slug);
  const nSrc = a.sources.length;
  const bars = bar('embed', a.embed_score, REPORT.max_embed, 'e')
             + bar('term',  a.term_score,  REPORT.max_term,  't')
             + bar('disc',  a.discord_score, REPORT.max_discord, 'd');
  const sourcesInner = a.sources.map(sourceHTML).join('');
  return `
    <div class="card${isOpen ? ' open' : ''}" data-slug="${escHtml(a.slug)}">
      <div class="card-head" onclick="toggleCard('${escHtml(a.slug)}')">
        <span class="card-slug">${escHtml(a.slug)}</span>
        <span class="card-title">${escHtml(a.title)}</span>
        <div class="card-meta">
          <span class="cat-badge ${catClass(a.artefact_category)}">${escHtml(a.artefact_category || '?')}</span>
          <span class="src-count">${nSrc} src${nSrc !== 1 ? 's' : ''}</span>
          <span class="expand-icon">\u25b6</span>
        </div>
      </div>
      <div class="score-bars">${bars}</div>
      <div class="sources">${sourcesInner}</div>
    </div>`;
}

// ---------- render artefacts ----------
function filteredSorted() {
  let arts = REPORT.artefacts.slice();
  if (currentCat) arts = arts.filter(a => a.artefact_category === currentCat);
  if (searchQuery) {
    const q = searchQuery.toLowerCase();
    arts = arts.filter(a =>
      a.slug.toLowerCase().includes(q) || a.title.toLowerCase().includes(q)
    );
  }
  arts.sort((a, b) => {
    switch (currentSort) {
      case 'embed':   return b.embed_score - a.embed_score || b.total - a.total;
      case 'term':    return b.term_score  - a.term_score  || b.total - a.total;
      case 'discord': return b.discord_score - a.discord_score || b.total - a.total;
      case 'total':   return b.total - a.total;
      case 'name':    return a.slug.localeCompare(b.slug);
    }
    return 0;
  });
  return arts;
}

function renderArtefacts() {
  const arts = filteredSorted();
  const list = document.getElementById('artefact-list');
  document.getElementById('art-count').textContent = arts.length;
  if (arts.length === 0) {
    list.innerHTML = '<div class="empty-state">no artefacts match this filter</div>';
    return;
  }
  list.innerHTML = arts.map(cardHTML).join('');
}

// ---------- toggle card ----------
function toggleCard(slug) {
  if (openCards.has(slug)) openCards.delete(slug);
  else openCards.add(slug);
  const el = document.querySelector(`.card[data-slug="${CSS.escape(slug)}"]`);
  if (el) el.classList.toggle('open', openCards.has(slug));
}

// ---------- render orphans ----------
function simClass(sim) {
  if (sim == null) return 'sim-low';
  if (sim < 0.50)  return 'sim-low';
  if (sim < 0.60)  return 'sim-mid';
  return 'sim-high';
}

function renderOrphans() {
  const orphans = REPORT.orphans.slice();
  const col = orphanSort.col;
  const dir = orphanSort.dir === 'asc' ? 1 : -1;
  orphans.sort((a, b) => {
    let av = a[col], bv = b[col];
    if (av == null) av = -Infinity;
    if (bv == null) bv = -Infinity;
    if (typeof av === 'string') return dir * av.localeCompare(bv);
    return dir * (av - bv);
  });
  document.getElementById('orp-count').textContent = orphans.length;
  const tbody = document.getElementById('orphan-tbody');
  tbody.innerHTML = orphans.map(o => {
    const sim = o.sim != null ? o.sim.toFixed(3) : '\u2014';
    const simCls = simClass(o.sim);
    const sample = (o.sample || '').replace(/\n/g, ' ').trim();
    return `
      <tr>
        <td><span class="${simCls}">${sim}</span></td>
        <td>
          <div class="orphan-ref">${escHtml(o.best_art || '\u2014')}</div>
          <div class="orphan-channel">${escHtml(o.best_cat || '')}</div>
        </td>
        <td>${o.p4 || 0}</td>
        <td style="color:var(--text-dim)">${escHtml(o.basis)}</td>
        <td>
          <div class="orphan-ref">${escHtml(o.ref)}</div>
          <div class="orphan-channel">${escHtml(o.channel)}</div>
          ${sample ? `<div class="orphan-sample">${escHtml(sample)}</div>` : ''}
        </td>
      </tr>`;
  }).join('');
  document.querySelectorAll('.orphan-table th').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.col === col)
      th.classList.add(orphanSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
  });
}

// ---------- header stats ----------
function renderHeader() {
  const R = REPORT;
  document.getElementById('gen-time').textContent = `generated ${R.generated_at}`;
  document.getElementById('header-stats').innerHTML = [
    `<span class="stat-chip"><strong>${R.n_artefacts}</strong> artefacts</span>`,
    R.n_sources ? `<span class="stat-chip"><strong>${R.n_sources}</strong> sources scanned</span>` : '',
    R.n_orphans ? `<span class="stat-chip"><strong>${R.n_orphans}</strong> orphans</span>` : '',
    `<span class="stat-chip"><strong>${R.n_rows}</strong> linkage rows</span>`,
  ].filter(Boolean).join('');
}

// ---------- filter chips ----------
function buildChips() {
  const cats = ALL_CATS.filter(c => REPORT.artefacts.some(a => a.artefact_category === c));
  const wrap = document.getElementById('filter-chips');
  const makeChip = (cat, label, active) => {
    const chip = document.createElement('button');
    chip.className = 'chip' + (active ? ' active' : '');
    chip.dataset.cat = cat;
    chip.textContent = label;
    chip.onclick = () => {
      currentCat = cat;
      document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      renderArtefacts();
    };
    return chip;
  };
  wrap.appendChild(makeChip('', 'all', true));
  cats.forEach(c => wrap.appendChild(makeChip(c, c, false)));
}

// ---------- tabs ----------
document.addEventListener('click', e => {
  const btn = e.target.closest('.tab-btn');
  if (!btn) return;
  const tab = btn.dataset.tab;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById(`tab-${tab}`).classList.add('active');
});

// ---------- orphan table sort ----------
document.querySelectorAll('.orphan-table th[data-col]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    orphanSort.dir = (orphanSort.col === col && orphanSort.dir === 'asc') ? 'desc' : 'asc';
    orphanSort.col = col;
    renderOrphans();
  });
});

// ---------- sort & search ----------
document.getElementById('sort-select').addEventListener('change', e => {
  currentSort = e.target.value;
  renderArtefacts();
});
document.getElementById('search').addEventListener('input', e => {
  searchQuery = e.target.value.trim();
  renderArtefacts();
});

// ---------- utility ----------
function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ---------- init ----------
renderHeader();
buildChips();
renderArtefacts();
renderOrphans();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a self-contained HTML linkage map from output reports."
    )
    parser.add_argument(
        "--csv",
        default=os.path.expanduser("~/discord-output/linkage_report_full.csv"),
        help="Path to linkage_report_full.csv (default: ~/discord-output/linkage_report_full.csv)",
    )
    parser.add_argument(
        "--stats",
        default=os.path.expanduser("~/discord-output/source_stats.json"),
        help="Path to source_stats.json (default: ~/discord-output/source_stats.json; optional)",
    )
    parser.add_argument(
        "--out",
        default=os.path.expanduser("~/discord-output/linkage_map.html"),
        help="Output HTML path (default: ~/discord-output/linkage_map.html)",
    )
    parser.add_argument(
        "--guild-id",
        default="1481783256641699840",
        help="Discord guild (server) ID for deep-linking Discord threads "
             "(default: 1481783256641699840, the OB1 server). "
             "Find it in the server URL: discord.com/channels/{guild_id}/...",
    )
    parser.add_argument(
        "--gh-owner",
        default="NateBJones-Projects",
        help="GitHub repo owner for PR/issue links (default: NateBJones-Projects)",
    )
    parser.add_argument(
        "--gh-repo",
        default="OB1",
        help="GitHub repo name for PR/issue links (default: OB1)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"ERROR: CSV not found: {args.csv}")
        print("Run cluster_embed.py first to generate linkage_report_full.csv.")
        return

    print(f"Reading CSV: {args.csv}")
    if os.path.exists(args.stats):
        print(f"Reading stats: {args.stats}")
    else:
        print(f"Stats not found ({args.stats}) \u2014 orphan section will be empty")
    if args.guild_id:
        print(f"Discord guild ID: {args.guild_id} \u2014 Discord thread links enabled")
    else:
        print("No --guild-id supplied \u2014 Discord thread links will be plain text")

    report = build_report(
        args.csv, args.stats,
        guild_id=args.guild_id,
        gh_owner=args.gh_owner,
        gh_repo=args.gh_repo,
    )
    print(f"  {report['n_artefacts']} artefacts | "
          f"{report['n_sources']} sources | "
          f"{report['n_orphans']} orphans | "
          f"{report['n_rows']} linkage rows")

    report_json = json.dumps(report, separators=(',', ':'))
    html = HTML_TEMPLATE.replace("__REPORT_DATA__", report_json)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nWritten: {args.out}")
    print(f"\nTo fetch:")
    print(f"  scp ubuntu@144.24.44.81:{args.out} ~/Desktop/linkage_map.html")


if __name__ == "__main__":
    main()
