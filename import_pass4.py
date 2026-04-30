#!/usr/bin/env python3
"""
import_pass4.py — Load pre-built GitHub linkages into discord_archive.db.

Usage:
  python3 import_pass4.py [--db PATH] [--json PATH] [--out-dir PATH]

Defaults:
  --db       ~/discord-capture/discord_archive.db
  --json     ~/discord-capture/pass4_linkages.json
  --out-dir  ~/discord-output
"""

import sqlite3, json, csv, re, os, argparse
from collections import defaultdict
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",      default=os.path.expanduser("~/discord-capture/discord_archive.db"))
    parser.add_argument("--json",    default=os.path.expanduser("~/discord-capture/pass4_linkages.json"))
    parser.add_argument("--out-dir", default=os.path.expanduser("~/discord-output"))
    args = parser.parse_args()

    with open(args.json) as f:
        rows = json.load(f)
    print(f"Loaded {len(rows)} rows from {args.json}")

    conn = sqlite3.connect(args.db)
    conn.execute("DELETE FROM artefact_linkages WHERE pass = 4")
    for r in rows:
        conn.execute(
            """INSERT INTO artefact_linkages
               (pass, match_basis, channel_id, channel_name, category,
                artefact_slug, artefact_title, artefact_category,
                score, window_start, window_end, github_refs, sample_text)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r["pass"], r["match_basis"], r.get("channel_id"),
             r["channel_name"], r["category"],
             r["artefact_slug"], r["artefact_title"], r["artefact_category"],
             r["score"], r["window_start"], r["window_end"],
             r["github_refs"] if isinstance(r["github_refs"], str)
                              else json.dumps(r["github_refs"]),
             r.get("sample_text","")[:500]),
        )
    conn.commit()
    print(f"Inserted {len(rows)} GitHub rows into artefact_linkages")

    # Read all rows and regenerate reports
    os.makedirs(args.out_dir, exist_ok=True)
    cur = conn.execute(
        """SELECT pass, match_basis, channel_name, category,
                  artefact_slug, artefact_title, artefact_category,
                  score, window_start, window_end, github_refs, sample_text
           FROM artefact_linkages ORDER BY artefact_slug, score DESC"""
    )
    fields = ["pass","match_basis","channel_name","category","artefact_slug",
              "artefact_title","artefact_category","score","window_start",
              "window_end","github_refs","sample_text"]
    all_rows = [dict(zip(fields, row)) for row in cur.fetchall()]
    conn.close()

    # CSV
    csv_path = os.path.join(args.out_dir, "linkage_report_full.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(all_rows)
    print(f"Written: {csv_path}")

    # Markdown summary
    md_path = os.path.join(args.out_dir, "linkage_report_full.md")
    _write_md(all_rows, md_path)
    print(f"Written: {md_path}")

    # Quick top-15 to stdout
    by_slug = defaultdict(lambda: {"disc": 0, "gh": 0})
    for r in all_rows:
        slug = r["artefact_slug"]
        if not slug: continue
        if r["pass"] in (1, 2, 3): by_slug[slug]["disc"] += r["score"]
        else:                       by_slug[slug]["gh"]   += r["score"]
    print("\nTop artefacts (Discord score | GitHub score | total):")
    totals = {sl: v["disc"] + v["gh"] for sl, v in by_slug.items()}
    for sl, tot in sorted(totals.items(), key=lambda x: -x[1])[:15]:
        d, g = by_slug[sl]["disc"], by_slug[sl]["gh"]
        print(f"  {tot:4d}  ({d:3d}D + {g:3d}G)  {sl}")


def _write_md(all_rows, path):
    by_artefact = defaultdict(list)
    for r in all_rows:
        if r["artefact_slug"]:
            by_artefact[r["artefact_slug"]].append(r)

    lines = ["# OB1 Discord + GitHub → Artefact Linkage Report", "",
             f"Generated: {datetime.now().isoformat()}",
             f"Total rows: {len(all_rows)}  |  Artefacts matched: {len(by_artefact)}", ""]

    lines += ["## Artefacts by combined score", ""]
    def total(entries): return sum(e["score"] for e in entries)
    for slug in sorted(by_artefact, key=lambda s: -total(by_artefact[s])):
        entries = by_artefact[slug]
        disc = sum(e["score"] for e in entries if e["pass"] in (1,2,3))
        gh   = sum(e["score"] for e in entries if e["pass"] == 4)
        title = entries[0]["artefact_title"]
        lines.append(f"### `{slug}` — {title}  (total: {disc+gh} | Discord: {disc} | GitHub: {gh})")
        seen = set()
        for e in sorted(entries, key=lambda x: -x["score"]):
            ch = e["channel_name"]
            if ch in seen: continue
            seen.add(ch)
            basis = e["match_basis"].replace("github_","gh:").replace("message_body","Discord")
            sample = (e.get("sample_text") or "")[:100].replace("\n"," ")
            lines.append(f"  - [{basis}, {e['score']}] **{ch}**: {sample}…")
        lines.append("")

    lines += ["## GitHub references (all passes)", ""]
    all_refs = defaultdict(set)
    for r in all_rows:
        refs = r.get("github_refs")
        if refs:
            parsed = json.loads(refs) if isinstance(refs, str) else refs
            for ref in parsed:
                all_refs[ref].add(r["channel_name"])
    for ref in sorted(all_refs, key=lambda x: int(re.search(r"\d+", x).group())):
        lines.append(f"- `{ref}` — {'; '.join(sorted(all_refs[ref]))}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
