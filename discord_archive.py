#!/usr/bin/env python3
"""
discord_archive.py — Archive a Discord server's messages to SQLite.

Uses DiscordChatExporter CLI (dcex) for message retrieval.
Runs incrementally: each sync only fetches messages newer than the last export.

Usage:
    python3 discord_archive.py --init     # First run: discover channels + full export
    python3 discord_archive.py --sync     # Subsequent runs: incremental update
    python3 discord_archive.py --channels # Just list channels, no export
    python3 discord_archive.py --stats    # Print archive statistics
"""

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        print(f"ERROR: config.json not found at {config_path}")
        print("Run setup.sh first, then edit config.json.")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    required = ["token", "guild_id"]
    for key in required:
        if not config.get(key) or config[key].startswith("YOUR_"):
            print(f"ERROR: config.json is missing a value for '{key}'")
            print("Edit config.json before running.")
            sys.exit(1)

    config.setdefault("db_path", "discord_archive.db")
    config.setdefault("dcex_path", "dcex")
    config.setdefault("exclude_channel_types", [])
    config.setdefault("log_level", "INFO")
    return config


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS guilds (
    guild_id    TEXT PRIMARY KEY,
    name        TEXT,
    first_seen  TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id  TEXT PRIMARY KEY,
    guild_id    TEXT,
    name        TEXT,
    category    TEXT,
    type        TEXT,
    topic       TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    message_id          TEXT PRIMARY KEY,
    channel_id          TEXT,
    author_id           TEXT,
    author_name         TEXT,
    author_nickname     TEXT,
    is_bot              INTEGER DEFAULT 0,
    timestamp           TEXT,
    timestamp_edited    TEXT,
    content             TEXT,
    reply_to_message_id TEXT,
    raw_json            TEXT,
    ingested_at         TEXT
);

CREATE TABLE IF NOT EXISTS export_state (
    channel_id      TEXT PRIMARY KEY,
    last_message_id TEXT,
    last_export_at  TEXT,
    total_messages  INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_msg_channel   ON messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_msg_author    ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_msg_reply     ON messages(reply_to_message_id);
"""


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# DiscordChatExporter CLI helpers
# ---------------------------------------------------------------------------

def run_dcex(args: list, config: dict) -> subprocess.CompletedProcess:
    """Run a dcex command and return the result."""
    cmd = [config["dcex_path"]] + args
    logging.debug(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"dcex error (exit {result.returncode}):\n{result.stderr}")
    return result


def list_channels(config: dict) -> list[dict]:
    """
    Fetch all channels in the guild.
    Returns a list of dicts with keys: channel_id, name, category.

    dcex channels output format (one channel per line):
        <channel_id> | <category> / <name>   (channel with a category)
        <channel_id> | <name>                 (channel with no category)

    Channel type is not included in the listing output — it is captured
    from the exported JSON during ingest.
    """
    result = run_dcex(
        ["channels", "--token", config["token"], "--guild", config["guild_id"]],
        config
    )
    if result.returncode != 0:
        logging.error("Failed to list channels. Check your token and guild_id.")
        return []

    channels = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        # Split on the first pipe only — gives [channel_id, rest]
        parts = [p.strip() for p in line.split("|", 1)]
        if len(parts) != 2:
            logging.warning(f"Unexpected channel line format: {line!r}")
            continue

        channel_id, rest = parts[0], parts[1]

        # rest is either "Category / name" or just "name"
        if " / " in rest:
            category, name = rest.split(" / ", 1)
        else:
            category, name = "", rest

        channels.append({
            "channel_id": channel_id,
            "type":       "",   # not available from listing; populated during ingest
            "category":   category.strip(),
            "name":       name.strip(),
        })

    return channels


def export_channel(channel_id: str, after_message_id: str | None,
                   output_dir: Path, config: dict) -> Path | None:
    """
    Export a channel's messages to JSON.
    Returns the path to the exported JSON file, or None on failure.

    Uses --after to fetch only messages newer than after_message_id
    (incremental sync). On first run, after_message_id is None (full export).
    """
    args = [
        "export",
        "--token",   config["token"],
        "--channel", channel_id,
        "--format",  "Json",
        "--output",  str(output_dir),
    ]
    if after_message_id:
        args += ["--after", after_message_id]

    result = run_dcex(args, config)

    if result.returncode != 0:
        # Some channels (e.g. announcement-only, voice, stage) may not be
        # exportable — log and continue rather than crashing.
        logging.warning(f"Export failed for channel {channel_id}: {result.stderr.strip()}")
        return None

    # dcex names the output file based on the channel name and date range.
    # Find it in the output directory.
    json_files = sorted(output_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not json_files:
        logging.warning(f"No JSON file produced for channel {channel_id}")
        return None

    return json_files[-1]  # most recently modified


# ---------------------------------------------------------------------------
# JSON parsing + ingestion
# ---------------------------------------------------------------------------

def parse_export(json_path: Path) -> tuple[dict, list[dict]]:
    """
    Parse a dcex JSON export file.
    Returns (channel_meta, messages_list).

    dcex JSON structure:
    {
      "guild": { "id": ..., "name": ... },
      "channel": { "id": ..., "type": ..., "categoryId": ..., "category": ...,
                   "name": ..., "topic": ... },
      "messages": [
        { "id": ..., "type": ..., "timestamp": ..., "timestampEdited": ...,
          "content": ..., "isPinned": ...,
          "author": { "id": ..., "name": ..., "nickname": ..., "isBot": ... },
          "reference": { "messageId": ... },   <-- present if it's a reply
          ... },
        ...
      ]
    }
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    channel_meta = {
        "guild_id":   data["guild"]["id"],
        "guild_name": data["guild"]["name"],
        "channel_id": data["channel"]["id"],
        "name":       data["channel"]["name"],
        "type":       data["channel"]["type"],
        "category":   data["channel"].get("category", ""),
        "topic":      data["channel"].get("topic", ""),
    }

    messages = []
    for m in data.get("messages", []):
        author = m.get("author", {})
        reference = m.get("reference", {})
        messages.append({
            "message_id":          m["id"],
            "channel_id":          channel_meta["channel_id"],
            "author_id":           author.get("id", ""),
            "author_name":         author.get("name", ""),
            "author_nickname":     author.get("nickname", ""),
            "is_bot":              1 if author.get("isBot", False) else 0,
            "timestamp":           m.get("timestamp", ""),
            "timestamp_edited":    m.get("timestampEdited", ""),
            "content":             m.get("content", ""),
            "reply_to_message_id": reference.get("messageId", ""),
            "raw_json":            json.dumps(m, ensure_ascii=False),
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })

    return channel_meta, messages


def ingest_export(conn: sqlite3.Connection, json_path: Path) -> int:
    """
    Parse a dcex JSON export and insert new messages into the database.
    Returns the number of new messages inserted.
    """
    channel_meta, messages = parse_export(json_path)

    # Upsert guild
    conn.execute("""
        INSERT INTO guilds (guild_id, name, first_seen)
        VALUES (:guild_id, :guild_name, :now)
        ON CONFLICT(guild_id) DO UPDATE SET name = excluded.name
    """, {**channel_meta, "now": datetime.now(timezone.utc).isoformat()})

    # Upsert channel — type is populated here from the export JSON
    conn.execute("""
        INSERT INTO channels (channel_id, guild_id, name, category, type, topic)
        VALUES (:channel_id, :guild_id, :name, :category, :type, :topic)
        ON CONFLICT(channel_id) DO UPDATE SET
            name     = excluded.name,
            category = excluded.category,
            type     = excluded.type,
            topic    = excluded.topic
    """, channel_meta)

    # Insert messages (ignore duplicates — safe for reruns)
    inserted = 0
    for msg in messages:
        try:
            conn.execute("""
                INSERT INTO messages
                    (message_id, channel_id, author_id, author_name, author_nickname,
                     is_bot, timestamp, timestamp_edited, content,
                     reply_to_message_id, raw_json, ingested_at)
                VALUES
                    (:message_id, :channel_id, :author_id, :author_name, :author_nickname,
                     :is_bot, :timestamp, :timestamp_edited, :content,
                     :reply_to_message_id, :raw_json, :ingested_at)
            """, msg)
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # already in DB — skip

    conn.commit()
    return inserted


def update_export_state(conn: sqlite3.Connection, channel_id: str):
    """Update the high-water mark for a channel after a successful sync."""
    row = conn.execute("""
        SELECT message_id, timestamp
        FROM messages
        WHERE channel_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (channel_id,)).fetchone()

    if row:
        total = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel_id = ?", (channel_id,)
        ).fetchone()[0]

        conn.execute("""
            INSERT INTO export_state (channel_id, last_message_id, last_export_at, total_messages)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                last_message_id = excluded.last_message_id,
                last_export_at  = excluded.last_export_at,
                total_messages  = excluded.total_messages
        """, (channel_id, row["message_id"],
              datetime.now(timezone.utc).isoformat(), total))
        conn.commit()


def get_last_message_id(conn: sqlite3.Connection, channel_id: str) -> str | None:
    row = conn.execute(
        "SELECT last_message_id FROM export_state WHERE channel_id = ?",
        (channel_id,)
    ).fetchone()
    return row["last_message_id"] if row else None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_channels(config: dict, conn: sqlite3.Connection):
    """List all channels in the guild."""
    print(f"Fetching channel list for guild {config['guild_id']}...")
    channels = list_channels(config)
    if not channels:
        print("No channels returned. Check your token and guild_id.")
        return

    # Group by category for readability
    by_category: dict[str, list] = {}
    for ch in channels:
        cat = ch["category"] or "(no category)"
        by_category.setdefault(cat, []).append(ch)

    print(f"\n{len(channels)} channels found:\n")
    for cat, chs in sorted(by_category.items()):
        print(f"  [{cat}]")
        for ch in chs:
            print(f"    {ch['channel_id']}  #{ch['name']}")
    print()


def cmd_init(config: dict, conn: sqlite3.Connection):
    """
    First-run: discover all channels and do a full export of each.
    This can take a while depending on server size.
    """
    print("=== Initial full export ===")
    print(f"Guild: {config['guild_id']}")
    print()

    channels = list_channels(config)
    if not channels:
        print("No channels found. Aborting.")
        return

    print(f"Found {len(channels)} channels, exporting all.")
    print()

    total_inserted = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for i, ch in enumerate(channels, 1):
            ch_id = ch["channel_id"]
            label = f"#{ch['name']} [{ch['category'] or 'no category'}]"
            print(f"[{i:3}/{len(channels)}] {label} ... ", end="", flush=True)

            json_path = export_channel(ch_id, after_message_id=None, output_dir=tmp_path, config=config)

            if json_path is None:
                print("SKIPPED (export failed)")
                continue

            try:
                n = ingest_export(conn, json_path)
                update_export_state(conn, ch_id)
                print(f"{n} messages")
                total_inserted += n
            except Exception as e:
                print(f"ERROR ({e})")
                logging.exception(f"Failed to ingest {json_path}")
            finally:
                json_path.unlink(missing_ok=True)

    print(f"\nDone. {total_inserted} messages ingested into {config['db_path']}")


def cmd_sync(config: dict, conn: sqlite3.Connection):
    """
    Incremental sync: fetch only messages newer than the last export per channel.
    Discovers any new channels that weren't present on --init.
    """
    print(f"=== Incremental sync — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    channels = list_channels(config)
    if not channels:
        print("No channels found.")
        return

    total_new = 0
    channels_updated = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for ch in channels:
            ch_id = ch["channel_id"]
            last_id = get_last_message_id(conn, ch_id)
            label = f"#{ch['name']}"

            json_path = export_channel(ch_id, after_message_id=last_id,
                                       output_dir=tmp_path, config=config)
            if json_path is None:
                continue

            try:
                n = ingest_export(conn, json_path)
                update_export_state(conn, ch_id)
                if n > 0:
                    print(f"  {label}: +{n} new messages")
                    total_new += n
                    channels_updated += 1
            except Exception as e:
                print(f"  {label}: ERROR — {e}")
                logging.exception(f"Failed to ingest {json_path}")
            finally:
                json_path.unlink(missing_ok=True)

    if total_new == 0:
        print("  No new messages.")
    else:
        print(f"\n  Total: +{total_new} messages across {channels_updated} channels")


def cmd_stats(config: dict, conn: sqlite3.Connection):
    """Print summary statistics about the archive."""
    total_msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    total_channels = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    total_authors = conn.execute("SELECT COUNT(DISTINCT author_id) FROM messages").fetchone()[0]

    oldest = conn.execute("SELECT MIN(timestamp) FROM messages").fetchone()[0]
    newest = conn.execute("SELECT MAX(timestamp) FROM messages").fetchone()[0]

    print(f"\n=== Archive statistics ===")
    print(f"  Database:   {config['db_path']}")
    print(f"  Messages:   {total_msgs:,}")
    print(f"  Channels:   {total_channels}")
    print(f"  Authors:    {total_authors}")
    print(f"  Date range: {oldest[:10] if oldest else 'n/a'} → {newest[:10] if newest else 'n/a'}")

    print(f"\n  Top channels by message count:")
    rows = conn.execute("""
        SELECT c.name, c.category, COUNT(*) as n
        FROM messages m
        JOIN channels c ON c.channel_id = m.channel_id
        GROUP BY m.channel_id
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()
    for row in rows:
        cat = f"[{row['category']}] " if row["category"] else ""
        print(f"    {row['n']:>6,}  {cat}#{row['name']}")

    print(f"\n  Top authors by message count:")
    rows = conn.execute("""
        SELECT author_name, COUNT(*) as n
        FROM messages
        WHERE is_bot = 0
        GROUP BY author_id
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()
    for row in rows:
        print(f"    {row['n']:>6,}  {row['author_name']}")

    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Archive a Discord server's messages to SQLite."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--init",     action="store_true", help="First run: full export of all channels")
    group.add_argument("--sync",     action="store_true", help="Incremental sync (new messages only)")
    group.add_argument("--channels", action="store_true", help="List channels without exporting")
    group.add_argument("--stats",    action="store_true", help="Show archive statistics")

    parser.add_argument("--config", default="config.json",
                        help="Path to config file (default: config.json)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    config = load_config(script_dir / args.config)

    logging.basicConfig(
        level=getattr(logging, config.get("log_level", "INFO")),
        format="%(levelname)s: %(message)s"
    )

    db_path = script_dir / config["db_path"]
    conn = open_db(str(db_path))

    try:
        if args.channels:
            cmd_channels(config, conn)
        elif args.init:
            cmd_init(config, conn)
        elif args.sync:
            cmd_sync(config, conn)
        elif args.stats:
            cmd_stats(config, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
