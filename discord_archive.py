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
    config.setdefault("log_level", "INFO")
    return config


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# Base schema — only references columns that exist from the start.
# Columns added by migrations must NOT be indexed here.
SCHEMA = """
CREATE TABLE IF NOT EXISTS guilds (
    guild_id    TEXT PRIMARY KEY,
    name        TEXT,
    first_seen  TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id        TEXT PRIMARY KEY,
    guild_id          TEXT,
    name              TEXT,
    category          TEXT,
    type              TEXT,
    topic             TEXT,
    is_forum          INTEGER DEFAULT 0,
    parent_channel_id TEXT
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

CREATE INDEX IF NOT EXISTS idx_msg_channel    ON messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_msg_timestamp  ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_msg_author     ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_msg_reply      ON messages(reply_to_message_id);
"""

# Migrations run after SCHEMA, each wrapped in try/except so they are safe
# to run against both new and existing databases.
MIGRATIONS = [
    "ALTER TABLE channels ADD COLUMN is_forum INTEGER DEFAULT 0",
    "ALTER TABLE channels ADD COLUMN parent_channel_id TEXT",
    # Index on parent_channel_id must come after the column is guaranteed to exist
    "CREATE INDEX IF NOT EXISTS idx_chan_parent ON channels(parent_channel_id)",
]


def open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    for sql in MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column/index already exists
    return conn


# ---------------------------------------------------------------------------
# DiscordChatExporter CLI helpers
# ---------------------------------------------------------------------------

def run_dcex(args: list, config: dict) -> subprocess.CompletedProcess:
    """Run a dcex command and return the CompletedProcess (always, even on error)."""
    cmd = [config["dcex_path"]] + args
    logging.debug(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True)


def is_forum_error(stderr: str) -> bool:
    return "is a forum and cannot be exported directly" in stderr


def list_channels(config: dict) -> list[dict]:
    """
    Fetch all channels in the guild.

    dcex output format (one line per channel):
        <channel_id> | <category> / <name>
        <channel_id> | <name>               (no category)
    """
    result = run_dcex(
        ["channels", "--token", config["token"], "--guild", config["guild_id"]],
        config
    )
    if result.returncode != 0:
        logging.error(f"Failed to list channels:\n{result.stderr}")
        return []

    channels = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|", 1)]
        if len(parts) != 2:
            logging.warning(f"Unexpected channel line: {line!r}")
            continue
        channel_id, rest = parts
        if " / " in rest:
            category, name = rest.split(" / ", 1)
        else:
            category, name = "", rest
        channels.append({
            "channel_id": channel_id,
            "category":   category.strip(),
            "name":       name.strip(),
        })
    return channels


def list_threads(channel_id: str, config: dict) -> list[dict]:
    """
    List all threads in a forum channel.

    dcex threads output format (one line per thread):
        <thread_id> | <forum_name> / <thread_title>
        <thread_id> | <thread_title>
    """
    result = run_dcex(
        ["threads", "--token", config["token"], "--channel", channel_id],
        config
    )
    if result.returncode != 0:
        logging.warning(f"Failed to list threads for {channel_id}:\n{result.stderr}")
        return []

    threads = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|", 1)]
        if len(parts) != 2:
            logging.warning(f"Unexpected thread line: {line!r}")
            continue
        thread_id, rest = parts
        # Strip the forum channel name prefix if present ("forum_name / thread_title")
        name = rest.split(" / ", 1)[-1].strip()
        threads.append({"thread_id": thread_id, "name": name})
    return threads


def export_channel_raw(channel_id: str, after_message_id: str | None,
                       output_dir: Path, config: dict) -> tuple[Path | None, bool]:
    """
    Export a single channel or thread to JSON.

    Returns (json_path, is_forum) where is_forum=True means the channel is a
    Discord Forum and must be exported thread-by-thread instead.
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
        if is_forum_error(result.stderr):
            return None, True
        logging.warning(f"Export failed for {channel_id}: {result.stderr.strip()}")
        return None, False

    json_files = sorted(output_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not json_files:
        logging.warning(f"No JSON file produced for channel {channel_id}")
        return None, False

    return json_files[-1], False


# ---------------------------------------------------------------------------
# JSON parsing + ingestion
# ---------------------------------------------------------------------------

def parse_export(json_path: Path) -> tuple[dict, list[dict]]:
    """Parse a dcex JSON export. Returns (channel_meta, messages)."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    channel_meta = {
        "guild_id":          data["guild"]["id"],
        "guild_name":        data["guild"]["name"],
        "channel_id":        data["channel"]["id"],
        "name":              data["channel"]["name"],
        "type":              data["channel"].get("type", ""),
        "category":          data["channel"].get("category", ""),
        "topic":             data["channel"].get("topic", ""),
        "parent_channel_id": None,  # set by caller for thread exports
    }

    messages = []
    for m in data.get("messages", []):
        author    = m.get("author", {})
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


def ingest_export(conn: sqlite3.Connection, json_path: Path,
                  parent_channel_id: str | None = None,
                  is_forum: bool = False) -> int:
    """
    Parse a dcex JSON export and insert new messages.
    Returns the number of new messages inserted.
    """
    channel_meta, messages = parse_export(json_path)
    channel_meta["parent_channel_id"] = parent_channel_id
    channel_meta["is_forum"] = 1 if is_forum else 0

    conn.execute("""
        INSERT INTO guilds (guild_id, name, first_seen)
        VALUES (:guild_id, :guild_name, :now)
        ON CONFLICT(guild_id) DO UPDATE SET name = excluded.name
    """, {**channel_meta, "now": datetime.now(timezone.utc).isoformat()})

    conn.execute("""
        INSERT INTO channels
            (channel_id, guild_id, name, category, type, topic, is_forum, parent_channel_id)
        VALUES
            (:channel_id, :guild_id, :name, :category, :type, :topic, :is_forum, :parent_channel_id)
        ON CONFLICT(channel_id) DO UPDATE SET
            name              = excluded.name,
            category          = excluded.category,
            type              = excluded.type,
            topic             = excluded.topic,
            is_forum          = excluded.is_forum,
            parent_channel_id = excluded.parent_channel_id
    """, channel_meta)

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
            pass

    conn.commit()
    return inserted


def update_export_state(conn: sqlite3.Connection, channel_id: str):
    """Record the newest message_id seen for a channel or thread."""
    row = conn.execute("""
        SELECT message_id FROM messages
        WHERE channel_id = ?
        ORDER BY timestamp DESC LIMIT 1
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


def mark_channel_as_forum(conn: sqlite3.Connection, channel_id: str,
                           category: str, name: str, guild_id: str):
    """Record a forum channel in the channels table even if we can't export it directly."""
    conn.execute("""
        INSERT INTO channels (channel_id, guild_id, name, category, type, is_forum)
        VALUES (?, ?, ?, ?, 'GuildForum', 1)
        ON CONFLICT(channel_id) DO UPDATE SET
            name     = excluded.name,
            category = excluded.category,
            is_forum = 1
    """, (channel_id, guild_id, name, category))
    conn.commit()


# ---------------------------------------------------------------------------
# Forum export: enumerate threads and export each one
# ---------------------------------------------------------------------------

def export_forum(channel_id: str, channel_name: str, channel_category: str,
                 guild_id: str, output_dir: Path,
                 conn: sqlite3.Connection, config: dict) -> int:
    """
    Export all threads of a forum channel.
    Returns total messages inserted across all threads.
    """
    mark_channel_as_forum(conn, channel_id, channel_category, channel_name, guild_id)

    threads = list_threads(channel_id, config)
    if not threads:
        logging.warning(f"No threads found in forum #{channel_name}")
        return 0

    total = 0
    for thread in threads:
        tid  = thread["thread_id"]
        name = thread["name"]
        last_id = get_last_message_id(conn, tid)

        json_path, _ = export_channel_raw(tid, last_id, output_dir, config)
        if json_path is None:
            logging.warning(f"  thread '{name}' ({tid}): export failed, skipping")
            continue

        try:
            n = ingest_export(conn, json_path, parent_channel_id=channel_id)
            update_export_state(conn, tid)
            if n > 0:
                logging.info(f"  thread '{name}': {'+' if last_id else ''}{n} messages")
            total += n
        except Exception as e:
            logging.error(f"  thread '{name}': ERROR — {e}")
        finally:
            json_path.unlink(missing_ok=True)

    return total


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
    """First-run full export of all channels and forum threads."""
    print("=== Initial full export ===")
    print(f"Guild: {config['guild_id']}")
    print()

    channels = list_channels(config)
    if not channels:
        print("No channels found. Aborting.")
        return

    guild_id = config["guild_id"]

    print(f"Found {len(channels)} channels, exporting all.")
    print()

    total_inserted = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for i, ch in enumerate(channels, 1):
            ch_id  = ch["channel_id"]
            label  = f"#{ch['name']} [{ch['category'] or 'no category'}]"
            print(f"[{i:3}/{len(channels)}] {label} ... ", end="", flush=True)

            json_path, forum = export_channel_raw(ch_id, None, tmp_path, config)

            if forum:
                print("(forum)", flush=True)
                n = export_forum(ch_id, ch["name"], ch["category"],
                                 guild_id, tmp_path, conn, config)
                print(f"           → {n} messages across threads")
                total_inserted += n
                continue

            if json_path is None:
                print("SKIPPED")
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
    """Incremental sync: new messages only, including new forum threads."""
    print(f"=== Incremental sync — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    channels = list_channels(config)
    if not channels:
        print("No channels found.")
        return

    guild_id = config["guild_id"]
    total_new = 0
    channels_updated = 0

    known_forums = {
        row["channel_id"]
        for row in conn.execute("SELECT channel_id FROM channels WHERE is_forum = 1")
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for ch in channels:
            ch_id = ch["channel_id"]
            label = f"#{ch['name']}"

            if ch_id in known_forums:
                n = export_forum(ch_id, ch["name"], ch["category"],
                                 guild_id, tmp_path, conn, config)
                if n > 0:
                    print(f"  {label} (forum): +{n} messages")
                    total_new += n
                    channels_updated += 1
                continue

            last_id   = get_last_message_id(conn, ch_id)
            json_path, forum = export_channel_raw(ch_id, last_id, tmp_path, config)

            if forum:
                n = export_forum(ch_id, ch["name"], ch["category"],
                                 guild_id, tmp_path, conn, config)
                if n > 0:
                    print(f"  {label} (forum, new): +{n} messages")
                    total_new += n
                    channels_updated += 1
                continue

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
        print(f"\n  Total: +{total_new} messages across {channels_updated} channels/forums")


def cmd_stats(config: dict, conn: sqlite3.Connection):
    """Print summary statistics about the archive."""
    total_msgs    = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    total_chan     = conn.execute("SELECT COUNT(*) FROM channels WHERE is_forum = 0 AND parent_channel_id IS NULL").fetchone()[0]
    total_forums   = conn.execute("SELECT COUNT(*) FROM channels WHERE is_forum = 1").fetchone()[0]
    total_threads  = conn.execute("SELECT COUNT(*) FROM channels WHERE parent_channel_id IS NOT NULL").fetchone()[0]
    total_authors  = conn.execute("SELECT COUNT(DISTINCT author_id) FROM messages").fetchone()[0]

    oldest = conn.execute("SELECT MIN(timestamp) FROM messages").fetchone()[0]
    newest = conn.execute("SELECT MAX(timestamp) FROM messages").fetchone()[0]

    print(f"\n=== Archive statistics ===")
    print(f"  Database:      {config['db_path']}")
    print(f"  Messages:      {total_msgs:,}")
    print(f"  Channels:      {total_chan}")
    print(f"  Forums:        {total_forums}  ({total_threads} threads)")
    print(f"  Authors:       {total_authors}")
    print(f"  Date range:    {oldest[:10] if oldest else 'n/a'} → {newest[:10] if newest else 'n/a'}")

    print(f"\n  Top channels/threads by message count:")
    rows = conn.execute("""
        SELECT
            c.name,
            c.category,
            c.is_forum,
            p.name  AS parent_name,
            COUNT(*) AS n
        FROM messages m
        JOIN channels c ON c.channel_id = m.channel_id
        LEFT JOIN channels p ON p.channel_id = c.parent_channel_id
        GROUP BY m.channel_id
        ORDER BY n DESC
        LIMIT 15
    """).fetchall()
    for row in rows:
        if row["parent_name"]:
            label = f"#{row['parent_name']} › {row['name']}"
        elif row["is_forum"]:
            label = f"#{row['name']} (forum)"
        else:
            cat = f"[{row['category']}] " if row["category"] else ""
            label = f"{cat}#{row['name']}"
        print(f"    {row['n']:>6,}  {label}")

    print(f"\n  Top authors by message count:")
    rows = conn.execute("""
        SELECT author_name, COUNT(*) AS n
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
