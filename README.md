# discord-capture

Archive a Discord server's messages to SQLite using DiscordChatExporter CLI.
Runs incrementally — each sync fetches only messages newer than the last export.

## Files

- `setup.sh` — install dependencies, download dcex if needed
- `discord_archive.py` — main ingest script
- `config.json.template` — copy to `config.json` and edit
- `discord_archive.db` — created on first run (gitignored)

## Setup

```bash
git clone git@github.com:sidney/discord-capture.git ~/OpenSources/discord-capture
cd ~/OpenSources/discord-capture
chmod +x setup.sh
./setup.sh
```

Then edit `config.json` with your token and guild ID (see below).

## Getting your Discord user token

1. Open Discord in a web browser (discord.com/channels/@me)
2. Open DevTools (F12 → Network tab)
3. Press Ctrl+R to reload
4. In the Network tab, filter requests by "api"
5. Click any request to `discord.com/api` → Headers → look for `Authorization` in Request Headers
6. That value is your token — copy it to `config.json`

**Keep this token secret.** It provides full access to your Discord account.
Never commit `config.json` — it is gitignored.

## Getting the OB1 guild ID

1. In Discord, enable Developer Mode: User Settings → Advanced → Developer Mode
2. Right-click the OB1 server icon in the sidebar
3. Click "Copy Server ID"
4. Paste into `config.json` as `guild_id`

## Usage

```bash
# First run — exports everything (may take several minutes)
python3 discord_archive.py --init

# List channels without exporting
python3 discord_archive.py --channels

# Incremental sync — only fetches new messages
python3 discord_archive.py --sync

# Archive statistics
python3 discord_archive.py --stats
```

## Scheduling with cron (Oracle VM)

Create the logs directory, then add to crontab (`crontab -e`):

```bash
mkdir -p ~/discord-capture/logs
```

```
# Sync twice daily at 06:00 and 18:00
0 6,18 * * * cd ~/discord-capture && python3 discord_archive.py --sync >> logs/archive.log 2>&1
```

## Querying the database

```bash
sqlite3 discord_archive.db
```

```sql
-- Recent messages in a channel
SELECT timestamp, author_name, content
FROM messages m JOIN channels c ON c.channel_id = m.channel_id
WHERE c.name = 'general'
ORDER BY timestamp DESC LIMIT 20;

-- Messages by a specific author
SELECT timestamp, content FROM messages
WHERE author_name = 'someuser'
ORDER BY timestamp;

-- Threaded replies
SELECT m1.timestamp, m1.author_name, m1.content,
       m2.content AS reply_to
FROM messages m1
JOIN messages m2 ON m2.message_id = m1.reply_to_message_id
LIMIT 20;

-- Channel summary
SELECT c.name, c.category, s.total_messages, s.last_export_at
FROM export_state s JOIN channels c ON c.channel_id = s.channel_id
ORDER BY s.total_messages DESC;
```

## Notes on Terms of Service

This tool uses a Discord user token for automated access, which violates
Discord's Terms of Service. The practical risk for read-only personal archiving
of a server you are legitimately a member of is low, but be aware of it.

If you later obtain bot access from the server admins, only the export step
changes — the ingest pipeline and SQLite schema remain the same.
