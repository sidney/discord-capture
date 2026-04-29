# discord-capture

Archive a Discord server's messages to SQLite using DiscordChatExporter CLI.
Runs incrementally — each sync fetches only messages newer than the last export.

## Files

- `setup.sh` — install dependencies, download dcex if needed
- `discord_archive.py` — main script
- `watchdog.sh` — cron-driven watchdog that keeps the daemon alive
- `config.json.template` — copy to `config.json` and edit
- `discord_archive.db` — created on first run (gitignored)

## Setup (Oracle VM)

```bash
git clone git@github.com:sidney/discord-capture.git ~/discord-capture
cd ~/discord-capture
chmod +x setup.sh
./setup.sh
```

Then edit `config.json`.

## Setup (Mac, for testing)

```bash
git clone git@github.com:sidney/discord-capture.git ~/OpenSource/discord-capture
cd ~/OpenSource/discord-capture
```

Download the macOS ARM dcex binary manually (setup.sh handles Linux only):

```bash
curl -L https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.osx-arm64.zip -o dcex-mac.zip
unzip dcex-mac.zip -d dcex-mac
chmod +x dcex-mac/DiscordChatExporter.Cli
```

Set `"dcex_path": "dcex-mac/DiscordChatExporter.Cli"` in `config.json`.

## Getting your Discord user token

1. Open Discord in a web browser (discord.com/channels/@me)
2. Open DevTools (F12 → Network tab) and press Ctrl+R
3. Click any request to `discord.com/api` → Headers → `Authorization`
4. Copy that value to `config.json` as `token`

**Keep this token secret.** Never commit `config.json` — it is gitignored.
Set file permissions: `chmod 600 config.json`

## Getting the OB1 guild ID

1. Enable Developer Mode: User Settings → Advanced → Developer Mode
2. Right-click the OB1 server icon → Copy Server ID
3. Paste into `config.json` as `guild_id`

## OCI Vault (Oracle VM only)

Instead of storing the token in `config.json`, store it in OCI Vault:

1. Create a Vault and a software-protected master key in the OCI Console
2. Create a secret containing your Discord token
3. Copy the secret's OCID into `config.json` as `vault_secret_ocid`
4. Leave `token` blank (or remove it)
5. Create a dynamic group for your VM instance
6. Add an IAM policy: `Allow dynamic-group <name> to read secret-family in compartment <name>`
7. Install the OCI SDK: `pip install oci --break-system-packages`

The script fetches the token at startup using instance principal auth — no
additional credentials needed on the VM. On Mac (where Vault is unreachable)
it falls back to `config.json` automatically.

To rotate the token: update the secret in Vault, then `kill $(cat /tmp/discord_archive.pid)`.
The watchdog restarts the daemon with the new token.

## Usage

```bash
# First run — full export
python3 discord_archive.py --init

# Incremental sync
python3 discord_archive.py --sync

# Long-running daemon (syncs every sync_interval_hours)
python3 discord_archive.py --daemon

# One-time backfill of archived forum threads (requires MANAGE_THREADS permission)
python3 discord_archive.py --backfill

# List channels
python3 discord_archive.py --channels

# Statistics
python3 discord_archive.py --stats
```

## Daemon and watchdog (Oracle VM)

The daemon runs a sync loop internally. A lightweight cron watchdog checks every
15 minutes whether the daemon is alive and restarts it if not.

```bash
mkdir -p ~/discord-capture/logs
crontab -e
```

Add one line:

```
*/15 * * * * /home/ubuntu/discord-capture/watchdog.sh
```

To manually stop the daemon:

```bash
kill $(cat /tmp/discord_archive.pid)
```

Logs are written to `logs/daemon.log`.

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

-- Messages in a forum thread
SELECT timestamp, author_name, content
FROM messages m JOIN channels c ON c.channel_id = m.channel_id
WHERE c.name = 'Go local instead of Supabase?'
ORDER BY timestamp;

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
