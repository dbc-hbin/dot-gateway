"""Native supervisor launcher for the reviewed Go gateway; no model runtime."""
import fcntl
import hashlib
import os
from pathlib import Path

root = Path('/opt/assistant-project')
binary = root / 'discord-go-gateway/bin/dot-gateway'
expected = 'REPLACE_WITH_REVIEWED_BINARY_SHA256'
current = root / '.discord-runtime/bridge.sqlite3'
if not current.is_file():
    raise SystemExit('Existing ledger missing; refusing to create a new database')
if hashlib.sha256(binary.read_bytes()).hexdigest() != expected:
    raise SystemExit('Reviewed binary digest mismatch')
fd = os.open('/opt/assistant-shared/.discord-private/bridge.sqlite3.lock', os.O_RDONLY | os.O_NOFOLLOW)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.set_inheritable(fd, True)
env = os.environ.copy()
env.update({
    'DISCORD_OWNER_ID': '100000000000000005',
    'DISCORD_ALLOWED_DM_IDS': '100000000000000005',
    'DISCORD_EXPECTED_BOT_ID': '100000000000000001',
    'DISCORD_GUILD_ID': '100000000000000002',
    'DISCORD_GUILD_CHANNEL_ID': '100000000000000004',
    'DISCORD_GUILD_MODE': 'all',
    'DISCORD_ENABLE_MESSAGE_CONTENT': 'true',
    'BRIDGE_DB': str(current),
    'BRIDGE_HTTP_KEEPALIVE_SECONDS': '300',
    'DISCORD_BOT_TOKEN_FILE': '/opt/assistant-shared/.discord-private/bot-token',
})
env.pop('DISCORD_BOT_TOKEN', None)
env.pop('PYTHONPATH', None)
os.execve(str(binary), [str(binary), 'gateway'], env)

