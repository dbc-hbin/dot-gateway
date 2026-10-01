from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    from .proxy import ProxyConfig

from .model import Envelope, text_units


class ConfigError(ValueError):
    pass


def snowflake(value: str) -> bool:
    return bool(re.fullmatch(r"[1-9][0-9]{0,19}", value)) and int(value) < 2**64


@dataclass(frozen=True)
class Policy:
    owner_id: str
    allowed_dm_ids: frozenset[str]
    platform: str = "discord"
    guild_id: str = ""
    guild_channel_id: str = ""
    guild_mode: str = "mention"
    message_content_approved: bool = False

    def __post_init__(self) -> None:
        if not snowflake(self.owner_id):
            raise ConfigError("DISCORD_OWNER_ID must be an explicit numeric user ID")
        if self.allowed_dm_ids != frozenset({self.owner_id}):
            raise ConfigError("DISCORD_ALLOWED_DM_IDS must contain only the owner ID")
        if bool(self.guild_id) != bool(self.guild_channel_id):
            raise ConfigError("configure both DISCORD_GUILD_ID and DISCORD_GUILD_CHANNEL_ID")
        if self.guild_id and not (snowflake(self.guild_id) and snowflake(self.guild_channel_id)):
            raise ConfigError("guild and channel must be explicit numeric IDs")
        if self.guild_mode not in {"mention", "all"}:
            raise ConfigError("DISCORD_GUILD_MODE must be mention or all")
        if self.guild_mode == "all" and (not self.guild_id or not self.message_content_approved):
            raise ConfigError("all mode requires a pinned guild channel and separately approved Message Content intent")
        if self.message_content_approved and (not self.guild_id or self.guild_mode != "all"):
            raise ConfigError("Message Content intent is only allowed in explicitly configured all mode")

    def allows(self, event: Envelope) -> bool:
        return (
            event.platform == self.platform
            and ((event.route_kind == "dm" and event.guild_id is None)
                 or (event.route_kind == "guild_text" and bool(self.guild_id)
                     and event.guild_id == self.guild_id
                     and event.conversation_id == self.guild_channel_id
                     and (self.guild_mode == "all" or event.bot_mentioned)))
            and not event.sender_is_bot
            and event.sender_id == self.owner_id
            and event.sender_id in self.allowed_dm_ids
            and snowflake(event.conversation_id)
            and snowflake(event.event_id)
        )

    def accepts(self, event: Envelope) -> bool:
        try:
            return self.allows(event) and bool(event.text.strip()) and text_units(event.text) <= 8000
        except (UnicodeError, AttributeError, TypeError):
            return False


@dataclass(frozen=True)
class Settings:
    policy: Policy
    db_path: Path
    expected_bot_id: str = ""
    token: str = field(default="", repr=False)
    proxy_config: ProxyConfig | None = field(default=None, repr=False)
    http_keepalive_seconds: float = 120

    @classmethod
    def load(cls, env: Mapping[str, str] | None = None, *, live: bool = False,
             validate_proxy: bool = False) -> "Settings":
        env = os.environ if env is None else env
        from .proxy import ProxyConfig
        # Queue/status/recovery commands must work even if network setup is invalid.
        proxy_config = ProxyConfig.load(env) if live or validate_proxy else None
        owner = env.get("DISCORD_OWNER_ID", "").strip()
        allowed = frozenset(x.strip() for x in env.get("DISCORD_ALLOWED_DM_IDS", "").split(",") if x.strip())
        content_gate = env.get("DISCORD_ENABLE_MESSAGE_CONTENT", "false").strip().lower()
        if content_gate not in {"true", "false"}:
            raise ConfigError("DISCORD_ENABLE_MESSAGE_CONTENT must be true or false")
        policy = Policy(owner, allowed, guild_id=env.get("DISCORD_GUILD_ID", "").strip(),
                        guild_channel_id=env.get("DISCORD_GUILD_CHANNEL_ID", "").strip(),
                        guild_mode=env.get("DISCORD_GUILD_MODE", "mention").strip(),
                        message_content_approved=content_gate == "true")
        expected_bot_id = env.get("DISCORD_EXPECTED_BOT_ID", "").strip()
        if expected_bot_id and (not snowflake(expected_bot_id) or expected_bot_id == owner):
            raise ConfigError("DISCORD_EXPECTED_BOT_ID must be a distinct numeric bot ID")
        token = ""
        if live:
            if not snowflake(expected_bot_id) or expected_bot_id == owner:
                raise ConfigError("DISCORD_EXPECTED_BOT_ID must pin the bot user ID for live mode")
            token = env.get("DISCORD_BOT_TOKEN", "").strip()
            filename = env.get("DISCORD_BOT_TOKEN_FILE", "").strip()
            if token and filename:
                raise ConfigError("provide only one token source")
            if filename:
                # Open without following a symlink and validate the actual opened file.
                fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, "r", encoding="utf-8") as handle:
                    info = os.fstat(handle.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                        raise ConfigError("token file must be owner-owned, regular, and mode 600 or stricter")
                    token = handle.read(4097).strip()
            if not token or len(token) > 4096 or any(not 33 <= ord(x) <= 126 for x in token):
                raise ConfigError("a securely provisioned bot token is required")
        try:
            keepalive=float(env.get('BRIDGE_HTTP_KEEPALIVE_SECONDS','120'))
        except (ValueError,TypeError):
            raise ConfigError('BRIDGE_HTTP_KEEPALIVE_SECONDS must be between15 and600') from None
        if not 15 <= keepalive <= 600:
            raise ConfigError('BRIDGE_HTTP_KEEPALIVE_SECONDS must be between15 and600')
        return cls(policy, Path(env.get("BRIDGE_DB", ".state/bridge.sqlite3")), expected_bot_id, token, proxy_config, keepalive)
