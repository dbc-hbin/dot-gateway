from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Awaitable, Callable, Protocol


def text_units(text: str) -> int:
    """Conservative UTF-16 budget; never split a Unicode code point."""
    return len(text.encode("utf-16-le")) // 2


def split_text(text: str, limit: int = 1900, maximum: int = 16000) -> list[str]:
    if limit < 2 or not text.strip() or text_units(text) > maximum:
        raise ValueError("reply must be nonempty and within the configured text budget")
    parts, current, used = [], [], 0
    for char in text:
        size = text_units(char)
        if used + size > limit:
            parts.append("".join(current))
            current, used = [], 0
        current.append(char)
        used += size
    if current:
        parts.append("".join(current))
    if any(not part.strip() for part in parts):
        raise ValueError("reply would create a whitespace-only chunk")
    return parts


@dataclass(frozen=True)
class Envelope:
    platform: str
    event_id: str
    conversation_id: str
    sender_id: str
    text: str
    received_at: float
    route_kind: str = "dm"
    reply_to_event_id: str | None = None
    sender_is_bot: bool = False
    guild_id: str | None = None
    bot_mentioned: bool = False

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class OutboundChunk:
    reply_id: str
    index: int
    text: str
    source: Envelope


@dataclass(frozen=True)
class SendResult:
    # sent = acknowledged with a remote ID; failed = known not sent;
    # uncertain = request may have reached the service. Never auto-retry it.
    state: str
    message_id: str | None = None
    code: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {"sent", "failed", "uncertain"}:
            raise ValueError("invalid delivery state")
        if self.state == "sent" and not self.message_id:
            raise ValueError("sent requires a remote message ID")


InboundHandler = Callable[[Envelope], Awaitable[None]]


class AccessPolicy(Protocol):
    def allows(self, event: Envelope) -> bool: ...

    def accepts(self, event: Envelope) -> bool: ...


class TextAdapter(Protocol):
    platform: str
    # False until authenticated route validation succeeds, and on disconnect/close.
    ready: bool

    async def run(self, on_message: InboundHandler) -> None: ...

    async def send(self, chunk: OutboundChunk) -> SendResult: ...

    async def close(self) -> None: ...
