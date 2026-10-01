"""Insane Search web backend for Hermes."""
from __future__ import annotations

from .provider import InsaneSearchProvider


def register(ctx) -> None:
    ctx.register_web_search_provider(InsaneSearchProvider())
