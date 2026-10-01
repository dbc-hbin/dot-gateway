"""Hermes web provider backed by insane-search and ego-browser."""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

from agent.web_search_provider import WebSearchProvider

logger = logging.getLogger(__name__)
_SOURCE_ROOT = Path.home() / ".hermes" / "insane-search" / "skills" / "insane-search"
_BROWSER_TOOL = "ego-browser"
_BROWSER_ACTION = (
    "delegate to a browser agent using the ego-browser skill for "
    "openOrReuseTab(url) -> snapshotText/DOM/CDP inspection"
)


def _ensure_source_path() -> None:
    source = str(_SOURCE_ROOT)
    if source not in sys.path:
        sys.path.insert(0, source)


def _browser_escalation_hint(result: Any, url: str) -> str:
    """Preserve engine-gated interactive guidance in the per-URL error.

    Core web tools may discard metadata. Terminal 404/auth/paywall/CAPTCHA
    failures do not request interactive inspection and receive no hint.
    """
    required = bool(getattr(result, "interactive_browser_required", False))
    tool = getattr(result, "recommended_tool", None)
    if not required and tool != _BROWSER_TOOL:
        return ""

    return (
        f"CLI ego-browser fallback failed for {url}; {_BROWSER_ACTION}."
    )


class InsaneSearchProvider(WebSearchProvider):
    @property
    def name(self) -> str:
        return "insane-search"

    @property
    def display_name(self) -> str:
        return "Insane Search (ego-browser)"

    def is_available(self) -> bool:
        return (_SOURCE_ROOT / "engine" / "__init__.py").is_file()

    def supports_search(self) -> bool:
        return True

    def supports_extract(self) -> bool:
        return True

    def search(self, query: str, limit: int = 5) -> dict[str, Any]:
        """Discover public URLs with DDGS; insane-search handles extraction."""
        try:
            from ddgs import DDGS

            safe_limit = max(1, min(int(limit), 20))
            results: list[dict[str, Any]] = []
            with DDGS(timeout=10) as client:
                for position, hit in enumerate(
                    client.text(query, max_results=safe_limit), start=1
                ):
                    if position > safe_limit:
                        break
                    results.append(
                        {
                            "title": str(hit.get("title") or ""),
                            "url": str(hit.get("href") or hit.get("url") or ""),
                            "description": str(hit.get("body") or ""),
                            "position": position,
                        }
                    )
            return {"success": True, "data": {"web": results}}
        except Exception as exc:
            logger.warning("Insane Search discovery failed: %s", exc)
            return {"success": False, "error": f"Search discovery failed: {exc}"}

    async def extract(self, urls: list[str], **kwargs: Any) -> list[dict[str, Any]]:
        _ensure_source_path()
        from engine import fetch

        max_chars = int(kwargs.get("max_chars") or 200_000)

        def fetch_one(url: str) -> dict[str, Any]:
            try:
                result = fetch(
                    url,
                    max_attempts=None,
                    enable_playwright=True,
                    enable_phase0=True,
                    enable_extraction=True,
                    enable_markdown=True,
                    enable_maincontent=False,
                )
                if not result.ok:
                    error = result.summary or result.stop_reason or "insane-search failed"
                    hint = _browser_escalation_hint(result, url)
                    if hint:
                        error = f"{error} {hint}"
                    return {
                        "url": url,
                        "title": "",
                        "content": "",
                        "raw_content": "",
                        "metadata": result.to_dict(),
                        "error": error,
                    }
                content = result.to_untrusted_text()[:max_chars]
                return {
                    "url": result.final_url or url,
                    "title": "",
                    "content": content,
                    "raw_content": content,
                    "metadata": result.to_dict(),
                }
            except Exception as exc:
                return {
                    "url": url,
                    "title": "",
                    "content": "",
                    "raw_content": "",
                    "metadata": {},
                    "error": f"{type(exc).__name__}: {exc}",
                }

        return await asyncio.gather(*(asyncio.to_thread(fetch_one, url) for url in urls))

    def get_setup_schema(self) -> dict[str, Any]:
        return {
            "name": self.display_name,
            "badge": "local",
            "tag": (
                "No API key; HTTP-first adaptive public-page extraction, then "
                "ego-browser CLI. Interactive failures can escalate to a browser "
                "agent using the ego-browser skill for snapshot/DOM/CDP inspection."
            ),
            "env_vars": [],
        }
