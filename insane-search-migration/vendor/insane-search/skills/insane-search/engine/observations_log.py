"""Append-only operational observations for profile tuning.

Route learning remains exclusively in ``learning.py``. This module only
records outcomes to ``observations/fetch-YYYY-MM-DD.jsonl`` so repeated
cross-site evidence can be reviewed before changing WAF profiles.

Env override: ``INSANE_OBSERVATIONS_DIR``.
Logging is best-effort and never changes a fetch outcome.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

_SKILL_DIR = Path(__file__).resolve().parent.parent


def _obs_dir() -> Path:
    override = os.environ.get("INSANE_OBSERVATIONS_DIR")
    return Path(override) if override else _SKILL_DIR / "observations"


def log_fetch(url: str, result) -> None:
    try:
        trace = getattr(result, "trace", []) or []
        winner = next(
            (a for a in reversed(trace) if getattr(a, "verdict", "") in ("strong_ok", "weak_ok")),
            None,
        )
        planned_attempts = getattr(result, "planned_attempts", 0)
        executed_attempts = getattr(result, "executed_attempts", 0)
        grid_exhausted = bool(getattr(result, "grid_exhausted", False))
        stop_reason = getattr(result, "stop_reason", "")
        untried_routes = list(getattr(result, "untried_routes", []) or [])
        interactive_required = bool(
            getattr(result, "interactive_browser_required", False)
        )
        must_invoke_mcp = bool(
            getattr(result, "must_invoke_playwright_mcp", interactive_required)
        )
        entry = {
            "ts": int(time.time()),
            "url": url,
            "domain": (urlparse(url).hostname or "").lower(),
            "ok": bool(getattr(result, "ok", False)),
            "verdict": getattr(result, "verdict", ""),
            "profile_used": getattr(result, "profile_used", None),
            "attempts": len(trace),
            "planned_attempts": planned_attempts,
            "executed_attempts": executed_attempts,
            "grid_exhausted": grid_exhausted,
            "stop_reason": stop_reason,
            "interactive_browser_required": interactive_required,
            # R6 is duplicated as a compact object so log consumers do not
            # need to reconstruct the failure gate from unrelated fields.
            "r6": {
                "grid_exhausted": grid_exhausted,
                "planned_attempts": planned_attempts,
                "executed_attempts": executed_attempts,
                "stop_reason": stop_reason,
                "untried_routes": untried_routes,
                "interactive_browser_required": interactive_required,
                "must_invoke_playwright_mcp": must_invoke_mcp,
            },
        }
        if interactive_required:
            recommendation = {
                "recommended_tool": getattr(result, "recommended_tool", None),
                "recommended_action": getattr(result, "recommended_action", None),
                "fallback_stage": getattr(result, "fallback_stage", None),
            }
            entry.update(recommendation)
            entry["r6"].update(recommendation)
        failures = []
        for index, attempt in enumerate(trace):
            verdict = getattr(attempt, "verdict", "")
            error = getattr(attempt, "error", None)
            reasons = list(getattr(attempt, "reasons", []) or [])
            if error or verdict not in ("strong_ok", "weak_ok"):
                failures.append({
                    "index": index,
                    "phase": getattr(attempt, "phase", ""),
                    "executor": getattr(attempt, "executor", ""),
                    "url": getattr(attempt, "url", ""),
                    "status": getattr(attempt, "status", 0),
                    "verdict": verdict,
                    "error": error,
                    "reasons": reasons,
                })
        if failures:
            entry["failed_attempts"] = failures
        if winner is not None:
            entry["winner"] = {
                "phase": getattr(winner, "phase", ""),
                "executor": getattr(winner, "executor", ""),
                "transform": getattr(winner, "url_transform", ""),
                "impersonate": getattr(winner, "impersonate", None),
                "referer": getattr(winner, "referer", ""),
                "status": getattr(winner, "status", 0),
                "body_size": getattr(winner, "body_size", 0),
            }
        directory = _obs_dir()
        directory.mkdir(parents=True, exist_ok=True)
        day = time.strftime("%Y-%m-%d", time.gmtime())
        with open(directory / f"fetch-{day}.jsonl", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        return
