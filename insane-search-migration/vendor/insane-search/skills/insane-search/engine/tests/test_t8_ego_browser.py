#!/usr/bin/env python3
"""Regression contracts for passive ego capture, cleanup, and escalation."""
from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import engine.executor as ex
import engine.fetch_chain as fc
from engine.fetch_chain import Attempt
from engine.validators import Verdict


def test_challenge_is_not_retried_and_cleanup_follows_capture() -> None:
    html = "<html><title>Just a moment...</title>window._cf_chl_opt</html>"
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        if len(calls) == 1:
            return SimpleNamespace(returncode=0, stdout="", stderr=json.dumps({
                "html": html, "status": 200, "finalUrl": "https://example.test/page",
                "automation": "ego_browser", "challenge": {}, "observations": [],
            }))
        return SimpleNamespace(returncode=0, stdout="", stderr='{"done":true}')
    with patch.object(ex.shutil, "which", return_value="/bin/ego-browser"), patch.object(ex.subprocess, "run", side_effect=run):
        attempt, content = ex.run_ego_browser_fallback("https://example.test/page", profile_id="unknown_challenge", max_browser_attempts=2)
    assert attempt.verdict == Verdict.CHALLENGE.value
    assert content == html
    assert len(calls) == 2  # one capture and one final cleanup, not a challenge retry
    assert all(command == ["/bin/ego-browser", "nodejs"] and kwargs.get("input") for command, kwargs in calls)
    assert len(attempt._ego_browser_attempts) == 1


def test_user_takeover_never_retries_or_cleans_up() -> None:
    with patch.object(ex.shutil, "which", return_value="/bin/ego-browser"), patch.object(ex.subprocess, "run", return_value=SimpleNamespace(
        returncode=0, stderr="", stdout=json.dumps({"html": "", "error": "user is controlling", "controlRequired": True})
    )) as run:
        attempt, html = ex.run_ego_browser_fallback("https://example.test/page", profile_id="unknown_challenge")
    assert run.call_count == 1
    assert html == ""
    assert "browser_control_required" in attempt.reasons
    assert fc._browser_recommendation([attempt], "exhausted", True) == (False, None, None, None)


def test_invalid_capture_is_cleaned_up_without_retry() -> None:
    with patch.object(ex.shutil, "which", return_value="/bin/ego-browser"), patch.object(ex.subprocess, "run", side_effect=[
        SimpleNamespace(returncode=0, stderr="", stdout="not an envelope"),
        SimpleNamespace(returncode=0, stderr="", stdout='{"done":true}'),
    ]) as run:
        attempt, html = ex.run_ego_browser_fallback("https://example.test/page", profile_id="unknown_challenge")
    assert run.call_count == 2
    assert attempt.verdict == Verdict.UNKNOWN.value
    assert html == ""
    assert attempt.error


def test_interactive_escalation_excludes_terminal_results() -> None:
    attempt = Attempt(phase="fallback", executor="ego_browser", url="https://example.test/page",
                      url_transform="original", impersonate=None, referer="", verdict=Verdict.CHALLENGE.value)
    required, tool, action, stage = fc._browser_recommendation([attempt], "exhausted", True)
    assert required and tool == "ego-browser" and stage == "browser"
    assert action
    attempt.verdict = Verdict.NOT_FOUND.value
    assert fc._browser_recommendation([attempt], "not_found", False) == (False, None, None, None)
    assert fc.ego_browser_requested(SimpleNamespace(interactive_browser_required=False, recommended_tool=None, must_invoke_playwright_mcp=True)) is False
    assert fc.ego_browser_requested(SimpleNamespace(interactive_browser_required=True, recommended_tool="ego-browser")) is True


if __name__ == "__main__":
    for name, test in sorted(list(globals().items())):
        if name.startswith("test_"):
            test()
    print("ego browser capture and escalation contracts passed")
