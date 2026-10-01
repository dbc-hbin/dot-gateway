#!/usr/bin/env python3
"""Regression tests for engine-native CAPTCHA clearance."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

import engine.captcha as captcha  # noqa: E402
import engine.executor as ex  # noqa: E402
from engine.validators import Verdict  # noqa: E402


def t_detects_interstitial_and_not_comment_widget() -> None:
    hits = captcha.detect_markers(
        "<html><title>Just a moment...</title>window._cf_chl_opt</html>"
    )
    assert "Just a moment..." in hits
    assert "window._cf_chl_opt" in hits
    assert captcha.detect_markers("<html><body>normal article</body></html>") == []
    comment_html = (
        "<html><body><article>" + ("long article text " * 80)
        + "</article><form id='comment'><textarea></textarea>"
        + "<div class='g-recaptcha cf-turnstile'></div></form></body></html>"
    )
    assert captcha.detect_markers(comment_html) == []
    assert captcha.challenge_state(comment_html) == [], captcha.challenge_state(comment_html)
    human = captcha.detect_markers(
        "<html><body><h1>Verify you are human</h1><p>checking your browser</p></body></html>"
    )
    assert any("verify you are human" in h.lower() for h in human), human
    print("  v marker detection (state vs comment-form widget)")


def t_fullpage_turnstile_is_unresolved() -> None:
    html = '<html><body><div class="cf-turnstile"></div></body></html>'
    assert captcha.detect_markers(html) == []
    state = captcha.challenge_state(html)
    assert "cf-turnstile" in state, state
    print("  v full-page turnstile stays unresolved without English phrase")


def t_sync_clearance_uses_mouse_hold_once() -> None:
    html = ["<html><body>Just a moment... window._cf_chl_opt<div id='px-captcha'>hold</div></body></html>"]
    held = {"n": 0}

    class FakeMouse:
        def click(self, x, y):
            html[0] = "<!doctype html><html><body>" + ("rendered " * 80) + "</body></html>"
        def move(self, x, y):
            return None
        def down(self):
            held["n"] += 1
            html[0] = "<!doctype html><html><body>" + ("rendered " * 80) + "</body></html>"
        def up(self):
            return None

    class FakeEl:
        def is_visible(self):
            return True
        def click(self, timeout=0):
            return None
        def get_attribute(self, name):
            return "px-captcha" if name in ("id", "class") else ""
        def bounding_box(self):
            return {"x": 10, "y": 10, "width": 80, "height": 40}
        def inner_text(self):
            return "Press & Hold"

    class FakeLocator:
        def __init__(self, sel=""):
            self.sel = sel
        def count(self):
            if "px-captcha" in self.sel or "button" in self.sel:
                return 1
            return 0
        def nth(self, i):
            return FakeEl()

    class FakePage:
        mouse = FakeMouse()
        frames = []
        def content(self):
            return html[0]
        def evaluate(self, script):
            return {"clicked": 0, "held": 0, "holdTargets": []}
        def locator(self, sel):
            return FakeLocator(sel)
        def reload(self, **kwargs):
            return None
        def wait_for_timeout(self, ms):
            return None

    page = FakePage()
    page.frames = [page]
    result = captcha.clear_playwright_sync(page, timeout_ms=2000)
    assert result["resolved"] is True
    assert held["n"] == 1, held["n"]
    print("  v sync Playwright mouse hold once")


def t_mcp_stub_still_zero_work() -> None:
    att, content = ex.run_playwright_fallback(
        "https://turnstile.test/", profile_id="cloudflare_turnstile",
        force_executor="playwright_mcp")
    assert att.verdict == Verdict.UNKNOWN.value
    assert content == ""
    assert "MCP" in (att.error or "")
    print("  v MCP stub remains zero-work")


def t_unobserved_status_with_body_is_not_unknown() -> None:
    from engine.validators import validate

    class R:
        status_code = 0
        text = "<html><body>" + ("clean article text " * 80) + "</body></html>"
        headers = {}
        cookies = type("C", (), {"jar": iter(())})()
    vr = validate(R())
    assert vr.verdict == Verdict.WEAK_OK, vr.verdict
    assert any("status=0_unobserved" in r for r in vr.reasons)
    empty = type("E", (), {"status_code": 0, "text": "", "headers": {}, "cookies": type("C", (), {"jar": iter(())})()})()
    vr2 = validate(empty)
    assert vr2.verdict == Verdict.UNKNOWN, vr2.verdict
    print("  v status=0 with body still validates; empty stays UNKNOWN")


def t_envelope_missing_status_is_unknown_not_200() -> None:
    record = ex._parse_envelope_record(
        '{"html":"<html>' + ("x" * 80) + '</html>","finalUrl":"https://example.test/"}',
        "https://example.test/",
    )
    assert record["status"] == 0, record["status"]
    print("  v missing browser status is 0, not 200")


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("t_") and callable(v)]
    failed = 0
    for test in tests:
        try:
            test()
        except AssertionError as e:
            print(f"  x {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  x {test.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{'OK' if failed == 0 else 'FAIL'}: {len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
