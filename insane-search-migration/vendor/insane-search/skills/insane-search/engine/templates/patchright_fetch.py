"""patchright fallback: patched Playwright driving SYSTEM Chrome (channel=chrome).

Second choice behind nodriver for protocol-fingerprinting gates: Patchright
removes the Runtime.enable / Target.setAutoAttach startup leaks, and
channel="chrome" gives a real Chrome TLS + version stamp instead of the
bundled Chromium. Apache-2.0 (nodriver is AGPL-3.0), so this is the
license-safe default when redistribution matters.

stdin:  {"url": str, "timeout": ms, "waitSelector": str?, "clearCaptcha": bool}
stdout: JSON envelope or page HTML. stderr: diagnostics. exit 0 on content, 1 on failure.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    args = json.load(sys.stdin)
    from patchright.sync_api import sync_playwright
    from engine.captcha import CHALLENGE_TIMEOUT_MS, clear_playwright_sync

    timeout_ms = min(int(args.get("timeout", CHALLENGE_TIMEOUT_MS)), CHALLENGE_TIMEOUT_MS)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        try:
            page = browser.new_page()
            resp = page.goto(args["url"], timeout=timeout_ms, wait_until="domcontentloaded")
            challenge = {}
            if args.get("clearCaptcha", True):
                try:
                    challenge = clear_playwright_sync(page, timeout_ms=timeout_ms)
                except Exception as e:
                    print(f"captcha clearance failed: {e}", file=sys.stderr)
            else:
                page.wait_for_timeout(5000)
            sel = args.get("waitSelector")
            if sel:
                try:
                    page.wait_for_selector(sel, timeout=10000)
                except Exception as e:
                    print(f"best-effort waitSelector failed: {e}", file=sys.stderr)
            html = page.content() or ""
            status = 0
            try:
                if resp is not None:
                    status = int(resp.status)
            except Exception:
                status = 0
            if not status:
                try:
                    status = int(page.evaluate(
                        "() => (performance.getEntriesByType('navigation')[0]||{}).responseStatus || 0"
                    ) or 0)
                except Exception:
                    status = 0
            sys.stdout.write(json.dumps({
                "html": html,
                "finalUrl": page.url,
                "status": status,
                "cookies": [],
                "userAgent": None,
                "automation": "patchright",
                "innerText": "",
                "challenge": challenge,
            }))
        finally:
            try:
                browser.close()
            except Exception as e:
                print(f"best-effort browser close failed: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
