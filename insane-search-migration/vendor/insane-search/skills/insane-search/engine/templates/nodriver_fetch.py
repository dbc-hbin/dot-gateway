"""nodriver fallback: drive system Chrome over raw CDP, no Playwright shim.

Empirical rationale (2026 anti-detect bench): gates that fingerprint the
automation PROTOCOL (Runtime.enable / Target.setAutoAttach) block every
Playwright-based driver regardless of patch quality, while a plain CDP
WebSocket control plane passes. nodriver is that control plane.

stdin:  {"url": str, "timeout": ms, "waitSelector": str?, "clearCaptcha": bool}
stdout: JSON envelope or page HTML. stderr: diagnostics. exit 0 on content, 1 on failure.
"""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


async def main() -> int:
    args = json.load(sys.stdin)
    import nodriver as uc
    from engine.captcha import CHALLENGE_TIMEOUT_MS, clear_nodriver

    timeout_ms = min(int(args.get("timeout", CHALLENGE_TIMEOUT_MS)), CHALLENGE_TIMEOUT_MS)
    browser = await uc.start(headless=True)
    try:
        tab = await browser.get(args["url"])
        await tab.sleep(2)
        challenge = {}
        if args.get("clearCaptcha", True):
            try:
                challenge = await clear_nodriver(tab, timeout_ms=timeout_ms)
            except Exception as e:
                print(f"captcha clearance failed: {e}", file=sys.stderr)
        else:
            await tab.sleep(6)
        sel = args.get("waitSelector")
        if sel:
            try:
                await tab.select(sel, timeout=10)
            except Exception as e:
                print(f"best-effort waitSelector failed: {e}", file=sys.stderr)
        html = await tab.get_content() or ""
        sys.stdout.write(json.dumps({
            "html": html,
            "finalUrl": args["url"],
            "status": 0,
            "cookies": [],
            "userAgent": None,
            "automation": "nodriver",
            "innerText": "",
            "challenge": challenge,
        }))
    finally:
        try:
            browser.stop()
        except Exception as e:
            print(f"best-effort browser stop failed: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
