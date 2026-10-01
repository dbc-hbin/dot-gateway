"""Capability-matched executor for fallback attempts.

The fetch_chain's probe/grid phase uses curl_cffi directly. When curl can't
punch through (JS challenge, real-TLS detection, public CAPTCHA widget),
this module routes to the right browser executor based on the profile's
`capabilities_needed` tags and `fallback_when_challenge` list:

    needs_protocol_stealth                → protocol_stealth_chrome
    needs_real_tls_stack + needs_js_exec  → playwright_real_chrome.js
    needs_js_exec only                    → ego-browser CLI
    needs_mobile_context (+ real_tls)     → playwright_mobile_chrome.js
    ego_browser                          → ego-browser CLI (isolated task space)

Local templates run engine.captcha clearance: click public
verify widgets, hold press-and-hold gates, wait for JS interstitials, then
reload so clearance cookies can land. Login/paywall credentials are never
filled.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import uuid
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Optional

from .captcha import CHALLENGE_TIMEOUT_MS, build_ego_browser_script, build_ego_browser_cleanup_script
from .fetch_chain import Attempt
from .validators import Verdict, validate
from .waf_detector import load_profile


TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _profile_dir_for(url: str, choice: str) -> str:
    """Per-host + per-device Chrome profile directory.

    The host is hashed (never stored as a site name) so the No-Site-Name Rule
    holds while each host keeps an isolated, reusable profile. Desktop and
    mobile get separate subdirs so emulation state never bleeds across.
    """
    import hashlib
    from urllib.parse import urlsplit
    host = (urlsplit(url).hostname or "unknown").lower()
    host_hash = hashlib.sha1(host.encode("utf-8", "ignore")).hexdigest()[:16]
    device = "mobile" if "mobile" in choice else "desktop"
    return os.path.join(tempfile.gettempdir(), ".insane_pw", host_hash, device)


def _node_available() -> bool:
    return shutil.which("node") is not None


def _chrome_channel_available() -> bool:
    if not _node_available():
        return False
    if shutil.which("npx") is None:
        return False
    return True


def _pick_executor(capabilities: list[str], device_class: str) -> str:
    caps = set(capabilities or [])
    if device_class == "mobile" or "needs_mobile_context" in caps:
        if "needs_real_tls_stack" in caps:
            return "playwright_mobile_chrome"
        return "playwright_mcp_mobile"
    if "needs_protocol_stealth" in caps:
        return "protocol_stealth_chrome"
    if "needs_real_tls_stack" in caps:
        return "playwright_real_chrome"
    if "needs_js_exec" in caps:
        return "ego_browser"
    return "playwright_real_chrome"


def _module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


def _auto_install(pkg: str) -> bool:
    if os.environ.get("INSANE_AUTO_INSTALL", "").strip() not in ("1", "true", "yes"):
        return False
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", pkg, "-q"],
                       capture_output=True, timeout=180, check=False)
    except Exception:
        return False
    importlib.invalidate_caches()
    return _module_available(pkg)


def _run_python_template(template: str, args: dict, timeout: int = 90) -> tuple[int, str, str]:
    path = os.path.join(TEMPLATES_DIR, template)
    if not os.path.isfile(path):
        return 127, "", f"template not found: {path}"
    try:
        proc = subprocess.run(
            [sys.executable, path], input=json.dumps(args), cwd=TEMPLATES_DIR,
            capture_output=True, text=True, timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout after {timeout}s"
    except Exception as e:
        return 1, "", f"{type(e).__name__}:{e}"


def _run_protocol_stealth(
    att: Attempt, url: str, *, success_selectors: Optional[list[str]], timeout: int, t0: float,
) -> tuple[Attempt, str]:
    """nodriver (raw CDP, no Playwright shim) first, patchright channel=chrome next."""
    args: dict = {
        "url": url,
        "timeout": CHALLENGE_TIMEOUT_MS,  # clearance budget, not the 180s subprocess envelope
        "clearCaptcha": True,
    }
    if success_selectors:
        args["waitSelector"] = success_selectors[0]
    template_timeout = max(int(timeout or 0), CHALLENGE_TIMEOUT_MS // 1000) + 30
    for pkg, template in (("nodriver", "nodriver_fetch.py"), ("patchright", "patchright_fetch.py")):
        if not _module_available(pkg) and not _auto_install(pkg):
            continue
        rc, stdout, stderr = _run_python_template(template, args, timeout=template_timeout)
        att.executor = f"protocol_stealth_chrome:{pkg}"
        att.elapsed_s = round(time.time() - t0, 3)
        if rc != 0 or not stdout:
            att.error = f"{pkg}: {(stderr or 'no stdout')[:200]}"
            continue
        html, final_url, status, cookies, user_agent, automation, inner_text = _parse_envelope(stdout, url)
        if not html:
            html = stdout
            final_url, status = url, 0
        resp = _FakeResp(html, status=status, final_url=final_url or url)
        vr = validate(resp, success_selectors=success_selectors)
        att.status = status
        att.body_size = len(html)
        att.verdict = vr.verdict.value
        att.reasons = list(vr.reasons) + ([f"automation:{automation}"] if automation else [])
        att.url = final_url or url
        if inner_text:
            att._inner_text = inner_text
        if vr.verdict in (Verdict.STRONG_OK, Verdict.WEAK_OK) and cookies:
            _bridge_cookies_to_pool(url, cookies, user_agent)
        return att, html
    att.elapsed_s = round(time.time() - t0, 3)
    if not att.error:
        att.error = "nodriver/patchright not installed (pip install nodriver, or INSANE_AUTO_INSTALL=1)"
    att.verdict = Verdict.UNKNOWN.value
    return att, ""


def _run_node_template(template: str, args: dict, timeout: int = 90) -> tuple[int, str, str]:
    path = os.path.join(TEMPLATES_DIR, template)
    if not os.path.isfile(path):
        return 127, "", f"template not found: {path}"
    try:
        proc = subprocess.run(
            ["node", path],
            input=json.dumps(args),
            cwd=TEMPLATES_DIR,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout after {timeout}s"
    except Exception as e:
        return 1, "", f"{type(e).__name__}:{e}"


class _FakeCookies:
    class _Jar:
        def __iter__(self):
            return iter([])

    def __init__(self):
        self.jar = self._Jar()

    def __iter__(self):
        return iter([])


class _FakeResp:
    """Minimal response shim so validators.validate() works on Playwright HTML."""

    def __init__(self, html: str, status: int = 200, final_url: str = ""):
        self.text = html
        self.status_code = status
        self.url = final_url
        self.cookies = _FakeCookies()
        self.headers = {}


def _parse_envelope_record(stdout: str, url: str) -> dict:
    """Parse the final JSON envelope and retain bounded capture diagnostics."""
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            envelope = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(envelope, dict) or "html" not in envelope:
            continue
        if "status" not in envelope or envelope.get("status") is None:
            status = 0
        else:
            try:
                status = int(envelope.get("status"))
            except (TypeError, ValueError):
                status = 0
        challenge = envelope.get("challenge")
        if not isinstance(challenge, dict):
            challenge = {}
        observations = envelope.get("observations")
        if not isinstance(observations, list):
            observations = []
        return {
            "html": str(envelope.get("html") or ""),
            "final_url": str(envelope.get("finalUrl") or url),
            "status": status,
            "automation": str(envelope.get("automation") or "ego_browser"),
            "inner_text": str(envelope.get("innerText") or "")[:1_000_000],
            "cookies": envelope.get("cookies") or [],
            "user_agent": envelope.get("userAgent") or None,
            "challenge": challenge,
            "observations": observations[:128],
            "error": str(envelope.get("error") or ""),
            "control_required": envelope.get("controlRequired") is True,
        }
    raise ValueError("ego-browser returned no rendered-page envelope")


def _parse_envelope(stdout: str, url: str):
    """Return (html, final_url, status, cookies, user_agent, automation, inner_text)."""
    s = stdout.lstrip()
    if s[:1] == "{":
        try:
            env = json.loads(s)
            if isinstance(env, dict) and "html" in env:
                html = env.get("html", "") or ""
                final_url = env.get("finalUrl", "") or url
                if "status" not in env or env.get("status") is None:
                    status = 0
                else:
                    try:
                        status = int(env.get("status"))
                    except (TypeError, ValueError):
                        status = 0
                cookies = env.get("cookies") or []
                user_agent = env.get("userAgent") or None
                automation = env.get("automation") or None
                inner_text = (env.get("innerText") or "")[:1_000_000]
                return html, final_url, status, cookies, user_agent, automation, inner_text
        except Exception:
            pass
        try:
            record = _parse_envelope_record(stdout, url)
            return (
                record["html"], record["final_url"], record["status"],
                record.get("cookies") or [], record.get("user_agent"),
                record.get("automation"), record["inner_text"],
            )
        except ValueError:
            pass
    return stdout, url, 200, [], None, None, ""


def _bridge_cookies_to_pool(url: str, cookies: list, user_agent: Optional[str]) -> None:
    try:
        from .transport import POOL, pool_enabled, _host_of
        if not pool_enabled():
            return
        POOL.inject_cookies(_host_of(url), "chrome", cookies, user_agent=user_agent)
    except Exception:
        pass


def _attach_challenge_reasons(attempt: Attempt, diagnostics: dict) -> None:
    if not diagnostics:
        return
    initial_markers = diagnostics.get("initialMarkers") or []
    final_markers = diagnostics.get("finalMarkers") or []
    if initial_markers:
        attempt.reasons.append("challenge_initial_markers:" + ",".join(map(str, initial_markers)))
    if final_markers:
        attempt.reasons.append("challenge_final_markers:" + ",".join(map(str, final_markers)))
    if "waitedMs" in diagnostics:
        attempt.reasons.append(f"challenge_wait_ms:{diagnostics.get('waitedMs')}")
    if diagnostics.get("captchaClicks"):
        attempt.reasons.append(f"captcha_clicks:{diagnostics.get('captchaClicks')}")
    if diagnostics.get("domStable"):
        attempt.reasons.append("dom_stable:true")
    elif initial_markers:
        attempt.reasons.append("dom_stable:false")


def resolve_ego_browser_cli() -> Optional[str]:
    """Resolve the CLI even when a GUI-launched host omits the user bin directory."""
    executable = shutil.which("ego-browser")
    if executable:
        return executable
    installed = os.path.expanduser("~/.local/bin/ego-browser")
    if os.path.isfile(installed) and os.access(installed, os.X_OK):
        return installed
    return None


def run_ego_browser_fallback(
    url: str, *, profile_id: str, success_selectors: Optional[list[str]] = None,
    device_class: str = "auto", timeout: int = 90, max_browser_attempts: int = 2,
    profile_dir: Optional[str] = None, force_executor: Optional[str] = None,
) -> tuple[Attempt, str]:
    """Capture once without solving challenges; clean up in a dedicated CLI round."""
    del profile_id, device_class, profile_dir, force_executor
    started = time.time()
    attempt = Attempt(phase="fallback", executor="ego_browser", url=url,
                      url_transform="original", impersonate=None, referer="")
    attempt._ego_browser_attempts = [attempt]
    attempt.verdict = Verdict.UNKNOWN.value
    if max_browser_attempts <= 0:
        attempt.error = "ego-browser budget exhausted (max_browser_attempts=0)"
        attempt.reasons.append("browser_budget:0")
        return attempt, ""
    executable = resolve_ego_browser_cli()
    if executable is None:
        attempt.error = "ego-browser CLI is not installed or not on PATH"
        attempt.reasons.append("ego_browser_unavailable")
        return attempt, ""
    task_name = "insane-search extraction " + uuid.uuid4().hex
    timeout = max(1, int(timeout or 90))
    selector = success_selectors[0] if success_selectors else None
    html = ""
    control_required = False
    try:
        proc = subprocess.run(
            [executable, "nodejs"],
            input=build_ego_browser_script(url, selector, task_name, timeout),
            capture_output=True, text=True, timeout=timeout + 15, check=False,
        )
        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        control_required = bool(proc.returncode) and bool(re.search(
            r"user.{0,30}control|inactive|not assigned|user.owned|delegated.to.user",
            output, re.I))
        if proc.returncode:
            attempt.reasons.append(f"process_exit:{proc.returncode}")
            raise ValueError((proc.stderr or proc.stdout or "ego-browser failed")[-500:])
        record = _parse_envelope_record(output, url)
        control_required = control_required or record["control_required"]
        if record["error"]:
            raise ValueError(record["error"])
        html = record["html"]
        result = validate(_FakeResp(html, status=record["status"], final_url=record["final_url"]),
                          success_selectors=success_selectors)
        attempt.status = record["status"]
        attempt.url = record["final_url"]
        attempt.body_size = len(html)
        attempt.verdict = result.verdict.value
        attempt.reasons.extend(result.reasons)
        attempt.reasons.append("automation:ego_browser")
        _attach_challenge_reasons(attempt, record["challenge"])
        if record["inner_text"]:
            attempt._inner_text = record["inner_text"]
        if result.verdict in (Verdict.STRONG_OK, Verdict.WEAK_OK) and record["cookies"]:
            _bridge_cookies_to_pool(url, record["cookies"], record["user_agent"])
    except subprocess.TimeoutExpired as exc:
        attempt.error = f"ego-browser timed out after {timeout + 15}s"
        attempt.reasons.append("timeout")
        control_required = bool(re.search(r"user.{0,30}control|inactive|not assigned|user.owned",
                                         str(exc.stdout or "") + str(exc.stderr or ""), re.I))
    except Exception as exc:
        attempt.error = f"ego-browser: {type(exc).__name__}: {exc}"
    finally:
        if not control_required:
            try:
                cleanup = subprocess.run(
                    [executable, "nodejs"], input=build_ego_browser_cleanup_script(task_name),
                    capture_output=True, text=True, timeout=20, check=False,
                )
                completed = {}
                for line in ((cleanup.stdout or "") + "\n" + (cleanup.stderr or "")).splitlines():
                    try:
                        value = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if isinstance(value, dict):
                        completed = value
                if cleanup.returncode or not completed.get("done"):
                    attempt.reasons.append("cleanup_incomplete:" + task_name)
                    control_required = bool(completed.get("controlRequired")) or bool(re.search(
                        r"user.{0,30}control|inactive|not assigned|user.owned", cleanup.stderr or "", re.I))
            except Exception as exc:
                attempt.reasons.append(f"cleanup_failed:{task_name}:{type(exc).__name__}")
        if control_required:
            attempt.reasons.append("browser_control_required")
            attempt.error = "Browser control requires explicit user confirmation; stop and ask the user."
            attempt.verdict = Verdict.UNKNOWN.value
            html = ""
        attempt.elapsed_s = round(time.time() - started, 3)
    return attempt, html


def run_playwright_fallback(
    url: str,
    *,
    profile_id: str,
    success_selectors: Optional[list[str]] = None,
    device_class: str = "auto",
    timeout: int = 90,
    profile_dir: Optional[str] = None,
    force_executor: Optional[str] = None,
) -> tuple[Attempt, str]:
    """Invoke the appropriate browser executor, including public CAPTCHA clearance."""
    profile = load_profile(profile_id)
    capabilities = profile.get("capabilities_needed") or []
    choice = force_executor or _pick_executor(capabilities, device_class)

    t0 = time.time()
    att = Attempt(
        phase="fallback",
        executor=choice,
        url=url,
        url_transform="original",
        impersonate=None,
        referer="",
    )

    if choice == "ego_browser":
        return run_ego_browser_fallback(
            url,
            profile_id=profile_id,
            success_selectors=success_selectors,
            device_class=device_class,
            timeout=max(timeout, CHALLENGE_TIMEOUT_MS // 1000),
            max_browser_attempts=2,
            profile_dir=profile_dir,
            force_executor=choice,
        )

    if choice == "protocol_stealth_chrome":
        return _run_protocol_stealth(att, url, success_selectors=success_selectors, timeout=timeout, t0=t0)

    if choice.startswith("playwright_mcp"):
        att.error = (
            "Interactive inspection requires delegating a browser agent using the "
            "ego-browser skill after CLI capture; no MCP executor is available."
        )
        att.verdict = Verdict.UNKNOWN.value
        att.elapsed_s = round(time.time() - t0, 3)
        return att, ""

    if not _chrome_channel_available():
        att.error = "node/npx not available for local Playwright template"
        att.verdict = Verdict.UNKNOWN.value
        att.elapsed_s = round(time.time() - t0, 3)
        return att, ""

    template_map = {
        "playwright_real_chrome": "playwright_real_chrome.js",
        "playwright_mobile_chrome": "playwright_mobile_chrome.js",
    }
    template = template_map.get(choice)
    if template is None:
        att.error = f"no template for executor {choice}"
        att.verdict = Verdict.UNKNOWN.value
        att.elapsed_s = round(time.time() - t0, 3)
        return att, ""

    captcha_timeout = CHALLENGE_TIMEOUT_MS // 1000
    args: dict = {
        "url": url,
        "profileDir": profile_dir or _profile_dir_for(url, choice),
        "timeout": CHALLENGE_TIMEOUT_MS,  # clearance budget
        "clearCaptcha": True,
    }
    if choice == "playwright_mobile_chrome":
        args["device"] = "iPhone 13 Pro"
    if success_selectors:
        args["waitSelector"] = success_selectors[0]

    rc, stdout, stderr = _run_node_template(
        template, args, timeout=max(int(timeout or 0), captcha_timeout) + 30)
    att.elapsed_s = round(time.time() - t0, 3)

    if rc != 0 or not stdout:
        att.error = (stderr or "no stdout")[:300]
        att.verdict = Verdict.UNKNOWN.value
        return att, ""

    html, final_url, status, cookies, user_agent, automation, inner_text = _parse_envelope(stdout, url)
    resp = _FakeResp(html, status=status, final_url=final_url)
    vr = validate(resp, success_selectors=success_selectors)
    att.status = status
    att.body_size = len(html)
    att.verdict = vr.verdict.value
    att.reasons = list(vr.reasons) + ([f"automation:{automation}"] if automation else [])
    att.url = final_url or url
    try:
        env = json.loads(stdout.lstrip()) if stdout.lstrip()[:1] == "{" else {}
        if isinstance(env, dict):
            _attach_challenge_reasons(att, env.get("challenge") or {})
    except Exception:
        pass

    if vr.verdict in (Verdict.STRONG_OK, Verdict.WEAK_OK) and cookies:
        _bridge_cookies_to_pool(url, cookies, user_agent)
    if inner_text:
        att._inner_text = inner_text
    return att, html
