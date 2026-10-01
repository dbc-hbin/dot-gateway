"""Generic public-page challenge / CAPTCHA clearance.

Browser fallbacks (Playwright templates, nodriver, patchright) call
this to click public verify widgets, hold press-and-hold gates, and wait
for JS interstitials to drop. No host branches, no login/paywall fills, no
third-party CAPTCHA farms — clearance comes from a real browser session.

Trusted holds go through Playwright `page.mouse` or nodriver CDP
`Input.dispatchMouseEvent`. In-page `dispatchEvent` is never used for holds
(those events have isTrusted=false).
"""
from __future__ import annotations

import asyncio
import inspect
import json
import re
import time
from typing import Any, Optional

from .validators import HARD_CHALLENGE_MARKERS


# Click targets. Press-and-hold widgets live only in HOLD_WIDGET_SELECTORS
# so a PX node is not clicked and then held again.
WIDGET_SELECTORS: list[str] = [
    ".recaptcha-checkbox-border",
    ".recaptcha-checkbox",
    ".cb-lb",
    ".cf-turnstile",
    "#challenge-stage input",
    "#challenge-form input",
    "#sec-if-cpt-container input",
    "#cf-stage input",
    'label[for*="captcha"]',
]

# One CSS group so Playwright/sync locators return unique nodes.
HOLD_WIDGET_SELECTORS: list[str] = [
    "#px-captcha, .px-captcha",
]

WIDGET_POLL_MARKERS: list[str] = [
    "px-captcha",
    "cf-turnstile",
    "cf-challenge",
    "h-captcha",
    "hcaptcha",
    "g-recaptcha",
    "arkose",
    "funcaptcha",
]

INTERSTITIAL_PHRASES: list[str] = [
    "verify you are human",
    "verify you're human",
    "i am not a robot",
    "i'm not a robot",
    "human verification",
    "begin verification",
    "press & hold",
    "press and hold",
    "confirm you are a human",
    "checking your browser",
]

FULLPAGE_CONTAINER_TOKENS: list[str] = [
    'id="challenge-stage"',
    "id='challenge-stage'",
    'id="cf-stage"',
    "id='cf-stage'",
    "sec-if-cpt-container",
    "cf-challenge-running",
    'id="px-captcha"',
    "id='px-captcha'",
]

CAPTCHA_TEXT_RE = (
    r"verify you are human|verify you'?re human|i am human|i'?m not a robot|"
    r"human verification|begin verification|press (and|&) hold|"
    r"confirm you are a human|로봇이 아닙니다|사람 인증|누르고 있"
)
HOLD_TEXT_RE = (
    r"press (and|&) hold|press & hold|confirm you are a human|누르고 있"
)
IFRAME_RE = (
    r"challenge|captcha|turnstile|recaptcha|hcaptcha|h-captcha|"
    r"cloudflare|akamai|datadome|perimeterx|funcaptcha|arkose"
)

POLL_INTERVAL_MS = 250
CHALLENGE_TIMEOUT_MS = 90_000
HOLD_MS = 8_000
DOM_STABLE_POLLS = 2
SPARSE_BODY_CHARS = 400


def state_markers() -> list[str]:
    """HARD + interstitial copy. Widget source tokens are not included."""
    return list(HARD_CHALLENGE_MARKERS) + list(INTERSTITIAL_PHRASES)


def widget_markers() -> list[str]:
    return list(WIDGET_POLL_MARKERS)


def challenge_markers() -> list[str]:
    return state_markers()


def detect_markers(html: str, *, markers: Optional[list[str]] = None) -> list[str]:
    """Return challenge-copy / HARD markers present in *html*."""
    lowered = (html or "").lower()
    found: list[str] = []
    for marker in markers or state_markers():
        needle = str(marker).lower()
        if needle and needle in lowered:
            found.append(marker)
    return found


def infer_fullpage_widgets(html: str) -> list[str]:
    """Distinguish a full-page challenge widget from an inert comment-form one.

    HTML-only: a widget plus a sparse body (or a challenge-stage container)
    is unresolved. A long article with a textarea + recaptcha is not.
    """
    lowered = (html or "").lower()
    found: list[str] = []
    for tok in FULLPAGE_CONTAINER_TOKENS:
        if tok in lowered:
            found.append(tok)
    widgets = [w for w in WIDGET_POLL_MARKERS if w in lowered]
    if not widgets:
        return found
    visible = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html or "")
    visible = re.sub(r"(?s)<[^>]+>", " ", visible)
    visible = re.sub(r"\s+", " ", visible).strip()
    sparse = len(visible) < SPARSE_BODY_CHARS
    has_textarea = "<textarea" in lowered
    if has_textarea:
        return found
    if sparse:
        found.extend(widgets)
        return found
    if "<article" not in lowered and (
        "cf-turnstile" in widgets or "px-captcha" in widgets or "cf-challenge" in widgets
    ):
        found.extend(w for w in widgets if w in ("cf-turnstile", "px-captcha", "cf-challenge"))
    return found


def challenge_state(html: str, *, extra: Optional[list[str]] = None) -> list[str]:
    """Wait-loop state: HARD/phrases + full-page widgets + optional live extras."""
    hits = detect_markers(html)
    for item in infer_fullpage_widgets(html):
        if item not in hits:
            hits.append(item)
    for item in extra or []:
        if item and item not in hits:
            hits.append(item)
    return hits


# Collects targets; does not hold. Holds must be trusted Input / page.mouse.
PAGE_SOLVE_JS = r"""
(async () => {
  const TEXT = /verify you are human|verify you'?re human|i am human|i'?m not a robot|human verification|begin verification|로봇이 아닙니다|사람 인증/i;
  const HOLD = /press (and|&) hold|press & hold|confirm you are a human|누르고 있/i;
  const IFRAME = /challenge|captcha|turnstile|recaptcha|hcaptcha|h-captcha|cloudflare|akamai|datadome|perimeterx|funcaptcha|arkose/i;
  const CTX = /captcha|challenge|turnstile|recaptcha|hcaptcha|h-captcha|px-captcha|not a robot|human verification|sec-if-cpt/i;
  const SPECIFIC = [
    '.recaptcha-checkbox-border',
    '.recaptcha-checkbox',
    '.cb-lb',
    '.cf-turnstile',
    '#challenge-stage input',
    '#challenge-form input',
    '#sec-if-cpt-container input',
    '#cf-stage input',
    'label[for*="captcha"]'
  ];
  const HOLD_SELS = ['#px-captcha', '.px-captcha'];
  const visible = (el) => {
    if (!el || !el.getBoundingClientRect) return false;
    const r = el.getBoundingClientRect();
    const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (style && (style.visibility === 'hidden' || style.display === 'none')) return false;
    return r.width > 2 && r.height > 2;
  };
  const captchaContext = (el) => {
    let n = el;
    for (let i = 0; i < 6 && n; i++) {
      const hay = [
        n.id,
        n.className,
        n.getAttribute && n.getAttribute('name'),
        n.getAttribute && n.getAttribute('aria-label'),
        n.getAttribute && n.getAttribute('title')
      ].filter(Boolean).join(' ');
      if (CTX.test(String(hay))) return true;
      n = n.parentElement;
    }
    return false;
  };
  const boxOf = (el) => {
    const r = el.getBoundingClientRect();
    return {
      x: r.x + Math.max(8, Math.min(r.width * 0.35, r.width / 2)),
      y: r.y + r.height / 2,
      width: r.width,
      height: r.height
    };
  };
  const keyOf = (box) => [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)].join(':');
  const seen = new Set();
  const holdTargets = [];
  const clickTargets = [];
  const pushHold = (el) => {
    if (!visible(el)) return;
    const box = boxOf(el);
    const k = keyOf(box);
    if (seen.has(k)) return;
    seen.add(k);
    holdTargets.push(box);
  };
  const pushClick = (el) => {
    if (!visible(el)) return;
    const box = boxOf(el);
    const k = 'c:' + keyOf(box);
    if (seen.has(k)) return;
    seen.add(k);
    clickTargets.push(box);
  };
  for (const sel of HOLD_SELS) {
    for (const el of document.querySelectorAll(sel)) pushHold(el);
  }
  for (const sel of SPECIFIC) {
    for (const el of document.querySelectorAll(sel)) {
      const label = ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || ''));
      if (HOLD.test(label)) pushHold(el);
      else pushClick(el);
    }
  }
  for (const el of document.querySelectorAll('input[type="checkbox"], [role="checkbox"]')) {
    if (!captchaContext(el)) continue;
    pushClick(el);
  }
  for (const el of document.querySelectorAll('button, input[type="button"], a, label, div[role="button"], span[role="button"]')) {
    if (!visible(el)) continue;
    const label = [el.innerText, el.getAttribute('aria-label'), el.getAttribute('title'), el.value]
      .filter(Boolean).join(' ');
    if (HOLD.test(label)) pushHold(el);
    else if (TEXT.test(label)) pushClick(el);
  }
  for (const iframe of document.querySelectorAll('iframe')) {
    const hay = [iframe.getAttribute('src'), iframe.getAttribute('title'), iframe.getAttribute('name')]
      .filter(Boolean).join(' ');
    if (!IFRAME.test(hay)) continue;
    pushClick(iframe);
  }
  const vw = window.innerWidth || 1;
  const vh = window.innerHeight || 1;
  const bodyText = (document.body && document.body.innerText || '').trim();
  const sparse = bodyText.length < 400;
  const fullPageWidgets = [];
  const widgetSels = ['.cf-turnstile', '.g-recaptcha', '.h-captcha', '#px-captcha', '.px-captcha', '#challenge-stage', '#cf-stage', '#sec-if-cpt-container'];
  for (const sel of widgetSels) {
    for (const el of document.querySelectorAll(sel)) {
      if (!visible(el)) continue;
      const r = el.getBoundingClientRect();
      const form = el.closest && el.closest('form');
      const commentish = form && form.querySelector('textarea');
      const stage = el.closest && el.closest('#challenge-stage, #cf-stage, #cf-challenge-running, #sec-if-cpt-container, #px-captcha');
      const areaRatio = (r.width * r.height) / (vw * vh);
      const large = (r.width / vw) > 0.35 || (r.height / vh) > 0.2 || areaRatio > 0.12;
      const center = r.left < vw / 2 && r.right > vw / 2 && r.top < vh / 2 && r.bottom > vh / 2;
      if (commentish && !stage && !large && !center && !sparse) continue;
      if (!(stage || large || center || sparse)) continue;
      if (!fullPageWidgets.includes(sel)) fullPageWidgets.push(sel);
    }
  }
  return {
    clicked: clickTargets.length,
    held: 0,
    holdTargets,
    clickTargets,
    fullPageWidgets
  };
})()
""".strip()


def _marker_hits_js() -> str:
    return (
        "function markerHits(html, title, body) {\n"
        "  const lower = [html, title, body].map((text) => String(text || '').toLowerCase());\n"
        "  return hardMarkers.filter((marker) => {\n"
        "    const needle = String(marker).toLowerCase();\n"
        "    return lower.some((text) => text.includes(needle));\n"
        "  });\n"
        "}\n"
    )


def playwright_page_helpers_js(hard_markers: Optional[list[str]] = None) -> str:
    """JS helpers that operate on a Playwright `page` object."""
    markers = json.dumps(hard_markers or state_markers(), ensure_ascii=False)
    selectors = json.dumps(WIDGET_SELECTORS, ensure_ascii=False)
    hold_selectors = json.dumps(HOLD_WIDGET_SELECTORS, ensure_ascii=False)
    return f"""
const hardMarkers = {markers};
const widgetSelectors = {selectors};
const holdWidgetSelectors = {hold_selectors};
const CAPTCHA_TEXT = /{CAPTCHA_TEXT_RE}/i;
const HOLD_TEXT = /{HOLD_TEXT_RE}/i;
const CAPTCHA_IFRAME = /{IFRAME_RE}/i;
const POLL_INTERVAL_MS = {POLL_INTERVAL_MS};
const CHALLENGE_TIMEOUT_MS = {CHALLENGE_TIMEOUT_MS};
const HOLD_MS = {HOLD_MS};
const DOM_STABLE_POLLS = {DOM_STABLE_POLLS};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const actedBoxes = new Set();
const boxKey = (box) => box
  ? [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)].join(':')
  : '';

async function clickPoint(page, box) {{
  if (!box || !page.mouse) return false;
  const k = boxKey(box);
  if (k && actedBoxes.has(k)) return false;
  const x = box.x + Math.max(8, Math.min(box.width * 0.35, box.width / 2));
  const y = box.y + box.height / 2;
  try {{
    await page.mouse.move(x, y, {{ steps: 8 }});
    await sleep(80);
    await page.mouse.click(x, y);
    if (k) actedBoxes.add(k);
    return true;
  }} catch (_) {{
    return false;
  }}
}}

async function holdPoint(page, box, ms) {{
  if (!box || !page.mouse) return false;
  const k = 'h:' + boxKey(box);
  if (k && actedBoxes.has(k)) return false;
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  try {{
    await page.mouse.move(x, y, {{ steps: 6 }});
    await page.mouse.down();
    await sleep(ms);
    await page.mouse.up();
    if (k) actedBoxes.add(k);
    return true;
  }} catch (_) {{
    return false;
  }}
}}

async function clickLocator(page, locator, requireCaptchaText = false, forceHold = false) {{
  try {{
    const count = await locator.count();
    let clicked = 0;
    for (let i = 0; i < Math.min(count, 12); i++) {{
      const el = locator.nth(i);
      const visible = await el.isVisible({{ timeout: 400 }}).catch(() => false);
      if (!visible) continue;
      const box = await el.boundingBox().catch(() => null);
      const label = [
        await el.innerText().catch(() => ''),
        await el.getAttribute('aria-label').catch(() => ''),
        await el.getAttribute('title').catch(() => ''),
        await el.getAttribute('value').catch(() => '')
      ].filter(Boolean).join(' ');
      const isHold = forceHold || HOLD_TEXT.test(label);
      const isCaptcha = CAPTCHA_TEXT.test(label);
      if (requireCaptchaText && !isHold && !isCaptcha) continue;
      if (isHold && await holdPoint(page, box, HOLD_MS)) {{
        clicked += 1;
        continue;
      }}
      if (isHold) continue;
      if (await clickPoint(page, box)) {{
        clicked += 1;
        continue;
      }}
      await el.click({{ timeout: 3000 }}).catch(() => {{}});
      clicked += 1;
    }}
    return clicked;
  }} catch (_) {{
    return 0;
  }}
}}

async function collectFullPageWidgets(page) {{
  try {{
    return await page.evaluate(() => {{
      const sels = ['.cf-turnstile', '.g-recaptcha', '.h-captcha', '#px-captcha', '.px-captcha', '#challenge-stage', '#cf-stage', '#sec-if-cpt-container'];
      const vw = window.innerWidth || 1;
      const vh = window.innerHeight || 1;
      const sparse = ((document.body && document.body.innerText) || '').trim().length < 400;
      const names = [];
      const vis = (el) => {{
        const r = el.getBoundingClientRect();
        const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
        if (style && (style.visibility === 'hidden' || style.display === 'none')) return false;
        return r.width > 2 && r.height > 2;
      }};
      for (const sel of sels) {{
        for (const el of document.querySelectorAll(sel)) {{
          if (!vis(el)) continue;
          const r = el.getBoundingClientRect();
          const form = el.closest && el.closest('form');
          const commentish = form && form.querySelector('textarea');
          const stage = el.closest && el.closest('#challenge-stage, #cf-stage, #cf-challenge-running, #sec-if-cpt-container, #px-captcha');
          const areaRatio = (r.width * r.height) / (vw * vh);
          const large = (r.width / vw) > 0.35 || (r.height / vh) > 0.2 || areaRatio > 0.12;
          const center = r.left < vw / 2 && r.right > vw / 2 && r.top < vh / 2 && r.bottom > vh / 2;
          if (commentish && !stage && !large && !center && !sparse) continue;
          if (!(stage || large || center || sparse)) continue;
          if (!names.includes(sel)) names.push(sel);
        }}
      }}
      return names;
    }});
  }} catch (_) {{
    return [];
  }}
}}

async function trySolveCaptcha(page) {{
  let clicked = 0;
  const frames = typeof page.frames === 'function' ? page.frames() : [];
  const targets = [page, ...frames];
  for (const target of targets) {{
    if (!target || typeof target.locator !== 'function') continue;
    for (const sel of holdWidgetSelectors) {{
      clicked += await clickLocator(page, target.locator(sel), false, true);
    }}
    for (const sel of widgetSelectors) {{
      if (sel.includes('px-captcha')) continue;
      clicked += await clickLocator(page, target.locator(sel));
    }}
    try {{
      const buttons = target.locator('button, input[type="button"], a, label, div[role="button"], span[role="button"]');
      clicked += await clickLocator(page, buttons, true);
    }} catch (_) {{}}
  }}
  try {{
    const iframes = page.locator('iframe');
    const n = Math.min(await iframes.count(), 10);
    for (let i = 0; i < n; i++) {{
      const iframe = iframes.nth(i);
      const hay = [
        await iframe.getAttribute('src').catch(() => ''),
        await iframe.getAttribute('title').catch(() => ''),
        await iframe.getAttribute('name').catch(() => '')
      ].filter(Boolean).join(' ');
      if (!CAPTCHA_IFRAME.test(hay)) continue;
      const box = await iframe.boundingBox().catch(() => null);
      if (await clickPoint(page, box)) clicked += 1;
    }}
  }} catch (_) {{}}
  return clicked;
}}

{_marker_hits_js()}

async function readState(page) {{
  let value = {{ html: '', title: '', body: '' }};
  try {{
    value = await page.evaluate(() => ({{
      html: document.documentElement?.outerHTML || '',
      title: document.title || '',
      body: document.body?.innerText || ''
    }}));
  }} catch (_) {{}}
  const html = String(value?.html || '');
  const title = String(value?.title || '');
  const body = String(value?.body || '');
  const hits = markerHits(html, title, body);
  const live = await collectFullPageWidgets(page);
  for (const name of live || []) {{
    if (!hits.includes(name)) hits.push(name);
  }}
  const fingerprint = [title, body, html.length, hits.join(',')].join('\\u0000');
  return {{ html, title, body, hits, fingerprint }};
}}

async function waitForChallengeToClear(page, state, timeoutMs) {{
  const budget = Math.max(1000, timeoutMs || CHALLENGE_TIMEOUT_MS);
  if (state.hits.length === 0) {{
    return {{ state, waitedMs: 0, resolved: true, domStable: false, captchaClicks: 0, polls: 0 }};
  }}
  const started = Date.now();
  const deadline = started + budget;
  let previous = state.fingerprint;
  let stable = 0;
  let polls = 0;
  let resolved = false;
  let captchaClicks = 0;
  let lastClick = 0;
  while (Date.now() < deadline) {{
    if (Date.now() - lastClick >= 2500) {{
      const n = await trySolveCaptcha(page);
      if (n) captchaClicks += n;
      lastClick = Date.now();
    }}
    await sleep(POLL_INTERVAL_MS);
    state = await readState(page);
    polls += 1;
    if (state.hits.length) {{
      previous = state.fingerprint;
      stable = 0;
      continue;
    }}
    resolved = true;
    if (state.fingerprint === previous) stable += 1;
    else stable = 1;
    previous = state.fingerprint;
    if (stable >= DOM_STABLE_POLLS) break;
  }}
  return {{
    state,
    waitedMs: Date.now() - started,
    resolved,
    domStable: resolved && stable >= DOM_STABLE_POLLS,
    polls,
    captchaClicks
  }};
}}

async function lastDocumentStatus(page) {{
  // PerformanceNavigationTiming.responseStatus is the current main-document
  // HTTP status after openTab/goto. Missing/0 means unobserved — never invent 200.
  try {{
    const fromPerf = await page.evaluate(() => {{
      const entries = (performance && performance.getEntriesByType)
        ? performance.getEntriesByType('navigation') : [];
      const nav = entries && entries[0];
      const status = nav && nav.responseStatus;
      return (typeof status === 'number' && status > 0) ? status : 0;
    }});
    if (fromPerf) return fromPerf;
  }} catch (_) {{}}
  return 0;
}}

async function readCookies(page) {{
  try {{
    const ctx = typeof page.context === 'function' ? page.context() : page.context;
    if (ctx && typeof ctx.cookies === 'function') return await ctx.cookies();
  }} catch (_) {{}}
  return [];
}}

async function clearPublicChallenge(page, opts = {{}}) {{
  const timeoutMs = Math.min(opts.timeoutMs || CHALLENGE_TIMEOUT_MS, CHALLENGE_TIMEOUT_MS);
  const deadline = Date.now() + timeoutMs;
  const remaining = () => Math.max(0, deadline - Date.now());
  const waitSelector = opts.waitSelector || null;
  if (waitSelector && typeof page.waitForSelector === 'function' && remaining() > 0) {{
    try {{ await page.waitForSelector(waitSelector, {{ timeout: Math.min(15000, remaining()) }}); }} catch (_) {{}}
  }}
  actedBoxes.clear();
  let captchaClicks = await trySolveCaptcha(page);
  await sleep(Math.min(800, remaining()));
  let state = await readState(page);
  const initialMarkers = state.hits.slice();
  let challengeWaitMs = 0;
  let challengeResolved = initialMarkers.length === 0;
  let domStable = false;
  let challengePolls = 0;
  if (initialMarkers.length && remaining() > 0) {{
    const waited = await waitForChallengeToClear(page, state, remaining());
    state = waited.state;
    challengeWaitMs += waited.waitedMs;
    challengeResolved = waited.resolved;
    domStable = waited.domStable;
    challengePolls += waited.polls || 0;
    captchaClicks += waited.captchaClicks || 0;
  }}
  state = await readState(page);
  if (state.hits.length && remaining() > 1000) {{
    try {{ await page.reload({{ waitUntil: 'domcontentloaded', timeout: Math.min(20000, remaining()) }}); }} catch (_) {{}}
    actedBoxes.clear();
    captchaClicks += await trySolveCaptcha(page);
    const waited = await waitForChallengeToClear(page, await readState(page), remaining());
    state = waited.state;
    challengeWaitMs += waited.waitedMs;
    challengeResolved = waited.resolved;
    domStable = waited.domStable;
    challengePolls += waited.polls || 0;
    captchaClicks += waited.captchaClicks || 0;
  }}
  const finalState = await readState(page);
  return {{
    initialMarkers,
    finalMarkers: finalState.hits,
    resolved: challengeResolved && finalState.hits.length === 0,
    waitedMs: challengeWaitMs,
    polls: challengePolls,
    captchaClicks,
    domStable
  }};
}}

""".strip()


def playwright_module_js() -> str:
    """Node module source for `templates/captcha_clear.js`."""
    return (
        "/** Generic Playwright CAPTCHA/challenge clearance. No host branches. */\n"
        "'use strict';\n\n"
        + playwright_page_helpers_js()
        + "\n\nmodule.exports = { clearPublicChallenge, trySolveCaptcha };\n"
    )


def build_ego_browser_script(url: str, wait_selector: Optional[str], task_name: str, timeout: int) -> str:
    """Read a rendered page through preloaded ego helpers; never solve challenges."""
    return "const target = " + json.dumps(url) + ";\nconst taskName = " + json.dumps(task_name) + ";\nconst selector = " + json.dumps(wait_selector) + ";\nconst timeout = " + str(timeout) + r""";
let task;
try {
  task = await useOrCreateTaskSpace(taskName);
  await openOrReuseTab(target, { wait: true, timeout });
  if (selector) await waitForElement(selector, { timeout });
  const page = await js(String.raw`(() => ({
    html: document.documentElement?.outerHTML || '',
    finalUrl: location.href,
    status: Number(performance.getEntriesByType('navigation')[0]?.responseStatus) || 0,
    innerText: (document.body?.innerText || '').slice(0, 1000000),
    userAgent: navigator.userAgent
  }))()`);
  const { cookies } = await cdp('Network.getCookies', { urls: [page.finalUrl] });
  cliLog(JSON.stringify({ ...page, cookies, automation: 'ego_browser',
    taskId: task.id, challenge: {}, observations: [] }));
} catch (error) {
  const message = String(error?.message || error);
  const controlRequired = /user.{0,30}control|inactive|not assigned|user.owned|delegated.to.user/i.test(message);
  cliLog(JSON.stringify({ html: '', finalUrl: target, status: 0, automation: 'ego_browser',
    innerText: '', cookies: [], userAgent: null, challenge: {}, observations: [],
    taskId: task?.id, error: message, controlRequired }));
}
"""


def build_ego_browser_cleanup_script(task_name: str) -> str:
    """Dedicated final invocation; never reclaim a user-controlled space."""
    return "const taskName = " + json.dumps(task_name) + r""";
const spaces = await listTaskSpaces();
const task = spaces.find(space => space.name === taskName || space.taskId === taskName);
if (!task) {
  cliLog(JSON.stringify({ done: true, absent: true }));
} else if (task.ownership !== 'agent') {
  cliLog(JSON.stringify({ done: false, controlRequired: true }));
} else {
  await useOrCreateTaskSpace(task.id);
  // closeTab uses normal task control checks; completeTaskSpace(keep:false)
  // would reclaim a space if the user took it between selection and cleanup.
  for (const tab of await listTabs()) await closeTab(tab);
  // Tab closure can be acknowledged before asynchronous task-space removal.
  // Poll only observation state; never retry a close or reclaim control.
  const deadline = Date.now() + 2000;
  let remaining = await listTaskSpaces();
  while (remaining.some(space => space.id === task.id) && Date.now() < deadline) {
    const current = remaining.find(space => space.id === task.id);
    if (current.ownership !== 'agent') break;
    await wait(0.1);
    remaining = await listTaskSpaces();
  }
  const current = remaining.find(space => space.id === task.id);
  cliLog(JSON.stringify({ done: !current,
    controlRequired: Boolean(current && current.ownership !== 'agent') }));
}
"""


def _eval_result_to_dict(result: Any) -> dict:
    if inspect.isawaitable(result):
        return {}
    if isinstance(result, dict):
        return dict(result)
    return {}


def _eval_solve(page: Any) -> dict:
    try:
        result = page.evaluate(PAGE_SOLVE_JS)
        return _eval_result_to_dict(result)
    except Exception:
        return {}


def _sync_html(page: Any) -> str:
    try:
        return str(page.content() or "")
    except Exception:
        try:
            return str(page.evaluate("() => document.documentElement?.outerHTML || ''") or "")
        except Exception:
            return ""


def _box_key(box: Optional[dict], prefix: str = "") -> str:
    if not box:
        return ""
    return (
        f"{prefix}{round(float(box.get('x', 0)))}:"
        f"{round(float(box.get('y', 0)))}:"
        f"{round(float(box.get('width', 0)))}:"
        f"{round(float(box.get('height', 0)))}"
    )


def _sync_mouse_hold(page: Any, box: Optional[dict], ms: int = HOLD_MS) -> bool:
    mouse = getattr(page, "mouse", None)
    if not box or not mouse:
        return False
    try:
        x = float(box.get("x", 0)) + float(box.get("width", 0)) / 2
        y = float(box.get("y", 0)) + float(box.get("height", 0)) / 2
        if hasattr(mouse, "move"):
            mouse.move(x, y)
        mouse.down()
        try:
            page.wait_for_timeout(ms)
        except Exception:
            time.sleep(ms / 1000)
        mouse.up()
        return True
    except Exception:
        return False


def _sync_click_widgets(page: Any) -> dict:
    clicks = 0
    held = 0
    seen: set[str] = set()
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]
    if not frames:
        frames = [page]
    hold_re = re.compile(HOLD_TEXT_RE, re.I)

    def take(box: Optional[dict], prefix: str) -> bool:
        k = _box_key(box, prefix)
        if not k or k in seen:
            return False
        seen.add(k)
        return True

    for frame in frames:
        for sel in HOLD_WIDGET_SELECTORS:
            try:
                loc = frame.locator(sel)
                count = min(int(loc.count()), 8)
            except Exception:
                continue
            for i in range(count):
                el = loc.nth(i)
                try:
                    if not el.is_visible():
                        continue
                    box = el.bounding_box()
                    if not take(box, "h:"):
                        continue
                    if _sync_mouse_hold(page, box):
                        held += 1
                except Exception:
                    continue
        for sel in WIDGET_SELECTORS:
            if "px-captcha" in sel:
                continue
            try:
                loc = frame.locator(sel)
                count = min(int(loc.count()), 8)
            except Exception:
                continue
            for i in range(count):
                el = loc.nth(i)
                try:
                    if not el.is_visible():
                        continue
                    label = " ".join(filter(None, [
                        (el.inner_text() if hasattr(el, "inner_text") else "") or "",
                        el.get_attribute("aria-label") or "",
                        el.get_attribute("title") or "",
                    ]))
                    box = el.bounding_box()
                    if hold_re.search(label):
                        if take(box, "h:") and _sync_mouse_hold(page, box):
                            held += 1
                        continue
                    if not take(box, "c:"):
                        continue
                    el.click(timeout=2000)
                    clicks += 1
                except Exception:
                    continue
        try:
            buttons = frame.locator(
                'button, input[type="button"], a, label, div[role="button"], span[role="button"]'
            )
            n = min(int(buttons.count()), 12)
        except Exception:
            n = 0
        for i in range(n):
            el = buttons.nth(i)
            try:
                if not el.is_visible():
                    continue
                label = " ".join(filter(None, [
                    (el.inner_text() if hasattr(el, "inner_text") else "") or "",
                    el.get_attribute("aria-label") or "",
                    el.get_attribute("title") or "",
                    el.get_attribute("value") or "",
                ]))
                if not hold_re.search(label):
                    continue
                box = el.bounding_box()
                if take(box, "h:") and _sync_mouse_hold(page, box):
                    held += 1
            except Exception:
                continue
        try:
            iframes = frame.locator("iframe")
            n = min(int(iframes.count()), 8)
        except Exception:
            continue
        for i in range(n):
            iframe = iframes.nth(i)
            try:
                hay = " ".join(filter(None, [
                    iframe.get_attribute("src") or "",
                    iframe.get_attribute("title") or "",
                    iframe.get_attribute("name") or "",
                ]))
            except Exception:
                hay = ""
            if not re.search(IFRAME_RE, hay, re.I):
                continue
            try:
                box = iframe.bounding_box()
                if not take(box, "c:"):
                    continue
                if box and page.mouse:
                    page.mouse.click(box["x"] + box["width"] * 0.35, box["y"] + box["height"] / 2)
                    clicks += 1
            except Exception:
                continue
    return {"clicked": clicks, "held": held}


def clear_playwright_sync(page: Any, *, timeout_ms: int = CHALLENGE_TIMEOUT_MS) -> dict:
    """Drive a sync Playwright/Patchright page through public CAPTCHA widgets."""
    started = time.time()
    timeout_ms = min(int(timeout_ms), CHALLENGE_TIMEOUT_MS)
    deadline = started + max(1.0, timeout_ms / 1000)
    clicks = 0
    held = 0
    reloads = 0
    initial = challenge_state(_sync_html(page))

    def remaining_ms() -> int:
        return max(0, int((deadline - time.time()) * 1000))

    while time.time() < deadline:
        extra = _sync_click_widgets(page)
        clicks += int(extra.get("clicked") or 0)
        held += int(extra.get("held") or 0)
        last_hits = challenge_state(_sync_html(page))
        if not last_hits:
            break
        if reloads == 0 and time.time() - started > 8:
            try:
                page.reload(wait_until="domcontentloaded", timeout=min(20_000, remaining_ms() or 20_000))
                reloads += 1
            except Exception:
                pass
        try:
            page.wait_for_timeout(min(POLL_INTERVAL_MS * 4, remaining_ms() or POLL_INTERVAL_MS))
        except Exception:
            time.sleep(POLL_INTERVAL_MS / 1000)
    final = challenge_state(_sync_html(page))
    return {
        "initialMarkers": initial,
        "finalMarkers": final,
        "resolved": not final,
        "waitedMs": int((time.time() - started) * 1000),
        "captchaClicks": clicks,
        "held": held,
        "reloads": reloads,
    }


async def _nodriver_eval_solve(tab: Any) -> dict:
    """Run PAGE_SOLVE_JS as an expression that nodriver actually awaits."""
    try:
        result = await tab.evaluate(
            PAGE_SOLVE_JS, await_promise=True, return_by_value=True
        )
    except TypeError:
        try:
            result = await tab.evaluate(PAGE_SOLVE_JS)
        except Exception:
            return {}
    except Exception:
        return {}
    if isinstance(result, dict):
        return dict(result)
    return {}


async def _nodriver_cdp_mouse(tab: Any, event_type: str, x: float, y: float, *, buttons: int = 1) -> bool:
    """Trusted mouse event via CDP Input.dispatchMouseEvent (isTrusted=true)."""
    send = getattr(tab, "send", None)
    if send is None:
        return False
    try:
        from nodriver import cdp
        button = cdp.input_.MouseButton("left")
        await send(cdp.input_.dispatch_mouse_event(
            event_type,
            x=float(x),
            y=float(y),
            button=button,
            buttons=buttons,
            click_count=1,
        ))
        return True
    except Exception:
        pass
    params = {
        "type": event_type,
        "x": float(x),
        "y": float(y),
        "button": "left",
        "buttons": buttons,
        "clickCount": 1,
    }
    for args in (
        ("Input.dispatchMouseEvent", params),
        ({"method": "Input.dispatchMouseEvent", "params": params},),
    ):
        try:
            await send(*args)
            return True
        except TypeError:
            continue
        except Exception:
            continue
    return False


async def _nodriver_trusted_hold(tab: Any, x: float, y: float, ms: int = HOLD_MS) -> bool:
    move = getattr(tab, "mouse_move", None)
    try:
        if move:
            await move(x, y)
    except Exception:
        await _nodriver_cdp_mouse(tab, "mouseMoved", x, y, buttons=0)
    pressed = await _nodriver_cdp_mouse(tab, "mousePressed", x, y, buttons=1)
    if not pressed:
        return False
    try:
        sleeper = getattr(tab, "sleep", None)
        if sleeper:
            await sleeper(max(ms, 1) / 1000)
        else:
            await asyncio.sleep(max(ms, 1) / 1000)
    except Exception:
        await asyncio.sleep(max(ms, 1) / 1000)
    await _nodriver_cdp_mouse(tab, "mouseReleased", x, y, buttons=0)
    return True


async def _nodriver_trusted_click(tab: Any, x: float, y: float) -> bool:
    click = getattr(tab, "mouse_click", None)
    if click:
        try:
            await click(x, y)
            return True
        except Exception:
            pass
    await _nodriver_cdp_mouse(tab, "mouseMoved", x, y, buttons=0)
    if not await _nodriver_cdp_mouse(tab, "mousePressed", x, y, buttons=1):
        return False
    try:
        await asyncio.sleep(0.05)
    except Exception:
        pass
    await _nodriver_cdp_mouse(tab, "mouseReleased", x, y, buttons=0)
    return True


async def clear_nodriver(tab: Any, *, timeout_ms: int = CHALLENGE_TIMEOUT_MS) -> dict:
    """Drive a nodriver tab: collect targets, then trusted CDP mouse holds/clicks."""
    started = time.time()
    timeout_ms = min(int(timeout_ms), CHALLENGE_TIMEOUT_MS)
    deadline = started + max(1.0, timeout_ms / 1000)
    clicks = 0
    held = 0
    reloads = 0

    async def html() -> str:
        try:
            return str(await tab.get_content() or "")
        except Exception:
            return ""

    initial = challenge_state(await html())
    acted: set[str] = set()
    while time.time() < deadline:
        try:
            result = await _nodriver_eval_solve(tab)
        except Exception:
            result = {}
        extra = list(result.get("fullPageWidgets") or [])
        for box in result.get("holdTargets") or []:
            k = _box_key(box, "h:")
            if k in acted:
                continue
            if await _nodriver_trusted_hold(tab, float(box.get("x", 0)), float(box.get("y", 0))):
                acted.add(k)
                held += 1
        for box in result.get("clickTargets") or []:
            k = _box_key(box, "c:")
            if k in acted:
                continue
            if await _nodriver_trusted_click(tab, float(box.get("x", 0)), float(box.get("y", 0))):
                acted.add(k)
                clicks += 1
        last = challenge_state(await html(), extra=extra)
        if not last:
            break
        if reloads == 0 and time.time() - started > 8:
            try:
                await tab.reload()
                reloads += 1
                acted.clear()
            except Exception:
                pass
        try:
            await tab.sleep(1)
        except Exception:
            await asyncio.sleep(1)
    final = challenge_state(await html())
    return {
        "initialMarkers": initial,
        "finalMarkers": final,
        "resolved": not final,
        "waitedMs": int((time.time() - started) * 1000),
        "captchaClicks": clicks,
        "held": held,
        "reloads": reloads,
    }
