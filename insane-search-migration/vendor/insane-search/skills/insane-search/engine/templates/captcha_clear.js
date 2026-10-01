/** Generic Playwright CAPTCHA/challenge clearance. No host branches. */
'use strict';

const hardMarkers = ["sec-if-cpt-container", "Powered and protected by Akamai", "Just a moment...", "cf-chl-bypass", "window._cf_chl_opt", "orchestrate/chl_page", "Attention Required! | Cloudflare", "<title>Bot Challenge</title>", "The requested URL was rejected", "Request unsuccessful. Incapsula", "Please enable JS and disable any ad blocker", "verify you are human", "verify you're human", "i am not a robot", "i'm not a robot", "human verification", "begin verification", "press & hold", "press and hold", "confirm you are a human", "checking your browser"];
const widgetSelectors = [".recaptcha-checkbox-border", ".recaptcha-checkbox", ".cb-lb", ".cf-turnstile", "#challenge-stage input", "#challenge-form input", "#sec-if-cpt-container input", "#cf-stage input", "label[for*=\"captcha\"]"];
const holdWidgetSelectors = ["#px-captcha, .px-captcha"];
const CAPTCHA_TEXT = /verify you are human|verify you'?re human|i am human|i'?m not a robot|human verification|begin verification|press (and|&) hold|confirm you are a human|로봇이 아닙니다|사람 인증|누르고 있/i;
const HOLD_TEXT = /press (and|&) hold|press & hold|confirm you are a human|누르고 있/i;
const CAPTCHA_IFRAME = /challenge|captcha|turnstile|recaptcha|hcaptcha|h-captcha|cloudflare|akamai|datadome|perimeterx|funcaptcha|arkose/i;
const POLL_INTERVAL_MS = 250;
const CHALLENGE_TIMEOUT_MS = 90000;
const HOLD_MS = 8000;
const DOM_STABLE_POLLS = 2;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const actedBoxes = new Set();
const boxKey = (box) => box
  ? [Math.round(box.x), Math.round(box.y), Math.round(box.width), Math.round(box.height)].join(':')
  : '';

async function clickPoint(page, box) {
  if (!box || !page.mouse) return false;
  const k = boxKey(box);
  if (k && actedBoxes.has(k)) return false;
  const x = box.x + Math.max(8, Math.min(box.width * 0.35, box.width / 2));
  const y = box.y + box.height / 2;
  try {
    await page.mouse.move(x, y, { steps: 8 });
    await sleep(80);
    await page.mouse.click(x, y);
    if (k) actedBoxes.add(k);
    return true;
  } catch (_) {
    return false;
  }
}

async function holdPoint(page, box, ms) {
  if (!box || !page.mouse) return false;
  const k = 'h:' + boxKey(box);
  if (k && actedBoxes.has(k)) return false;
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  try {
    await page.mouse.move(x, y, { steps: 6 });
    await page.mouse.down();
    await sleep(ms);
    await page.mouse.up();
    if (k) actedBoxes.add(k);
    return true;
  } catch (_) {
    return false;
  }
}

async function clickLocator(page, locator, requireCaptchaText = false, forceHold = false) {
  try {
    const count = await locator.count();
    let clicked = 0;
    for (let i = 0; i < Math.min(count, 12); i++) {
      const el = locator.nth(i);
      const visible = await el.isVisible({ timeout: 400 }).catch(() => false);
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
      if (isHold && await holdPoint(page, box, HOLD_MS)) {
        clicked += 1;
        continue;
      }
      if (isHold) continue;
      if (await clickPoint(page, box)) {
        clicked += 1;
        continue;
      }
      await el.click({ timeout: 3000 }).catch(() => {});
      clicked += 1;
    }
    return clicked;
  } catch (_) {
    return 0;
  }
}

async function collectFullPageWidgets(page) {
  try {
    return await page.evaluate(() => {
      const sels = ['.cf-turnstile', '.g-recaptcha', '.h-captcha', '#px-captcha', '.px-captcha', '#challenge-stage', '#cf-stage', '#sec-if-cpt-container'];
      const vw = window.innerWidth || 1;
      const vh = window.innerHeight || 1;
      const sparse = ((document.body && document.body.innerText) || '').trim().length < 400;
      const names = [];
      const vis = (el) => {
        const r = el.getBoundingClientRect();
        const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
        if (style && (style.visibility === 'hidden' || style.display === 'none')) return false;
        return r.width > 2 && r.height > 2;
      };
      for (const sel of sels) {
        for (const el of document.querySelectorAll(sel)) {
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
        }
      }
      return names;
    });
  } catch (_) {
    return [];
  }
}

async function trySolveCaptcha(page) {
  let clicked = 0;
  const frames = typeof page.frames === 'function' ? page.frames() : [];
  const targets = [page, ...frames];
  for (const target of targets) {
    if (!target || typeof target.locator !== 'function') continue;
    for (const sel of holdWidgetSelectors) {
      clicked += await clickLocator(page, target.locator(sel), false, true);
    }
    for (const sel of widgetSelectors) {
      if (sel.includes('px-captcha')) continue;
      clicked += await clickLocator(page, target.locator(sel));
    }
    try {
      const buttons = target.locator('button, input[type="button"], a, label, div[role="button"], span[role="button"]');
      clicked += await clickLocator(page, buttons, true);
    } catch (_) {}
  }
  try {
    const iframes = page.locator('iframe');
    const n = Math.min(await iframes.count(), 10);
    for (let i = 0; i < n; i++) {
      const iframe = iframes.nth(i);
      const hay = [
        await iframe.getAttribute('src').catch(() => ''),
        await iframe.getAttribute('title').catch(() => ''),
        await iframe.getAttribute('name').catch(() => '')
      ].filter(Boolean).join(' ');
      if (!CAPTCHA_IFRAME.test(hay)) continue;
      const box = await iframe.boundingBox().catch(() => null);
      if (await clickPoint(page, box)) clicked += 1;
    }
  } catch (_) {}
  return clicked;
}

function markerHits(html, title, body) {
  const lower = [html, title, body].map((text) => String(text || '').toLowerCase());
  return hardMarkers.filter((marker) => {
    const needle = String(marker).toLowerCase();
    return lower.some((text) => text.includes(needle));
  });
}


async function readState(page) {
  let value = { html: '', title: '', body: '' };
  try {
    value = await page.evaluate(() => ({
      html: document.documentElement?.outerHTML || '',
      title: document.title || '',
      body: document.body?.innerText || ''
    }));
  } catch (_) {}
  const html = String(value?.html || '');
  const title = String(value?.title || '');
  const body = String(value?.body || '');
  const hits = markerHits(html, title, body);
  const live = await collectFullPageWidgets(page);
  for (const name of live || []) {
    if (!hits.includes(name)) hits.push(name);
  }
  const fingerprint = [title, body, html.length, hits.join(',')].join('\u0000');
  return { html, title, body, hits, fingerprint };
}

async function waitForChallengeToClear(page, state, timeoutMs) {
  const budget = Math.max(1000, timeoutMs || CHALLENGE_TIMEOUT_MS);
  if (state.hits.length === 0) {
    return { state, waitedMs: 0, resolved: true, domStable: false, captchaClicks: 0, polls: 0 };
  }
  const started = Date.now();
  const deadline = started + budget;
  let previous = state.fingerprint;
  let stable = 0;
  let polls = 0;
  let resolved = false;
  let captchaClicks = 0;
  let lastClick = 0;
  while (Date.now() < deadline) {
    if (Date.now() - lastClick >= 2500) {
      const n = await trySolveCaptcha(page);
      if (n) captchaClicks += n;
      lastClick = Date.now();
    }
    await sleep(POLL_INTERVAL_MS);
    state = await readState(page);
    polls += 1;
    if (state.hits.length) {
      previous = state.fingerprint;
      stable = 0;
      continue;
    }
    resolved = true;
    if (state.fingerprint === previous) stable += 1;
    else stable = 1;
    previous = state.fingerprint;
    if (stable >= DOM_STABLE_POLLS) break;
  }
  return {
    state,
    waitedMs: Date.now() - started,
    resolved,
    domStable: resolved && stable >= DOM_STABLE_POLLS,
    polls,
    captchaClicks
  };
}

async function lastDocumentStatus(page) {
  // PerformanceNavigationTiming.responseStatus is the current main-document
  // HTTP status after openTab/goto. Missing/0 means unobserved — never invent 200.
  try {
    const fromPerf = await page.evaluate(() => {
      const entries = (performance && performance.getEntriesByType)
        ? performance.getEntriesByType('navigation') : [];
      const nav = entries && entries[0];
      const status = nav && nav.responseStatus;
      return (typeof status === 'number' && status > 0) ? status : 0;
    });
    if (fromPerf) return fromPerf;
  } catch (_) {}
  return 0;
}

async function readCookies(page) {
  try {
    const ctx = typeof page.context === 'function' ? page.context() : page.context;
    if (ctx && typeof ctx.cookies === 'function') return await ctx.cookies();
  } catch (_) {}
  return [];
}

async function clearPublicChallenge(page, opts = {}) {
  const timeoutMs = Math.min(opts.timeoutMs || CHALLENGE_TIMEOUT_MS, CHALLENGE_TIMEOUT_MS);
  const deadline = Date.now() + timeoutMs;
  const remaining = () => Math.max(0, deadline - Date.now());
  const waitSelector = opts.waitSelector || null;
  if (waitSelector && typeof page.waitForSelector === 'function' && remaining() > 0) {
    try { await page.waitForSelector(waitSelector, { timeout: Math.min(15000, remaining()) }); } catch (_) {}
  }
  actedBoxes.clear();
  let captchaClicks = await trySolveCaptcha(page);
  await sleep(Math.min(800, remaining()));
  let state = await readState(page);
  const initialMarkers = state.hits.slice();
  let challengeWaitMs = 0;
  let challengeResolved = initialMarkers.length === 0;
  let domStable = false;
  let challengePolls = 0;
  if (initialMarkers.length && remaining() > 0) {
    const waited = await waitForChallengeToClear(page, state, remaining());
    state = waited.state;
    challengeWaitMs += waited.waitedMs;
    challengeResolved = waited.resolved;
    domStable = waited.domStable;
    challengePolls += waited.polls || 0;
    captchaClicks += waited.captchaClicks || 0;
  }
  state = await readState(page);
  if (state.hits.length && remaining() > 1000) {
    try { await page.reload({ waitUntil: 'domcontentloaded', timeout: Math.min(20000, remaining()) }); } catch (_) {}
    actedBoxes.clear();
    captchaClicks += await trySolveCaptcha(page);
    const waited = await waitForChallengeToClear(page, await readState(page), remaining());
    state = waited.state;
    challengeWaitMs += waited.waitedMs;
    challengeResolved = waited.resolved;
    domStable = waited.domStable;
    challengePolls += waited.polls || 0;
    captchaClicks += waited.captchaClicks || 0;
  }
  const finalState = await readState(page);
  return {
    initialMarkers,
    finalMarkers: finalState.hits,
    resolved: challengeResolved && finalState.hits.length === 0,
    waitedMs: challengeWaitMs,
    polls: challengePolls,
    captchaClicks,
    domStable
  };
}

module.exports = { clearPublicChallenge, trySolveCaptcha };

