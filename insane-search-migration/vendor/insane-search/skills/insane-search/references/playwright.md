# Browser fallback — ego-browser and local Playwright

> JS 렌더링 / JS 챌린지 사이트를 위한 접근. **WAF 프로파일의
> `capabilities_needed` 태그가 선택을 결정**한다. 사용자가 직접 고를 필요 없다.

## 두 Approach 요약

| Approach | 실행기 | TLS 스택 | 적합 WAF | 한계 |
|----------|--------|----------|----------|------|
| **1. ego-browser CLI, then browser agent** | engine `ego_browser` executor; browser-agent inspection only after CLI failure | ego lite task space | leftover interactive page after engine fallback | Interactive inspection is delegated, not launched from Python; wait for `interactive_browser_required` |
| **2. Local Node + `channel:'chrome'`** | `engine/templates/playwright_real_chrome.js` | 시스템 설치 실제 Chrome | Akamai Bot Manager, PerimeterX, DataDome 강화 설정 | Node + Chrome 시스템 설치 필요 |

`engine/executor.py`가 프로파일 태그를 보고 자동 라우팅하므로, 이 선택을 스킬 외부에서 의식할 필요는 없다.

## Approach 1 — ego-browser CLI, then browser-agent inspection

Phase 0/HTTP/TLS remains first. Engine Phase 3 invokes `["ego-browser", "nodejs"]` with script **stdin**, not script argv. Delegate interactive work to a browser agent using the `ego-browser` skill only when `interactive_browser_required=true` or `recommended_tool="ego-browser"` after CLI failure. This is not an MCP dispatch. Terminal 404/auth/paywall/CAPTCHA walls are not escalated.

### 기본 워크플로

The browser agent reads the ego-browser skill, then uses the preloaded helpers:

```bash
ego-browser nodejs <<'EOF'
const task = await useOrCreateTaskSpace('inspect extraction failure')
await openOrReuseTab('https://example.com', { wait: true, timeout: 20 })
cliLog({ taskId: task.id, snapshot: await snapshotText() })
cliLog(await js(String.raw`(() => ({
  html: document.documentElement.outerHTML,
  finalUrl: location.href,
  innerText: document.body?.innerText || ''
}))()`))
EOF
```

Reuse the returned task id in later rounds. Use `js` for DOM extraction and `cdp` for protocol inspection; `js` returns values directly and cannot capture Node closures. Use `cliLog` for all terminal output. Re-fetch discovered public `/api`, `/graphql`, or JSON URLs with `python3 -m engine`.

After a prior round confirms completion, the engine runs cleanup in a **separate later CLI invocation**. It checks the recorded task's agent ownership, selects that existing task by numeric id, closes its tabs with `closeTab(tab)`, and verifies that the task is absent. It does not call `completeTaskSpace(..., { keep: false })`, which can implicitly reclaim a user-owned task. Ownership/control errors stop cleanup without takeover. Delegated interactive work follows the ego-browser skill's task lifecycle and explicit user-confirmation rules.

The engine envelope preserves `html`, `finalUrl`, `status`, `automation`, `innerText`, `cookies`, `userAgent`, `challenge`, and `observations`; an unavailable navigation status is `0`, not an invented HTTP 200.

### 주의

- Stop on user-owned/takeover/inactive task-space errors and obtain explicit confirmation before reclaiming control; never create another space to evade user control.
- Manual login, CAPTCHA, or confirmation requires user handoff and explicit approval before resuming.
- If real-Chrome TLS is required, `engine/executor.py` continues to Approach 2. 수동 선택 불필요.

## Approach 2 — Local Node + Real Chrome

### 의존성 (최초 1회)

로컬 의존성은 `engine/templates/package.json`으로 관리한다 (executor가 그 디렉토리를 cwd로 실행하므로 전역 설치 대신 로컬 설치).

```bash
# Node (시스템 설치)
node -v   # v18+ 권장

# 템플릿 로컬 의존성 (patchright 최우선 + playwright-extra/stealth 폴백)
cd engine/templates && npm install

# 시스템 Chrome 바이너리 (번들 Chromium 아님)
npx patchright install chrome
```

**Patchright 우선**: 템플릿(`playwright_real_chrome.js`)은 `require('patchright')`를 최우선 시도한다. Patchright는 Playwright API 호환 drop-in 포크로, 2026년 탐지가 노리는 CDP `Runtime.enable`(콘솔-attach) 누출을 자체 패치한다 — 그래서 patchright 경로에서는 stealth 플러그인을 얹지 않는다. patchright가 없으면 playwright-extra+stealth, 그것도 없으면 plain playwright로 폴백한다(모두 `channel:'chrome'` 유지). 결과 envelope의 `automation` 필드로 어느 경로가 쓰였는지 확인할 수 있다.

### 호출 (engine 내부)

```python
from insane_search.engine.executor import run_playwright_fallback

attempt, html = run_playwright_fallback(
    "https://example.com/path",
    profile_id="akamai_bot_manager",
    success_selectors=["article"],
    device_class="desktop",   # "desktop" | "mobile" | "auto"
)
```

내부에서 `engine/templates/playwright_real_chrome.js` 또는 `playwright_mobile_chrome.js`를 Node로 실행하고 HTML을 받아온다. 템플릿은 **URL과 셀렉터 파라미터만** 받으며 사이트별 분기가 없다.

### 데스크톱 템플릿 (`playwright_real_chrome.js`)

```js
const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const ctx = await chromium.launchPersistentContext(profileDir, {
  channel: 'chrome',        // ← 핵심: 번들 Chromium 아닌 실제 Chrome
  headless: false,          // Akamai는 headless 탐지. headful 필요.
  viewport: { width: 1366, height: 900 },
});
```

### 모바일 템플릿 (`playwright_mobile_chrome.js`)

```js
const { chromium, devices } = require('playwright-extra');
const iPhone = devices['iPhone 13 Pro'];

const ctx = await chromium.launchPersistentContext(profileDir, {
  channel: 'chrome',          // TLS는 실제 Chrome
  ...iPhone,                  // UA/viewport/isMobile/hasTouch 자동 주입
  headless: false,
});
```

**주의**: `channel:'chrome'` + `devices[...]` 조합은 TLS 핑거프린트를 Chrome으로 유지하면서 HTTP 레이어(UA/viewport)만 모바일로 바꾼다. WAF가 실제 Chrome으로 인식해서 관대한 경우가 많다.

## 선택 규칙 (자동)

`engine/waf_profiles.yaml`의 `capabilities_needed` 태그가 결정한다:

| 태그 조합 | 선택 실행기 | 대표 케이스 |
|----------|-------------|-------------|
| `needs_real_tls_stack` + `needs_js_exec` | Approach 2 (real_chrome) | Akamai Bot Manager |
| `needs_js_exec` only | ego-browser CLI, then browser agent if the interactive gate requests it | Cloudflare Turnstile |
| `needs_real_tls_stack` only | Approach 2 (real_chrome) | 일부 DataDome 설정 |
| 둘 다 없음 | curl 체인에서 해결. Playwright 안 씀 | F5 BIG-IP (TLS만 대응 필요) |

`device_class="mobile"`이 지정되면 real_chrome → mobile 변종으로 swap.

## 공통 검증

두 Approach 모두 최종 HTML을 `engine/validators.py:validate()`로 재검증한다. 즉 Playwright가 HTML을 받아와도 **챌린지 페이지 또는 빈 SPA면 여전히 CHALLENGE 판정**. 자동으로 다음 조합이나 failure 보고로 이어진다.

## 디버깅 팁

- `profileDir`를 고정 경로로 두면 세션·쿠키가 유지되어 재시도 빠름 (`/tmp/.insane_pw_profile`)
- Akamai 재시도가 잦으면 `profileDir`를 삭제해 fresh 상태로 리셋
- 실패 시 `result.trace`의 `error` 필드에 Node stderr 200자가 포함됨

## 사이트 예시 (독자 이해용, 코드 분기 근거 아님)

> 이 섹션은 **설명 목적**이며 `engine/**` 코드에는 반영되지 않는다.

- **Cloudflare 기본 챌린지**: ego-browser CLI first; browser-agent inspection only if `interactive_browser_required`
- **Akamai Bot Manager**: Approach 2: real-Chrome TLS capability routing
- **SSR 블로그 플랫폼**: curl_cffi safari만으로 HTML 수신. Playwright 불필요
- **검색 결과 JS 렌더링 SPA**: ego-browser CLI, then browser-agent `openOrReuseTab` → `snapshotText` / DOM / CDP inspection

실제 라우팅은 프로파일 태그가 결정한다. 위 예시는 참고일 뿐 코드 분기 근거로 쓰지 않는다.
