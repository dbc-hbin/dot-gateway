# Linux headed browser adapter

This replaces the Mac-only real-browser backend. The current fetching,
validation, extraction, private queue client and lifecycle launcher are Go;
the browser layer is the verified Node/Playwright helper. Python is not a
production dependency. The vendor snapshot and optional historical Python
oracle artifacts remain unchanged. See `NATIVE_ENGINE_PARITY.md` at the project
root for the current Go engine's verified behavior and known differences.

## Runtime

- Official npm `playwright` **1.63.0**, pinned in package-lock.json
- System `/usr/bin/chromium`, tested **151.0.7922.173**
- `headless: false`, `chromiumSandbox: true`, normal certificate verification
- A separately launched, owned browser and ephemeral context; never a user
  browser/profile or an undisclosed debugging endpoint
- Passive capture, matching the current Mac ego capture path. No automatic
  CAPTCHA clicks, login submissions, payment or paywall bypass. Challenge
  results recommend reopening a public page for interactive inspection
- Desktop and the original iPhone 13 Pro mobile context remain distinct
- Public cookies are returned only inside the internal browser envelope and
  bridged into the Go engine's HTTP cookie jar after successful validation.
  The native helper keeps a bounded in-memory per-origin/device public cookie
  jar for retries (15-minute TTL, at most 64 entries). Authentication/paywall
  results discard cookies. Cookies are not written to user-facing diagnostics

## Setup (current Go client and launcher)

The core runtime and private queue client/launcher are Go. Node/Playwright is
retained for the verified browser layer. Python is not required for this path;
older Python files and the .venv are reference/test artifacts, not launcher
requirements.

From the project root:

    npm ci --prefix runtime/browser --ignore-scripts
    go build -mod=readonly -buildvcs=false -o bin/headed-broker ./cmd/headed-broker

Start the following in a terminal in the **real graphical Linux desktop**:

    bin/headed-broker start --root /absolute/project

For this VM's already configured credential-free native proxy:

    bin/headed-broker start --root /opt/assistant-project/insane-search-migration --proxy-config /opt/assistant-project/gateway_native_proxy_config.json

The Go launcher detaches Node, verifies PID, boot ID, start ticks and a fresh
heartbeat, and returns. It does not register boot autostart. Its private
advisory startup lock serializes stale-lock recovery and readiness. The public
fetch client renews a per-request lease and cancels by removing only its own
UUID files. The helper is not a cron job and never posts to Discord or invokes
a model. Without a real display, start and restart fail before changing the
working helper. DNS failures in the caller are validated independently by the
native browser worker; HTTP paths remain fail-closed.

Status and lifecycle commands:

    bin/headed-broker status
    bin/headed-broker stop --timeout 30s
    bin/headed-broker restart --root /absolute/project --proxy-config /path/to/existing-proxy.json

Use restart only within the user's authorized deployment scope. An existing
helper is left unchanged until a controlled restart is authorized. A known-dead
native process can be reconciled for restart; PID reuse or mismatched identity
fails closed. Direct PID signalling is not used across execution namespaces.

The exec sandbox has no desktop sockets. It sends requests to this explicitly
launched native worker using the private runtime/browser/.queue directory.
`INSANE_BROWSER_QUEUE` can select another private, same-owner queue. No hidden
CDP port is discovered. Official Playwright creates a CDP session only for its
own freshly launched page to stop unsafe redirect responses before Chromium
follows them (ordinary Playwright route handlers cover only the first URL).


## IPC and boundaries

- Request: UUID-named JSON with `{id, expiresAt, args: {url, timeout, device,
  waitSelector}}`; maximum 32 KiB, at most 180 seconds of capture time
- Response: same UUID and `{ok, envelope}` or `{ok:false, error}`
- Envelope retains `html`, `finalUrl`, actual HTTP `status` (0 if unobserved),
  `cookies`, `userAgent`, `automation`, `innerText`, `challenge`, and
  `observations`. It adds terminal-boundary and headed/sandbox diagnostics
- Private queue ownership/mode are checked. Output files are mode 0600.
  A client lease renewed during polling lets the helper cancel a disappeared
  caller within ten seconds. Expired or unleased work is rejected; late
  responses are reaped. Navigation has an overall deadline; cleanup is bounded
- Only HTTP(S) URLs without userinfo are accepted. IP literals and resolved
  private/loopback/link-local/reserved destinations are denied by default.
  Each initial request and main-page redirect response is checked. If the caller
  sandbox has no DNS, the browser adapter delegates unresolved-host validation
  to this native worker; the worker still fails closed on unresolved or private
  answers before navigation. HTTP paths do not skip their own DNS check. Child
  document navigations, popups, service workers and WebSockets are blocked
- The native verification program alone grants an exact ephemeral local
  fixture origin; queue requests cannot turn on local/private access
- Login/password forms and detected paywall text produce `auth_required`
  even when the server responded 200. HTTP status is not fabricated
- HTML/visible text are bounded and remain untrusted web content. The existing
  engine's prompt-injection metadata and untrusted wrapper are preserved

These URL checks are application defenses, not a replacement for network-level
isolation or a guarantee against all DNS rebinding races. No browser/OS network
or security policy is changed by this adapter.

## Verification

    go test -mod=readonly -race ./internal/headed ./cmd/headed-broker
    node --test runtime/browser/test_adapter.cjs

Run `node runtime/browser/native_verify.cjs` in the native terminal for real
headed tests. `INSANE_BROWSER_PROXY_CONFIG` may be required for its final
public page request. The suite covers rendered JS, public-cookie reuse,
login/paywall stops, a hung renderer followed by a new successful capture,
redirect-to-private and private WebSocket denial, and public page fetching.
The checked public success fixture is `https://example.com/`; the VM gateway
blocked the Playwright documentation site, as recorded in
`verification/PUBLIC_NETWORK_NOTE.md`.

`native-adapter-page.png` and `native-smoke.png` are **page captures**, not proof
of desktop visibility. Real desktop visibility was separately verified through
CUA screenshots showing the Chromium tab/address bar and the dedicated window
`insane-search headed adapter verification - Chromium` (window 37748740).
`native-verification.json` records machine-checked results without cookies.

Historical Python adapter and upstream engine tests are optional comparison
oracles, not prerequisites for installation, launch or current Go verification.
If the reference overlay and its optional environment are present, the adapter
oracle can run with `.venv/bin/python -m pytest -q tests/test_headed_browser.py`.
Original engine tests are separate scripts, not one uniform pytest suite;
run each with `PYTHONPATH=runtime/insane-search .venv/bin/python <test_script>`.
`test_u5` executes and exits during pytest import. The prior recorded results are
in `verification/upstream-tests.json` and logs. Release archives may omit these
Python reference files and the `.venv` entirely.

Official references:
- https://playwright.dev/docs/api/class-browsertype#browser-type-launch
- https://playwright.dev/docs/api/class-browsercontext#browser-context-route
- https://playwright.dev/docs/api/class-browsercontext#browser-context-route-web-socket
- https://chromedevtools.github.io/devtools-protocol/tot/Fetch/

## Go verification

    go test -mod=readonly -race ./internal/headed ./cmd/headed-broker
    bin/headed-broker verify --url https://example.com/ --timeout 45s

Sixteen deterministic Go tests cover queue cancellation and exact cleanup,
proof/identity checks, unsafe URL rejection before IPC, stale service status,
startup exclusion, display refusal, stop identity and dead/reused-PID restart
handling. The direct Go client to the existing native Playwright helper returned
HTTP 200 and visible public text (verification/go-client-playwright.json).
