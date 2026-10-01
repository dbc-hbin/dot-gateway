# Native Go engine migration: verified scope and explicit differences

The candidate executable is `bin/insane.native-candidate`. It is built with Go 1.27.1 and `CGO_ENABLED=0`, and is statically linked. Production collector, fetch transport, policy, validators, search, extraction, state and browser queue client are native Go. No production CLI path imports or launches Python. Python remains only in the immutable source snapshot, optional development oracle regeneration, and the preserved hybrid rollback implementation.

The selected browser layer remains the separately verified headed Playwright 1.63.0/Node service with real Chromium, isolated profile and Chromium sandbox. The Go client communicates through the existing private bounded queue. No lifecycle restart or browser replacement is implied by this migration. The alternative agent-browser candidate was not selected because its safeguards did not meet this project's requirements.

## Feature matrix

| Source capability | Native implementation | Evidence or explicit difference |
|---|---|---|
| Collector seven sources, 40/4/16 limits, stable ranking | Preserved in explicit legacy profile | 1,310 multilingual policy fixtures plus full scan/dedup/state goldens from immutable Python |
| Independent body eligibility, trust-gated body suppression, title-only exception | Preserved | Golden scan cycles, Unicode fingerprints, secret redaction, 2,500-key trimming |
| JSON/HTML listing and source-specific detail parsing | Native DOM/JSON parser | Browser preformatted JSON is unwrapped; recognized empty arrays distinguished from blocked/unrecognized pages; content-free source diagnostics added. Listings resolve relative links against the observed final response URL |
| URL canonicalization, full casefold and Python regex assertions | Preserved native Go | Generated Unicode 15 tables, contextual Greek final sigma, regexp2 lookarounds, source-derived goldens |
| TLS client diversity | Real uTLS presets | Safari 16.0, iOS 14, Chrome 133, Android 11/OkHttp, Firefox 120, Edge 85; tested with genuine TLS handshakes and certificate rejection |
| curl_cffi full version inventory and browser HTTP/2 wire identity | Different | Source version variants collapse to supported uTLS families; HTTP/2 settings/header ordering are Go's, not byte-identical browser/curl impersonation. No claim of identical WAF success rates |
| HTTP/1.1 and HTTP/2, compression | Native Go | Both protocol routes supported; gzip, zlib/raw-deflate and Brotli decompression; 25 MiB output cap |
| Redirect/SSRF and credentials guard | Preserved and strengthened | All initial/redirect DNS answers checked; known private/reserved/metadata addresses blocked; public IP is pinned into direct dial or proxy CONNECT; URL/proxy userinfo rejected; TLS certificate checks never disabled |
| Exec environment DNS unavailable | Fail closed | No unverified proxy remote-DNS fallback. Non-secret diagnostics record the failed HTTP route, then the existing strictly guarded native headed browser may run |
| Cookie/session persistence | Preserved in-process | Per-host/profile cookie jars and TLS session cache; once-per-host/profile guarded root warmup; successful Chromium cookies/UA seed Chrome only. No auth/paywall/blocked cookie seeding |
| Socket pool behavior | Different | Cookies and TLS resumption persist, but HTTP connections are currently scoped to each request rather than curl_cffi's long-lived pooled sockets |
| WAF detection and diversity planner | Ported | Same source profiles embedded as static data; cookie/header/body signals, profile interleaving, mobile transforms and referers retained; unsupported version-level avoid ordering is collapsed at family boundary |
| Transient retries and budgets | Ported | Probe-only 429/502/503/504 retry, numeric Retry-After, ten-second total sleep cap, context cancellation; grid/browser limits reported honestly |
| Response validators and injection envelopes | Ported with safety fixes | 182 source-derived validator goldens and 30 exact content-safety/wrapper goldens. Native code deliberately rejects source false-positive success on HTTP 403. Login/paywall boundaries and headed terminal reasons override selectors |
| Empty JSON listing | Explicit source-only policy | Observed 2xx empty arrays are accepted only for collection listings; generic validator still returns suspect for `[]`; `{}` and error HTML do not silently count as an empty scan |
| Reddit phase 0 | Ported | Guarded public RSS then JSON routes; exact hostname matching prevents substring host confusion |
| X/Twitter phase 0 | Ported | Public tweet-result, oEmbed and profile syndication routes; no account login |
| YouTube phase 0 | Native public metadata | Reads literal public watch-page player JSON, video details/microformat/caption-track descriptors, then oEmbed fallback. Login/age/content-check and unavailable statuses stop. Does not execute page JavaScript, download media or caption content, or reproduce yt-dlp's full extractor/download capability |
| Threads phase 0 | Ported public inline JSON | Selects video-version block nearest the requested post code; returns links only, no media download |
| PDF, JSON-LD, render rescue, markdown, main content | Native Go | See NATIVE_EXTRACTION.md for HTML5/BeautifulSoup, formatting, PDF text-layer/OCR and heuristic differences |
| Public search | Native Go | Original success/data.web and result mapping/1..20 clamp retained; DDG public HTML then Bing RSS/HTML replace Python DDGS. Provider choice/ranking differs and diagnostics identify failures |
| Per-host learned routes | Persisted native JSON | 30-day TTL, 500-entry LRU default, two-strike real-failure eviction; transient/budget/auth/not-found outcomes do not strike. Atomic private files and non-blocking flock; optional learning can never wait indefinitely or fail a fetch |
| Observations history | Not migrated | No prior cookies, credentials, sessions or observation history copied. Result traces expose current operation; the old append-only observations logging hook is not recreated |
| Browser recovery | Existing verified backend | Go private queue client handles bounded requests/cancellation and verifies headed+sandbox flags. Browser helper lifecycle changes require separate authorized handling |
| Cron/classification/delivery | Still paused/inert | Imported prompt, model/provider, schedule and delivery destination preserved as data; no scheduler activation, model invocation or Discord sends |

## Bounded wide-recall extension

The CLI defaults to the intentional wide-recall extension; legacy remains available with `--profile legacy`. See [WIDE_RECALL.md](docs/WIDE_RECALL.md) for 120/12/48 bounds, public pagination, expanded lexical review clues, unchanged safety boundaries and fingerprint migration. Oracle tests still verify legacy behavior, not an asserted identity for the wider profile.

## Verification

Normal `go test ./...` uses only native Go test execution and checked-in oracle outputs. The corpus was generated from the immutable Python source, not the Go implementation. Explicit regeneration uses `INSANE_RECORD_ORACLES=1 ./tools/go.sh test ./internal/collector`; original retired bridge tests are opt-in with `-tags pythonoracle`. Python is not needed to run the normal suite or the candidate.

```sh
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/insane.native-candidate ./cmd/insane
PATH=/nonexistent INSANE_ROOT="$PWD" bin/insane.native-candidate scan \
  --fixture testdata/scan.json --dry-run --state /tmp/insane-native-empty-state.json
```

The native suite includes real local TLS tests for all six ClientHello presets, certificate failure, compression, cancellation and body limits; deterministic engine planning/retry/terminal/browser-cookie tests; learning restart/strike/TTL/cap/contention tests; source-derived policy and validator goldens; DOM/PDF/search fixtures; and private queue/lifecycle tests. No model call, cron activation or external delivery is part of the suite.

Public runtime testing must check `result.ok`, actual HTTP status, parsed counts and failure diagnostics. An empty candidates array or a zero exit code alone does not prove sources were successfully scanned. CLI fetch/search preserve structured unsuccessful-result contracts.

## Commands

```sh
bin/insane.native-candidate fetch --url https://example.com --timeout 60s --max-attempts 2
bin/insane.native-candidate search --query 'official AI student plan' --limit 5 --timeout 60s
bin/insane.native-candidate scan --dry-run --timeout 20m --operation-timeout 5m \
  --state /tmp/insane-native-live/state.json --report /tmp/insane-native-live/report.json
bin/insane.native-candidate job
```

`--python` no longer exists. Project discovery uses the inert project configuration marker. `INSANE_ROOT` can select the project; `INSANE_LEARN`, `INSANE_LEARNED_PATH`, positive `INSANE_LEARN_TTL_DAYS`, and positive `INSANE_LEARN_MAX` configure optional learned-route persistence. Dry-run scans disable learning persistence as well as collector state writes.

Cookie values, HTTP headers and browser protocol envelopes remain internal. Public results contain content, validation/extraction/safety metadata and content-free attempt diagnostics; they do not serialize cookie jars or proxy credentials.

## Verified current DCInside redirect adaptation

The configured mobile AI-utilize listing was observed on 2026-10-01 to redirect to `https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize`. The native parser now recognizes title-cell links on that exact desktop gallery. It resolves relative hrefs against the actual fetch `FinalURL`, excludes reply-only/foreign-gallery/foreign-host/malformed links, and preserves the observed `/mgallery/board/view/` path.

Only that exact validated host/path with one `id=ai_utilize` and one ASCII-numeric `no` retains its essential query identity. Tracking/page fields are removed; different post numbers remain distinct; every other URL keeps the previous canonicalization policy. This is a narrow current-source adaptation, not an invented mirror or alternate source.

`verification/dcinside-native-backend-replay.json` records an offline replay of the authorized saved response through the production native backend: 48 recognized rows and 48 distinct canonical post identities, with the 40-row scan cap. The replay made zero network requests. Regression tests use a synthetic copy of the observed HTML structure, so normal tests do not depend on a live page or saved web capture.
