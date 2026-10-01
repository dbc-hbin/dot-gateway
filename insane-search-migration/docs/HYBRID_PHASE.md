# Go collector and orchestration

This is a faithful native-Go port of the promotion collector's policy and orchestration, with a deliberately retained Python fetch/extraction boundary. It is **not** a complete Go rewrite of Insane Search and does not replace ChatGPT's native web tools.

## Boundaries

Native Go owns:

- Seven listing sources, 40 scanned rows per source, global stable ranking, four selected rows per source and 16 selected rows overall
- Specific-AI relevance; question/job/editorial/key-sharing/trust/speculation/software-release/affiliate/account-sale/relay rejection; official vendor/first-party checks; concrete-benefit scoring
- Independent detail gating and the original title-only exception for qualifying gated posts, with gated bodies/excerpts never substituted
- URL canonicalization, Unicode fingerprints, new/updated detection, 2,500-entry insertion-order pruning, atomic private state, report generation and secret redaction
- CLI, process deadlines, process-group cancellation, state locking and inert imported-job configuration

Python remains responsible for the original DDGS public search/discovery method, TLS/client grid, validators, phase 0, challenge/content-safety diagnostics, URL transforms, browser escalation, markdown/main-content extraction and BeautifulSoup/source-specific listing/body parsing. `runtime/collector_bridge.py` loads the immutable original collector only for parsing/extraction helpers (or the network-disabled test oracle); its `main`, filters, rank, state and report logic are not used by production scans. `runtime/fetch_bridge.py` is a bounded JSON pass-through to the overlay engine, returning the complete fetch diagnostic result and text wrapped as untrusted web content. The Go collector report is still untrusted external data; no imported prompt is executed.

`runtime/search_bridge.py` loads the vendored provider's unchanged `search` method with an inert base-class import shim. It performs public DDGS discovery only, using `DDGS(timeout=10)` and clamping the result limit to 1..20. The original `success` + `data.web` (or `error`) contract, title/URL/description/position mapping, and failure behavior are retained. Wrapped `content` and safety `metadata` are added so result snippets remain visibly untrusted. It does not initialize a Hermes agent, a model provider, credentials or message delivery. DDGS 9.16.0 is pinned in the runtime dependency lock.

`vendor/` is the LF-normalized, line-checked source snapshot described in `SOURCE_TRANSFER.json`. It is **not** Go dependency vendoring. Never run `go mod vendor`. `tools/go.sh` sets `-mod=readonly -buildvcs=false` (this exported snapshot is not a Git checkout) and uses the supplied Go 1.27.1 toolchain. `regexp2 v1.11.5` is pinned in `go.mod`/`go.sum`.

## Build and test

The engine worker provisions `.venv`. Python differential tests require that interpreter plus BeautifulSoup and fail rather than silently skip if unavailable.

```sh
./tools/go.sh build -trimpath -o bin/insane ./cmd/insane
./tools/verify_collector.sh
./tools/go.sh test -race ./...
```

The suite compares the immutable Python collector and Go implementation on 1,310 deterministic multilingual policy cases, URL normalization, stable ties across the per-source cap, multiple full scan cycles, Unicode truncation/fingerprints, body gating, rejection, deduplication, and 2,500-entry state pruning. Network-free search mocks additionally verify the original search contract, result limits, ten-second DDGS timeout, failures and injection-bearing snippet wrapping. Additional tests verify cancellation with a descendant retaining stdout, credential-URL rejection, strict-state handling and overlap locking.

## Safe offline verification

```sh
bin/insane scan --fixture testdata/scan.json \
  --state /tmp/insane-demo/state.json --report /tmp/insane-demo/report.json
# Run again with the same state: candidates becomes [].
bin/insane scan --fixture testdata/scan.json \
  --state /tmp/insane-demo/state.json --report /tmp/insane-demo/second.json
# Do not persist state (reads still occur without --fixture):
bin/insane scan --fixture testdata/scan.json --dry-run \
  --state /tmp/insane-demo/dry-run-state.json
```

All fixture offers are synthetic and not current promotions. Fixtures perform no network, model calls or delivery. The first fixture scan emits three candidates; the second emits none. One synthetic source failure is expected. A qualifying title-only gated candidate intentionally matches the original collector and has an empty excerpt plus `trust_level_gated`; the downstream verification policy still requires independently checking the original/official offer.

## Live reads

```sh
# Original seven sources; bounded to 20 minutes overall and 5 minutes per operation.
bin/insane scan --dry-run --timeout 20m --operation-timeout 5m \
  --state /tmp/insane-live-preview/state.json --report /tmp/insane-live-preview/report.json
# Retained Python public search (no API/model key):
bin/insane search --query 'official AI student plan' --limit 5 --timeout 30s
# Retained Python engine pass-through, one URL:
bin/insane fetch --url https://example.com --timeout 2m --max-attempts 3
# Inspect configuration only:
bin/insane job
```

`INSANE_ROOT` can select the project directory, and `--python` can select an interpreter. Without `--dry-run`, default state is `data/ai-promotion-scanner/state.json`. Reports always go to stdout; `--report` additionally writes an atomic private JSON file. Source-level fetch failures are included in `failed_sources`; a process deadline/cancellation/error aborts the scan without saving new state. All-source failure is represented by the original report contract, not a success claim about collection.

The runtime/browser worker owns the actual headed desktop adapter and its setup. No headless or Xvfb substitution is made here. Interactive/login/permission limits remain visible in engine diagnostics.

## Explicit compatibility and safety choices

- Go RE2 cannot implement the source's lookbehind/lookahead or Python word boundaries. Native-Go `regexp2` preserves the assertions. Generated Python Unicode 15.0 character classes implement `\w`, `\d`, `\s`, while generated full casefold/lowercase tables preserve multilingual fingerprints and URL host casing. Special Python ignore-case matches are normalized where needed; whole-string Greek final-sigma lowercase is handled with generated Cased/Case_Ignorable properties. Policy and secret-redaction regexes are compiled once and immutable; the pinned regexp2 source documents concurrent use as safe. Policy regex matching has a 250 ms safety deadline; timeout aborts rather than silently allowing content.
- Python helper requests/responses are bounded at 16 MiB, stderr at 256 KiB. The Go process deadline kills the owned Python process group and has a two-second pipe wait cap. The bridge does not evaluate input as code.
- Credential-bearing URLs are rejected before network activity, including URLs discovered in listings. This tightens the original network boundary.
- The deployed CLI initializes state only if the path is absent. Existing unreadable, oversized, malformed or incorrectly typed state fails closed and is never overwritten. This intentionally tightens the original script's reset-on-error behavior. A Linux `flock` sidecar prevents overlapping state writers and automatically unlocks after a crash. Dry runs do not create a state lock or save state.
- `config/imported-job.disabled.json` preserves the original prompt, provider/model, 240-minute interval, error and delivery destination as inert data. Both outer and imported enabled flags are false. No scheduler is installed or started, no provider credential is copied/recreated, and no model or Discord delivery is invoked. Enabling a scheduler or choosing a working classification/delivery route is a separate step.

Regenerate embedded policy/Unicode data only deliberately with `python3 tools/generate_policy.py`, then rerun differential tests. The source snapshot must remain unchanged.

## Measured redaction optimization

`runtime/redaction-benchmark.txt` records a 25-iteration microbenchmark on this VM: precompiled redaction took 28,880 ns/op, versus 30,159,336 ns/op when recompiling the three Unicode-aware expressions each call. The unchanged full non-race Go oracle suite fell from 36.603 seconds in one pre-fix run to 3.418 seconds after the fix. These compare two versions of this implementation under this VM workload, not Go against Python and not end-to-end network scan throughput. Reproduce the local comparison with:

```sh
./tools/go.sh test ./internal/collector -run '^$' -bench BenchmarkRedact -benchtime=25x -count=1
```
