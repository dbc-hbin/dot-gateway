> Source-only recovery edition. Live identity values are deliberately replaced by inert examples. See the repository-root RECOVERY.md before setup. Historical verification links may refer to records intentionally excluded from this repository.

# Insane Search: native Go migration for the VM

`scan` defaults to the bounded wide-recall review profile (120 rows/source, up to 3 public pages, 12 details/source, 48 total). See [wide-recall behavior and legacy compatibility](docs/WIDE_RECALL.md). Candidates require assistant verification and selection.

The promotion collector, search, HTTP/TLS engine, validators, extraction, persistent learning and browser queue client now run in Go. **Python is not a production dependency.** The selected headed browser layer remains Playwright 1.63.0/Node plus Chromium, as requested. The alternative agent-browser candidate was evaluated but not selected.

The verified native runtime is installed as `bin/insane` (2026-10-01 UTC). The same build is retained as `bin/insane.native-candidate`; the original cron remains paused. Building or testing this project does not activate a scheduler, call a model, create credentials or send Discord messages. See [verified deployment](VERIFIED_DEPLOYMENT.md) for evidence and remaining limits.

## Read first

- [Native feature/parity matrix](NATIVE_ENGINE_PARITY.md): preserved behavior, safety changes, exact TLS versions, and remaining differences
- [Collector and CLI guide](COLLECTOR.md): fixture, scan, search and fetch commands
- [Bounded Discord report publisher](docs/DISCORD_REPORT.md): prepared-report delivery, exact target preflight, durable single-attempt receipts and offline tests
- [Native extraction/search details](NATIVE_EXTRACTION.md): DOM/Markdown/PDF/search limitations
- [Headed browser guide](runtime/browser/README.md): existing verified browser service, Go queue client and lifecycle commands
- [Source provenance](SOURCE_TRANSFER.json): the line-checked, LF-normalized Mac text snapshot; not a claim of original byte identity

`vendor/` is immutable reference source, not Go module vendoring. Original Python files are retained there for provenance. Checked-in golden fixtures were generated from that original source; normal Go tests do not invoke Python. Historical hybrid instructions are isolated in [docs/HYBRID_PHASE.md](docs/HYBRID_PHASE.md).

## Build and native verification

On this VM, `tools/go.sh` selects Go 1.27.1 and the shared module/build caches. For another installation, set `GO_BIN` to that installation's Go binary and provide writable `GOPATH`/`GOCACHE` values. `-mod=readonly -buildvcs=false` deliberately avoids treating the source snapshot as vendored Go modules or as a Git checkout.

```sh
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/insane.native-candidate ./cmd/insane
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/headed-broker ./cmd/headed-broker
./tools/verify_collector.sh
```

The candidate is statically linked. No Python virtual environment, pip install, libcurl or OpenSSL runtime is needed. Go library dependencies are pinned in `go.mod`/`go.sum`. Playwright's separate Node dependencies are pinned in `runtime/browser/package-lock.json`; a fresh browser-layer installation uses `npm ci --prefix runtime/browser` and an approved real headed Chromium installation. Do not replace the existing helper or weaken its sandbox/egress safeguards as a setup shortcut.

## Offline proof without Python on PATH

```sh
PATH=/nonexistent INSANE_ROOT="$PWD" bin/insane.native-candidate scan \
  --fixture testdata/scan.json --dry-run --state /tmp/insane-native-fixture.json
```

The fixture contains synthetic offers only. A persisted first scan yields three candidates; repeating it with the same state yields none. The native verifier checks this, secret redaction and disabled job flags. Live collection additionally reports recognized/parsed/scanned counts per source; empty output alone is not treated as proof of successful collection.

## Public read commands

```sh
bin/insane.native-candidate fetch --url https://example.com --timeout 60s --max-attempts 2
bin/insane.native-candidate search --query 'official AI student plan' --limit 5 --timeout 60s
bin/insane.native-candidate scan --dry-run --timeout 20m --operation-timeout 5m \
  --state /tmp/insane-live/state.json --report /tmp/insane-live/report.json
bin/insane.native-candidate job
bin/headed-broker status
```

The HTTP layer uses genuine uTLS ClientHello presets and verifies certificates. It checks and pins public DNS destinations on every hop. If the restricted execution environment cannot resolve a hostname safely, HTTP fails closed and the already-running, strictly guarded headed service can handle the public read. There is no Python or unchecked proxy-DNS fallback.

## Verification evidence

- `verification/native-go-race.txt` and `verification/native-go-vet.txt`: final native race/vet results
- `verification/native-final-verification.txt`: normal Go tests plus no-Python-PATH CLI verification
- `verification/native-build-info.txt`: static candidate build settings and dependency versions
- `verification/native-fixture-no-python.json`: offline native runtime output
- `verification/native-go-vulnerability-check.txt`: reachability scan; no reachable/imported-package vulnerabilities reported, with two module-only advisories that were not called. This is not a claim that every dependency is vulnerability-free

Read the parity matrix before treating the port as identical to curl_cffi/DDGS/BeautifulSoup/yt-dlp. The supported functionality is native, but their network fingerprints, ranking, formatting and optional media extraction capabilities are not universally identical.
