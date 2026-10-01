# Native Go collector, search and fetch

`scan` defaults to the bounded wide-recall review profile (120 rows/source, up to 3 public pages, 12 details/source, 48 total). See [wide-recall behavior and legacy compatibility](docs/WIDE_RECALL.md). Candidates require assistant verification and selection.

The active migration candidate is `bin/insane.native-candidate`. It is statically linked (`CGO_ENABLED=0`) and needs no Python runtime. The existing headed Playwright/Node browser service remains selected, as requested; the native Go queue client uses it when HTTP cannot establish a safe successful response.

See [NATIVE_ENGINE_PARITY.md](NATIVE_ENGINE_PARITY.md) for the exact feature matrix, tests, remaining behavior differences and operational safeguards. See [NATIVE_EXTRACTION.md](NATIVE_EXTRACTION.md) for native HTML/PDF/search parsing differences. [docs/HYBRID_PHASE.md](docs/HYBRID_PHASE.md) is historical context for the superseded hybrid implementation, not current setup guidance.

## Build and verify

```sh
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/insane.native-candidate ./cmd/insane
./tools/verify_collector.sh
```

Normal tests execute native Go only. They compare against checked-in golden outputs generated from the immutable original Python source. Optional oracle regeneration and retired hybrid-bridge tests are development-only and do not participate in production collection. Do not run `go mod vendor`: `vendor/` is the immutable, normalized Mac source snapshot, not Go module vendoring.

## Offline fixture

```sh
PATH=/nonexistent INSANE_ROOT="$PWD" bin/insane.native-candidate scan \
  --fixture testdata/scan.json --state /tmp/insane-demo/state.json \
  --report /tmp/insane-demo/first.json
PATH=/nonexistent INSANE_ROOT="$PWD" bin/insane.native-candidate scan \
  --fixture testdata/scan.json --state /tmp/insane-demo/state.json \
  --report /tmp/insane-demo/second.json
```

The synthetic fixture produces three candidates on the first scan and none on the second. It includes an expected synthetic source failure and intentionally preserves the original qualifying-title-only gated-post behavior. These are test offers, not current promotions.

## Public reads

```sh
bin/insane.native-candidate fetch --url https://example.com --timeout 60s --max-attempts 2
bin/insane.native-candidate search --query 'official AI student plan' --limit 5 --timeout 60s
bin/insane.native-candidate scan --dry-run --timeout 20m --operation-timeout 5m \
  --state /tmp/insane-preview/state.json --report /tmp/insane-preview/report.json
bin/insane.native-candidate job
```

A dry-run scan saves neither collector state nor learned-route state. Reports always go to stdout; `--report` also writes a private atomic file. Native reports include per-source parsing diagnostics, so an empty candidates array cannot conceal unrecognized listing HTML. DCInside’s observed desktop redirect is supported using its actual final URL and narrowly preserved gallery/post query identity. Check structured fetch/search success and all failures; process exit status alone does not establish a successful scan.

The imported cron is still paused/inert. Neither this CLI nor its tests invoke a model, activate a scheduler, recreate credentials or send Discord messages. The previous verified hybrid binary is retained only as a rollback artifact until the parent task completes its controlled candidate verification. No browser restart or replacement is part of running these commands.
