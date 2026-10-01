#!/bin/sh
# Offline tests/build after dependencies have been installed from trusted sources.
# Does not launch a gateway, browser, scheduler, or send any request to Discord.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$HERE/.." && pwd)
GO_BIN=${GO_BIN:-go}
[ "$($GO_BIN version)" = 'go version go1.27.1 linux/amd64' ] || { echo 'Required Go: go1.27.1 linux/amd64' >&2; exit 1; }
[ "$(node --version)" = 'v24.19.0' ] || { echo 'Required Node: v24.19.0' >&2; exit 1; }
[ "$(npm --version)" = '11.9.0' ] || { echo 'Required npm: 11.9.0' >&2; exit 1; }
export GOTOOLCHAIN=local
export GOFLAGS='-mod=readonly -buildvcs=false'
case ${1:-build} in
  deps)
    # Registry access only; this does not download browsers or run npm lifecycle scripts.
    for project in recovery discord-go-gateway insane-search-migration; do
      (cd "$ROOT/$project"; "$GO_BIN" mod download; "$GO_BIN" mod verify)
    done
    npm ci --prefix "$ROOT/insane-search-migration/runtime/browser" --ignore-scripts
    ;;
  build)
    for project in recovery discord-go-gateway insane-search-migration; do
      (cd "$ROOT/$project"; "$GO_BIN" test -race ./... -count=1 -timeout 180s; "$GO_BIN" vet ./...)
    done
    (cd "$HERE"; CGO_ENABLED=0 "$GO_BIN" build -trimpath -o dot-recovery .)
    (cd "$ROOT/discord-go-gateway"; mkdir -p bin; CGO_ENABLED=0 "$GO_BIN" build -trimpath -o bin/dot-gateway ./cmd/dot-gateway; cp bin/dot-gateway bin/dot-bridge-cli)
    (cd "$ROOT/insane-search-migration"; mkdir -p bin; for name in insane headed-broker discord-report; do CGO_ENABLED=0 "$GO_BIN" build -trimpath -o "bin/$name" "./cmd/$name"; done)
    node --test "$ROOT/insane-search-migration/runtime/browser/test_adapter.cjs"
    ;;
  runtime-check)
    [ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ] || { echo 'Real graphical desktop session required' >&2; exit 1; }
    CHROMIUM=${INSANE_CHROMIUM_PATH:-/usr/bin/chromium}
    "$CHROMIUM" --version | grep -F '151.0.7922.173' >/dev/null || { echo 'Recorded Chromium build mismatch; review required' >&2; exit 1; }
    node -e 'if(require(process.argv[1]).version!=="1.63.0")process.exit(1)' "$ROOT/insane-search-migration/runtime/browser/node_modules/playwright/package.json"
    printf '%s\n' 'Version/display variables verified; an authorized real headed browser smoke test is still required'
    ;;
  *) echo 'usage: sh recovery/build.sh deps|build|runtime-check' >&2; exit 2;;
esac
