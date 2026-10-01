#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
./tools/go.sh test ./... -count=1 -timeout 90s
./tools/go.sh vet ./...
mkdir -p bin
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/insane.native-candidate ./cmd/insane
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
PATH=/nonexistent INSANE_ROOT="$ROOT" bin/insane.native-candidate scan --profile legacy --fixture testdata/scan.json --state "$TMP/state.json" --report "$TMP/first.json" > /dev/null
PATH=/nonexistent INSANE_ROOT="$ROOT" bin/insane.native-candidate scan --profile legacy --fixture testdata/scan.json --state "$TMP/state.json" --report "$TMP/second.json" > /dev/null
test "$(grep -c '"change":' "$TMP/first.json")" -eq 3
grep -q '"candidates": \[\]' "$TMP/second.json"
if grep -q 'abcdefghijklmnop' "$TMP/first.json"; then echo 'redaction check failed' >&2; exit 1; fi
test "$(grep -c '"enabled": false' config/imported-job.disabled.json)" -eq 2
printf '%s\n' 'Native CLI verified with PATH=/nonexistent: 3 synthetic candidates, second scan deduplicated, secrets redacted, imported job disabled'
