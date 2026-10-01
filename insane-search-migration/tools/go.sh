#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export GOPATH=${GOPATH:-${HOME}/.cache/assistant-go-path}
export GOCACHE=${GOCACHE:-${HOME}/.cache/assistant-go-cache}
# vendor/ is the immutable source snapshot, not Go module vendoring.
export GOFLAGS="${GOFLAGS:-} -mod=readonly -buildvcs=false"
cd "$ROOT"
exec "${GO_BIN:-go}" "$@"
