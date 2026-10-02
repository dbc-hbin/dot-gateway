#!/bin/sh
# Invoke from a supported native host-lifetime terminal, never a short-lived
# tool-call proxy scope. proxy.json is explicit operator input, never overwritten.
set -eu
umask 077
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$HERE/recover_gateway.py" "$@"
