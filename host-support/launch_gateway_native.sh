#!/bin/bash
set -eu
ROOT=/opt/assistant-project
"$ROOT/.hermes-venv/bin/python" "$ROOT/gateway_daemon.py" launch \
  --runtime "$ROOT/.gateway-supervisor" \
  --proxy-config "$ROOT/gateway_native_proxy_config.json" \
  --result-file "$ROOT/gateway_native_launch.json"
