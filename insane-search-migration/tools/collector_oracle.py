#!/usr/bin/env python3
"""Development-only, network-disabled immutable collector oracle.

Production is native Go. This optional generator helper imports the original
collector through an inert engine stub. It cannot make network requests and is
never imported or launched by the production CLI.
"""
from __future__ import annotations
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 16 * 1024 * 1024


def load_collector():
    engine = types.ModuleType("engine")
    def disabled(*args, **kwargs):
        raise RuntimeError("network is disabled in the pure collector oracle")
    engine.fetch = disabled
    sys.modules["engine"] = engine
    spec = importlib.util.spec_from_file_location("original_collector", ROOT / "vendor/scripts/scan_ai_promotions.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(request):
    op = request.get("op")
    module = load_collector()
    if op == "parse":
        source, url, text = request["source"], request["url"], request["text"]
        return {"rows": module.parse_json_listing(source, url, text) or module.parse_html_listing(source, url, text)}
    if op == "policy":
        results = []
        for row in request["rows"]:
            title, excerpt = row.get("title", ""), row.get("listing_excerpt", "")
            source, url = row.get("source", ""), row.get("url", "")
            blob = title + " " + excerpt
            results.append({"worth": module.worth_collecting(title, excerpt, source, url),
                "rejected": module.listing_rejected(title, excerpt, url) or "",
                "rank": module.rank_score(title, excerpt, source, url),
                "matches": module.matches(blob), "redacted": module.redact(blob),
                "casefold": blob.casefold(), "stripped": module.strip_poison(blob),
                "canonical": module.canonical_url(url)})
        return {"results": results}
    if op == "fixture":
        fixture = request["fixture"]
        def listing(source, url):
            value = fixture.get("listings", {}).get(source, {})
            return [dict(row, source=source) for row in value.get("rows", [])], value.get("error") or None
        def body(row):
            value = fixture.get("bodies", {}).get(row["url"], {})
            return value.get("text", ""), value.get("error") or None
        module.collect_listing, module.fetch_body = listing, body
        with tempfile.TemporaryDirectory(prefix="insane-oracle-") as directory:
            module.STATE_PATH = Path(directory) / "state.json"
            if "state" in request:
                module.STATE_PATH.write_text(json.dumps(request["state"], ensure_ascii=False))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                module.main()
            return {"report": json.loads(buffer.getvalue()), "state": json.loads(module.STATE_PATH.read_text())}
    raise ValueError("unknown operation")


def main():
    raw = sys.stdin.buffer.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise ValueError("bridge input exceeds 16 MiB")
    request = json.loads(raw)
    with contextlib.redirect_stdout(sys.stderr):
        result = run(request)
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
    if len(encoded) > LIMIT:
        raise ValueError("bridge output exceeds 16 MiB")
    sys.stdout.buffer.write(encoded + b"\n")

if __name__ == "__main__":
    main()
