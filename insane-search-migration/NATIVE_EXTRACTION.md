# Native Go search and extraction

The production search/extraction packages call no Python, DDGS, BeautifulSoup,
markdownify, Resiliparse, PDF utilities, or model. Browser rendering remains the
separately verified headed browser layer selected by the user.

## Implemented

- Native public DuckDuckGo HTML discovery with Bing RSS/HTML fallback,
  bounded provider requests, 1–20 result clamp, wrapped-link decoding, deduplication,
  and the original success/data.web/title/url/description/position fields.
  Provider status/parse diagnostics contain no fetched content
- Discourse, V2EX and DCInside listing parsing; source-compatible entity/text
  cleanup and URL resolution; original DCInside description precedence
- DCInside's observed mobile-to-desktop redirect is supported using its real
  gall.dcinside.com title-cell links, with gallery id and numeric post id checks.
  Reply counters, ads and unrelated-gallery links are excluded. The collector
  retains essential desktop query identifiers rather than merging all posts
- Browser API viewers containing pre-element JSON are unwrapped before parsing.
  Valid empty JSON listings are distinguishable from blocked, unrecognized,
  malformed or unparseable pages. The native collector exposes per-source
  recognition/format/parsed/scanned/fetch-error diagnostics
- DOM-to-Markdown preserves headings, paragraphs, links, emphasis, lists, tables,
  blockquotes and fenced code while excluding script/style/head noise
- Source-compatible JSON-LD rescue thresholds, rendered-inner-text rescue,
  title/OG metadata and extraction-quality diagnostics
- Native PDF text-layer extraction, title metadata and explicit malformed,
  encrypted, no-text-layer and resource-limit diagnostics

## Explicit differences and limits

- Search preserves the public provider schema, not DDGS's backend selection,
  result ordering or aggregation. No exact DDGS ranking parity is claimed
- HTML uses the Go HTML5 parser instead of BeautifulSoup's Python parser.
  Malformed-document repair and unusual text-node boundaries can differ
- Markdown formatting is a native implementation. Semantic structures are
  tested; byte-identical markdownify output is not claimed
- Main-content extraction is a semantic DOM heuristic with the original
  200-character acceptance floor, not Resiliparse. Its metadata says
  algorithm=native_dom_heuristic; a short extraction falls back to raw/Markdown
- PDF extraction uses a text-layer parser, not OCR. Complex layout, tables,
  unusual fonts and variable-width character maps may differ from pdfplumber.
  It does not execute PDF actions, load files/URLs, or accept passwords
- HTML input is capped at 25 MiB; rescue scans at 2 million Unicode characters;
  extracted text at 1 million. JSON-LD is capped at ten 200,000-character blocks
- PDF input is capped at 25 MiB, pages at 80, decoded content streams at 2 MiB
  each, font maps at 1 MiB, fonts at 128/page, operators at 200,000, cumulative
  source reads at 128 MiB, and output at 1 million characters. A five-second
  cooperative parse deadline is checked during reads/operators; this is not a
  process-isolation or hard memory-limit guarantee
- Media metadata/captions are owned by the native engine's platform routes.
  This extraction package does not replace yt-dlp's full site catalog

## Libraries and verification

- golang.org/x/net/html v0.59.0, official Go project, BSD-3-Clause
- github.com/ledongthuc/pdf v0.0.0-20260907135840-6c8c28e0e8a0,
  maintained pure-Go rsc/pdf fork, BSD-3-Clause

Versions are pinned centrally in go.mod/go.sum. Original source and its MIT
license remain unchanged under vendor/. See package registry license links:
https://pkg.go.dev/golang.org/x/net/html
https://pkg.go.dev/github.com/ledongthuc/pdf

Run:

    ./tools/go.sh test -race -count=1 ./internal/extract ./internal/search
    ./tools/go.sh vet ./internal/extract ./internal/search

Tests use only Go and static fixtures. Parser golden fixtures were generated once
from the immutable source as a development oracle; running tests does not need
Python. PDF fixtures are actual PDF files constructed in Go, including compressed
streams. Search tests inject a guarded fetch function and make no live requests.
