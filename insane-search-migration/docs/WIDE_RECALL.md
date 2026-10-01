# Bounded wide-recall review queue

`insane scan` now defaults to `--profile wide`. It gathers more public leads for
assistant review; it does **not** accept offers, invoke a model, or publish a
report. Final verification and selection remain with the reviewing assistant.

## Profiles and explicit limits

| Profile | Listing rows/source | Public pages/source | Detail reads/source | Total detail reads/candidates |
|---|---:|---:|---:|---:|
| `wide` (CLI default) | 120 | 3 | 12 | 48 |
| `legacy` | 40 | 1 | 4 | 16 |

Override a limit with `--max-scan-per-source`, `--max-listing-pages`,
`--max-per-source`, or `--max-total`. Zero uses the profile default. Values outside
the hard bounds (200 rows, 5 pages, 24 per-source details, 96 total details) fail
before collection. The total default scan deadline remains 20 minutes, the
per-operation deadline remains 5 minutes, and existing transport/body/browser
budgets remain enforced. Sources and details are read sequentially; continuation
pages have cancellable 250 ms spacing. This is bounded discovery, not an unlimited
crawl. A cap limits reads; it is not a guaranteed number of returned candidates.

Only observed, allowlisted public pagination is followed: Discourse's advertised
`more_topics_url` and the configured DCInside gallery's numbered links. Each next
page must be the immediately following page on the same permitted source route.
Credentials, foreign hosts, ports, extra query fields and repeated pages are
rejected. V2EX's latest/hot API stays single-page because it does not advertise a
supported continuation. Empty/repeated pages and row/page caps stop collection.
Later-page failures retain earlier successful rows and expose a continuation
diagnostic. Public DNS/SSRF/TLS guards, headed Playwright fallback, login/trust
boundaries, and request cancellation are unchanged.

## Broader matching, separate final judgment

Wide matching recognizes additional multilingual AI names and benefit wording,
including plural “credits”, “offer”, “bonus”, “체험권” and “赠金”. An AI clue and a
benefit clue are still needed (the AI-specific DCInside gallery supplies AI
context). A known vendor is no longer required alongside a weak benefit clue.
Question, editorial, release, speculation and referral/self-promotion heuristics
can produce review candidates with `review_*` hints rather than silently excluding
them. These remain reasons to inspect/reject a candidate during final review.
No flag establishes legitimacy, current availability, eligibility or value.

Credential/key sharing, account sales, unrelated financial/proxy promotions and
relay/pool safeguards remain blocking. A larger pool must not be achieved by
decoding keys or bypassing access gates. Trust-gated bodies are always suppressed;
only the existing narrow publicly qualifying title-only behavior is retained.
Report text/errors are redacted, and wide listing URLs containing userinfo or
recognized credential text are excluded. All source content remains untrusted.

The report's `collection` diagnostics give configured bounds, scanned rows,
unique eligible rows, selected details, unchanged rows and `review_only: true`.
Every wide candidate has `assistant_review_required`; `broadened_recall` identifies
material that the legacy heuristic would have excluded. `quality_flags` are hints.

## Deduplication and intentional parity differences

Unseen URLs are ranked before previously fingerprinted URLs in the wide profile,
followed by known URLs never evaluated under wide rules, then oldest evaluations.
The old stable score/source/title ordering breaks ties. This prevents unchanged
high-ranked posts from consuming every detail slot while new or previously rejected
posts wait. Known rows rotate through refresh within the remaining budget. Pagination
deduplicates canonical URLs before the wide row cap. Legacy scans preserve the
original raw-row cap and ranking, including duplicate rows counting toward 40.

State keeps its version-1 URL-to-SHA256 fingerprints and 2,500-key cap, adding an
optional `wide_reviewed_v1` object mapping retained URLs to positive integer review
sequences. This bounded evaluation-order metadata records neither acceptance nor
delivery. Invalid history fails closed; legacy scans preserve it. Recovery tools
must preserve this optional field to retain migration/refresh progress.
Legacy-eligible offers retain their fingerprints. Newly wide-only eligible rows
use a `wide:v1 ` prefix inside the hashed input: a legacy detail rejection that was
already fingerprinted gets one fresh review, then subsequent wide scans deduplicate
it normally. No live state rewrite or clearing is required for migration. A
separate delivery/review ledger remains responsible for final publication dedup.

The `Scan` Go entrypoint and Python-oracle tests remain legacy; the explicit
`ScanWithOptions` entrypoint and CLI default use the new behavior. The immutable
vendor snapshot and policy vocabulary are not rewritten to disguise these
intentional changes. Synthetic tests cover recall gains, blocking/redaction,
fingerprint migration, unseen-first fairness and hard limits; pagination tests
cover hostile links, duplicates, cancellation and partial failures.

```sh
bin/insane scan --profile wide --dry-run --timeout 20m \
  --state /tmp/review-only/state.json --report /tmp/review-only/report.json
bin/insane scan --profile legacy --fixture testdata/scan.json --dry-run \
  --state /tmp/legacy-fixture/state.json
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
```

Dry-run scans write neither collector nor learned-route state. Use a disposable
state copy for before/after comparisons, never clear production state for testing.
