# Latency improvements and diagnostic boundaries

This change keeps the current assistant as the only reply author. It adds no
model client, provider, credentials, external listener, caching of authorization,
or retry of uncertain delivery. Source publication does not deploy the gateway.

## Changes

- Successful reply delivery clears the receipt reaction without adding a
  completion checkmark. Initial receipt, processing typing, and failure feedback
  remain unchanged. Historical success reactions are not bulk-modified.
- Claim acquisition materializes at most 16 pending envelopes per keyset page,
  ordered by `(created,id)`. Pages run inside the original `BEGIN IMMEDIATE`
  transaction. Revoked and busy-conversation candidates do not stop later pages;
  replay, expiry, immutable binding, tombstones, and the 1,000-entry capacity stay
  intact. Metadata is selected before envelope decoding.
- Long polls arm the existing private file watcher before asynchronous Unix IPC
  setup, then select both wake sources. A healthy idle socket no longer masks a
  file-only notification. Dial and handshake retain 1-second/2-second caps but
  also obey the poll deadline. Recovery polling remains 1 second when connected
  and 250 ms when IPC is unavailable. Watchers close on return or deadline.
- Identity preflight failure cancels the sibling channel lookup and its rate wait,
  joins it, and retains the identity error. Successful sends still validate both
  endpoints before every POST, including every chunk.
- `SendGuardedMeasured` returns the exact send's immutable measurement while the
  send lock is held. `RecordResultMeasured` stores numeric/boolean metrics in the
  existing result transaction, associated with `(reply_id,chunk_index,attempt)`.
  Retention is capped at 2,048 records. A savepoint isolates ordinary telemetry
  statement errors from delivery state. Transaction-wide I/O/rollback failures
  still stop the dispatcher; the existing sending-to-uncertain recovery prevents
  a blind resend. No result is correlated with the mutable global 3-second health
  snapshot. The old health snapshot remains for compatibility.
- A POST attempted through a transport with no connection tracing is conservatively
  `uncertain` on error. Missing instrumentation is not proof of no delivery.

## CLI phase trace (opt-in)

Set `BRIDGE_PHASE_TRACE=1` for a consumer CLI invocation. One JSON-lines batch is
appended to `BRIDGE_DB + ".phase-trace"` after the stdout write returns. The feature
is off by default. It does not change stdout, including the exact empty response
`{"message":null}` required by `bin/dot-bridge-listen`.

Each batch contains an opaque invocation ID, an optional validated opaque inbound
ID, and up to 32 allowlisted phase points with UTC wall-clock and monotonic elapsed
nanoseconds. No reply text, claim token, route, user ID, credential, error detail,
or arbitrary caller-supplied label is recorded.

Recorded boundaries include CLI entry, store-open start/return, final claim-call
start/return, reply input open/read, reply queue call/return, notification
start/return, stdout-ready, write-start, and write-finish or write-failure. For a
long poll, claim timing is the final acquisition attempt, not the whole idle wait.
`claim_returned` and `queue_returned` occur after their durable transactions return.
The queue-call span aggregates validation, envelope decoding, SQLite waiting and
commit; it does not separately instrument those internal subphases.
A phase called `finished` denotes local completion, not success of a remote action.

The private regular-file sink refuses symlinks, extra hard links, shared modes,
non-owner paths, and blocking advisory-lock acquisition. It is capped at 1 MiB;
on overflow, the newest complete batch replaces the old contents. Each batch is
at most 8 KiB, has no fsync or SQL writes, and the CLI waits at most 10 ms for its
best-effort logging worker, subject to ordinary scheduling. Process exit may drop
an incomplete diagnostic batch. Ignore incomplete final JSON lines. Trace failures
never change an operation's return code or cause retry of a committed reply.

Important interpretation:

- An OS stdout write is not tool-result delivery and does not prove model receipt
- The listener uses command substitution, so process-exit/trace flushing can still
  precede the wrapper's result availability
- This repository has no documented host callback for tool-result receipt, model
  inference start/end, or tool scheduling. It does not invent those timestamps
- A supported outer host can separately capture immediately after its awaited
  `next` result and immediately before invoking `reply`; label these host/tool
  boundaries, and retain the invocation/inbound correlation. Do not call the gap
  pure “reasoning time”
- Existing ledger `claimed` and `reply_queued` times are sampled before commit;
  the new CLI return phases distinguish commit return from those older samples

## Sender measurement semantics

The persisted table is `send_measurements`. Its `measurement` JSON contains send
lock wait, total/preflight/POST duration, and separate identity GET, channel GET,
and POST snapshots. Each snapshot covers rate-limit wait, attempted request,
headers/body completion, connection reuse, and DNS/connect/TLS observations.

Observation booleans distinguish missing traces from measured zero. `Reused` in
legacy/global diagnostics still describes only the POST; a reused POST does not
imply warm GET connections. Total `seconds` excludes send-lock waiting; rate wait
is included in each request duration. Header/body offsets are relative to that
request phase start. `body_complete` requires observed EOF; close/error alone does
not assert full consumption. No raw URL, host, header, address, or payload is stored.

## Synthetic evidence (2026-10-01)

Temporary SQLite fixtures and local transports only; these are measurements on
this machine, not production latency promises. The actual observed multi-second
assistant/tool interval is outside these optimizations.

| Measurement | Before | After |
|---|---:|---:|
| CLI `next`, one 8,000-character message, median / p95, n=60 | 4.769 / 6.160 ms | 4.558 / 5.906 ms |
| CLI `next`, 1,000 × 8,000-character backlog, median / p95, n=60 | 58.235 / 68.024 ms | 5.417 / 6.742 ms |
| In-process claim, 1,000-message backlog, median of six benchmark runs | 51.615 ms | 1.438 ms |
| Same in-process claim, allocated bytes | 37.87 MB | 0.603 MB |
| In-process one-row claim median | 0.375 ms | 0.414 ms |

Opt-in trace overhead, n=100: median 4.155 ms disabled versus 4.387 ms enabled
(+0.232 ms); p95 5.132 versus 6.309 ms. The small one-row differences are noisy and
must not be interpreted as guaranteed speedups. Result-persistence microbenchmarks
with/without metrics overlapped (roughly 0.5–0.8 ms at 100 iterations); typed metrics
added about 3.2 KiB and 25 allocations per result in that fixture.

| Simulated wake-path case, n=5 | Serialized baseline median / max | Multiplexed median / max |
|---|---:|---:|
| Healthy idle socket, file-only event | 1000.397 / 1000.633 ms | 0.068 / 0.271 ms |
| Stalled handshake, file-only event | 2000.581 / 2001.262 ms | 0.077 / 0.104 ms |

These reproduce the old serialized wait versus the new multiplexed path using
`net.Pipe` plus real inotify with race detection enabled. They exclude SQL and
process startup. Run the opt-in simulation with:

```sh
DOT_GATEWAY_WAKE_TAIL_SIMULATION=1 go test -race ./internal/bridge -run '^TestWakeLatencySimulation$' -count=1 -v
```

The IPC tests use real inotify and `net.Pipe` where Unix socket creation is denied
by the executor. Native Unix tests remain in the suite and skip on that explicit
restriction; production/native-host behavior is not claimed to be reverified.

Reproduce the CLI fixtures with:

```sh
python3 scripts/benchmark_latency.py --before /path/to/baseline-cli --after /path/to/candidate-cli
```

Use reviewed native compiled CLI binaries for both arguments, never a deployed
`bin/dot-bridge` wrapper. The script rejects non-ELF files before executing either
candidate, constructs a clean environment, and creates temporary databases. ELF
is only a format check: arbitrary supplied executables are not authenticated or
sandboxed by this harness. Go microbenchmarks:

```sh
go test ./internal/bridge -run '^$' -bench 'BenchmarkClaimPendingBacklog|BenchmarkRecordResultMeasurement' -benchtime=100x -count=3
```

## Regression coverage

- Multi-page revoked/busy traversal, tied FIFO, bounded decoding, transaction
  rollback, concurrent claims, expiry and consumer replay, capacity/tombstones
- Simultaneous file/socket wake, stalled/invalid/failed IPC, deadline bounds,
  cancellation, mode/path checks, descriptor cleanup and recovery status
- Empty stdout compatibility, opt-in behavior, phase order, delayed input,
  store contention, failed stdout recovery, unsafe/locked/full trace sinks,
  retention, data exclusion and committed-reply idempotency
- Per-attempt/chunk/diagnostic correlation, telemetry-failure isolation,
  rate waits, cold GET/reused POST, response completion, concurrent trace callbacks,
  cancellation/error precedence, fresh DM recipients and uncertain delivery
